import logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy.orm import Session
from sqlalchemy import select

from app.models.trend import Site, TrendArticle
from app.services.trend_service import TrendService, TrendMatchBatch
from app.services.image_service import ImageService
from crawlers.theqoo import TheqooCrawler
from crawlers.ruliweb import RuliwebCrawler
from crawlers.dogdrip import DogdripCrawler
from crawlers.ppomppu import PpomppuCrawler
from crawlers.instiz import InstizCrawler
from crawlers.todayhumor import TodayhumorCrawler
from crawlers.natepann import NatepannCrawler
from crawlers.bobaedream import BobaedreamCrawler
from crawlers.dcinside import DcinsideCrawler
from crawlers.inven import InvenCrawler
from crawlers.orbi import OrbiCrawler
from crawlers.cook82 import Cook82Crawler

logger = logging.getLogger(__name__)


def _get_blocked_crawlers() -> dict:
    """차단된 사이트 크롤러를 lazy import (patchright/scrapling은 self-hosted runner만 설치)"""
    result = {}
    try:
        from crawlers.fmkorea import FmKoreaCrawler
        result["fmkorea"] = FmKoreaCrawler
    except ImportError:
        pass
    try:
        from crawlers.arcalive import ArcaliveCrawler
        result["arcalive"] = ArcaliveCrawler
    except ImportError:
        pass
    try:
        from crawlers.coinpan import CoinpanCrawler
        result["coinpan"] = CoinpanCrawler
    except ImportError:
        pass
    try:
        from crawlers.mlbpark import MlbparkCrawler
        result["mlbpark"] = MlbparkCrawler
    except ImportError:
        pass
    try:
        from crawlers.slrclub import SlrclubCrawler
        result["slrclub"] = SlrclubCrawler
    except ImportError:
        pass
    return result


class CrawlerService:
    """크롤링 서비스"""

    CRAWLERS = {
        # "clien": ClienCrawler,  # 추천글 기능 일시 중단 (2026-03-12~)
        "theqoo": TheqooCrawler,
        "ruliweb": RuliwebCrawler,  # 재추가 2026-05-24: memekase 비교 후 짤 확산 핵심 노드로 판단
        "dogdrip": DogdripCrawler,  # 추가 2026-05-24: memekase가 "밈 확산 허브"로 분류
        "ppomppu": PpomppuCrawler,
        "instiz": InstizCrawler,
        "todayhumor": TodayhumorCrawler,
        "natepann": NatepannCrawler,
        "bobaedream": BobaedreamCrawler,
        "dcinside": DcinsideCrawler,
        "inven": InvenCrawler,
        "orbi": OrbiCrawler,
        "cook82": Cook82Crawler,
        **_get_blocked_crawlers(),
    }

    # 데이터센터 IP에서 차단되는 사이트 (self-hosted runner 전용)
    BLOCKED_SITES = {"fmkorea", "arcalive", "coinpan", "mlbpark", "slrclub"}

    # 오래된 글 필터: published_at가 이 일수보다 과거이면 저장하지 않는다.
    # 리스트 페이지가 아카이브 구간을 반환하는 사고(SLR클럽 hot_article)에 대한 안전망.
    MAX_ARTICLE_AGE_DAYS = 7

    def __init__(self, db: Session = None):
        self.db = db
        self.trend_service = TrendService(db)
        self.image_service = ImageService()
        from app.core.config import get_settings
        settings = get_settings()
        self.r2_account_id = settings.r2_account_id
        self.r2_access_key_id = settings.r2_access_key_id
        self.r2_secret_access_key = settings.r2_secret_access_key
        self.r2_bucket_name = settings.r2_bucket_name

    def get_or_create_site(self, crawler) -> Site:
        """사이트 정보 조회 또는 생성"""
        site = self.db.execute(
            select(Site).where(Site.name == crawler.site_name)
        ).scalar_one_or_none()

        if not site:
            site = Site(
                name=crawler.site_name,
                display_name=crawler.display_name,
                base_url=crawler.base_url,
            )
            self.db.add(site)
            self.db.flush()

        return site

    def crawl_site(self, site_name: str) -> dict:
        """특정 사이트 크롤링"""
        if site_name not in self.CRAWLERS:
            return {"error": f"Unknown site: {site_name}"}

        crawler_class = self.CRAWLERS[site_name]

        with crawler_class() as crawler:
            site = self.get_or_create_site(crawler)
            self.db.commit()
            referer = crawler.base_url

            # 기존 URL을 전달하여 상세 페이지 방문 스킵 (FmKorea 등)
            # 최근 7일만 조회 — 리스트 페이지에 7일 넘은 글은 거의 안 나타남
            recent_cutoff = datetime.utcnow() - timedelta(days=7)
            existing = self.db.execute(
                select(TrendArticle.url).where(
                    TrendArticle.site_id == site.id,
                    TrendArticle.created_at > recent_cutoff,
                )
            ).all()
            skip_urls = set(row[0] for row in existing)

            # DB 커넥션 반환 (크롤링 중 idle timeout 방지)
            self.db.close()

            articles = crawler.get_popular_articles(skip_urls=skip_urls)

            # URL 중복 제거 (같은 글이 여러 리스트 페이지에 등장하는 경우)
            seen_urls = set()
            unique_articles = []
            for a in articles:
                if a.url not in seen_urls:
                    seen_urls.add(a.url)
                    unique_articles.append(a)
            articles = unique_articles

            # 오래된 글 필터링 (published_at가 있고 MAX_ARTICLE_AGE_DAYS 초과)
            stale_cutoff = datetime.utcnow() - timedelta(days=self.MAX_ARTICLE_AGE_DAYS)
            before_filter = len(articles)
            articles = [
                a for a in articles
                if a.published_at is None or a.published_at >= stale_cutoff
            ]
            stale_count = before_filter - len(articles)
            if stale_count:
                logger.info(
                    f"[{site_name}] Filtered {stale_count} stale articles "
                    f"(>{self.MAX_ARTICLE_AGE_DAYS} days old)"
                )

            # 1단계: 새 글만 이미지+비디오 다운로드 + pHash 병렬 처리
            # (DB 없이 순수 네트워크 작업)
            image_results = self._prefetch_media([a for a in articles if a.url not in skip_urls], referer)

            # DB 재연결 후 저장
            # (sessionmaker가 pool_pre_ping으로 유효한 커넥션 제공)
            self.db.connection()

            # 0단계: 이미 DB에 있는 글 URL 필터링
            existing_urls = set(
                row[0] for row in self.db.execute(
                    select(TrendArticle.url).where(
                        TrendArticle.url.in_([a.url for a in articles])
                    )
                ).all()
            )
            new_articles = [a for a in articles if a.url not in existing_urls]
            logger.info(f"[{site_name}] {len(articles)} found, {len(new_articles)} new, {len(existing_urls)} skipped (already in DB)")

            # 2단계: 트렌드 매칭 후보를 사이트당 1회 조회 (글마다 7일치 pHash 창을 훑던 것을
            # 대체, 2026-09-14). 저장은 이 후보 dict만 본다.
            match_batch = self.trend_service.start_match_batch([
                self._first_phash(image_results.get(a.url)) for a in new_articles
            ])

            # 3단계: DB 저장
            processed = 0
            skipped = 0
            for article_data in new_articles:
                try:
                    image_result = image_results.get(article_data.url)
                    result = self._save_article(article_data, site, image_result, match_batch)
                    if result:
                        self.db.commit()
                        processed += 1
                    else:
                        skipped += 1
                except Exception as e:
                    self.db.rollback()
                    logger.warning(f"Error processing article '{article_data.title[:30]}': {e}")
                    continue

            logger.info(f"[{site_name}] {len(articles)} found, {processed} saved, {skipped} skipped")

        return {
            "site": site_name,
            "articles_found": len(articles),
            "processed": processed,
            "skipped": skipped,
        }

    @staticmethod
    def crawl_all_parallel(max_workers: int = 5, only: list[str] | None = None, exclude: list[str] | None = None) -> list[dict]:
        """사이트 병렬 크롤링 (각 스레드가 독립 DB 세션 사용)

        Args:
            only: 지정 시 해당 사이트만 크롤링
            exclude: 지정 시 해당 사이트 제외
        """
        from app.core.database import SyncSessionLocal

        sites = set(CrawlerService.CRAWLERS.keys())
        if only:
            sites = sites & set(only)
        if exclude:
            sites = sites - set(exclude)

        def _crawl_one(site_name: str) -> dict:
            with SyncSessionLocal() as db:
                service = CrawlerService(db)
                return service.crawl_site(site_name)

        results = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(_crawl_one, name): name
                for name in sites
            }
            for future in as_completed(futures):
                site_name = futures[future]
                try:
                    result = future.result()
                except Exception as e:
                    logger.warning(f"[{site_name}] crawl failed: {e}")
                    result = {"site": site_name, "error": str(e)}
                results.append(result)

        return results

    def crawl_all(self) -> list[dict]:
        """모든 활성 사이트 크롤링 (순차, 단일 세션)"""
        results = []
        for site_name in self.CRAWLERS:
            try:
                result = self.crawl_site(site_name)
            except Exception as e:
                logger.warning(f"[{site_name}] crawl failed: {e}")
                result = {"site": site_name, "error": str(e)}
            results.append(result)
        return results

    def _prefetch_media(self, articles, referer: str | None = None) -> dict:
        """모든 글의 이미지+비디오를 병렬 다운로드.
        Returns: {article_url: [result_dict, ...]} 매핑
        """
        # (article_url, index, url, media_type) 튜플 리스트
        # 기사당 최대 10장 이미지 + 5개 비디오. 대표 이미지는 앞쪽에 있고,
        # 50장까지 다운받던 기존 설정은 네트워크 시간 낭비가 컸음.
        tasks = []
        for article in articles:
            for i, img_url in enumerate(article.image_urls[:10]):
                tasks.append((article.url, i, img_url, "image"))
            # 비디오는 이미지 뒤에 붙임
            offset = len(article.image_urls[:10])
            for j, vid_url in enumerate(article.video_urls[:5]):
                tasks.append((article.url, offset + j, vid_url, "video"))

        if not tasks:
            return {}

        results = {}  # {article_url: [(index, result), ...]}
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {}
            for art_url, idx, url, media_type in tasks:
                if media_type == "video":
                    fut = executor.submit(self.image_service.process_video, url, referer)
                else:
                    fut = executor.submit(self.image_service.process_image, url, referer)
                futures[fut] = (art_url, idx)

            for future in as_completed(futures):
                art_url, idx = futures[future]
                try:
                    result = future.result()
                    if result:
                        if art_url not in results:
                            results[art_url] = []
                        results[art_url].append((idx, result))
                except Exception:
                    pass

        # 인덱스 순으로 정렬
        for art_url in results:
            results[art_url] = [r for _, r in sorted(results[art_url])]

        return results

    @staticmethod
    def _first_phash(image_results: list | None) -> str | None:
        """트렌드 매칭에 쓰는 첫 번째 이미지(비디오 아닌)의 pHash."""
        for r in image_results or []:
            if r.get("phash"):
                return r["phash"]
        return None

    def _save_article(
        self,
        article_data,
        site: Site,
        image_results: list | None,
        match_batch: TrendMatchBatch | None = None,
    ) -> bool:
        """글 DB 저장 (이미지/비디오 결과가 이미 있는 상태).

        match_batch: crawl_site가 사이트당 1회 조회한 트렌드 매칭 후보. None이면 이 글의
        phash 하나로 조회한다.
        """
        if not image_results:
            return False

        first_phash = self._first_phash(image_results)
        if not first_phash:
            return False

        trend = self.trend_service.find_or_create_trend(
            first_phash,
            article_data.title,
            match_batch,
        )

        article = self.trend_service.add_article_to_trend(
            trend,
            {
                "title": article_data.title,
                "url": article_data.url,
                "view_count": article_data.view_count,
                "like_count": article_data.like_count,
                "comment_count": article_data.comment_count,
                "published_at": article_data.published_at,
                "content": article_data.content,
            },
            site.id,
        )

        if article is None:
            return False  # 같은 사이트 중복 → 스킵

        # 모든 이미지 캐싱 + DB 저장
        from datetime import datetime
        now = datetime.utcnow()

        for i, img_result in enumerate(image_results):
            storage_key = None
            media_type = img_result.get("media_type", "image")
            hash_prefix = (img_result.get("phash") or "nohash")[:8]

            r2_ready = bool(self.r2_account_id and self.r2_access_key_id and
                            self.r2_secret_access_key and self.r2_bucket_name)

            if media_type == "video" and img_result.get("raw_data"):
                # 비디오 → 원본 MP4 업로드
                if r2_ready:
                    storage_key = f"{now.year}/{now.month:02d}/{now.day:02d}/{trend.id}_{i}_{hash_prefix}.mp4"
                    if not self.image_service.upload_to_r2(
                        img_result["raw_data"], storage_key,
                        self.r2_account_id, self.r2_access_key_id, self.r2_secret_access_key,
                        self.r2_bucket_name, content_type="video/mp4",
                    ):
                        storage_key = None
            elif img_result.get("is_gif") and img_result.get("raw_data"):
                # 애니메이션 GIF → 원본 그대로 업로드
                if r2_ready:
                    storage_key = f"{now.year}/{now.month:02d}/{now.day:02d}/{trend.id}_{i}_{hash_prefix}.gif"
                    if not self.image_service.upload_to_r2(
                        img_result["raw_data"], storage_key,
                        self.r2_account_id, self.r2_access_key_id, self.r2_secret_access_key,
                        self.r2_bucket_name, content_type="image/gif",
                    ):
                        storage_key = None
            else:
                webp_data = img_result.get("webp_data")
                if webp_data and r2_ready:
                    storage_key = f"{now.year}/{now.month:02d}/{now.day:02d}/{trend.id}_{i}_{hash_prefix}.webp"
                    if not self.image_service.upload_to_r2(
                        webp_data, storage_key,
                        self.r2_account_id, self.r2_access_key_id, self.r2_secret_access_key,
                        self.r2_bucket_name,
                    ):
                        storage_key = None

            self.trend_service.add_image_to_trend(trend, {
                "url": img_result["url"],
                "phash": img_result.get("phash"),
                "width": img_result.get("width"),
                "height": img_result.get("height"),
                "storage_key": storage_key,
                "media_type": media_type,
            })

        self.db.flush()
        self.db.refresh(trend)
        self.trend_service.update_trend_stats(trend)
        if match_batch is not None:
            # 뒤 글이 이 글과 같은 트렌드로 묶이게 (글 단위 조회 때는 커밋 뒤라 창에 보였다)
            match_batch.record(trend.id, trend.title, [r.get("phash") for r in image_results])
        return True
