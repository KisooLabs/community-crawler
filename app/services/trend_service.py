import math
import re
from datetime import datetime, timedelta
from collections import defaultdict
from sqlalchemy import select, update, func, text
from sqlalchemy.orm import Session, selectinload

from app.models.trend import Trend, TrendImage, TrendArticle
from app.services.image_service import ImageService


class TrendService:
    """트렌드 관리 서비스"""

    # 점수 반감기 (시간). 4h→8h 완화 (2026-07-06 P1: 노출 풀 전원이 <20h로
    # 재고가 체류 상한이던 문제 — 풀 수명을 늘려 재고 확보).
    # 주의: update_scores()의 갱신 윈도우(72h)와 연동 — 반감기를 더 늘리면
    # 윈도우 밖에서 score>0.5로 얼어붙는 트렌드가 생기지 않는지 확인할 것.
    HALF_LIFE_HOURS = 8

    def __init__(self, db: Session = None):
        self.db = db
        self.image_service = ImageService()
        self._site_stats_cache = None

    MATCH_WINDOW_HOURS = 168  # 최근 7일 이미지만 비교

    MIN_OVERLAP_WORDS = 1  # 제목 매칭 최소 겹침 단어 수 (이미지 phash ≤ HASH_THRESHOLD 통과한 페어에만 적용)

    @staticmethod
    def _title_similar(title1: str, title2: str) -> bool:
        """두 제목의 유사도 확인 (2글자 이상 단어 겹침 기반, 최소 1개 겹침)"""
        words1 = set(w for w in re.split(r'\W+', title1) if len(w) >= 2)
        words2 = set(w for w in re.split(r'\W+', title2) if len(w) >= 2)
        if not words1 or not words2:
            return True  # 단어 추출 실패 시 이미지 매칭만 사용
        overlap = len(words1 & words2)
        return overlap >= TrendService.MIN_OVERLAP_WORDS

    @staticmethod
    def hamming(phash1: str, phash2: str) -> int:
        """64비트 pHash(16진 문자열) 해밍 거리. SQL의 bit_count(a # b)와 같은 값."""
        return (int(phash1, 16) ^ int(phash2, 16)).bit_count()

    def find_trend_candidates(self, phashes: list[str]) -> dict[str, list[tuple[int, str]]]:
        """최근 7일 이미지 중 각 입력 phash와 HASH_THRESHOLD 이내인 트렌드를 한 번에 조회.

        Returns: {input_phash: [(trend_id, trend_title), ...]} (후보가 없는 phash는 키가 없다).
        제목 유사도는 여기서 보지 않는다 (find_or_create_trend가 판정).

        크롤 배치(사이트 1회분)당 1회 호출한다. 글마다 7일치를 훑던 때는 크롤 1회에
        새 글 수(~150)만큼 창 전체를 다시 읽었다 (2026-09-14 Supabase Disk IO 후속).
        비트열은 CTE에서 한 번만 변환하고, trends는 hits 행에 대해서만 조회한다. JOIN으로 쓰면
        플래너가 trends 전체(22만 행)를 해시 조인하고 temp로 넘친다 (2026-09-14 EXPLAIN).
        """
        phashes = sorted(set(p for p in phashes if p))
        if not phashes:
            return {}
        cutoff = datetime.utcnow() - timedelta(hours=self.MATCH_WINDOW_HOURS)
        rows = self.db.execute(
            text("""
                WITH inputs AS MATERIALIZED (
                    SELECT phash, ('x' || phash)::bit(64) AS bits
                    FROM unnest(CAST(:phashes AS text[])) AS p(phash)
                ),
                win AS MATERIALIZED (
                    SELECT trend_id, phash, ('x' || phash)::bit(64) AS bits
                    FROM trend_images
                    WHERE phash IS NOT NULL AND created_at > :cutoff
                ),
                hits AS MATERIALIZED (
                    SELECT i.phash AS input_phash, w.trend_id
                    FROM inputs i JOIN win w ON bit_count(i.bits # w.bits) <= :threshold
                )
                SELECT h.input_phash, h.trend_id,
                       (SELECT t.title FROM trends t WHERE t.id = h.trend_id) AS trend_title
                FROM hits h
            """),
            {"phashes": phashes, "cutoff": cutoff,
             "threshold": ImageService.HASH_THRESHOLD},
        ).all()

        candidates: dict[str, list[tuple[int, str]]] = defaultdict(list)
        seen: set[tuple[str, int]] = set()
        for row in rows:
            if row.trend_title is None or (row.input_phash, row.trend_id) in seen:
                continue
            seen.add((row.input_phash, row.trend_id))
            candidates[row.input_phash].append((row.trend_id, row.trend_title))
        return dict(candidates)

    def start_match_batch(self, phashes: list[str]) -> "TrendMatchBatch":
        """크롤 배치의 트렌드 매칭 후보를 1회 조회해 TrendMatchBatch로 돌려준다."""
        return TrendMatchBatch(self.find_trend_candidates(phashes))

    def find_or_create_trend(
        self,
        image_phash: str,
        title: str,
        batch: "TrendMatchBatch | None" = None,
    ) -> Trend:
        """유사한 트렌드 찾기 또는 새로 생성.

        batch가 있으면 그 후보만 본다 (사이트당 1회 조회분 + 같은 배치에서 먼저 저장된 글).
        없으면 이 phash 하나로 조회한다 (단건 호출용).
        """
        if batch is not None:
            candidates = batch.candidates(image_phash)
        else:
            candidates = self.find_trend_candidates([image_phash]).get(image_phash, [])

        for trend_id, trend_title in candidates:
            if self._title_similar(title, trend_title):
                trend = self.db.get(Trend, trend_id)
                if trend:
                    return trend

        # 새 트렌드 생성
        trend = Trend(title=title, score=1.0, site_count=1)
        self.db.add(trend)
        self.db.flush()
        return trend

    def add_article_to_trend(
        self,
        trend: Trend,
        article_data: dict,
        site_id: int,
    ) -> TrendArticle | None:
        """트렌드에 원본 글 추가 (같은 URL 또는 같은 사이트면 스킵)"""
        existing = self.db.execute(
            select(TrendArticle).where(
                TrendArticle.trend_id == trend.id,
                TrendArticle.url == article_data["url"],
            )
        ).scalar_one_or_none()

        if existing:
            return existing

        # 같은 사이트에서 이미 글이 있으면 스킵 (리포스트 중복 방지)
        same_site = self.db.execute(
            select(TrendArticle).where(
                TrendArticle.trend_id == trend.id,
                TrendArticle.site_id == site_id,
            )
        ).scalar_one_or_none()

        if same_site:
            return None

        article = TrendArticle(
            trend_id=trend.id,
            site_id=site_id,
            title=article_data["title"],
            url=article_data["url"],
            view_count=article_data.get("view_count", 0),
            like_count=article_data.get("like_count", 0),
            comment_count=article_data.get("comment_count", 0),
            published_at=article_data.get("published_at"),
            content=article_data.get("content"),
        )
        self.db.add(article)
        return article

    def add_image_to_trend(
        self,
        trend: Trend,
        image_data: dict,
    ) -> TrendImage:
        """트렌드에 이미지 추가 (URL 또는 pHash 중복 시 스킵)"""
        # URL 중복 체크
        existing = self.db.execute(
            select(TrendImage).where(
                TrendImage.trend_id == trend.id,
                TrendImage.url == image_data["url"],
            )
        ).scalar_one_or_none()

        if existing:
            if not existing.storage_key and image_data.get("storage_key"):
                existing.storage_key = image_data["storage_key"]
            return existing

        # pHash 유사도 체크 (같은 이미지, 다른 URL/압축) — DB에서 해밍 거리 계산
        phash = image_data.get("phash")
        if phash:
            match = self.db.execute(
                text("""
                    SELECT id FROM trend_images
                    WHERE trend_id = :trend_id
                      AND phash IS NOT NULL
                      AND bit_count(('x' || phash)::bit(64) # ('x' || :input_phash)::bit(64)) <= :threshold
                    LIMIT 1
                """),
                {"trend_id": trend.id, "input_phash": phash,
                 "threshold": ImageService.HASH_THRESHOLD},
            ).first()
            if match:
                existing_img = self.db.get(TrendImage, match.id)
                if existing_img:
                    if not existing_img.storage_key and image_data.get("storage_key"):
                        existing_img.storage_key = image_data["storage_key"]
                    return existing_img

        image = TrendImage(
            trend_id=trend.id,
            url=image_data["url"],
            storage_key=image_data.get("storage_key"),
            phash=image_data.get("phash"),
            width=image_data.get("width"),
            height=image_data.get("height"),
            media_type=image_data.get("media_type", "image"),
            order=len(trend.images),
        )
        self.db.add(image)
        return image

    def _get_site_stats(self) -> dict:
        """각 사이트의 최근 engagement 평균 (정규화용, 세션 내 캐싱)"""
        if self._site_stats_cache is not None:
            return self._site_stats_cache

        cutoff = datetime.utcnow() - timedelta(hours=72)
        rows = self.db.execute(
            select(
                TrendArticle.site_id,
                func.avg(TrendArticle.view_count),
                func.avg(TrendArticle.like_count),
                func.avg(TrendArticle.comment_count),
            )
            .where(TrendArticle.created_at > cutoff)
            .group_by(TrendArticle.site_id)
        ).all()

        stats = {}
        for site_id, avg_views, avg_likes, avg_comments in rows:
            stats[site_id] = {
                "avg_views": max(float(avg_views or 0), 1),
                "avg_likes": max(float(avg_likes or 0), 1),
                "avg_comments": max(float(avg_comments or 0), 1),
            }
        self._site_stats_cache = stats
        return stats

    def calculate_score(self, trend: Trend) -> float:
        """트렌드 점수 계산 (사이트별 정규화 + 다양성 + 시간 감쇠)"""
        articles = trend.articles
        site_stats = self._get_site_stats()

        # 사이트별 정규화: 각 글의 engagement를 해당 사이트 평균으로 나눔
        # → "사이트 내에서 얼마나 핫한가" (1.0 = 평균, 2.0 = 평균의 2배)
        norm_views = 0.0
        norm_likes = 0.0
        norm_comments = 0.0
        for a in articles:
            s = site_stats.get(a.site_id)
            if s:
                norm_views += a.view_count / s["avg_views"]
                norm_likes += a.like_count / s["avg_likes"]
                norm_comments += a.comment_count / s["avg_comments"]
            else:
                # 새 사이트 (통계 없음) → 기본값 사용
                norm_views += math.log1p(a.view_count / 100)
                norm_likes += math.log1p(a.like_count)
                norm_comments += math.log1p(a.comment_count)

        engagement = (
            1
            + math.log1p(norm_views) * 1.0
            + math.log1p(norm_likes) * 2.0
            + math.log1p(norm_comments) * 1.5
        )

        # 사이트 다양성 보너스
        diversity = 1 + math.log(max(trend.site_count, 1))

        base_score = engagement * diversity

        # 시간 감쇠
        hours_old = (datetime.utcnow() - trend.created_at).total_seconds() / 3600
        decay = 0.5 ** (hours_old / self.HALF_LIFE_HOURS)

        return base_score * decay

    def update_trend_stats(self, trend: Trend):
        """트렌드 통계 업데이트"""
        # 고유 사이트 수 계산
        site_ids = set(a.site_id for a in trend.articles)
        trend.site_count = len(site_ids)

        # 점수 재계산
        trend.score = self.calculate_score(trend)
        trend.updated_at = datetime.utcnow()

    # 순위 기반 누적 다양성 페널티: 1위부터 순서대로 확정하며, 같은 사이트/
    # 카테고리가 위에 k번 등장했으면 adjusted = original × decay^min(k, CAP).
    # 점수가 압도적이면 페널티를 이기고 올라오므로 재밌는 글은 묻히지 않음.
    SITE_DIVERSITY_DECAY = 0.85      # 사이트 등장 1회당 (0.85^3=61%)
    CATEGORY_DIVERSITY_DECAY = 0.90  # 카테고리 등장 1회당 — 사이트보다 완만 (0.90^7=48%)
    # 페널티 지수 상한. 목적은 상위권 다양성이지 꼬리 절멸이 아님 — 무제한
    # 누적이면 다수 사이트(fmkorea 등)의 30번째 트렌드가 ×0.009로 뭉개져
    # 노출 풀이 오히려 줄어듦 (2026-07-06 시뮬: 무제한 46 vs cap=6 78).
    # 바닥: site 0.85^6=0.38, category 0.90^6=0.53.
    DIVERSITY_PENALTY_CAP = 6

    def _apply_rank_diversity(self, trends: list[Trend]):
        """순위 기반 누적 사이트·카테고리 다양성 페널티.

        원본 점수 순으로 훑으며, 해당 트렌드의 primary site가 위에 k_s번,
        category(LLM 분류, 미분류 None은 집계 제외)가 k_c번 등장했으면
        score *= SITE_DECAY^min(k_s,CAP) × CATEGORY_DECAY^min(k_c,CAP).
        """
        sorted_trends = sorted(trends, key=lambda t: t.score, reverse=True)
        site_counts: dict[int, int] = defaultdict(int)
        category_counts: dict[str, int] = defaultdict(int)

        for trend in sorted_trends:
            penalty = 1.0

            if trend.articles:
                primary_site = trend.articles[0].site_id
                k = min(site_counts[primary_site], self.DIVERSITY_PENALTY_CAP)
                if k > 0:
                    penalty *= self.SITE_DIVERSITY_DECAY ** k
                site_counts[primary_site] += 1

            if trend.category:
                k = min(category_counts[trend.category], self.DIVERSITY_PENALTY_CAP)
                if k > 0:
                    penalty *= self.CATEGORY_DIVERSITY_DECAY ** k
                category_counts[trend.category] += 1

            if penalty < 1.0:
                trend.score *= penalty

    def update_scores(self) -> int:
        """모든 트렌드 점수 업데이트 (시간 감쇠 + 사이트·카테고리 다양성)"""
        # 최근 72시간 내 트렌드만 업데이트.
        # 48h→72h: 반감기 8h 기준 base score가 커도 72h면 0.5 아래로 확실히
        # 내려감 (48h 윈도우면 base>32인 트렌드가 score>0.5로 얼어붙어 영구 노출).
        cutoff = datetime.utcnow() - timedelta(hours=72)

        trends = self.db.execute(
            select(Trend)
            .options(selectinload(Trend.articles))
            .where(Trend.created_at > cutoff)
        ).scalars().all()

        for trend in trends:
            trend.score = self.calculate_score(trend)

        self._apply_rank_diversity(trends)

        self.db.commit()
        return len(trends)


class TrendMatchBatch:
    """크롤 배치(사이트 1회분)의 트렌드 매칭 후보.

    TrendService.find_trend_candidates가 돌려준 dict에, 이 배치에서 저장이 끝난 글의
    이미지를 record()로 더한다. 글 단위로 조회하던 때는 앞 글이 커밋된 뒤라 그 이미지도
    창에 보였으므로, 같은 배치 안의 매칭(같은 사이트 재게시 → 같은 트렌드로 묶여
    add_article_to_trend가 스킵)을 그대로 유지하기 위한 것이다.
    """

    def __init__(self, db_candidates: dict[str, list[tuple[int, str]]]):
        self._db = db_candidates
        self._saved: list[tuple[str, int, str]] = []  # (phash, trend_id, trend_title)

    def candidates(self, phash: str) -> list[tuple[int, str]]:
        """이 phash의 후보 [(trend_id, trend_title), ...]. DB 조회분 뒤에 배치 내 저장분."""
        found = list(self._db.get(phash, []))
        seen = {trend_id for trend_id, _ in found}
        for saved_phash, trend_id, trend_title in self._saved:
            if trend_id in seen:
                continue
            if TrendService.hamming(phash, saved_phash) <= ImageService.HASH_THRESHOLD:
                found.append((trend_id, trend_title))
                seen.add(trend_id)
        return found

    def record(self, trend_id: int, trend_title: str, phashes: list[str]) -> None:
        """저장이 끝난 글의 이미지 phash들을 그 트렌드 후보로 등록."""
        for phash in phashes:
            if phash:
                self._saved.append((phash, trend_id, trend_title))
