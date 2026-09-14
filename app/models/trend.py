from datetime import datetime
from sqlalchemy import Column, Integer, String, Text, DateTime, Float, ForeignKey, Index, text
from sqlalchemy.orm import relationship

from app.core.database import Base


class Site(Base):
    """크롤링 대상 사이트"""
    __tablename__ = "sites"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), unique=True, nullable=False)  # 예: "fmkorea", "clien"
    display_name = Column(String(100), nullable=False)  # 예: "에펨코리아", "클리앙"
    base_url = Column(String(500), nullable=False)
    is_active = Column(Integer, default=1)  # 1: 활성, 0: 비활성
    created_at = Column(DateTime, default=datetime.utcnow)

    articles = relationship("TrendArticle", back_populates="site")


class Trend(Base):
    """트렌드 클러스터"""
    __tablename__ = "trends"

    id = Column(Integer, primary_key=True, index=True)
    title = Column(String(500), nullable=False)
    score = Column(Float, default=0.0)
    site_count = Column(Integer, default=1)  # 발견된 사이트 수
    category = Column(String(30))  # LLM 편집 레이어(zzalmunity enrichment)가 기록 — 여기서는 랭킹 다양성 페널티용 읽기 전용
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    images = relationship("TrendImage", back_populates="trend", order_by="TrendImage.order")
    articles = relationship("TrendArticle", back_populates="trend")

    __table_args__ = (
        Index("idx_trends_score", "score"),
        Index("idx_trends_created_at", "created_at"),
    )


class TrendImage(Base):
    """트렌드에 포함된 이미지"""
    __tablename__ = "trend_images"

    id = Column(Integer, primary_key=True, index=True)
    trend_id = Column(Integer, ForeignKey("trends.id"), nullable=False)
    url = Column(String(1000), nullable=False)  # 원본 이미지 URL
    storage_key = Column(String(500))  # Supabase Storage 경로
    phash = Column(String(64))  # pHash 값 (16진수 문자열)
    media_type = Column(String(10), default="image")  # "image" or "video"
    width = Column(Integer)
    height = Column(Integer)
    order = Column(Integer, default=0)  # 트렌드 내 이미지 순서
    created_at = Column(DateTime, default=datetime.utcnow)

    trend = relationship("Trend", back_populates="images")

    __table_args__ = (
        Index("idx_trend_images_phash", "phash"),
        Index("idx_trend_images_trend_id", "trend_id"),
        # Partial index for find_or_create_trend's 7-day pHash window. Without it every
        # new article seq-scanned the whole table (2026-09-14 Supabase Disk IO incident).
        Index(
            "idx_trend_images_created_at_phash",
            "created_at",
            postgresql_where=text("phash IS NOT NULL"),
        ),
    )


class TrendArticle(Base):
    """트렌드의 원본 글"""
    __tablename__ = "trend_articles"

    id = Column(Integer, primary_key=True, index=True)
    trend_id = Column(Integer, ForeignKey("trends.id"), nullable=False)
    site_id = Column(Integer, ForeignKey("sites.id"), nullable=False)
    title = Column(String(500), nullable=False)
    url = Column(String(1000), nullable=False)
    content = Column(Text)  # 본문 텍스트 (선택적)
    view_count = Column(Integer, default=0)
    like_count = Column(Integer, default=0)
    comment_count = Column(Integer, default=0)
    published_at = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)

    trend = relationship("Trend", back_populates="articles")
    site = relationship("Site", back_populates="articles")

    __table_args__ = (
        Index("idx_trend_articles_trend_id", "trend_id"),
        Index("idx_trend_articles_site_id", "site_id"),
        # DB-level guard: one article per site per trend
        Index("uq_trend_articles_trend_site", "trend_id", "site_id", unique=True),
        # URL dedup lookups (url IN (...)) and the per-site skip-URL query (2026-09-14)
        Index("idx_trend_articles_url", "url"),
        Index("idx_trend_articles_site_created", "site_id", "created_at"),
    )
