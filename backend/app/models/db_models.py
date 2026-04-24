"""
app/models/db_models.py – SQLAlchemy ORM Models

Maps to tables created by scripts/init_db.sql.
Used by the Kafka worker services (TenantResolver, ImpactAnalysisService).
"""

import uuid
from datetime import datetime, timezone

from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, relationship


class Base(DeclarativeBase):
    pass


def _now():
    return datetime.now(timezone.utc)


class Tenant(Base):
    __tablename__ = "tenants"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(Text, nullable=False)
    plan = Column(String(50), nullable=False, default="free")
    is_active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime(timezone=True), nullable=False, default=_now)
    updated_at = Column(DateTime(timezone=True), onupdate=_now)

    installations = relationship("GitHubInstallation", back_populates="tenant")
    analyses = relationship("PRAnalysis", back_populates="tenant")

    def __repr__(self):
        return f"<Tenant id={self.id} name={self.name}>"


class GitHubInstallation(Base):
    __tablename__ = "github_installations"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    installation_id = Column(Text, nullable=False, unique=True)
    tenant_id = Column(UUID(as_uuid=True), ForeignKey("tenants.id"), nullable=False)
    schema_name = Column(Text)                 # per-tenant schema name
    account_login = Column(Text)
    account_type = Column(Text)
    is_active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime(timezone=True), nullable=False, default=_now)
    updated_at = Column(DateTime(timezone=True), onupdate=_now)

    tenant = relationship("Tenant", back_populates="installations")

    def __repr__(self):
        return (
            f"<GitHubInstallation installation_id={self.installation_id} "
            f"tenant_id={self.tenant_id}>"
        )


class PRAnalysis(Base):
    __tablename__ = "pr_analyses"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id = Column(UUID(as_uuid=True), ForeignKey("tenants.id"), nullable=False)
    installation_id = Column(Text, nullable=False)
    repo_full_name = Column(Text, nullable=False)
    pr_number = Column(Integer, nullable=False)
    pr_title = Column(Text)
    pr_url = Column(Text)
    branch_name = Column(Text)
    author_name = Column(Text)
    total_impacted_queries = Column(Integer, default=0)
    analysis_data = Column(JSONB)
    status = Column(
        String(20), nullable=False, default="pending"
    )  # pending | processing | done | failed
    idempotency_key = Column(Text, unique=True)
    processed_at = Column(DateTime(timezone=True))
    created_at = Column(DateTime(timezone=True), nullable=False, default=_now)
    updated_at = Column(DateTime(timezone=True), onupdate=_now)

    tenant = relationship("Tenant", back_populates="analyses")

    def __repr__(self):
        return (
            f"<PRAnalysis pr={self.pr_number} "
            f"repo={self.repo_full_name} status={self.status}>"
        )
