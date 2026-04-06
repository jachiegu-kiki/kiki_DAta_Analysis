# backend/app/models/__init__.py
# backend/app/models/user.py
import uuid
from sqlalchemy import Column, String, Boolean, DateTime, ForeignKey, func
from sqlalchemy.dialects.postgresql import UUID
from app.core.database import Base


class UserModel(Base):
    __tablename__ = "dim_users"

    id               = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    employee_id      = Column(String(32), ForeignKey("dim_advisor.advisor_id"), nullable=True)
    username         = Column(String(64), nullable=False, unique=True)
    hashed_password  = Column(String(256), nullable=False)
    role             = Column(String(16), nullable=False)   # ADMIN / MANAGER / ADVISOR
    department_scope = Column(String(64))                   # MANAGER 专用
    is_active        = Column(Boolean, default=True)
    last_login       = Column(DateTime(timezone=True))
    created_at       = Column(DateTime(timezone=True), server_default=func.now())
