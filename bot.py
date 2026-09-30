# -*- coding: utf-8 -*-
# ============================================================
# 🤖 MODERN HOSTING BOT — Aiogram 3.x
# ============================================================
# PART 1/5 : Config + Database + Engine
# ============================================================
"""
Full-featured Telegram Bot Hosting Platform
- Owner approval for ALL uploads (non-admin)
- Auto module install (Python pip + Node npm)
- Missing import detection + auto-retry
- ZIP with requirements.txt / package.json
- Crash log auto-send to uploader
- Env isolation (no TOKEN leak)
- Ban / Limits / Subscription / Mandatory channels
- Multi-admin, Broadcast, Audit logs
- Web keep-alive (Render / Railway / Koyeb friendly)
"""

import asyncio
import io
import os
import re
import sys
import time
import uuid
import json
import signal
import zipfile
import tempfile
import shutil
import platform
import subprocess
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Sequence, Any, Union

import psutil
from aiohttp import web
from loguru import logger
from pydantic_settings import BaseSettings, SettingsConfigDict

from sqlalchemy import (
    String, Integer, BigInteger, DateTime, Text, Boolean,
    Float, select, update, delete, func, and_, or_,
)
from sqlalchemy.ext.asyncio import (
    AsyncSession, async_sessionmaker, create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.enums import ParseMode, ChatMemberStatus, ChatType
from aiogram.exceptions import (
    TelegramBadRequest, TelegramForbiddenError,
    TelegramRetryAfter, TelegramUnauthorizedError,
)
from aiogram.filters import Command, CommandStart, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message, CallbackQuery, Document, InlineKeyboardMarkup,
    ReplyKeyboardMarkup, KeyboardButton, BotCommand,
    BotCommandScopeDefault, FSInputFile, BufferedInputFile,
    ChatMemberUpdated, ChatJoinRequest,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder


# ============================================================
# ⚙️ CONFIG — Pydantic Settings
# ============================================================

class Settings(BaseSettings):
    """All bot config from .env"""
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # --- Bot Credentials ---
    bot_token: str
    owner_id: int
    admin_ids: str = ""  # comma separated

    # --- Links ---
    your_username: str = "Kon_Hu_Mai"
    update_channel: str = "Kon_Hu_Mai"
    support_group: str = ""

    # --- User Limits ---
    free_user_limit: int = 50
    subscribed_user_limit: int = 20
    admin_limit: int = 999
    owner_limit: int = 999999

    # --- File Size ---
    max_file_size_mb: int = 20

    # --- Feature Toggles ---
    owner_approval_required: bool = True
    auto_install_modules: bool = True
    forward_uploads_to_owner: bool = True
    enable_web_keepalive: bool = True

    # --- Server (Web Keepalive) ---
    port: int = 8080
    host: str = "0.0.0.0"

    # --- Paths ---
    db_url: str = "sqlite+aiosqlite:///data/bot.db"
    upload_dir: str = "upload_bots"
    data_dir: str = "data"
    log_dir: str = "data/logs"

    # --- Timeouts (seconds) ---
    precheck_timeout: int = 15
    install_timeout: int = 180
    max_run_time_hours: int = 24
    session_timeout: int = 3600

    # --- Rate Limits ---
    rate_limit_uploads_per_min: int = 5
    rate_limit_commands_per_min: int = 30

    # --- Computed ---
    @property
    def admins(self) -> set[int]:
        s = {self.owner_id}
        for x in self.admin_ids.split(","):
            x = x.strip()
            if x.isdigit():
                s.add(int(x))
        return s

    @property
    def max_file_size(self) -> int:
        return self.max_file_size_mb * 1024 * 1024

    @property
    def upload_path(self) -> Path:
        p = Path(self.upload_dir)
        p.mkdir(exist_ok=True, parents=True)
        return p

    @property
    def data_path(self) -> Path:
        p = Path(self.data_dir)
        p.mkdir(exist_ok=True, parents=True)
        return p

    @property
    def log_path(self) -> Path:
        p = Path(self.log_dir)
        p.mkdir(exist_ok=True, parents=True)
        return p


# Singleton
settings = Settings()


# ============================================================
# 📝 LOGGER — Loguru with rotation
# ============================================================

logger.remove()

# Console (colored)
logger.add(
    sys.stdout,
    format=(
        "<green>{time:HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{line}</cyan> | "
        "<level>{message}</level>"
    ),
    level="INFO",
    colorize=True,
)

# All logs (rotating daily)
logger.add(
    settings.log_path / "bot_{time:YYYY-MM-DD}.log",
    rotation="50 MB",
    retention="30 days",
    compression="zip",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{line} | {message}",
    level="DEBUG",
    encoding="utf-8",
    enqueue=True,
)

# Errors only
logger.add(
    settings.log_path / "errors_{time:YYYY-MM-DD}.log",
    rotation="10 MB",
    retention="60 days",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{line} | {message}\n{exception}",
    level="ERROR",
    encoding="utf-8",
    backtrace=True,
    diagnose=True,
    enqueue=True,
)

logger.info("🚀 Logger initialized")


# ============================================================
# 🗄️ DATABASE MODELS
# ============================================================

class Base(DeclarativeBase):
    """Base for all models"""
    pass


class User(Base):
    """Registered users"""
    __tablename__ = "users"

    user_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    username: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    first_name: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    last_name: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    language_code: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)

    joined_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    last_seen: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    is_banned: Mapped[bool] = mapped_column(Boolean, default=False)
    ban_reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    banned_by: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    banned_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    custom_limit: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    custom_limit_set_by: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)

    is_subscribed: Mapped[bool] = mapped_column(Boolean, default=False)
    subscription_expiry: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    subscription_added_by: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)

    total_uploads: Mapped[int] = mapped_column(Integer, default=0)
    total_approved: Mapped[int] = mapped_column(Integer, default=0)
    total_rejected: Mapped[int] = mapped_column(Integer, default=0)

    def __repr__(self):
        return f"<User {self.user_id}>"


class UserFile(Base):
    """User uploaded files"""
    __tablename__ = "user_files"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(BigInteger, index=True)
    file_name: Mapped[str] = mapped_column(String(255))
    file_type: Mapped[str] = mapped_column(String(10))  # py / js
    file_size: Mapped[int] = mapped_column(Integer, default=0)
    uploaded_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    is_running: Mapped[bool] = mapped_column(Boolean, default=False)
    pid: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    restart_count: Mapped[int] = mapped_column(Integer, default=0)
    last_crash_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    def __repr__(self):
        return f"<UserFile {self.file_name} of {self.user_id}>"


class AdminModel(Base):
    """Admins list"""
    __tablename__ = "admins"

    user_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    added_by: Mapped[int] = mapped_column(BigInteger)
    added_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f"<Admin {self.user_id}>"


class MandatoryChannel(Base):
    """Mandatory channels"""
    __tablename__ = "mandatory_channels"

    channel_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    channel_username: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    channel_name: Mapped[str] = mapped_column(String(255))
    invite_link: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    added_by: Mapped[int] = mapped_column(BigInteger)
    added_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f"<Channel {self.channel_name}>"


class PendingApproval(Base):
    """Pending file approvals (from users)"""
    __tablename__ = "pending_approvals"

    approval_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    user_id: Mapped[int] = mapped_column(BigInteger, index=True)
    user_name: Mapped[str] = mapped_column(String(128))
    file_name: Mapped[str] = mapped_column(String(255))
    file_type: Mapped[str] = mapped_column(String(10))
    file_size: Mapped[int] = mapped_column(Integer)
    content: Mapped[bytes] = mapped_column()
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    status: Mapped[str] = mapped_column(String(20), default="pending")
    reviewed_by: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    reviewed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    def __repr__(self):
        return f"<Approval {self.approval_id}: {self.status}>"


class InstallLog(Base):
    """Module install logs"""
    __tablename__ = "install_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(BigInteger, index=True)
    module_name: Mapped[str] = mapped_column(String(255))
    package_name: Mapped[str] = mapped_column(String(255))
    installer: Mapped[str] = mapped_column(String(20))  # pip / npm
    status: Mapped[str] = mapped_column(String(20))     # success / failed / error
    log: Mapped[str] = mapped_column(Text)
    installed_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f"<InstallLog {self.module_name}: {self.status}>"


class BotStats(Base):
    """Global bot stats (singleton row id=1)"""
    __tablename__ = "bot_stats"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    is_locked: Mapped[bool] = mapped_column(Boolean, default=False)
    lock_reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    total_uploads: Mapped[int] = mapped_column(Integer, default=0)
    total_approved: Mapped[int] = mapped_column(Integer, default=0)
    total_rejected: Mapped[int] = mapped_column(Integer, default=0)
    total_installs: Mapped[int] = mapped_column(Integer, default=0)
    total_crashes: Mapped[int] = mapped_column(Integer, default=0)

    last_broadcast_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    def __repr__(self):
        return f"<BotStats locked={self.is_locked}>"


class BroadcastHistory(Base):
    """Broadcast history"""
    __tablename__ = "broadcast_history"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    admin_id: Mapped[int] = mapped_column(BigInteger)
    text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    media_type: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    media_file_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    sent_count: Mapped[int] = mapped_column(Integer, default=0)
    failed_count: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f"<Broadcast by {self.admin_id}>"


class AuditLog(Base):
    """Admin action audit log"""
    __tablename__ = "audit_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    admin_id: Mapped[int] = mapped_column(BigInteger, index=True)
    action: Mapped[str] = mapped_column(String(64))
    target_id: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    details: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f"<Audit {self.action} by {self.admin_id}>"


# ============================================================
# 🗄️ DATABASE ENGINE + SESSION
# ============================================================

engine = create_async_engine(
    settings.db_url,
    echo=False,
    future=True,
    pool_pre_ping=True,
)

async_session = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


async def init_db():
    """Create all tables + ensure BotStats row"""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with async_session() as s:
        stats = await s.get(BotStats, 1)
        if not stats:
            s.add(BotStats(id=1))
            await s.commit()

    logger.success("✅ Database initialized")


async def close_db():
    """Dispose engine on shutdown"""
    await engine.dispose()
    logger.info("Database connection closed")


# ============================================================
# 🗄️ DB HELPERS — USERS
# ============================================================

async def db_get_user(user_id: int) -> Optional[User]:
    """Get user by ID"""
    async with async_session() as s:
        return await s.get(User, user_id)


async def db_upsert_user(user_id: int, **kwargs) -> User:
    """Create or update a user"""
    async with async_session() as s:
        u = await s.get(User, user_id)
        if not u:
            u = User(user_id=user_id, **kwargs)
            s.add(u)
        else:
            for k, v in kwargs.items():
                if v is not None:
                    setattr(u, k, v)
            u.last_seen = datetime.utcnow()
        await s.commit()
        await s.refresh(u)
        return u


async def db_get_all_user_ids() -> list[int]:
    """Get all non-banned user IDs"""
    async with async_session() as s:
        r = await s.execute(select(User.user_id).where(User.is_banned == False))
        return [row[0] for row in r.all()]


async def db_get_all_users_count() -> int:
    async with async_session() as s:
        r = await s.execute(select(func.count(User.user_id)).where(User.is_banned == False))
        return r.scalar() or 0


async def db_get_banned_count() -> int:
    async with async_session() as s:
        r = await s.execute(select(func.count(User.user_id)).where(User.is_banned == True))
        return r.scalar() or 0


async def db_ban_user(user_id: int, reason: str, banned_by: int) -> bool:
    async with async_session() as s:
        u = await s.get(User, user_id)
        if not u:
            return False
        u.is_banned = True
        u.ban_reason = reason
        u.banned_by = banned_by
        u.banned_at = datetime.utcnow()
        await s.commit()
        return True


async def db_unban_user(user_id: int) -> bool:
    async with async_session() as s:
        u = await s.get(User, user_id)
        if not u:
            return False
        u.is_banned = False
        u.ban_reason = None
        u.banned_by = None
        u.banned_at = None
        await s.commit()
        return True


async def db_is_banned(user_id: int) -> bool:
    u = await db_get_user(user_id)
    return u.is_banned if u else False


async def db_set_custom_limit(user_id: int, limit: int, admin_id: int) -> bool:
    async with async_session() as s:
        u = await s.get(User, user_id)
        if not u:
            u = User(user_id=user_id, custom_limit=limit, custom_limit_set_by=admin_id)
            s.add(u)
        else:
            u.custom_limit = limit
            u.custom_limit_set_by = admin_id
        await s.commit()
        return True


async def db_remove_custom_limit(user_id: int) -> bool:
    async with async_session() as s:
        u = await s.get(User, user_id)
        if not u:
            return False
        u.custom_limit = None
        u.custom_limit_set_by = None
        await s.commit()
        return True


async def db_set_subscription(user_id: int, days: int, admin_id: int) -> datetime:
    async with async_session() as s:
        u = await s.get(User, user_id)
        if not u:
            u = User(user_id=user_id)
            s.add(u)
        now = datetime.utcnow()
        base = u.subscription_expiry if (u.subscription_expiry and u.subscription_expiry > now) else now
        u.is_subscribed = True
        u.subscription_expiry = base + timedelta(days=days)
        u.subscription_added_by = admin_id
        await s.commit()
        return u.subscription_expiry


async def db_remove_subscription(user_id: int) -> bool:
    async with async_session() as s:
        u = await s.get(User, user_id)
        if not u:
            return False
        u.is_subscribed = False
        u.subscription_expiry = None
        await s.commit()
        return True


async def db_incr_user_stat(user_id: int, field: str, amount: int = 1):
    """Increment total_uploads / total_approved / total_rejected"""
    async with async_session() as s:
        u = await s.get(User, user_id)
        if u and hasattr(u, field):
            setattr(u, field, getattr(u, field) + amount)
            await s.commit()


# ============================================================
# 🗄️ DB HELPERS — FILES
# ============================================================

async def db_add_user_file(user_id: int, file_name: str,
                            file_type: str, size: int = 0) -> UserFile:
    async with async_session() as s:
        # remove old entry with same name
        await s.execute(delete(UserFile).where(
            UserFile.user_id == user_id,
            UserFile.file_name == file_name,
        ))
        f = UserFile(
            user_id=user_id, file_name=file_name,
            file_type=file_type, file_size=size,
        )
        s.add(f)
        await s.commit()
        await s.refresh(f)
        return f


async def db_get_user_files(user_id: int) -> Sequence[UserFile]:
    async with async_session() as s:
        r = await s.execute(
            select(UserFile)
            .where(UserFile.user_id == user_id)
            .order_by(UserFile.file_name)
        )
        return r.scalars().all()


async def db_get_user_file(user_id: int, file_name: str) -> Optional[UserFile]:
    async with async_session() as s:
        r = await s.execute(
            select(UserFile).where(
                UserFile.user_id == user_id,
                UserFile.file_name == file_name,
            )
        )
        return r.scalar_one_or_none()


async def db_get_user_file_count(user_id: int) -> int:
    async with async_session() as s:
        r = await s.execute(
            select(func.count(UserFile.id)).where(UserFile.user_id == user_id)
        )
        return r.scalar() or 0


async def db_delete_user_file(user_id: int, file_name: str) -> bool:
    async with async_session() as s:
        r = await s.execute(
            delete(UserFile).where(
                UserFile.user_id == user_id,
                UserFile.file_name == file_name,
            )
        )
        await s.commit()
        return r.rowcount > 0


async def db_update_file_running(user_id: int, file_name: str,
                                  is_running: bool, pid: int = None):
    async with async_session() as s:
        await s.execute(
            update(UserFile)
            .where(UserFile.user_id == user_id, UserFile.file_name == file_name)
            .values(
                is_running=is_running,
                pid=pid,
                started_at=datetime.utcnow() if is_running else None,
            )
        )
        await s.commit()


async def db_incr_restart_count(user_id: int, file_name: str):
    async with async_session() as s:
        await s.execute(
            update(UserFile)
            .where(UserFile.user_id == user_id, UserFile.file_name == file_name)
            .values(restart_count=UserFile.restart_count + 1)
        )
        await s.commit()


async def db_mark_crash(user_id: int, file_name: str):
    async with async_session() as s:
        await s.execute(
            update(UserFile)
            .where(UserFile.user_id == user_id, UserFile.file_name == file_name)
            .values(last_crash_at=datetime.utcnow(), is_running=False, pid=None)
        )
        await s.commit()


# ============================================================
# 🗄️ DB HELPERS — ADMINS
# ============================================================

async def db_get_all_admins() -> list[int]:
    async with async_session() as s:
        r = await s.execute(select(AdminModel.user_id))
        return [row[0] for row in r.all()]


async def db_add_admin(user_id: int, added_by: int) -> bool:
    async with async_session() as s:
        if await s.get(AdminModel, user_id):
            return False
        s.add(AdminModel(user_id=user_id, added_by=added_by))
        await s.commit()
        return True


async def db_remove_admin(user_id: int) -> bool:
    async with async_session() as s:
        r = await s.execute(
            delete(AdminModel).where(AdminModel.user_id == user_id)
        )
        await s.commit()
        return r.rowcount > 0


# ============================================================
# 🗄️ DB HELPERS — MANDATORY CHANNELS
# ============================================================

async def db_get_channels() -> Sequence[MandatoryChannel]:
    async with async_session() as s:
        r = await s.execute(select(MandatoryChannel))
        return r.scalars().all()


async def db_add_channel(cid: str, username: str, name: str,
                          invite_link: str, added_by: int) -> bool:
    async with async_session() as s:
        existing = await s.get(MandatoryChannel, cid)
        if existing:
            existing.channel_username = username
            existing.channel_name = name
            existing.invite_link = invite_link
        else:
            s.add(MandatoryChannel(
                channel_id=cid, channel_username=username,
                channel_name=name, invite_link=invite_link,
                added_by=added_by,
            ))
        await s.commit()
        return True


async def db_remove_channel(cid: str) -> bool:
    async with async_session() as s:
        r = await s.execute(
            delete(MandatoryChannel).where(MandatoryChannel.channel_id == cid)
        )
        await s.commit()
        return r.rowcount > 0


# ============================================================
# 🗄️ DB HELPERS — APPROVALS
# ============================================================

async def db_save_approval(aid: str, user_id: int, user_name: str,
                            file_name: str, file_type: str, content: bytes):
    async with async_session() as s:
        s.add(PendingApproval(
            approval_id=aid, user_id=user_id, user_name=user_name,
            file_name=file_name, file_type=file_type,
            file_size=len(content), content=content,
        ))
        await s.commit()


async def db_get_approval(aid: str) -> Optional[PendingApproval]:
    async with async_session() as s:
        return await s.get(PendingApproval, aid)


async def db_update_approval(aid: str, status: str, reviewed_by: int) -> bool:
    async with async_session() as s:
        p = await s.get(PendingApproval, aid)
        if p:
            p.status = status
            p.reviewed_by = reviewed_by
            p.reviewed_at = datetime.utcnow()
            await s.commit()
            return True
    return False


async def db_cleanup_old_approvals(days: int = 7):
    async with async_session() as s:
        cutoff = datetime.utcnow() - timedelta(days=days)
        await s.execute(
            delete(PendingApproval).where(PendingApproval.created_at < cutoff)
        )
        await s.commit()


# ============================================================
# 🗄️ DB HELPERS — BOT STATS
# ============================================================

async def db_stats() -> BotStats:
    async with async_session() as s:
        st = await s.get(BotStats, 1)
        if not st:
            st = BotStats(id=1)
            s.add(st)
            await s.commit()
        return st


async def db_toggle_lock(reason: str = None) -> bool:
    async with async_session() as s:
        st = await s.get(BotStats, 1)
        if st:
            st.is_locked = not st.is_locked
            st.lock_reason = reason if st.is_locked else None
            await s.commit()
            return st.is_locked
    return False


async def db_incr_stat(field: str, amount: int = 1):
    async with async_session() as s:
        st = await s.get(BotStats, 1)
        if st and hasattr(st, field):
            setattr(st, field, getattr(st, field) + amount)
            await s.commit()


# ============================================================
# 🗄️ DB HELPERS — INSTALL LOGS
# ============================================================

async def db_log_install(user_id: int, module: str, package: str,
                          installer: str, status: str, log: str):
    async with async_session() as s:
        s.add(InstallLog(
            user_id=user_id, module_name=module, package_name=package,
            installer=installer, status=status, log=log[:5000],
        ))
        await s.commit()


async def db_get_recent_installs(limit: int = 20) -> Sequence[InstallLog]:
    async with async_session() as s:
        r = await s.execute(
            select(InstallLog)
            .order_by(InstallLog.installed_at.desc())
            .limit(limit)
        )
        return r.scalars().all()


# ============================================================
# 🗄️ DB HELPERS — AUDIT
# ============================================================

async def db_audit(admin_id: int, action: str,
                    target_id: int = None, details: str = None):
    async with async_session() as s:
        s.add(AuditLog(
            admin_id=admin_id, action=action,
            target_id=target_id, details=details,
        ))
        await s.commit()


async def db_get_recent_audits(limit: int = 30) -> Sequence[AuditLog]:
    async with async_session() as s:
        r = await s.execute(
            select(AuditLog)
            .order_by(AuditLog.created_at.desc())
            .limit(limit)
        )
        return r.scalars().all()


# ============================================================
# 🗄️ DB HELPERS — BROADCAST
# ============================================================

async def db_save_broadcast(admin_id: int, text: str,
                             media_type: str = None,
                             media_file_id: str = None,
                             sent: int = 0, failed: int = 0):
    async with async_session() as s:
        s.add(BroadcastHistory(
            admin_id=admin_id, text=text,
            media_type=media_type, media_file_id=media_file_id,
            sent_count=sent, failed_count=failed,
        ))
        await s.commit()


# ============================================================
# 🗄️ END PART 1
# ============================================================
# ============================================================
# 🎬 SCRIPT RUNNER — Async Python + Node.js
# ============================================================
# PART 2/5 : Script Runner + Auto Install + ZIP Processing
# ============================================================

# ------------------------------------------------------------
# In-memory running processes registry
# {script_key: {process, log_file, ...}}
# ------------------------------------------------------------
running_processes: dict[str, dict] = {}
_running_lock = asyncio.Lock()


# ------------------------------------------------------------
# Telegram module name → PyPI package name mapping
# ------------------------------------------------------------
TELEGRAM_MODULES = {
    # Frameworks
    'telebot': 'pyTelegramBotAPI',
    'telegram': 'python-telegram-bot',
    'python_telegram_bot': 'python-telegram-bot',
    'aiogram': 'aiogram',
    'pyrogram': 'pyrogram',
    'telethon': 'telethon',
    'telepot': 'telepot',
    'tgcrypto': 'TgCrypto',

    # Common non-telegram
    'bs4': 'beautifulsoup4',
    'requests': 'requests',
    'pillow': 'Pillow',
    'PIL': 'Pillow',
    'cv2': 'opencv-python',
    'yaml': 'PyYAML',
    'dotenv': 'python-dotenv',
    'dateutil': 'python-dateutil',
    'pandas': 'pandas',
    'numpy': 'numpy',
    'flask': 'Flask',
    'django': 'Django',
    'sqlalchemy': 'SQLAlchemy',
    'psutil': 'psutil',
    'pymongo': 'pymongo',
    'motor': 'motor',
    'redis': 'redis',
    'aiohttp': 'aiohttp',
    'httpx': 'httpx',
    'uvicorn': 'uvicorn',
    'fastapi': 'fastapi',
    'gtts': 'gTTS',
    'moviepy': 'moviepy',
    'pydub': 'pydub',
    'wikipedia': 'wikipedia',
    'googlesearch': 'googlesearch-python',
    'yt_dlp': 'yt-dlp',
    'youtube_dl': 'youtube-dl',
    'instaloader': 'instaloader',
    'twint': 'twint',
    'pytz': 'pytz',
    'tzdata': 'tzdata',
    'cryptography': 'cryptography',
    'jwt': 'PyJWT',
    'passlib': 'passlib',
    'bcrypt': 'bcrypt',
    'lxml': 'lxml',
    'selenium': 'selenium',
    'playwright': 'playwright',
    'openpyxl': 'openpyxl',
    'xlsxwriter': 'XlsxWriter',
    'docx': 'python-docx',
    'pptx': 'python-pptx',
    'qrcode': 'qrcode',
    'barcode': 'python-barcode',
    'pyzbar': 'pyzbar',
    'pdfplumber': 'pdfplumber',
    'PyPDF2': 'PyPDF2',
    'pypdf': 'pypdf',
    'reportlab': 'reportlab',
    'fpdf': 'fpdf2',
    'emoji': 'emoji',
    'textblob': 'textblob',
    'nltk': 'nltk',
    'sklearn': 'scikit-learn',
    'scipy': 'scipy',
    'matplotlib': 'matplotlib',
    'seaborn': 'seaborn',
    'plotly': 'plotly',
    'sympy': 'sympy',

    # Core — skip install
    'asyncio': None, 'json': None, 'datetime': None, 'os': None,
    'sys': None, 're': None, 'time': None, 'math': None,
    'random': None, 'logging': None, 'threading': None,
    'subprocess': None, 'zipfile': None, 'tempfile': None,
    'shutil': None, 'sqlite3': None, 'atexit': None, 'signal': None,
    'io': None, 'typing': None, 'pathlib': None, 'collections': None,
    'functools': None, 'itertools': None, 'base64': None, 'uuid': None,
    'hashlib': None, 'hmac': None, 'secrets': None, 'string': None,
    'struct': None, 'binascii': None, 'codecs': None, 'glob': None,
    'pickle': None, 'csv': None, 'configparser': None, 'argparse': None,
}


# ------------------------------------------------------------
# User folder helpers
# ------------------------------------------------------------

def get_user_folder(user_id: int) -> Path:
    """Get/create user's upload folder"""
    folder = settings.upload_path / str(user_id)
    folder.mkdir(exist_ok=True, parents=True)
    return folder


def get_script_key(user_id: int, file_name: str) -> str:
    return f"{user_id}_{file_name}"


def get_log_path(user_id: int, file_name: str) -> Path:
    folder = get_user_folder(user_id)
    stem = Path(file_name).stem
    return folder / f"{stem}.log"


# ------------------------------------------------------------
# Safe ENV (no TOKEN / secrets leak to user scripts)
# ------------------------------------------------------------

def build_safe_env(user_folder: Path) -> dict:
    """Build isolated env — main bot secrets NOT included"""
    base_path = os.environ.get("PATH", "/usr/local/bin:/usr/bin:/bin")
    return {
        'PATH': base_path,
        'HOME': str(user_folder),
        'LANG': 'en_US.UTF-8',
        'LC_ALL': 'en_US.UTF-8',
        'PYTHONUNBUFFERED': '1',
        'PYTHONDONTWRITEBYTECODE': '1',
        'PYTHONIOENCODING': 'utf-8',
        'TZ': 'UTC',
    }


# ------------------------------------------------------------
# Process helpers
# ------------------------------------------------------------

async def kill_process_tree(pid: int, timeout: float = 3.0):
    """Kill process and all children (async-safe via thread)"""
    def _kill():
        try:
            parent = psutil.Process(pid)
        except psutil.NoSuchProcess:
            return

        try:
            children = parent.children(recursive=True)
        except psutil.NoSuchProcess:
            children = []

        # Terminate children first
        for c in children:
            try:
                c.terminate()
            except psutil.NoSuchProcess:
                pass

        # Wait, then force kill
        gone, alive = psutil.wait_procs(children, timeout=timeout)
        for p in alive:
            try:
                p.kill()
            except psutil.NoSuchProcess:
                pass

        # Terminate parent
        try:
            parent.terminate()
            parent.wait(timeout=timeout)
        except psutil.TimeoutExpired:
            try:
                parent.kill()
            except psutil.NoSuchProcess:
                pass
        except psutil.NoSuchProcess:
            pass

    await asyncio.to_thread(_kill)


async def is_script_running(user_id: int, file_name: str) -> bool:
    """Check if script is currently running"""
    key = get_script_key(user_id, file_name)
    async with _running_lock:
        info = running_processes.get(key)
    if not info:
        return False
    return info['process'].returncode is None


def _get_running_info(user_id: int, file_name: str) -> Optional[dict]:
    """Non-async accessor (called from within lock)"""
    return running_processes.get(get_script_key(user_id, file_name))


async def _register_running(user_id: int, file_name: str, info: dict):
    key = get_script_key(user_id, file_name)
    async with _running_lock:
        running_processes[key] = info


async def _unregister_running(user_id: int, file_name: str) -> Optional[dict]:
    key = get_script_key(user_id, file_name)
    async with _running_lock:
        return running_processes.pop(key, None)


# ------------------------------------------------------------
# MODULE INSTALLATION
# ------------------------------------------------------------

async def install_pip_module(bot: Bot, module_name: str,
                              chat_id: int, user_id: int,
                              manual: bool = False) -> tuple[bool, str]:
    """Install a Python module via pip"""
    package_name = TELEGRAM_MODULES.get(module_name.lower(), module_name)

    # Skip stdlib
    if package_name is None:
        return True, f"'{module_name}' is stdlib — no install needed"

    # Notify
    try:
        if manual:
            await bot.send_message(
                chat_id,
                f"🔄 <b>Manual install</b>\n"
                f"📦 Module: <code>{module_name}</code>\n"
                f"📦 Package: <code>{package_name}</code>",
                parse_mode=ParseMode.HTML,
            )
        else:
            await bot.send_message(
                chat_id,
                f"🐍 <b>Auto-installing missing module</b>\n"
                f"📦 <code>{module_name}</code> → <code>{package_name}</code>",
                parse_mode=ParseMode.HTML,
            )
    except Exception:
        pass

    try:
        proc = await asyncio.create_subprocess_exec(
            sys.executable, '-m', 'pip', 'install',
            '--no-warn-script-location',
            '--disable-pip-version-check',
            package_name,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )

        try:
            stdout, _ = await asyncio.wait_for(
                proc.communicate(),
                timeout=settings.install_timeout,
            )
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            msg = f"⏱️ Install timeout ({settings.install_timeout}s): `{package_name}`"
            await db_log_install(user_id, module_name, package_name, "pip", "timeout", msg)
            try:
                await bot.send_message(chat_id, f"❌ {msg}", parse_mode=ParseMode.MARKDOWN)
            except Exception:
                pass
            return False, msg

        output = stdout.decode('utf-8', errors='ignore')

        if proc.returncode == 0:
            await db_log_install(user_id, module_name, package_name, "pip", "success", output)
            await db_incr_stat("total_installs")
            try:
                await bot.send_message(
                    chat_id,
                    f"✅ <b>Installed</b> <code>{package_name}</code>",
                    parse_mode=ParseMode.HTML,
                )
            except Exception:
                pass
            return True, output
        else:
            short = output[-1500:] if len(output) > 1500 else output
            await db_log_install(user_id, module_name, package_name, "pip", "failed", output)
            try:
                await bot.send_message(
                    chat_id,
                    f"❌ <b>Install failed</b> for <code>{package_name}</code>\n\n"
                    f"<pre>{short}</pre>",
                    parse_mode=ParseMode.HTML,
                )
            except Exception:
                pass
            return False, output

    except Exception as e:
        err = f"Install exception: {e}"
        logger.exception(f"pip install error: {e}")
        await db_log_install(user_id, module_name, package_name, "pip", "error", err)
        try:
            await bot.send_message(chat_id, f"❌ {err}")
        except Exception:
            pass
        return False, err


async def install_npm_module(bot: Bot, module_name: str,
                              user_folder: Path, chat_id: int,
                              user_id: int, manual: bool = False) -> tuple[bool, str]:
    """Install a Node.js module via npm (local to user folder)"""
    try:
        if manual:
            await bot.send_message(
                chat_id,
                f"🔄 <b>Installing Node module</b>\n📦 <code>{module_name}</code>",
                parse_mode=ParseMode.HTML,
            )
        else:
            await bot.send_message(
                chat_id,
                f"🟠 <b>Auto-installing</b> Node module <code>{module_name}</code>",
                parse_mode=ParseMode.HTML,
            )
    except Exception:
        pass

    try:
        proc = await asyncio.create_subprocess_exec(
            'npm', 'install', module_name,
            '--no-audit', '--no-fund', '--loglevel=error',
            cwd=str(user_folder),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )

        try:
            stdout, _ = await asyncio.wait_for(
                proc.communicate(),
                timeout=settings.install_timeout,
            )
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            msg = f"⏱️ npm install timeout ({settings.install_timeout}s)"
            await db_log_install(user_id, module_name, module_name, "npm", "timeout", msg)
            try:
                await bot.send_message(chat_id, f"❌ {msg}")
            except Exception:
                pass
            return False, msg

        output = stdout.decode('utf-8', errors='ignore')

        if proc.returncode == 0:
            await db_log_install(user_id, module_name, module_name, "npm", "success", output)
            await db_incr_stat("total_installs")
            try:
                await bot.send_message(
                    chat_id,
                    f"✅ <b>Node module installed</b> <code>{module_name}</code>",
                    parse_mode=ParseMode.HTML,
                )
            except Exception:
                pass
            return True, output
        else:
            short = output[-1500:] if len(output) > 1500 else output
            await db_log_install(user_id, module_name, module_name, "npm", "failed", output)
            try:
                await bot.send_message(
                    chat_id,
                    f"❌ <b>npm install failed</b> for <code>{module_name}</code>\n\n"
                    f"<pre>{short}</pre>",
                    parse_mode=ParseMode.HTML,
                )
            except Exception:
                pass
            return False, output

    except FileNotFoundError:
        err = "npm not found. Ensure Node.js is installed."
        await db_log_install(user_id, module_name, module_name, "npm", "error", err)
        try:
            await bot.send_message(chat_id, f"❌ {err}")
        except Exception:
            pass
        return False, err

    except Exception as e:
        err = f"npm exception: {e}"
        logger.exception(f"npm install error: {e}")
        await db_log_install(user_id, module_name, module_name, "npm", "error", err)
        try:
            await bot.send_message(chat_id, f"❌ {err}")
        except Exception:
            pass
        return False, err


# ------------------------------------------------------------
# Missing import detection
# ------------------------------------------------------------

async def _try_install_missing_python(bot: Bot, error_text: str,
                                        chat_id: int, user_id: int) -> Optional[str]:
    """Parse ModuleNotFoundError from stderr, install, return module name"""
    match = re.search(r"ModuleNotFoundError: No module named '([^']+)'", error_text)
    if not match:
        match = re.search(r"ImportError: No module named '([^']+)'", error_text)
    if not match:
        return None

    raw_module = match.group(1).strip().strip("'\"")
    # Take only the top-level module (a.b.c → a)
    top_module = raw_module.split(".")[0]

    logger.info(f"Missing Python module detected: {top_module} (from {raw_module})")
    success, _ = await install_pip_module(bot, top_module, chat_id, user_id)
    return top_module if success else None


async def _try_install_missing_node(bot: Bot, error_text: str,
                                      user_folder: Path,
                                      chat_id: int, user_id: int) -> Optional[str]:
    """Parse 'Cannot find module' from stderr, install, return module name"""
    match = re.search(r"Cannot find module '([^']+)'", error_text)
    if not match:
        match = re.search(r"Error: Cannot find module \"([^\"]+)\"", error_text)
    if not match:
        return None

    raw_module = match.group(1).strip()
    # Skip relative/core paths
    if raw_module.startswith('.') or raw_module.startswith('/'):
        return None
    # Top-level only (@scope/pkg → @scope/pkg ; a/b/c → a)
    if raw_module.startswith('@'):
        parts = raw_module.split('/')
        top_module = '/'.join(parts[:2]) if len(parts) >= 2 else raw_module
    else:
        top_module = raw_module.split('/')[0]

    logger.info(f"Missing Node module detected: {top_module}")
    success, _ = await install_npm_module(bot, top_module, user_folder, chat_id, user_id)
    return top_module if success else None


# ------------------------------------------------------------
# PRE-CHECK (short trial run — catches syntax/import errors)
# ------------------------------------------------------------

async def _precheck_python(bot: Bot, script_path: Path, user_folder: Path,
                             chat_id: int, user_id: int) -> tuple[bool, str, Optional[str]]:
    """
    Run script for up to N seconds.
    Returns (is_ok, error_output, missing_module_name)
    """
    log_file = None
    try:
        log_file = open(get_log_path(user_id, script_path.name), 'w',
                        encoding='utf-8', errors='ignore')
        log_file.write(f"=== PRE-CHECK @ {datetime.utcnow()} ===\n")
        log_file.flush()
    except Exception as e:
        logger.error(f"precheck log open failed: {e}")

    try:
        proc = await asyncio.create_subprocess_exec(
            sys.executable, '-u', str(script_path),
            cwd=str(user_folder),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            stdin=asyncio.subprocess.DEVNULL,
            env=build_safe_env(user_folder),
        )

        try:
            stdout, _ = await asyncio.wait_for(
                proc.communicate(),
                timeout=settings.precheck_timeout,
            )
            output = stdout.decode('utf-8', errors='ignore')

            if log_file and not log_file.closed:
                log_file.write(f"\n[exit code: {proc.returncode}]\n")
                log_file.write(output)
                log_file.flush()

            if proc.returncode == 0:
                return True, output, None

            # Look for missing module
            missing = await _try_install_missing_python(bot, output, chat_id, user_id)
            if missing:
                return False, output, missing

            # Not a module issue — real error
            return False, output, None

        except asyncio.TimeoutError:
            # Timed out = script is probably a long-running bot. Good.
            logger.info(f"precheck passed (timeout = long running bot): {script_path.name}")
            try:
                proc.kill()
                await proc.wait()
            except Exception:
                pass
            if log_file and not log_file.closed:
                log_file.write("\n[precheck timeout — assumed long-running bot]\n")
                log_file.flush()
            return True, "", None

    except Exception as e:
        logger.exception(f"precheck error: {e}")
        return False, f"Pre-check exception: {e}", None
    finally:
        if log_file and not log_file.closed:
            log_file.close()


async def _precheck_node(bot: Bot, script_path: Path, user_folder: Path,
                           chat_id: int, user_id: int) -> tuple[bool, str, Optional[str]]:
    """Same as precheck but for Node.js"""
    log_file = None
    try:
        log_file = open(get_log_path(user_id, script_path.name), 'w',
                        encoding='utf-8', errors='ignore')
        log_file.write(f"=== PRE-CHECK (node) @ {datetime.utcnow()} ===\n")
        log_file.flush()
    except Exception as e:
        logger.error(f"precheck log open failed: {e}")

    try:
        proc = await asyncio.create_subprocess_exec(
            'node', str(script_path),
            cwd=str(user_folder),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            stdin=asyncio.subprocess.DEVNULL,
            env=build_safe_env(user_folder),
        )

        try:
            stdout, _ = await asyncio.wait_for(
                proc.communicate(),
                timeout=settings.precheck_timeout,
            )
            output = stdout.decode('utf-8', errors='ignore')

            if log_file and not log_file.closed:
                log_file.write(f"\n[exit code: {proc.returncode}]\n")
                log_file.write(output)
                log_file.flush()

            if proc.returncode == 0:
                return True, output, None

            missing = await _try_install_missing_node(
                bot, output, user_folder, chat_id, user_id
            )
            if missing:
                return False, output, missing

            return False, output, None

        except asyncio.TimeoutError:
            logger.info(f"precheck passed (timeout = long running bot): {script_path.name}")
            try:
                proc.kill()
                await proc.wait()
            except Exception:
                pass
            if log_file and not log_file.closed:
                log_file.write("\n[precheck timeout — assumed long-running bot]\n")
                log_file.flush()
            return True, "", None

    except FileNotFoundError:
        return False, "Node.js not found. Ensure Node is installed.", None
    except Exception as e:
        logger.exception(f"precheck node error: {e}")
        return False, f"Pre-check exception: {e}", None
    finally:
        if log_file and not log_file.closed:
            log_file.close()


# ------------------------------------------------------------
# MAIN SCRIPT RUNNER (long-running)
# ------------------------------------------------------------

async def run_python_script(bot: Bot, user_id: int, file_name: str,
                              reply_chat_id: int,
                              attempt: int = 1, max_attempts: int = 2) -> bool:
    """Run a Python script with full logging + crash notify"""
    key = get_script_key(user_id, file_name)
    folder = get_user_folder(user_id)
    script_path = folder / file_name

    if not script_path.exists():
        await bot.send_message(reply_chat_id, f"❌ Script <code>{file_name}</code> not found!", parse_mode=ParseMode.HTML)
        return False

    # Already running?
    if await is_script_running(user_id, file_name):
        await bot.send_message(
            reply_chat_id,
            f"⚠️ <code>{file_name}</code> is already running.",
            parse_mode=ParseMode.HTML,
        )
        return False

    # Clean stale entry
    await _unregister_running(user_id, file_name)

    if attempt > max_attempts:
        await bot.send_message(
            reply_chat_id,
            f"❌ Failed to run <code>{file_name}</code> after {max_attempts} attempts.",
            parse_mode=ParseMode.HTML,
        )
        return False

    # --- PRE-CHECK (only first attempt) ---
    if attempt == 1:
        ok, err_output, missing = await _precheck_python(
            bot, script_path, folder, reply_chat_id, user_id
        )
        if missing:
            # Retry after installing module
            await bot.send_message(
                reply_chat_id,
                f"🔄 Retrying <code>{file_name}</code> after install...",
                parse_mode=ParseMode.HTML,
            )
            await asyncio.sleep(2)
            return await run_python_script(bot, user_id, file_name, reply_chat_id, attempt + 1, max_attempts)

        if not ok:
            # Real error — show user
            short = err_output[-1500:] if len(err_output) > 1500 else err_output
            await bot.send_message(
                reply_chat_id,
                f"❌ <b>Script error during pre-check</b>\n"
                f"📁 <code>{file_name}</code>\n\n"
                f"<pre>{short or '(no output)'}</pre>\n\n"
                f"💡 Fix the script and re-upload.",
                parse_mode=ParseMode.HTML,
            )
            return False

    # --- LONG RUN ---
    log_path = get_log_path(user_id, file_name)
    try:
        log_file = open(log_path, 'a', encoding='utf-8', errors='ignore')
        log_file.write(f"\n\n=== MAIN RUN @ {datetime.utcnow()} (attempt {attempt}) ===\n")
        log_file.flush()
    except Exception as e:
        logger.error(f"main log open failed: {e}")
        await bot.send_message(reply_chat_id, f"❌ Log file error: {e}")
        return False

    try:
        proc = await asyncio.create_subprocess_exec(
            sys.executable, '-u', str(script_path),
            cwd=str(folder),
            stdout=log_file,
            stderr=asyncio.subprocess.STDOUT,
            stdin=asyncio.subprocess.DEVNULL,
            env=build_safe_env(folder),
        )

        info = {
            'process': proc,
            'log_file': log_file,
            'file_name': file_name,
            'user_id': user_id,
            'folder': folder,
            'log_path': log_path,
            'start_time': datetime.utcnow(),
            'reply_chat_id': reply_chat_id,
            'type': 'py',
            'script_path': str(script_path),
        }
        await _register_running(user_id, file_name, info)
        await db_update_file_running(user_id, file_name, True, proc.pid)

        logger.success(f"✅ Started {key} PID={proc.pid}")

        await bot.send_message(
            reply_chat_id,
            f"✅ <b>Started</b> <code>{file_name}</code>\n"
            f"🆔 PID: <code>{proc.pid}</code>\n"
            f"📜 Logs will auto-send on crash.",
            parse_mode=ParseMode.HTML,
        )

        # Start background monitor
        asyncio.create_task(_monitor_process(bot, user_id, file_name))
        return True

    except FileNotFoundError as e:
        logger.error(f"Python interpreter missing: {e}")
        if log_file and not log_file.closed:
            log_file.close()
        await bot.send_message(reply_chat_id, f"❌ Python interpreter not found: {e}")
        return False
    except Exception as e:
        logger.exception(f"run_python_script error: {e}")
        if log_file and not log_file.closed:
            log_file.close()
        await _unregister_running(user_id, file_name)
        await bot.send_message(reply_chat_id, f"❌ Start error: {e}")
        return False


async def run_node_script(bot: Bot, user_id: int, file_name: str,
                            reply_chat_id: int,
                            attempt: int = 1, max_attempts: int = 2) -> bool:
    """Run a Node.js script with same lifecycle as Python"""
    key = get_script_key(user_id, file_name)
    folder = get_user_folder(user_id)
    script_path = folder / file_name

    if not script_path.exists():
        await bot.send_message(reply_chat_id, f"❌ Script <code>{file_name}</code> not found!", parse_mode=ParseMode.HTML)
        return False

    if await is_script_running(user_id, file_name):
        await bot.send_message(
            reply_chat_id,
            f"⚠️ <code>{file_name}</code> is already running.",
            parse_mode=ParseMode.HTML,
        )
        return False

    await _unregister_running(user_id, file_name)

    if attempt > max_attempts:
        await bot.send_message(
            reply_chat_id,
            f"❌ Failed to run <code>{file_name}</code> after {max_attempts} attempts.",
            parse_mode=ParseMode.HTML,
        )
        return False

    # Pre-check
    if attempt == 1:
        ok, err_output, missing = await _precheck_node(
            bot, script_path, folder, reply_chat_id, user_id
        )
        if missing:
            await bot.send_message(
                reply_chat_id,
                f"🔄 Retrying <code>{file_name}</code> after install...",
                parse_mode=ParseMode.HTML,
            )
            await asyncio.sleep(2)
            return await run_node_script(bot, user_id, file_name, reply_chat_id, attempt + 1, max_attempts)

        if not ok:
            short = err_output[-1500:] if len(err_output) > 1500 else err_output
            await bot.send_message(
                reply_chat_id,
                f"❌ <b>Node script error</b>\n"
                f"📁 <code>{file_name}</code>\n\n"
                f"<pre>{short or '(no output)'}</pre>",
                parse_mode=ParseMode.HTML,
            )
            return False

    # Long run
    log_path = get_log_path(user_id, file_name)
    try:
        log_file = open(log_path, 'a', encoding='utf-8', errors='ignore')
        log_file.write(f"\n\n=== MAIN RUN (node) @ {datetime.utcnow()} (attempt {attempt}) ===\n")
        log_file.flush()
    except Exception as e:
        logger.error(f"node log open failed: {e}")
        await bot.send_message(reply_chat_id, f"❌ Log file error: {e}")
        return False

    try:
        proc = await asyncio.create_subprocess_exec(
            'node', str(script_path),
            cwd=str(folder),
            stdout=log_file,
            stderr=asyncio.subprocess.STDOUT,
            stdin=asyncio.subprocess.DEVNULL,
            env=build_safe_env(folder),
        )

        info = {
            'process': proc,
            'log_file': log_file,
            'file_name': file_name,
            'user_id': user_id,
            'folder': folder,
            'log_path': log_path,
            'start_time': datetime.utcnow(),
            'reply_chat_id': reply_chat_id,
            'type': 'js',
            'script_path': str(script_path),
        }
        await _register_running(user_id, file_name, info)
        await db_update_file_running(user_id, file_name, True, proc.pid)

        logger.success(f"✅ Started {key} (node) PID={proc.pid}")

        await bot.send_message(
            reply_chat_id,
            f"✅ <b>Started (Node)</b> <code>{file_name}</code>\n"
            f"🆔 PID: <code>{proc.pid}</code>",
            parse_mode=ParseMode.HTML,
        )

        asyncio.create_task(_monitor_process(bot, user_id, file_name))
        return True

    except FileNotFoundError as e:
        logger.error(f"node not found: {e}")
        if log_file and not log_file.closed:
            log_file.close()
        await bot.send_message(reply_chat_id, "❌ Node.js not found. Ensure Node is installed.")
        return False
    except Exception as e:
        logger.exception(f"run_node_script error: {e}")
        if log_file and not log_file.closed:
            log_file.close()
        await _unregister_running(user_id, file_name)
        await bot.send_message(reply_chat_id, f"❌ Start error: {e}")
        return False


# ------------------------------------------------------------
# PROCESS MONITOR (crash detection + auto-notify)
# ------------------------------------------------------------

async def _monitor_process(bot: Bot, user_id: int, file_name: str):
    """
    Monitor a running process.
    - If exits in first 30s → crash, notify uploader with log
    - Else → assumed healthy
    """
    await asyncio.sleep(3)
    info = _get_running_info(user_id, file_name)
    if not info:
        return

    proc = info['process']

    # Check every 5 sec for 30 sec
    for _ in range(6):
        await asyncio.sleep(5)
        # Still registered?
        current = _get_running_info(user_id, file_name)
        if not current or current is not info:
            return
        if proc.returncode is not None:
            await _handle_crash(bot, user_id, file_name, proc.returncode)
            return

    logger.info(f"✅ {file_name} survived 30s — assumed healthy")


async def _handle_crash(bot: Bot, user_id: int, file_name: str, return_code: int):
    """Handle a crashed script: send logs to uploader"""
    info = await _unregister_running(user_id, file_name)
    if not info:
        return

    # Close log file
    lf = info.get('log_file')
    if lf and not lf.closed:
        try:
            lf.close()
        except Exception:
            pass

    await db_update_file_running(user_id, file_name, False, None)
    await db_mark_crash(user_id, file_name)
    await db_incr_stat("total_crashes")

    # Read log
    log_path = info.get('log_path')
    content = "(no log)"
    try:
        if log_path and Path(log_path).exists():
            with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read() or "(empty)"
            if len(content) > 2500:
                content = "... (truncated)\n" + content[-2500:]
    except Exception as e:
        content = f"(log read error: {e})"

    # Escape for HTML
    safe_content = (
        content
        .replace('&', '&amp;')
        .replace('<', '&lt;')
        .replace('>', '&gt;')
    )

    try:
        await bot.send_message(
            info['reply_chat_id'],
            f"❌ <b>Script crashed</b>\n"
            f"📁 <code>{file_name}</code>\n"
            f"🔢 Exit code: <code>{return_code}</code>\n\n"
            f"📜 <b>Log:</b>\n<pre>{safe_content}</pre>",
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        logger.error(f"Failed to send crash log: {e}")

    logger.warning(f"💥 {file_name} crashed (RC={return_code})")


# ------------------------------------------------------------
# STOP SCRIPT
# ------------------------------------------------------------

async def stop_script(bot: Bot, user_id: int, file_name: str,
                        notify_chat: int = None) -> bool:
    """Stop a running script gracefully"""
    info = await _unregister_running(user_id, file_name)
    if not info:
        return False

    proc = info['process']
    pid = proc.pid

    try:
        await kill_process_tree(pid)
    except Exception as e:
        logger.error(f"kill tree error: {e}")

    if info.get('log_file') and not info['log_file'].closed:
        try:
            info['log_file'].close()
        except Exception:
            pass

    await db_update_file_running(user_id, file_name, False, None)
    logger.info(f"🛑 Stopped {user_id}_{file_name}")

    if notify_chat:
        try:
            await bot.send_message(
                notify_chat,
                f"🛑 <b>Stopped</b> <code>{file_name}</code>",
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            pass

    return True


async def restart_script(bot: Bot, user_id: int, file_name: str,
                           reply_chat_id: int) -> bool:
    """Stop then start"""
    await stop_script(bot, user_id, file_name)
    await asyncio.sleep(1.5)

    folder = get_user_folder(user_id)
    script_path = folder / file_name
    if not script_path.exists():
        await bot.send_message(reply_chat_id, f"❌ File missing: {file_name}")
        return False

    await db_incr_restart_count(user_id, file_name)

    ext = script_path.suffix.lower()
    if ext == '.py':
        return await run_python_script(bot, user_id, file_name, reply_chat_id)
    elif ext == '.js':
        return await run_node_script(bot, user_id, file_name, reply_chat_id)
    else:
        await bot.send_message(reply_chat_id, f"❌ Unknown file type: {ext}")
        return False


# ------------------------------------------------------------
# ZIP PROCESSING
# ------------------------------------------------------------

async def process_zip(bot: Bot, user_id: int, zip_content: bytes,
                       zip_name: str, reply_chat_id: int,
                       reply_message: Message = None) -> bool:
    """
    Extract ZIP, install requirements.txt / package.json,
    find main script, save to user folder, and run.
    """
    folder = get_user_folder(user_id)
    tmp_dir = None

    try:
        tmp_dir = tempfile.mkdtemp(prefix=f"zip_{user_id}_")
        tmp_path = Path(tmp_dir)
        zip_file = tmp_path / zip_name
        zip_file.write_bytes(zip_content)

        # Safe extract
        with zipfile.ZipFile(zip_file) as z:
            for member in z.infolist():
                target = os.path.abspath(os.path.join(tmp_dir, member.filename))
                if not target.startswith(os.path.abspath(tmp_dir) + os.sep):
                    raise ValueError(f"Unsafe ZIP path: {member.filename}")
            z.extractall(tmp_dir)

        # List contents
        items = list(tmp_path.iterdir())

        # Detect if ZIP has a single top-level folder → unwrap
        if len(items) == 1 and items[0].is_dir():
            items = list(items[0].iterdir())

        # Collect names
        names = [p.name for p in items]
        py_files = [n for n in names if n.endswith('.py')]
        js_files = [n for n in names if n.endswith('.js')]

        # --- Install requirements.txt ---
        req_file = tmp_path / "requirements.txt"
        if not req_file.exists():
            # Check inside single top folder
            for p in tmp_path.iterdir():
                if p.is_dir() and (p / "requirements.txt").exists():
                    req_file = p / "requirements.txt"
                    break

        if req_file.exists():
            await bot.send_message(
                reply_chat_id,
                "🔄 <b>Found requirements.txt</b> — installing...",
                parse_mode=ParseMode.HTML,
            )
            try:
                proc = await asyncio.create_subprocess_exec(
                    sys.executable, '-m', 'pip', 'install',
                    '-r', str(req_file),
                    '--no-warn-script-location',
                    '--disable-pip-version-check',
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.STDOUT,
                )
                try:
                    stdout, _ = await asyncio.wait_for(
                        proc.communicate(), timeout=settings.install_timeout * 2
                    )
                except asyncio.TimeoutError:
                    proc.kill()
                    await proc.wait()
                    await bot.send_message(reply_chat_id, "⏱️ pip install timeout")
                else:
                    out = stdout.decode('utf-8', errors='ignore')
                    if proc.returncode == 0:
                        await bot.send_message(reply_chat_id, "✅ Python deps installed")
                        await db_incr_stat("total_installs")
                    else:
                        short = out[-1200:]
                        await bot.send_message(
                            reply_chat_id,
                            f"❌ pip install failed\n<pre>{short}</pre>",
                            parse_mode=ParseMode.HTML,
                        )
            except Exception as e:
                logger.error(f"pip req install: {e}")
                await bot.send_message(reply_chat_id, f"❌ pip error: {e}")

        # --- Install package.json ---
        pkg_file = None
        for candidate in [tmp_path / "package.json"]:
            if candidate.exists():
                pkg_file = candidate
                break
        if not pkg_file:
            for p in tmp_path.iterdir():
                if p.is_dir() and (p / "package.json").exists():
                    pkg_file = p / "package.json"
                    break

        if pkg_file:
            await bot.send_message(
                reply_chat_id,
                "🔄 <b>Found package.json</b> — npm install...",
                parse_mode=ParseMode.HTML,
            )
            try:
                proc = await asyncio.create_subprocess_exec(
                    'npm', 'install', '--no-audit', '--no-fund', '--loglevel=error',
                    cwd=str(pkg_file.parent),
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.STDOUT,
                )
                try:
                    stdout, _ = await asyncio.wait_for(
                        proc.communicate(), timeout=settings.install_timeout * 2
                    )
                except asyncio.TimeoutError:
                    proc.kill()
                    await proc.wait()
                    await bot.send_message(reply_chat_id, "⏱️ npm install timeout")
                else:
                    out = stdout.decode('utf-8', errors='ignore')
                    if proc.returncode == 0:
                        await bot.send_message(reply_chat_id, "✅ Node deps installed")
                        await db_incr_stat("total_installs")
                    else:
                        short = out[-1200:]
                        await bot.send_message(
                            reply_chat_id,
                            f"❌ npm install failed\n<pre>{short}</pre>",
                            parse_mode=ParseMode.HTML,
                        )
            except FileNotFoundError:
                await bot.send_message(reply_chat_id, "❌ npm not found")
            except Exception as e:
                logger.error(f"npm install: {e}")
                await bot.send_message(reply_chat_id, f"❌ npm error: {e}")

        # --- Determine source folder (unwrapped) ---
        source_dir = tmp_path
        if len(list(tmp_path.iterdir())) == 1:
            only = list(tmp_path.iterdir())[0]
            if only.is_dir():
                source_dir = only

        # --- Copy everything into user folder ---
        for item in source_dir.iterdir():
            dest = folder / item.name
            if dest.exists():
                if dest.is_dir():
                    shutil.rmtree(dest, ignore_errors=True)
                else:
                    try:
                        dest.unlink()
                    except Exception:
                        pass
            shutil.move(str(item), str(dest))

        # --- Find main script ---
        items = sorted([p.name for p in folder.iterdir() if p.is_file()])
        main = None
        ftype = None

        preferred_py = ['main.py', 'bot.py', 'app.py', 'index.py']
        preferred_js = ['index.js', 'main.js', 'bot.js', 'app.js']

        for p in preferred_py:
            if p in items:
                main, ftype = p, 'py'
                break
        if not main:
            for p in preferred_js:
                if p in items:
                    main, ftype = p, 'js'
                    break
        if not main:
            py_in = [f for f in items if f.endswith('.py')]
            js_in = [f for f in items if f.endswith('.js')]
            if py_in:
                main, ftype = py_in[0], 'py'
            elif js_in:
                main, ftype = js_in[0], 'js'

        if not main:
            await bot.send_message(
                reply_chat_id,
                "❌ <b>No .py or .js found in ZIP</b>",
                parse_mode=ParseMode.HTML,
            )
            return False

        # Save to DB
        main_path = folder / main
        size = main_path.stat().st_size if main_path.exists() else 0
        await db_add_user_file(user_id, main, ftype, size)

        await bot.send_message(
            reply_chat_id,
            f"📦 <b>ZIP extracted</b>\n"
            f"🎯 Main: <code>{main}</code> ({ftype})\n"
            f"🚀 Starting...",
            parse_mode=ParseMode.HTML,
        )

        # Run
        if ftype == 'py':
            return await run_python_script(bot, user_id, main, reply_chat_id)
        else:
            return await run_node_script(bot, user_id, main, reply_chat_id)

    except zipfile.BadZipFile:
        await bot.send_message(reply_chat_id, "❌ Invalid or corrupted ZIP.")
        return False
    except Exception as e:
        logger.exception(f"process_zip error: {e}")
        await bot.send_message(reply_chat_id, f"❌ ZIP error: {e}")
        return False
    finally:
        if tmp_dir and os.path.exists(tmp_dir):
            shutil.rmtree(tmp_dir, ignore_errors=True)


# ------------------------------------------------------------
# CLEANUP ALL (on shutdown)
# ------------------------------------------------------------

async def cleanup_all_processes():
    """Kill all running user scripts"""
    async with _running_lock:
        keys = list(running_processes.keys())

    if not keys:
        logger.info("No processes to cleanup")
        return

    logger.warning(f"🛑 Cleaning up {len(keys)} processes...")
    for key in keys:
        try:
            async with _running_lock:
                info = running_processes.get(key)
            if not info:
                continue
            await kill_process_tree(info['process'].pid)
            if info.get('log_file') and not info['log_file'].closed:
                info['log_file'].close()
        except Exception as e:
            logger.error(f"cleanup error {key}: {e}")

    async with _running_lock:
        running_processes.clear()

    logger.success("✅ All processes cleaned up")


# ------------------------------------------------------------
# AUTO CLEANUP (old files / approvals)
# ------------------------------------------------------------

async def periodic_cleanup_task():
    """Background task: cleanup old approvals + empty folders"""
    while True:
        try:
            await asyncio.sleep(3600)  # every hour

            # Cleanup old approvals (>7 days)
            await db_cleanup_old_approvals(7)

            # Cleanup empty user folders
            for entry in settings.upload_path.iterdir():
                if not entry.is_dir():
                    continue
                try:
                    if not any(entry.iterdir()):
                        entry.rmdir()
                        logger.info(f"Removed empty folder: {entry}")
                except Exception:
                    pass

            logger.debug("Periodic cleanup done")

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"periodic cleanup error: {e}")


# ============================================================
# 🗄️ END PART 2
# ============================================================
# ============================================================
# 🔐 APPROVAL SYSTEM + HELPERS + MIDDLEWARES
# ============================================================
# PART 3/5 : Approval backend + Helpers + Middlewares
# ============================================================

# ------------------------------------------------------------
# GLOBAL RUNTIME CACHES
# ------------------------------------------------------------
_admin_cache: set[int] = set(settings.admins)
_ban_cache: dict[int, tuple[float, bool]] = {}      # uid → (timestamp, is_banned)
_channel_cache: dict[int, tuple[float, list]] = {}  # uid → (timestamp, missing_list)
_rate_limit: dict[int, list[float]] = {}            # uid → [timestamps]

BAN_CACHE_TTL = 300
CHANNEL_CACHE_TTL = 120
_RATE_LOCK = asyncio.Lock()


# ============================================================
# 🔧 HELPERS — Role / Limits / User info
# ============================================================

def is_owner(user_id: int) -> bool:
    return user_id == settings.owner_id


def is_admin(user_id: int) -> bool:
    """Sync check — uses cache (DB-check done lazily via middleware)"""
    return user_id == settings.owner_id or user_id in _admin_cache


async def is_admin_async(user_id: int) -> bool:
    """Async DB-backed check"""
    if is_admin(user_id):
        return True
    if await db_get_all_admins() and user_id in (await db_get_all_admins()):
        _admin_cache.add(user_id)
        return True
    return False


async def refresh_admin_cache():
    """Reload admins from DB into memory"""
    _admin_cache.clear()
    _admin_cache.update(settings.admins)
    admins = await db_get_all_admins()
    _admin_cache.update(admins)
    logger.info(f"👑 Admin cache: {len(_admin_cache)} admins")


async def get_user_limit(user_id: int) -> int:
    """Determine upload limit for a user"""
    if user_id == settings.owner_id:
        return settings.owner_limit
    if is_admin(user_id):
        return settings.admin_limit

    u = await db_get_user(user_id)
    if u:
        if u.custom_limit is not None:
            return u.custom_limit
        if (u.is_subscribed and u.subscription_expiry
                and u.subscription_expiry > datetime.utcnow()):
            return settings.subscribed_user_limit

    return settings.free_user_limit


async def get_user_status_text(user_id: int) -> str:
    """Human-readable role"""
    if user_id == settings.owner_id:
        return "👑 Owner"
    if is_admin(user_id):
        return "🛡️ Admin"
    u = await db_get_user(user_id)
    if u and u.is_subscribed and u.subscription_expiry and u.subscription_expiry > datetime.utcnow():
        return "⭐ Premium"
    return "🆓 Free"


async def is_user_banned(user_id: int) -> bool:
    """Cached ban check"""
    now = time.time()
    cached = _ban_cache.get(user_id)
    if cached:
        ts, val = cached
        if now - ts < BAN_CACHE_TTL:
            return val

    u = await db_get_user(user_id)
    val = bool(u and u.is_banned)
    _ban_cache[user_id] = (now, val)
    return val


def invalidate_ban_cache(user_id: int):
    _ban_cache.pop(user_id, None)


async def check_channels_sub(bot: Bot, user_id: int,
                               force: bool = False) -> tuple[bool, list]:
    """
    Returns (is_subscribed, missing_channels).
    If no channels configured → always True.
    """
    channels = await db_get_channels()
    if not channels:
        return True, []

    if is_admin(user_id):
        return True, []

    now = time.time()
    if not force:
        cached = _channel_cache.get(user_id)
        if cached:
            ts, missing = cached
            ttl = 5 if missing else CHANNEL_CACHE_TTL
            if now - ts < ttl:
                return (len(missing) == 0), missing

    async def check_one(ch: MandatoryChannel):
        try:
            member = await asyncio.wait_for(
                bot.get_chat_member(chat_id=ch.channel_id, user_id=user_id),
                timeout=3.5,
            )
            if member.status in (
                ChatMemberStatus.LEFT,
                ChatMemberStatus.KICKED,
            ):
                return ch
            return None
        except Exception as e:
            logger.debug(f"channel check {ch.channel_id}: {e}")
            return ch

    results = await asyncio.gather(
        *[check_one(c) for c in channels],
        return_exceptions=True,
    )
    missing = [r for r in results if isinstance(r, MandatoryChannel)]

    _channel_cache[user_id] = (now, missing)
    return (len(missing) == 0), missing


def invalidate_channel_cache(user_id: int = None):
    if user_id:
        _channel_cache.pop(user_id, None)
    else:
        _channel_cache.clear()


# ============================================================
# ⏱️ RATE LIMITING
# ============================================================

async def rate_limit_ok(user_id: int, bucket: str, per_minute: int) -> bool:
    """
    Simple token window rate limit.
    bucket: 'upload' | 'command'
    """
    key = (user_id, bucket)
    now = time.time()
    async with _RATE_LOCK:
        stamps = _rate_limit.setdefault(key, [])
        # Keep only last 60 sec
        stamps[:] = [t for t in stamps if now - t < 60]
        if len(stamps) >= per_minute:
            return False
        stamps.append(now)
        return True


# ============================================================
# 🎯 APPROVAL SYSTEM — Backend
# ============================================================

def make_approval_id() -> str:
    return uuid.uuid4().hex[:12]


def make_preview_html(content: bytes, file_type: str,
                       file_name: str, max_lines: int = 25) -> str:
    """HTML-safe preview for owner notification"""
    try:
        if file_type in ('py', 'js'):
            text = content.decode('utf-8', errors='ignore')
            lines = text.split('\n')
            preview = '\n'.join(lines[:max_lines])
            if len(lines) > max_lines:
                preview += f"\n... ({len(lines) - max_lines} more lines)"
        elif file_type == 'zip':
            with tempfile.NamedTemporaryFile(delete=False, suffix='.zip') as tmp:
                tmp.write(content)
                tmp_path = tmp.name
            try:
                with zipfile.ZipFile(tmp_path) as z:
                    names = z.namelist()
                    preview = f"ZIP ({len(names)} files):\n"
                    for n in names[:15]:
                        try:
                            info = z.getinfo(n)
                            preview += f"  • {n} ({info.file_size}b)\n"
                        except Exception:
                            preview += f"  • {n}\n"
                    if len(names) > 15:
                        preview += f"  ... +{len(names) - 15} more"
            finally:
                try:
                    os.unlink(tmp_path)
                except Exception:
                    pass
        else:
            preview = "(binary content)"
    except Exception as e:
        preview = f"(preview error: {e})"

    # Truncate + escape
    if len(preview) > 2000:
        preview = preview[:2000] + "\n...(truncated)"

    return (
        preview
        .replace('&', '&amp;')
        .replace('<', '&lt;')
        .replace('>', '&gt;')
    )


async def create_approval_request(bot: Bot, user_id: int, user_name: str,
                                    file_name: str, file_type: str,
                                    content: bytes, reply_chat_id: int) -> Optional[str]:
    """
    Create approval entry + notify all admins.
    Returns approval_id on success, None on failure.
    """
    aid = make_approval_id()

    try:
        await db_save_approval(
            aid=aid, user_id=user_id, user_name=user_name,
            file_name=file_name, file_type=file_type,
            content=content,
        )
    except Exception as e:
        logger.exception(f"save approval error: {e}")
        try:
            await bot.send_message(reply_chat_id, f"❌ Failed to queue approval: {e}")
        except Exception:
            pass
        return None

    preview = make_preview_html(content, file_type, file_name)

    caption = (
        f"🔔 <b>NEW UPLOAD — APPROVAL REQUIRED</b>\n\n"
        f"🆔 <b>Approval ID:</b> <code>{aid}</code>\n"
        f"👤 <b>User:</b> {user_name}\n"
        f"🆔 <b>User ID:</b> <code>{user_id}</code>\n"
        f"📁 <b>File:</b> <code>{file_name}</code>\n"
        f"📦 <b>Type:</b> {file_type.upper()}\n"
        f"💾 <b>Size:</b> {len(content)} bytes\n"
        f"🕐 <b>Time:</b> {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC\n\n"
        f"📄 <b>Preview:</b>\n<pre>{preview}</pre>"
    )

    # Build keyboard (styled)
    kb = InlineKeyboardBuilder()
    kb.button(text="🟢 Approve & Run", callback_data=f"appr:run:{aid}")
    kb.button(text="🔴 Reject", callback_data=f"appr:rej:{aid}")
    kb.button(text="📥 Download File", callback_data=f"appr:dl:{aid}")
    kb.button(text="👤 User Info", callback_data=f"appr:info:{aid}")
    kb.adjust(2, 2)

    # Notify owner + all admins
    recipients = set(_admin_cache) | {settings.owner_id}
    sent = 0

    for admin_id in recipients:
        try:
            if file_type in ('py', 'js'):
                bio = BufferedInputFile(content, filename=file_name)
                await bot.send_document(
                    chat_id=admin_id,
                    document=bio,
                    caption=caption,
                    reply_markup=kb.as_markup(),
                    parse_mode=ParseMode.HTML,
                )
            else:
                await bot.send_message(
                    chat_id=admin_id,
                    text=caption,
                    reply_markup=kb.as_markup(),
                    parse_mode=ParseMode.HTML,
                )
            sent += 1
        except TelegramForbiddenError:
            logger.debug(f"admin {admin_id} blocked bot")
        except Exception as e:
            logger.error(f"approval notify → {admin_id}: {e}")

    if sent == 0:
        try:
            await bot.send_message(
                reply_chat_id,
                "❌ No admin reachable. Try again later.",
            )
        except Exception:
            pass
        return None

    await db_incr_stat("total_uploads")
    await db_incr_user_stat(user_id, "total_uploads", 1)

    # Notify uploader
    try:
        await bot.send_message(
            reply_chat_id,
            f"⏳ <b>Submitted for approval</b>\n\n"
            f"🆔 <code>{aid}</code>\n"
            f"📁 <code>{file_name}</code>\n\n"
            f"Owner will review and you'll be notified.",
            parse_mode=ParseMode.HTML,
        )
    except Exception:
        pass

    logger.info(f"📥 Approval {aid} created (user={user_id}, file={file_name})")
    return aid


async def _edit_approval_msg(call: CallbackQuery, approval: PendingApproval,
                               status: str, note: str):
    """Update admin's approval message after action"""
    new_text = (
        f"{note}\n\n"
        f"🆔 <code>{approval.approval_id}</code>\n"
        f"👤 <code>{approval.user_id}</code>\n"
        f"📁 <code>{approval.file_name}</code>\n"
        f"🕐 {datetime.utcnow().strftime('%H:%M:%S')} UTC"
    )
    try:
        if call.message.caption:
            await call.message.edit_caption(
                caption=new_text,
                reply_markup=None,
                parse_mode=ParseMode.HTML,
            )
        else:
            await call.message.edit_text(
                text=new_text,
                reply_markup=None,
                parse_mode=ParseMode.HTML,
            )
    except TelegramBadRequest:
        pass
    except Exception as e:
        logger.debug(f"_edit_approval_msg: {e}")


# ------------------------------------------------------------
# APPROVAL CALLBACK HANDLERS (registered later in Part 5)
# ------------------------------------------------------------

async def handle_approval_run(bot: Bot, call: CallbackQuery, aid: str):
    """Admin approved → save + run"""
    if not is_admin(call.from_user.id):
        await call.answer("⚠️ Admin only.", show_alert=True)
        return

    approval = await db_get_approval(aid)
    if not approval:
        await call.answer("❌ Approval not found.", show_alert=True)
        return
    if approval.status != "pending":
        await call.answer(f"⚠️ Already {approval.status}.", show_alert=True)
        return

    user_id = approval.user_id
    file_name = approval.file_name
    file_type = approval.file_type
    content = approval.content

    # Re-check file limit
    limit = await get_user_limit(user_id)
    count = await db_get_user_file_count(user_id)
    if count >= limit:
        await db_update_approval(aid, "rejected", call.from_user.id)
        await db_incr_stat("total_rejected")
        await db_incr_user_stat(user_id, "total_rejected", 1)
        try:
            await bot.send_message(
                user_id,
                f"⚠️ File limit reached. <code>{file_name}</code> not added.",
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            pass
        await _edit_approval_msg(call, approval, "rejected", "⚠️ User hit limit")
        await call.answer("⚠️ User hit limit.", show_alert=True)
        return

    # Save & run
    folder = get_user_folder(user_id)
    path = folder / file_name

    try:
        if file_type == 'zip':
            # Process as ZIP
            asyncio.create_task(
                process_zip(bot, user_id, content, file_name, call.message.chat.id)
            )
        else:
            path.write_bytes(content)
            size = len(content)
            await db_add_user_file(user_id, file_name, file_type, size)

            # Notify user
            try:
                await bot.send_message(
                    user_id,
                    f"✅ <b>Approved!</b> Running <code>{file_name}</code>...",
                    parse_mode=ParseMode.HTML,
                )
            except Exception:
                pass

            # Run
            if file_type == 'py':
                asyncio.create_task(
                    run_python_script(bot, user_id, file_name, call.message.chat.id)
                )
            elif file_type == 'js':
                asyncio.create_task(
                    run_node_script(bot, user_id, file_name, call.message.chat.id)
                )

        await db_update_approval(aid, "approved", call.from_user.id)
        await db_incr_stat("total_approved")
        await db_incr_user_stat(user_id, "total_approved", 1)
        await db_audit(call.from_user.id, "approve_file", user_id, file_name)

        await _edit_approval_msg(
            call, approval, "approved",
            f"✅ Approved by {call.from_user.first_name or call.from_user.id}",
        )
        await call.answer("✅ Approved & running!", show_alert=False)

    except Exception as e:
        logger.exception(f"approval run error: {e}")
        await call.answer(f"❌ Error: {e}", show_alert=True)


async def handle_approval_reject(bot: Bot, call: CallbackQuery, aid: str):
    """Admin rejected → notify user"""
    if not is_admin(call.from_user.id):
        await call.answer("⚠️ Admin only.", show_alert=True)
        return

    approval = await db_get_approval(aid)
    if not approval:
        await call.answer("❌ Approval not found.", show_alert=True)
        return
    if approval.status != "pending":
        await call.answer(f"⚠️ Already {approval.status}.", show_alert=True)
        return

    await db_update_approval(aid, "rejected", call.from_user.id)
    await db_incr_stat("total_rejected")
    await db_incr_user_stat(approval.user_id, "total_rejected", 1)
    await db_audit(call.from_user.id, "reject_file", approval.user_id, approval.file_name)

    try:
        await bot.send_message(
            approval.user_id,
            f"🔴 <b>Rejected</b>\n\n"
            f"📁 <code>{approval.file_name}</code>\n"
            f"Contact owner for details.",
            parse_mode=ParseMode.HTML,
        )
    except Exception:
        pass

    await _edit_approval_msg(
        call, approval, "rejected",
        f"🔴 Rejected by {call.from_user.first_name or call.from_user.id}",
    )
    await call.answer("🔴 Rejected")


async def handle_approval_download(bot: Bot, call: CallbackQuery, aid: str):
    """Send file to admin"""
    if not is_admin(call.from_user.id):
        await call.answer("⚠️ Admin only.", show_alert=True)
        return

    approval = await db_get_approval(aid)
    if not approval:
        await call.answer("❌ Not found.", show_alert=True)
        return

    try:
        bio = BufferedInputFile(approval.content, filename=approval.file_name)
        await bot.send_document(
            call.from_user.id,
            bio,
            caption=f"📥 <code>{approval.file_name}</code>\nFrom: <code>{approval.user_id}</code>",
            parse_mode=ParseMode.HTML,
        )
        await call.answer("📥 Sent!")
    except Exception as e:
        await call.answer(f"❌ Error: {e}", show_alert=True)


async def handle_approval_info(bot: Bot, call: CallbackQuery, aid: str):
    """Show uploader info"""
    if not is_admin(call.from_user.id):
        await call.answer("⚠️ Admin only.", show_alert=True)
        return

    approval = await db_get_approval(aid)
    if not approval:
        await call.answer("❌ Not found.", show_alert=True)
        return

    u = await db_get_user(approval.user_id)
    count = await db_get_user_file_count(approval.user_id)
    limit = await get_user_limit(approval.user_id)

    text = (
        f"👤 <b>Uploader Info</b>\n\n"
        f"🆔 <code>{approval.user_id}</code>\n"
        f"👤 {approval.user_name}\n"
        f"📛 @{u.username if u and u.username else 'N/A'}\n"
        f"📁 Files: {count}/{limit}\n"
        f"🚫 Banned: {'Yes' if u and u.is_banned else 'No'}\n"
        f"⭐ Subscribed: {'Yes' if u and u.is_subscribed else 'No'}\n"
        f"📅 Joined: {u.joined_at.strftime('%Y-%m-%d') if u and u.joined_at else 'N/A'}"
    )
    try:
        await call.message.answer(text, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"approval_info: {e}")
    await call.answer()


# ============================================================
# 📢 BROADCAST ENGINE
# ============================================================

_broadcast_lock = asyncio.Lock()


async def execute_broadcast(bot: Bot, admin_id: int,
                              source_chat_id: int, source_message_id: int,
                              fallback_text: str = None,
                              status_message: Message = None,
                              delay: float = 0.05) -> tuple[int, int]:
    """
    Broadcast via copy_message (preserves formatting + premium emoji).
    Returns (sent, failed).
    """
    if _broadcast_lock.locked():
        logger.warning("Broadcast already running")
        return 0, 0

    async with _broadcast_lock:
        users = await db_get_all_user_ids()
        total = len(users)
        sent = 0
        failed = 0

        if not users:
            return 0, 0

        for i, uid in enumerate(users):
            try:
                if source_message_id and source_chat_id:
                    await bot.copy_message(
                        chat_id=uid,
                        from_chat_id=source_chat_id,
                        message_id=source_message_id,
                    )
                elif fallback_text:
                    await bot.send_message(
                        uid,
                        fallback_text,
                        parse_mode=ParseMode.HTML,
                    )
                sent += 1
            except TelegramRetryAfter as e:
                await asyncio.sleep(e.retry_after + 1)
                try:
                    if source_message_id and source_chat_id:
                        await bot.copy_message(
                            chat_id=uid,
                            from_chat_id=source_chat_id,
                            message_id=source_message_id,
                        )
                    elif fallback_text:
                        await bot.send_message(uid, fallback_text, parse_mode=ParseMode.HTML)
                    sent += 1
                except Exception:
                    failed += 1
            except TelegramForbiddenError:
                failed += 1
            except Exception as e:
                logger.debug(f"broadcast → {uid}: {e}")
                failed += 1

            await asyncio.sleep(delay)

            if status_message and (i + 1) % 15 == 0:
                try:
                    await status_message.edit_text(
                        f"📢 <b>Broadcasting...</b>\n\n"
                        f"✅ Sent: {sent}\n"
                        f"❌ Failed: {failed}\n"
                        f"📊 Progress: {i + 1}/{total}",
                        parse_mode=ParseMode.HTML,
                    )
                except Exception:
                    pass

        await db_save_broadcast(
            admin_id=admin_id, text=fallback_text or "(media)",
            sent=sent, failed=failed,
        )

        if status_message:
            try:
                await status_message.edit_text(
                    f"✅ <b>Broadcast complete</b>\n\n"
                    f"👥 Total: {total}\n"
                    f"✅ Sent: {sent}\n"
                    f"❌ Failed: {failed}",
                    parse_mode=ParseMode.HTML,
                )
            except Exception:
                pass

        logger.info(f"📢 Broadcast done: {sent}/{total} (failed={failed})")
        return sent, failed


# ============================================================
# 🛡️ MIDDLEWARES
# ============================================================

async def ban_check_message_mw(handler, event: Message, data: dict):
    """Message middleware — block banned users"""
    if not event.from_user:
        return await handler(event, data)

    uid = event.from_user.id
    # Owner/admin bypass
    if is_owner(uid) or is_admin(uid):
        return await handler(event, data)

    if await is_user_banned(uid):
        try:
            await event.answer("🚫 You are banned from using this bot.")
        except Exception:
            pass
        return

    return await handler(event, data)


async def ban_check_callback_mw(handler, event: CallbackQuery, data: dict):
    """Callback middleware — block banned users"""
    if not event.from_user:
        return await handler(event, data)

    uid = event.from_user.id
    if is_owner(uid) or is_admin(uid):
        return await handler(event, data)

    if await is_user_banned(uid):
        try:
            await event.answer("🚫 You are banned.", show_alert=True)
        except Exception:
            pass
        return

    return await handler(event, data)


# ============================================================
# 🧩 UTILITY HELPERS
# ============================================================

def fmt_bytes(n: int) -> str:
    for unit in ['B', 'KB', 'MB', 'GB']:
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def truncate(text: str, max_len: int = 100) -> str:
    if len(text) <= max_len:
        return text
    return text[:max_len - 3] + "..."


def html_escape(text: Any) -> str:
    return (
        str(text)
        .replace('&', '&amp;')
        .replace('<', '&lt;')
        .replace('>', '&gt;')
    )


def user_mention(user_id: int, name: str) -> str:
    return f'<a href="tg://user?id={user_id}">{html_escape(name)}</a>'


async def safe_send(bot: Bot, chat_id: int, text: str, **kwargs) -> Optional[Message]:
    """Send message, swallow errors"""
    try:
        return await bot.send_message(chat_id, text, **kwargs)
    except TelegramRetryAfter as e:
        await asyncio.sleep(e.retry_after + 1)
        try:
            return await bot.send_message(chat_id, text, **kwargs)
        except Exception as e2:
            logger.debug(f"safe_send retry failed: {e2}")
    except TelegramForbiddenError:
        logger.debug(f"bot blocked by {chat_id}")
    except Exception as e:
        logger.debug(f"safe_send: {e}")
    return None


async def safe_edit(message: Message, text: str, **kwargs) -> bool:
    """Edit message, swallow 'not modified' errors"""
    try:
        await message.edit_text(text, **kwargs)
        return True
    except TelegramBadRequest as e:
        if "not modified" in str(e).lower():
            return False
        logger.debug(f"safe_edit: {e}")
    except Exception as e:
        logger.debug(f"safe_edit: {e}")
    return False


# ============================================================
# 🗄️ END PART 3
# ============================================================
# ============================================================
# 🎨 KEYBOARDS (STYLED) + FSM + BOT SETUP + USER HANDLERS
# ============================================================
# PART 4/5 : Keyboards + FSM + Bot instance + User handlers
# ============================================================

# ------------------------------------------------------------
# 🤖 BOT + DISPATCHER + ROUTER
# ------------------------------------------------------------

session = AiohttpSession(timeout=60)

bot = Bot(
    token=settings.bot_token,
    session=session,
    default=DefaultBotProperties(
        parse_mode=ParseMode.HTML,
        link_preview_is_disabled=True,
    ),
)

dp = Dispatcher(storage=MemoryStorage())
router = Router()
dp.include_router(router)


# ------------------------------------------------------------
# Middleware registration
# ------------------------------------------------------------
router.message.middleware(ban_check_message_mw)
router.callback_query.middleware(ban_check_callback_mw)


# ============================================================
# 🎨 STYLE SYSTEM — Buttons with colors/emojis
# ============================================================
# Aiogram me direct colors nahi hote, lekin emoji + consistent
# ordering se professional look dete hain.
# Success → 🟢   Danger → 🔴   Primary → 🔵   Neutral → ⚪
# ============================================================

# Central place for reusable button text + callback patterns
class Btn:
    # Primary actions
    UPLOAD          = "🔵 Upload File"
    CHECK_FILES     = "🔵 My Files"
    INSTALL         = "🔵 Install Module"
    SPEED           = "🔵 Speed Test"
    MY_INFO         = "🔵 My Info"
    UPDATES         = "🔵 Updates Channel"
    CONTACT         = "🔵 Contact Owner"
    HELP            = "🔵 Help"

    # Success (green)
    APPROVE         = "🟢 Approve"
    START           = "🟢 Start"
    CONFIRM         = "🟢 Confirm"
    VERIFY          = "🟢 Verify"
    YES             = "🟢 Yes"

    # Danger (red)
    REJECT          = "🔴 Reject"
    STOP            = "🔴 Stop"
    DELETE          = "🔴 Delete"
    CANCEL          = "🔴 Cancel"
    BAN             = "🔴 Ban User"
    UNBAN           = "🟢 Unban User"

    # Neutral
    BACK            = "⚪ Back"
    REFRESH         = "🔄 Refresh"
    LOGS            = "📜 Logs"
    RESTART         = "🔄 Restart"
    DOWNLOAD        = "📥 Download"
    USER_INFO       = "👤 User Info"

    # Admin panel
    ADMIN_PANEL     = "🛡️ Admin Panel"
    USER_MGMT       = "👥 Users"
    CH_MGMT         = "📢 Channels"
    SUB_MGMT        = "💳 Subscriptions"
    BROADCAST       = "📣 Broadcast"
    STATS           = "📊 Statistics"
    LOCK            = "🔒 Lock Bot"
    UNLOCK          = "🔓 Unlock Bot"
    RUN_ALL         = "🚀 Run All Scripts"
    SETTINGS        = "⚙️ Settings"
    INSTALL_LOGS    = "📋 Install Logs"
    AUDIT_LOGS      = "📜 Audit Logs"


# ============================================================
# 🎨 KEYBOARD BUILDERS
# ============================================================

def kb_main_inline(user_id: int, is_admin_user: bool,
                    locked: bool = False) -> InlineKeyboardMarkup:
    """Main inline menu (styled)"""
    b = InlineKeyboardBuilder()

    # User row 1 — primary
    b.button(text=Btn.UPDATES,
             url=f"https://t.me/{settings.update_channel.lstrip('@')}")
    # User row 2 — core actions
    b.button(text=Btn.UPLOAD,      callback_data="upload")
    b.button(text=Btn.CHECK_FILES, callback_data="check_files")
    # User row 3
    b.button(text=Btn.SPEED,       callback_data="speed")
    b.button(text=Btn.MY_INFO,     callback_data="my_info")
    # User row 4
    b.button(text=Btn.INSTALL,     callback_data="manual_install")
    b.button(text=Btn.HELP,        callback_data="help")
    # User row 5 — contact
    b.button(text=Btn.CONTACT,
             url=f"https://t.me/{settings.your_username.lstrip('@')}")

    if is_admin_user:
        # Admin extra rows
        b.button(text=Btn.ADMIN_PANEL, callback_data="admin:panel")
        b.button(text=Btn.STATS,       callback_data="admin:stats")
        b.button(text=Btn.BROADCAST,   callback_data="admin:broadcast_init")
        b.button(text=Btn.RUN_ALL,     callback_data="admin:run_all")
        b.button(text=Btn.USER_MGMT,   callback_data="admin:users")
        b.button(text=Btn.CH_MGMT,     callback_data="admin:channels")
        b.button(text=Btn.SUB_MGMT,    callback_data="admin:subs")
        lock_btn = Btn.UNLOCK if locked else Btn.LOCK
        b.button(text=lock_btn,        callback_data="admin:toggle_lock")
        b.button(text=Btn.SETTINGS,    callback_data="admin:settings")
        b.adjust(1, 2, 2, 2, 1, 2, 2, 2, 2)
    else:
        b.adjust(1, 2, 2, 2, 1)

    return b.as_markup()


def kb_main_reply(is_admin_user: bool) -> ReplyKeyboardMarkup:
    """Reply keyboard (persistent bottom)"""
    b = ReplyKeyboardBuilder()
    if is_admin_user:
        b.button(text="🛡️ Admin Panel")
        b.button(text="📊 Statistics")
        b.button(text="📤 Upload File")
        b.button(text="📂 My Files")
        b.button(text="⚡ Speed Test")
        b.button(text="🆘 Help")
        b.adjust(2, 2, 2)
    else:
        b.button(text="📤 Upload File")
        b.button(text="📂 My Files")
        b.button(text="⚡ Speed Test")
        b.button(text="🆘 Help")
        b.adjust(2, 2)
    return b.as_markup(resize_keyboard=True)


def kb_cancel_only() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text=Btn.CANCEL, callback_data="cancel_fsm")
    b.adjust(1)
    return b.as_markup()


def kb_file_controls(user_id: int, file_name: str,
                      running: bool) -> InlineKeyboardMarkup:
    """Controls for a single uploaded file"""
    b = InlineKeyboardBuilder()

    if running:
        b.button(text=Btn.STOP,    callback_data=f"file:stop:{user_id}:{file_name}")
        b.button(text=Btn.RESTART, callback_data=f"file:restart:{user_id}:{file_name}")
    else:
        b.button(text=Btn.START,   callback_data=f"file:start:{user_id}:{file_name}")

    b.button(text=Btn.LOGS,    callback_data=f"file:logs:{user_id}:{file_name}")
    b.button(text=Btn.DELETE,  callback_data=f"file:delete:{user_id}:{file_name}")
    b.button(text=Btn.REFRESH, callback_data=f"file:info:{user_id}:{file_name}")
    b.button(text=Btn.BACK,    callback_data="check_files")
    b.adjust(2, 2, 1, 1)
    return b.as_markup()


def kb_approval(aid: str) -> InlineKeyboardMarkup:
    """Approval controls for admin"""
    b = InlineKeyboardBuilder()
    b.button(text="🟢 Approve & Run",   callback_data=f"appr:run:{aid}")
    b.button(text="🔴 Reject",          callback_data=f"appr:rej:{aid}")
    b.button(text="📥 Download File",   callback_data=f"appr:dl:{aid}")
    b.button(text="👤 User Info",       callback_data=f"appr:info:{aid}")
    b.adjust(2, 2)
    return b.as_markup()


def kb_channels_check(missing: list) -> InlineKeyboardMarkup:
    """Force-join keyboard"""
    b = InlineKeyboardBuilder()
    for ch in missing:
        if ch.invite_link:
            url = ch.invite_link
        elif ch.channel_username:
            url = f"https://t.me/{ch.channel_username.lstrip('@')}"
        else:
            url = f"https://t.me/c/{str(ch.channel_id).replace('-100', '')}"
        b.button(text=f"📢 {ch.channel_name}", url=url)
    b.button(text=Btn.VERIFY, callback_data="verify_channels")
    b.adjust(1)
    return b.as_markup()


def kb_admin_panel() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="➕ Add Admin",       callback_data="admin:add_admin")
    b.button(text="🔴 Remove Admin",    callback_data="admin:remove_admin")
    b.button(text="📋 List Admins",     callback_data="admin:list_admins")
    b.button(text=Btn.STATS,            callback_data="admin:stats")
    b.button(text=Btn.INSTALL_LOGS,     callback_data="admin:install_logs")
    b.button(text=Btn.AUDIT_LOGS,       callback_data="admin:audit_logs")
    b.button(text=Btn.SETTINGS,         callback_data="admin:settings")
    b.button(text=Btn.BACK,             callback_data="back_main")
    b.adjust(2, 1, 2, 2, 1)
    return b.as_markup()


def kb_user_management() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text=Btn.BAN,              callback_data="admin:ban")
    b.button(text=Btn.UNBAN,            callback_data="admin:unban")
    b.button(text="🔧 Set Limit",       callback_data="admin:set_limit")
    b.button(text="🗑️ Remove Limit",    callback_data="admin:remove_limit")
    b.button(text="ℹ️ User Info",       callback_data="admin:user_info")
    b.button(text=Btn.BACK,             callback_data="admin:panel")
    b.adjust(2, 2, 1, 1)
    return b.as_markup()


def kb_channels_management() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="➕ Add Channel",     callback_data="admin:channel_add")
    b.button(text="🔴 Remove Channel",  callback_data="admin:channel_remove")
    b.button(text="📋 List Channels",   callback_data="admin:channel_list")
    b.button(text=Btn.BACK,             callback_data="admin:panel")
    b.adjust(2, 1, 1)
    return b.as_markup()


def kb_subscription_management() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="➕ Add Sub",         callback_data="admin:sub_add")
    b.button(text="🔴 Remove Sub",      callback_data="admin:sub_remove")
    b.button(text="🔍 Check Sub",       callback_data="admin:sub_check")
    b.button(text=Btn.BACK,             callback_data="admin:panel")
    b.adjust(2, 1, 1)
    return b.as_markup()


def kb_admin_settings() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text=Btn.INSTALL_LOGS,     callback_data="admin:install_logs")
    b.button(text=Btn.AUDIT_LOGS,       callback_data="admin:audit_logs")
    b.button(text="🧹 Cleanup",         callback_data="admin:cleanup")
    b.button(text=Btn.BACK,             callback_data="admin:panel")
    b.adjust(2, 2)
    return b.as_markup()


def kb_confirm(action: str, yes_cb: str, no_cb: str = "cancel_fsm") -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text=Btn.YES,    callback_data=yes_cb)
    b.button(text=Btn.CANCEL, callback_data=no_cb)
    b.adjust(2)
    return b.as_markup()


def kb_back(cb: str = "back_main") -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text=Btn.BACK, callback_data=cb)
    b.adjust(1)
    return b.as_markup()


# ============================================================
# 🧩 FSM STATES
# ============================================================

class UploadStates(StatesGroup):
    waiting_file = State()


class InstallStates(StatesGroup):
    waiting_module = State()


class AdminStates(StatesGroup):
    broadcast_wait = State()
    add_admin_id = State()
    remove_admin_id = State()
    ban_user = State()
    unban_user = State()
    set_limit = State()
    remove_limit = State()
    user_info = State()
    channel_add = State()
    channel_remove = State()
    sub_add = State()
    sub_remove = State()
    sub_check = State()
    admin_install_for_user = State()


# ============================================================
# 🧠 USER STATE CACHE (FSM-independent quick data)
# ============================================================

_user_quick: dict[int, dict] = {}


def quick_set(uid: int, key: str, val: Any):
    _user_quick.setdefault(uid, {})[key] = val


def quick_get(uid: int, key: str, default: Any = None) -> Any:
    return _user_quick.get(uid, {}).get(key, default)


def quick_clear(uid: int):
    _user_quick.pop(uid, None)


# ============================================================
# 🏠 START + HELP
# ============================================================

@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    quick_clear(message.from_user.id)

    u = message.from_user

    # Register / update user
    await db_upsert_user(
        u.id,
        username=u.username,
        first_name=u.first_name,
        last_name=u.last_name,
        language_code=u.language_code,
    )

    # Admin panel shortcut
    if is_admin(u.id):
        # allow both start and admin panel to be accessible
        pass

    # Channel check (skip admins)
    if not is_admin(u.id):
        ok, missing = await check_channels_sub(bot, u.id, force=True)
        if not ok:
            text = (
                f"📢 <b>Join required channels first</b>\n\n"
                f"Please join all channels below, then tap <b>Verify</b>."
            )
            await message.answer(text, reply_markup=kb_channels_check(missing))
            return

    # Bot lock
    st = await db_stats()
    if st.is_locked and not is_admin(u.id):
        await message.answer(
            "🔒 <b>Bot is locked</b>\nAdmin has temporarily disabled access.",
        )
        return

    # Welcome
    limit = await get_user_limit(u.id)
    count = await db_get_user_file_count(u.id)
    status = await get_user_status_text(u.id)
    limit_str = f"{limit}" if limit < settings.owner_limit else "Unlimited"

    text = (
        f"👋 <b>Welcome, {html_escape(u.first_name or 'User')}!</b>\n\n"
        f"🆔 <b>ID:</b> <code>{u.id}</code>\n"
        f"🎭 <b>Status:</b> {status}\n"
        f"📁 <b>Files:</b> {count}/{limit_str}\n\n"
        f"🤖 <b>Upload and host your Python (.py) or Node (.js) bots.</b>\n"
        f"📦 Admin approval required for new uploads.\n\n"
        f"👇 Use buttons below."
    )

    await message.answer(text, reply_markup=kb_main_reply(is_admin(u.id)))
    await message.answer("⚙️ <b>Main Menu</b>", reply_markup=kb_main_inline(u.id, is_admin(u.id), st.is_locked))


@router.message(Command("help"))
@router.message(F.text == "🆘 Help")
async def cmd_help(event: Union[Message, CallbackQuery]):
    is_cb = isinstance(event, CallbackQuery)
    uid = event.from_user.id

    if not is_admin(uid):
        ok, missing = await check_channels_sub(bot, uid)
        if not ok:
            text = "📢 Join channels first."
            if is_cb:
                await event.answer("Join channels first!", show_alert=True)
                await event.message.answer(text, reply_markup=kb_channels_check(missing))
            else:
                await event.answer(text, reply_markup=kb_channels_check(missing))
            return

    text = (
        "🆘 <b>Help & Commands</b>\n\n"
        "<b>📌 User Commands</b>\n"
        "• /start — Start bot\n"
        "• /help — This message\n"
        "• /myinfo — Your stats\n"
        "• /speed — Bot speed test\n"
        "• /upload — Upload bot file\n"
        "• /files — Manage your files\n"
        "• /install — Install a module\n\n"

        "<b>🔧 Upload Rules</b>\n"
        "• Only <code>.py</code>, <code>.js</code>, or <code>.zip</code>\n"
        "• Max 20 MB per file\n"
        "• Auto-install of missing Python / Node modules\n"
        "• ZIP can include <code>requirements.txt</code> or <code>package.json</code>\n\n"

        "<b>👑 Admin Commands</b>\n"
        "• /admin — Admin panel\n"
        "• /stats — Bot statistics\n"
        "• /broadcast — Broadcast message\n"
        "• /lock — Lock bot\n"
        "• /unlock — Unlock bot\n\n"

        f"📢 Updates: @{settings.update_channel.lstrip('@')}\n"
        f"📞 Owner: @{settings.your_username.lstrip('@')}"
    )

    if is_cb:
        await event.answer()
        await event.message.answer(text, reply_markup=kb_main_inline(uid, is_admin(uid)))
    else:
        await event.answer(text, reply_markup=kb_main_inline(uid, is_admin(uid)))


@router.message(Command("myinfo"))
@router.callback_query(F.data == "my_info")
async def cmd_my_info(event: Union[Message, CallbackQuery]):
    is_cb = isinstance(event, CallbackQuery)
    uid = event.from_user.id

    if not is_admin(uid):
        ok, missing = await check_channels_sub(bot, uid)
        if not ok:
            if is_cb:
                await event.answer("Join channels first!", show_alert=True)
                await event.message.answer("📢 Join channels first.", reply_markup=kb_channels_check(missing))
            else:
                await event.answer("📢 Join channels first.", reply_markup=kb_channels_check(missing))
            return

    u = await db_get_user(uid)
    count = await db_get_user_file_count(uid)
    limit = await get_user_limit(uid)
    status = await get_user_status_text(uid)

    text = (
        f"👤 <b>My Info</b>\n\n"
        f"🆔 <code>{uid}</code>\n"
        f"👤 {html_escape(event.from_user.first_name or 'User')}\n"
        f"📛 @{event.from_user.username or 'N/A'}\n"
        f"🎭 Status: {status}\n"
        f"📁 Files: {count}/{limit if limit < settings.owner_limit else '∞'}\n"
        f"📤 Uploads: {u.total_uploads if u else 0}\n"
        f"✅ Approved: {u.total_approved if u else 0}\n"
        f"❌ Rejected: {u.total_rejected if u else 0}\n"
    )
    if u and u.is_subscribed and u.subscription_expiry:
        days = max((u.subscription_expiry - datetime.utcnow()).days, 0)
        text += f"⭐ Sub expires in: {days} days\n"

    markup = kb_back() if is_cb else kb_main_inline(uid, is_admin(uid))

    if is_cb:
        await event.answer()
        try:
            await event.message.edit_text(text, reply_markup=markup)
        except TelegramBadRequest:
            await event.message.answer(text, reply_markup=markup)
    else:
        await event.answer(text, reply_markup=markup)


@router.message(Command("speed"))
@router.message(F.text == "⚡ Speed Test")
@router.callback_query(F.data == "speed")
async def cmd_speed(event: Union[Message, CallbackQuery]):
    is_cb = isinstance(event, CallbackQuery)
    uid = event.from_user.id

    t0 = time.time()
    st = await db_stats()
    status_text = await get_user_status_text(uid)

    if is_cb:
        await event.answer("🏃 Testing...")
        msg = event.message
    else:
        msg = await event.answer("🏃 <b>Testing speed...</b>")

    latency = round((time.time() - t0) * 1000, 2)

    text = (
        f"⚡ <b>Speed Test</b>\n\n"
        f"⏱️ <b>Latency:</b> {latency} ms\n"
        f"🚦 <b>Bot status:</b> {'🔒 Locked' if st.is_locked else '🟢 Unlocked'}\n"
        f"👤 <b>Your role:</b> {status_text}\n"
        f"🕐 <b>Time:</b> {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC"
    )

    try:
        await msg.edit_text(text, reply_markup=kb_back() if is_cb else kb_main_inline(uid, is_admin(uid), st.is_locked))
    except TelegramBadRequest:
        await bot.send_message(uid, text)


@router.message(Command("stats"))
@router.callback_query(F.data == "stats")
async def cmd_stats(event: Union[Message, CallbackQuery]):
    is_cb = isinstance(event, CallbackQuery)
    uid = event.from_user.id

    total_users = await db_get_all_users_count()
    banned = await db_get_banned_count()
    st = await db_stats()
    running = sum(1 for k, v in running_processes.items() if v['process'].returncode is None)

    text = (
        f"📊 <b>Statistics</b>\n\n"
        f"👥 Users: <b>{total_users}</b>\n"
        f"🚫 Banned: <b>{banned}</b>\n"
        f"🟢 Running scripts: <b>{running}</b>\n"
        f"📤 Total uploads: <b>{st.total_uploads}</b>\n"
        f"✅ Approved: <b>{st.total_approved}</b>\n"
        f"❌ Rejected: <b>{st.total_rejected}</b>\n"
        f"📦 Installs: <b>{st.total_installs}</b>\n"
        f"💥 Crashes: <b>{st.total_crashes}</b>\n"
        f"🚦 Bot: {'🔒 Locked' if st.is_locked else '🟢 Unlocked'}"
    )

    if is_cb:
        await event.answer()
        try:
            await event.message.edit_text(text, reply_markup=kb_back() if not is_admin(uid) else kb_main_inline(uid, True, st.is_locked))
        except TelegramBadRequest:
            await event.message.answer(text)
    else:
        await event.answer(text, reply_markup=kb_main_inline(uid, is_admin(uid), st.is_locked))


# ============================================================
# 📤 UPLOAD FLOW
# ============================================================

@router.message(F.text == "📤 Upload File")
@router.message(Command("upload"))
@router.callback_query(F.data == "upload")
async def start_upload(event: Union[Message, CallbackQuery], state: FSMContext):
    is_cb = isinstance(event, CallbackQuery)
    uid = event.from_user.id

    # Bot lock
    st = await db_stats()
    if st.is_locked and not is_admin(uid):
        if is_cb:
            await event.answer("🔒 Bot locked.", show_alert=True)
        else:
            await event.answer("🔒 Bot locked by admin.")
        return

    # Channel check
    if not is_admin(uid):
        ok, missing = await check_channels_sub(bot, uid)
        if not ok:
            text = "📢 Join channels first."
            if is_cb:
                await event.answer("Join channels first!", show_alert=True)
                await event.message.answer(text, reply_markup=kb_channels_check(missing))
            else:
                await event.answer(text, reply_markup=kb_channels_check(missing))
            return

    # Rate limit
    if not await rate_limit_ok(uid, "upload", settings.rate_limit_uploads_per_min):
        if is_cb:
            await event.answer("⚠️ Too many uploads. Wait a minute.", show_alert=True)
        else:
            await event.answer("⚠️ Too many uploads. Please wait a minute.")
        return

    # File limit
    limit = await get_user_limit(uid)
    count = await db_get_user_file_count(uid)
    if count >= limit:
        if is_cb:
            await event.answer(f"⚠️ File limit reached ({count}/{limit}).", show_alert=True)
        else:
            await event.answer(f"⚠️ File limit reached ({count}/{limit}).")
        return

    await state.set_state(UploadStates.waiting_file)

    text = (
        f"📤 <b>Upload your bot</b>\n\n"
        f"Send a <code>.py</code>, <code>.js</code>, or <code>.zip</code> file.\n"
        f"Maximum size: <b>{settings.max_file_size_mb} MB</b>\n\n"
        f"💡 ZIP can contain <code>requirements.txt</code> or <code>package.json</code>."
    )

    if is_cb:
        await event.answer()
        await event.message.answer(text, reply_markup=kb_cancel_only())
    else:
        await event.answer(text, reply_markup=kb_cancel_only())


@router.message(UploadStates.waiting_file, F.document)
async def handle_document(message: Message, state: FSMContext):
    uid = message.from_user.id
    doc = message.document

    fname = doc.file_name or ""
    if not fname:
        await message.answer("⚠️ File has no name.")
        return

    ext = Path(fname).suffix.lower()
    if ext not in ('.py', '.js', '.zip'):
        await message.answer("⚠️ Only <code>.py</code>, <code>.js</code>, <code>.zip</code> allowed.")
        return

    if doc.file_size > settings.max_file_size:
        await message.answer(f"⚠️ File too large. Max: {settings.max_file_size_mb} MB.")
        return

    # Reset state
    await state.clear()

    wait = await message.answer(f"⏳ <b>Downloading</b> <code>{html_escape(fname)}</code>...")

    try:
        tg_file = await bot.get_file(doc.file_id)
        content = await bot.download_file(tg_file.file_path)

        # Forward to owner (optional backup)
        if settings.forward_uploads_to_owner:
            try:
                await bot.forward_message(
                    chat_id=settings.owner_id,
                    from_chat_id=message.chat.id,
                    message_id=message.message_id,
                )
            except Exception:
                pass

        # Admin → direct run
        if is_admin(uid):
            await wait.edit_text("🛡️ <b>Admin upload</b> — auto-approving...")

            folder = get_user_folder(uid)
            if ext == '.zip':
                asyncio.create_task(
                    process_zip(bot, uid, content, fname, message.chat.id, message)
                )
            else:
                path = folder / fname
                path.write_bytes(content)
                await db_add_user_file(uid, fname, ext.lstrip('.'), len(content))

                await db_incr_stat("total_uploads")
                await db_incr_user_stat(uid, "total_uploads", 1)
                await db_incr_stat("total_approved")
                await db_incr_user_stat(uid, "total_approved", 1)

                if ext == '.py':
                    asyncio.create_task(
                        run_python_script(bot, uid, fname, message.chat.id)
                    )
                elif ext == '.js':
                    asyncio.create_task(
                        run_node_script(bot, uid, fname, message.chat.id)
                    )
            return

        # Non-admin → approval
        await wait.edit_text("⏳ <b>Sending for approval...</b>")

        aid = await create_approval_request(
            bot=bot,
            user_id=uid,
            user_name=message.from_user.first_name or "User",
            file_name=fname,
            file_type=ext.lstrip('.'),
            content=content,
            reply_chat_id=message.chat.id,
        )

        if not aid:
            await wait.edit_text("❌ Failed to submit for approval. Try again.")
            return

        try:
            await wait.delete()
        except Exception:
            pass

    except Exception as e:
        logger.exception(f"upload error: {e}")
        try:
            await wait.edit_text(f"❌ <b>Error:</b> {html_escape(str(e))}")
        except Exception:
            await message.answer(f"❌ Error: {e}")


@router.message(UploadStates.waiting_file)
async def handle_wrong_upload(message: Message, state: FSMContext):
    await message.answer("⚠️ Send a <b>document</b> (.py / .js / .zip). /cancel to abort.")


# ============================================================
# 📂 CHECK FILES
# ============================================================

@router.message(F.text == "📂 My Files")
@router.message(Command("files"))
@router.callback_query(F.data == "check_files")
async def check_files(event: Union[Message, CallbackQuery]):
    is_cb = isinstance(event, CallbackQuery)
    uid = event.from_user.id

    if not is_admin(uid):
        ok, missing = await check_channels_sub(bot, uid)
        if not ok:
            text = "📢 Join channels first."
            if is_cb:
                await event.answer("Join channels first!", show_alert=True)
                await event.message.answer(text, reply_markup=kb_channels_check(missing))
            else:
                await event.answer(text, reply_markup=kb_channels_check(missing))
            return

    files = await db_get_user_files(uid)

    if not files:
        text = "📂 <b>No files yet.</b>\n\nUpload your first bot with the Upload button."
        if is_cb:
            await event.answer("No files.", show_alert=True)
            try:
                await event.message.edit_text(text, reply_markup=kb_main_inline(uid, is_admin(uid)))
            except TelegramBadRequest:
                pass
        else:
            await event.answer(text, reply_markup=kb_main_inline(uid, is_admin(uid)))
        return

    b = InlineKeyboardBuilder()
    for f in files:
        running = await is_script_running(uid, f.file_name)
        icon = "🟢" if running else "🔴"
        b.button(
            text=f"{icon} {f.file_name} ({f.file_type})",
            callback_data=f"file:info:{uid}:{f.file_name}",
        )
    b.button(text=Btn.BACK, callback_data="back_main")
    b.adjust(1)

    text = f"📂 <b>Your files ({len(files)})</b>\n\n🟢 Running  🔴 Stopped"

    if is_cb:
        await event.answer()
        try:
            await event.message.edit_text(text, reply_markup=b.as_markup())
        except TelegramBadRequest:
            await event.message.answer(text, reply_markup=b.as_markup())
    else:
        await event.answer(text, reply_markup=b.as_markup())


# ============================================================
# 🎛️ FILE CONTROLS — info / start / stop / restart / delete / logs
# ============================================================

async def _resolve_file_or_alert(call: CallbackQuery, owner_id: int, fname: str) -> bool:
    """Check ownership + existence"""
    req = call.from_user.id
    if not (req == owner_id or is_admin(req)):
        await call.answer("⚠️ You don't own this file.", show_alert=True)
        return False

    f = await db_get_user_file(owner_id, fname)
    if not f:
        await call.answer("❌ File not found.", show_alert=True)
        return False
    return True


@router.callback_query(F.data.startswith("file:"))
async def cb_file_router(call: CallbackQuery):
    parts = call.data.split(":", 3)
    # file:action:uid:filename
    if len(parts) < 4:
        await call.answer("Invalid data.", show_alert=True)
        return

    _, action, uid_s, fname = parts
    try:
        owner_id = int(uid_s)
    except ValueError:
        await call.answer("Bad uid.", show_alert=True)
        return

    if not await _resolve_file_or_alert(call, owner_id, fname):
        return

    if action == "info":
        await file_info_view(call, owner_id, fname)
    elif action == "start":
        await file_start_view(call, owner_id, fname)
    elif action == "stop":
        await file_stop_view(call, owner_id, fname)
    elif action == "restart":
        await file_restart_view(call, owner_id, fname)
    elif action == "delete":
        await file_delete_view(call, owner_id, fname)
    elif action == "logs":
        await file_logs_view(call, owner_id, fname)
    else:
        await call.answer("Unknown action.", show_alert=True)


async def file_info_view(call: CallbackQuery, owner_id: int, fname: str):
    f = await db_get_user_file(owner_id, fname)
    running = await is_script_running(owner_id, fname)

    text = (
        f"⚙️ <b>File Control</b>\n\n"
        f"📁 <code>{fname}</code>\n"
        f"📦 Type: <b>{f.file_type}</b>\n"
        f"💾 Size: <b>{fmt_bytes(f.file_size)}</b>\n"
        f"📅 Uploaded: {f.uploaded_at.strftime('%Y-%m-%d %H:%M') if f.uploaded_at else 'N/A'}\n"
        f"🔄 Restarts: <b>{f.restart_count}</b>\n"
        f"📊 Status: {'🟢 Running' if running else '🔴 Stopped'}"
    )
    if f.pid and running:
        text += f"\n🆔 PID: <code>{f.pid}</code>"
    if f.last_crash_at:
        text += f"\n💥 Last crash: {f.last_crash_at.strftime('%Y-%m-%d %H:%M')}"

    await call.answer()
    try:
        await call.message.edit_text(text, reply_markup=kb_file_controls(owner_id, fname, running))
    except TelegramBadRequest:
        await call.message.answer(text, reply_markup=kb_file_controls(owner_id, fname, running))


async def file_start_view(call: CallbackQuery, owner_id: int, fname: str):
    if await is_script_running(owner_id, fname):
        await call.answer("⚠️ Already running.", show_alert=True)
        return

    await call.answer("⏳ Starting...")
    folder = get_user_folder(owner_id)
    path = folder / fname

    if not path.exists():
        await call.message.answer("❌ File missing on disk.")
        return

    ext = path.suffix.lower()
    if ext == '.py':
        asyncio.create_task(run_python_script(bot, owner_id, fname, call.message.chat.id))
    elif ext == '.js':
        asyncio.create_task(run_node_script(bot, owner_id, fname, call.message.chat.id))
    else:
        await call.message.answer("❌ Unsupported file type.")


async def file_stop_view(call: CallbackQuery, owner_id: int, fname: str):
    if not await is_script_running(owner_id, fname):
        await call.answer("⚠️ Not running.", show_alert=True)
        return
    await call.answer("🛑 Stopping...")
    await stop_script(bot, owner_id, fname, notify_chat=call.message.chat.id)
    await asyncio.sleep(1)
    await file_info_view(call, owner_id, fname)


async def file_restart_view(call: CallbackQuery, owner_id: int, fname: str):
    await call.answer("🔄 Restarting...")
    asyncio.create_task(restart_script(bot, owner_id, fname, call.message.chat.id))
    await asyncio.sleep(2)
    await file_info_view(call, owner_id, fname)


async def file_delete_view(call: CallbackQuery, owner_id: int, fname: str):
    await call.answer()
    text = (
        f"🗑️ <b>Delete file?</b>\n\n"
        f"📁 <code>{fname}</code>\n"
        f"This also stops it if running."
    )
    kb = kb_confirm(
        action="delete_file",
        yes_cb=f"file:delete_confirm:{owner_id}:{fname}",
    )
    try:
        await call.message.edit_text(text, reply_markup=kb)
    except TelegramBadRequest:
        await call.message.answer(text, reply_markup=kb)


@router.callback_query(F.data.startswith("file:delete_confirm:"))
async def cb_file_delete_confirm(call: CallbackQuery):
    parts = call.data.split(":", 3)
    if len(parts) < 4:
        await call.answer("Invalid.", show_alert=True)
        return

    _, _, uid_s, fname = parts
    owner_id = int(uid_s)
    req = call.from_user.id

    if not (req == owner_id or is_admin(req)):
        await call.answer("⚠️ Not allowed.", show_alert=True)
        return

    await call.answer("🗑️ Deleting...")

    # Stop if running
    if await is_script_running(owner_id, fname):
        await stop_script(bot, owner_id, fname)

    # Remove files
    folder = get_user_folder(owner_id)
    for p in [folder / fname, get_log_path(owner_id, fname)]:
        if p.exists():
            try:
                p.unlink()
            except Exception:
                pass

    await db_delete_user_file(owner_id, fname)

    try:
        await call.message.edit_text(
            f"🗑️ <b>Deleted</b> <code>{fname}</code>",
            reply_markup=kb_back("check_files"),
        )
    except TelegramBadRequest:
        pass


async def file_logs_view(call: CallbackQuery, owner_id: int, fname: str):
    log_path = get_log_path(owner_id, fname)
    if not log_path.exists():
        await call.answer("⚠️ No logs yet.", show_alert=True)
        return

    try:
        content = log_path.read_text(encoding='utf-8', errors='ignore') or "(empty)"
    except Exception as e:
        await call.answer(f"❌ {e}", show_alert=True)
        return

    if len(content) > 3500:
        content = "...(truncated)\n" + content[-3500:]

    safe = html_escape(content)

    await call.answer()
    try:
        await call.message.answer(
            f"📜 <b>Logs:</b> <code>{fname}</code>\n<pre>{safe}</pre>",
            reply_markup=kb_back(f"file:info:{owner_id}:{fname}"),
        )
    except Exception as e:
        logger.error(f"logs send error: {e}")


# ============================================================
# 📦 MANUAL INSTALL
# ============================================================

@router.message(F.text == "📦 Install Module")
@router.message(Command("install"))
@router.callback_query(F.data == "manual_install")
async def cmd_manual_install(event: Union[Message, CallbackQuery], state: FSMContext):
    is_cb = isinstance(event, CallbackQuery)
    uid = event.from_user.id

    if not is_admin(uid):
        ok, missing = await check_channels_sub(bot, uid)
        if not ok:
            if is_cb:
                await event.answer("Join channels first!", show_alert=True)
            else:
                await event.answer("📢 Join channels first.", reply_markup=kb_channels_check(missing))
            return

    await state.set_state(InstallStates.waiting_module)

    text = (
        "📦 <b>Install a module</b>\n\n"
        "Send the module name:\n"
        "• Python: <code>requests</code>\n"
        "• Node.js: <code>npm:express</code>\n\n"
        "It will be installed globally."
    )

    if is_cb:
        await event.answer()
        await event.message.answer(text, reply_markup=kb_cancel_only())
    else:
        await event.answer(text, reply_markup=kb_cancel_only())


@router.message(InstallStates.waiting_module)
async def do_install_module(message: Message, state: FSMContext):
    await state.clear()
    uid = message.from_user.id
    module = (message.text or "").strip()

    if not module:
        await message.answer("⚠️ Empty. Try again with /install.")
        return

    if module.lower() == "/cancel":
        await message.answer("❌ Cancelled.")
        return

    is_node = module.lower().startswith("npm:")
    if is_node:
        name = module[4:].strip()
        folder = get_user_folder(uid)
        asyncio.create_task(
            install_npm_module(bot, name, folder, message.chat.id, uid, manual=True)
        )
    else:
        asyncio.create_task(
            install_pip_module(bot, module, message.chat.id, uid, manual=True)
        )


# ============================================================
# 📞 CONTACT / CANCEL / BACK
# ============================================================

@router.message(F.text == "📞 Contact Owner")
@router.message(Command("contact"))
@router.callback_query(F.data == "contact_owner")
async def cmd_contact(event: Union[Message, CallbackQuery]):
    is_cb = isinstance(event, CallbackQuery)
    b = InlineKeyboardBuilder()
    b.button(
        text="📞 Contact Owner",
        url=f"https://t.me/{settings.your_username.lstrip('@')}",
    )
    b.button(text=Btn.BACK, callback_data="back_main")
    b.adjust(1, 1)

    text = (
        f"📞 <b>Contact Owner</b>\n\n"
        f"Owner: @{settings.your_username.lstrip('@')}\n"
        f"Updates: @{settings.update_channel.lstrip('@')}"
    )

    if is_cb:
        await event.answer()
        try:
            await event.message.edit_text(text, reply_markup=b.as_markup())
        except TelegramBadRequest:
            await event.message.answer(text, reply_markup=b.as_markup())
    else:
        await event.answer(text, reply_markup=b.as_markup())


@router.message(Command("cancel"))
@router.callback_query(F.data == "cancel_fsm")
async def cmd_cancel(event: Union[Message, CallbackQuery], state: FSMContext):
    await state.clear()
    uid = event.from_user.id
    quick_clear(uid)

    text = "❌ <b>Cancelled.</b>\n\nSend /start to begin again."
    if isinstance(event, CallbackQuery):
        await event.answer("Cancelled")
        try:
            await event.message.edit_text(text, reply_markup=kb_main_inline(uid, is_admin(uid)))
        except TelegramBadRequest:
            await event.message.answer(text)
    else:
        await event.answer(text)


@router.callback_query(F.data == "back_main")
async def cb_back_main(call: CallbackQuery):
    uid = call.from_user.id
    st = await db_stats()

    if not is_admin(uid):
        ok, _ = await check_channels_sub(bot, uid)
        if not ok:
            await call.answer("Join channels first!", show_alert=True)
            return

    await call.answer()
    try:
        await call.message.edit_text(
            "⚙️ <b>Main Menu</b>",
            reply_markup=kb_main_inline(uid, is_admin(uid), st.is_locked),
        )
    except TelegramBadRequest:
        pass


@router.callback_query(F.data == "verify_channels")
async def cb_verify_channels(call: CallbackQuery):
    uid = call.from_user.id
    invalidate_channel_cache(uid)

    ok, missing = await check_channels_sub(bot, uid, force=True)
    if ok:
        await call.answer("🟢 Verified!")
        try:
            await call.message.delete()
        except Exception:
            pass
        st = await db_stats()
        await call.message.answer(
            "🟢 <b>Verified!</b>",
            reply_markup=kb_main_inline(uid, is_admin(uid), st.is_locked),
        )
    else:
        await call.answer(f"🔴 Still missing {len(missing)} channel(s).", show_alert=True)


# ============================================================
# 🎯 CATCH ALL (non-command text, not matched above)
# ============================================================

@router.message(F.text & ~F.text.startswith("/"))
async def fallback_text(message: Message, state: FSMContext):
    # If we're in a state, let state handlers handle it
    current = await state.get_state()
    if current is not None:
        return

    # Otherwise, echo help
    if message.text in ("📤 Upload File", "📂 My Files", "⚡ Speed Test", "🆘 Help"):
        return  # handled elsewhere

    text = (
        "🤔 <b>Unknown command</b>\n\n"
        "Try /help or use the buttons below."
    )
    await message.answer(text, reply_markup=kb_main_inline(message.from_user.id, is_admin(message.from_user.id)))


# ============================================================
# 🗄️ END PART 4
# ============================================================
# ============================================================
# 👑 ADMIN HANDLERS + MAIN ENTRY + WEB SERVER
# ============================================================
# PART 5/5 : Admin panel + Broadcast + Main + Web
# ============================================================

# ------------------------------------------------------------
# Admin gate helper
# ------------------------------------------------------------
async def _require_admin(call: CallbackQuery) -> bool:
    if not is_admin(call.from_user.id):
        await call.answer("🔴 Admin only.", show_alert=True)
        return False
    return True


# ============================================================
# 🛡️ ADMIN PANEL — Entry
# ============================================================

@router.message(Command("admin"))
@router.message(F.text == "🛡️ Admin Panel")
@router.callback_query(F.data == "admin:panel")
async def admin_panel(event: Union[Message, CallbackQuery]):
    is_cb = isinstance(event, CallbackQuery)
    uid = event.from_user.id

    if not is_admin(uid):
        if is_cb:
            await event.answer("🔴 Admin only.", show_alert=True)
        else:
            await event.answer("🔴 Admin only.")
        return

    st = await db_stats()
    total_users = await db_get_all_users_count()
    banned = await db_get_banned_count()
    running = sum(1 for k, v in running_processes.items() if v['process'].returncode is None)

    text = (
        f"🛡️ <b>Admin Panel</b>\n\n"
        f"👥 Users: <b>{total_users}</b>\n"
        f"🚫 Banned: <b>{banned}</b>\n"
        f"🟢 Running: <b>{running}</b>\n"
        f"📤 Uploads: <b>{st.total_uploads}</b>\n"
        f"✅ Approved: <b>{st.total_approved}</b>\n"
        f"❌ Rejected: <b>{st.total_rejected}</b>\n"
        f"📦 Installs: <b>{st.total_installs}</b>\n"
        f"💥 Crashes: <b>{st.total_crashes}</b>\n"
        f"🚦 Bot: {'🔒 Locked' if st.is_locked else '🟢 Unlocked'}"
    )
    markup = kb_admin_panel()

    if is_cb:
        await event.answer()
        try:
            await event.message.edit_text(text, reply_markup=markup)
        except TelegramBadRequest:
            await event.message.answer(text, reply_markup=markup)
    else:
        await event.answer(text, reply_markup=markup)


# ============================================================
# ✅ APPROVAL CALLBACKS
# ============================================================

@router.callback_query(F.data.startswith("appr:"))
async def cb_approval(call: CallbackQuery):
    if not await _require_admin(call):
        return

    parts = call.data.split(":", 2)
    if len(parts) < 3:
        await call.answer("Invalid data.", show_alert=True)
        return

    _, action, aid = parts

    if action == "run":
        await handle_approval_run(bot, call, aid)
    elif action == "rej":
        await handle_approval_reject(bot, call, aid)
    elif action == "dl":
        await handle_approval_download(bot, call, aid)
    elif action == "info":
        await handle_approval_info(bot, call, aid)
    else:
        await call.answer("Unknown action.", show_alert=True)


# ============================================================
# 🔒 LOCK / UNLOCK
# ============================================================

@router.message(Command("lock"))
async def cmd_lock(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("🔴 Admin only.")
        return
    st = await db_stats()
    if not st.is_locked:
        await db_toggle_lock("Locked via /lock")
    await db_audit(message.from_user.id, "lock_bot")
    await message.answer("🔒 <b>Bot locked.</b>")


@router.message(Command("unlock"))
async def cmd_unlock(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("🔴 Admin only.")
        return
    st = await db_stats()
    if st.is_locked:
        await db_toggle_lock()
    await db_audit(message.from_user.id, "unlock_bot")
    await message.answer("🟢 <b>Bot unlocked.</b>")


@router.callback_query(F.data == "admin:toggle_lock")
async def cb_toggle_lock(call: CallbackQuery):
    if not await _require_admin(call):
        return
    locked = await db_toggle_lock()
    await db_audit(call.from_user.id, "toggle_lock", None, f"locked={locked}")
    await call.answer("🔒 Locked" if locked else "🟢 Unlocked")
    try:
        await call.message.edit_reply_markup(
            reply_markup=kb_main_inline(call.from_user.id, True, locked)
        )
    except TelegramBadRequest:
        pass


# ============================================================
# 🚀 RUN ALL SCRIPTS
# ============================================================

@router.message(Command("runall"))
@router.callback_query(F.data == "admin:run_all")
async def cmd_run_all(event: Union[Message, CallbackQuery]):
    is_cb = isinstance(event, CallbackQuery)
    uid = event.from_user.id

    if not is_admin(uid):
        if is_cb:
            await event.answer("🔴 Admin only.", show_alert=True)
        else:
            await event.answer("🔴 Admin only.")
        return

    if is_cb:
        await event.answer("🚀 Starting...")
        chat_id = event.message.chat.id
    else:
        chat_id = event.chat.id

    status = await bot.send_message(chat_id, "🔄 <b>Scanning all user files...</b>")

    started = 0
    skipped = 0
    errors = 0

    all_ids = await db_get_all_user_ids()
    for target_uid in all_ids:
        files = await db_get_user_files(target_uid)
        for f in files:
            if await is_script_running(target_uid, f.file_name):
                skipped += 1
                continue

            folder = get_user_folder(target_uid)
            path = folder / f.file_name
            if not path.exists():
                skipped += 1
                continue

            try:
                if f.file_type == 'py':
                    asyncio.create_task(
                        run_python_script(bot, target_uid, f.file_name, chat_id)
                    )
                elif f.file_type == 'js':
                    asyncio.create_task(
                        run_node_script(bot, target_uid, f.file_name, chat_id)
                    )
                started += 1
                await asyncio.sleep(0.6)
            except Exception as e:
                logger.error(f"runall error for {f.file_name}: {e}")
                errors += 1

    await db_audit(uid, "run_all_scripts", None, f"started={started}")
    await status.edit_text(
        f"✅ <b>Run All Complete</b>\n\n"
        f"▶️ Started: <b>{started}</b>\n"
        f"⏭️ Skipped: <b>{skipped}</b>\n"
        f"❌ Errors: <b>{errors}</b>"
    )


# ============================================================
# 📣 BROADCAST FLOW
# ============================================================

@router.message(Command("broadcast"))
@router.callback_query(F.data == "admin:broadcast_init")
async def cmd_broadcast_init(event: Union[Message, CallbackQuery], state: FSMContext):
    is_cb = isinstance(event, CallbackQuery)
    uid = event.from_user.id

    if not is_admin(uid):
        if is_cb:
            await event.answer("🔴 Admin only.", show_alert=True)
        else:
            await event.answer("🔴 Admin only.")
        return

    await state.set_state(AdminStates.broadcast_wait)

    text = (
        "📣 <b>Broadcast Mode</b>\n\n"
        "Send the message you want to broadcast.\n"
        "• Text, photo, video, sticker — all supported\n"
        "• HTML formatting preserved\n"
        "• Premium emojis preserved\n\n"
        "Send /cancel to abort."
    )

    if is_cb:
        await event.answer()
        await event.message.answer(text, reply_markup=kb_cancel_only())
    else:
        await event.answer(text, reply_markup=kb_cancel_only())


@router.message(AdminStates.broadcast_wait)
async def do_broadcast_send(message: Message, state: FSMContext):
    await state.clear()
    uid = message.from_user.id

    if not is_admin(uid):
        await message.answer("🔴 Admin only.")
        return

    if message.text and message.text.strip() == "/cancel":
        await message.answer("🔴 Cancelled.")
        return

    total = await db_get_all_users_count()
    if total == 0:
        await message.answer("⚠️ No users to broadcast.")
        return

    status = await message.answer(
        f"📣 <b>Broadcasting...</b>\n\n"
        f"👥 Target: {total} users\n"
        f"⏳ Starting..."
    )

    # Run in background
    asyncio.create_task(
        execute_broadcast(
            bot=bot,
            admin_id=uid,
            source_chat_id=message.chat.id,
            source_message_id=message.message_id,
            fallback_text=message.html_text if message.text else None,
            status_message=status,
        )
    )

    await db_audit(uid, "broadcast", None, f"total={total}")


# ============================================================
# 👥 USER MANAGEMENT
# ============================================================

@router.callback_query(F.data == "admin:users")
async def cb_users_menu(call: CallbackQuery):
    if not await _require_admin(call):
        return
    await call.answer()
    text = (
        "👥 <b>User Management</b>\n\n"
        "Choose an action below."
    )
    try:
        await call.message.edit_text(text, reply_markup=kb_user_management())
    except TelegramBadRequest:
        await call.message.answer(text, reply_markup=kb_user_management())


@router.callback_query(F.data == "admin:ban")
async def cb_ban_init(call: CallbackQuery, state: FSMContext):
    if not await _require_admin(call):
        return
    await call.answer()
    await state.set_state(AdminStates.ban_user)
    await call.message.answer(
        "🚫 <b>Ban User</b>\n\n"
        "Send: <code>user_id reason</code>\n"
        "Example: <code>123456789 Spam</code>\n\n"
        "Send /cancel to abort.",
        reply_markup=kb_cancel_only(),
    )


@router.message(AdminStates.ban_user)
async def do_ban(message: Message, state: FSMContext):
    await state.clear()
    admin_id = message.from_user.id

    if not is_admin(admin_id):
        return

    if message.text and message.text.strip() == "/cancel":
        await message.answer("🔴 Cancelled.")
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 1 or not parts[0].isdigit():
        await message.answer("⚠️ Format: <code>user_id reason</code>")
        return

    target_id = int(parts[0])
    reason = parts[1] if len(parts) > 1 else "No reason"

    if is_admin(target_id):
        await message.answer("⚠️ Cannot ban an admin.")
        return

    if await db_ban_user(target_id, reason, admin_id):
        invalidate_ban_cache(target_id)

        # Stop their scripts
        files = await db_get_user_files(target_id)
        for f in files:
            if await is_script_running(target_id, f.file_name):
                await stop_script(bot, target_id, f.file_name)

        await db_audit(admin_id, "ban_user", target_id, reason)
        await message.answer(f"🟢 Banned <code>{target_id}</code>\nReason: {html_escape(reason)}")

        try:
            await bot.send_message(target_id, f"🚫 You are banned.\nReason: {html_escape(reason)}")
        except Exception:
            pass
    else:
        await message.answer("🔴 Failed to ban.")


@router.callback_query(F.data == "admin:unban")
async def cb_unban_init(call: CallbackQuery, state: FSMContext):
    if not await _require_admin(call):
        return
    await call.answer()
    await state.set_state(AdminStates.unban_user)
    await call.message.answer(
        "🟢 <b>Unban User</b>\n\nSend the user ID.\n/cancel to abort.",
        reply_markup=kb_cancel_only(),
    )


@router.message(AdminStates.unban_user)
async def do_unban(message: Message, state: FSMContext):
    await state.clear()
    admin_id = message.from_user.id

    if not is_admin(admin_id):
        return

    if message.text and message.text.strip() == "/cancel":
        await message.answer("🔴 Cancelled.")
        return

    try:
        target_id = int((message.text or "").strip())
    except ValueError:
        await message.answer("⚠️ Invalid ID.")
        return

    if await db_unban_user(target_id):
        invalidate_ban_cache(target_id)
        await db_audit(admin_id, "unban_user", target_id)
        await message.answer(f"🟢 Unbanned <code>{target_id}</code>")
        try:
            await bot.send_message(target_id, "🟢 You are unbanned.")
        except Exception:
            pass
    else:
        await message.answer("⚠️ User not found.")


@router.callback_query(F.data == "admin:set_limit")
async def cb_set_limit_init(call: CallbackQuery, state: FSMContext):
    if not await _require_admin(call):
        return
    await call.answer()
    await state.set_state(AdminStates.set_limit)
    await call.message.answer(
        "🔧 <b>Set Custom Limit</b>\n\nSend: <code>user_id limit</code>\n/cancel to abort.",
        reply_markup=kb_cancel_only(),
    )


@router.message(AdminStates.set_limit)
async def do_set_limit(message: Message, state: FSMContext):
    await state.clear()
    admin_id = message.from_user.id
    if not is_admin(admin_id):
        return
    if message.text and message.text.strip() == "/cancel":
        await message.answer("🔴 Cancelled.")
        return

    parts = (message.text or "").split()
    if len(parts) != 2 or not all(p.isdigit() for p in parts):
        await message.answer("⚠️ Format: <code>user_id limit</code>")
        return

    target_id, limit = int(parts[0]), int(parts[1])
    if limit < 1:
        await message.answer("⚠️ Limit must be > 0.")
        return

    await db_set_custom_limit(target_id, limit, admin_id)
    await db_audit(admin_id, "set_limit", target_id, f"limit={limit}")
    await message.answer(f"🟢 Limit for <code>{target_id}</code> → <b>{limit}</b>")

    try:
        await bot.send_message(target_id, f"⚙️ Your upload limit was set to <b>{limit}</b>.")
    except Exception:
        pass


@router.callback_query(F.data == "admin:remove_limit")
async def cb_remove_limit_init(call: CallbackQuery, state: FSMContext):
    if not await _require_admin(call):
        return
    await call.answer()
    await state.set_state(AdminStates.remove_limit)
    await call.message.answer(
        "🗑️ <b>Remove Custom Limit</b>\n\nSend the user ID.\n/cancel to abort.",
        reply_markup=kb_cancel_only(),
    )


@router.message(AdminStates.remove_limit)
async def do_remove_limit(message: Message, state: FSMContext):
    await state.clear()
    admin_id = message.from_user.id
    if not is_admin(admin_id):
        return
    if message.text and message.text.strip() == "/cancel":
        await message.answer("🔴 Cancelled.")
        return
    try:
        target_id = int((message.text or "").strip())
    except ValueError:
        await message.answer("⚠️ Invalid ID.")
        return

    ok = await db_remove_custom_limit(target_id)
    if ok:
        await db_audit(admin_id, "remove_limit", target_id)
        await message.answer(f"🟢 Limit removed for <code>{target_id}</code>")
        try:
            await bot.send_message(target_id, "⚙️ Your custom limit was removed.")
        except Exception:
            pass
    else:
        await message.answer("⚠️ Failed.")


@router.callback_query(F.data == "admin:user_info")
async def cb_user_info_init(call: CallbackQuery, state: FSMContext):
    if not await _require_admin(call):
        return
    await call.answer()
    await state.set_state(AdminStates.user_info)
    await call.message.answer(
        "ℹ️ <b>User Info</b>\n\nSend the user ID.\n/cancel to abort.",
        reply_markup=kb_cancel_only(),
    )


@router.message(AdminStates.user_info)
async def do_user_info(message: Message, state: FSMContext):
    await state.clear()
    admin_id = message.from_user.id
    if not is_admin(admin_id):
        return
    if message.text and message.text.strip() == "/cancel":
        await message.answer("🔴 Cancelled.")
        return
    try:
        target_id = int((message.text or "").strip())
    except ValueError:
        await message.answer("⚠️ Invalid ID.")
        return

    u = await db_get_user(target_id)
    if not u:
        await message.answer("⚠️ User not found.")
        return

    count = await db_get_user_file_count(target_id)
    limit = await get_user_limit(target_id)
    running = 0
    files = await db_get_user_files(target_id)
    for f in files:
        if await is_script_running(target_id, f.file_name):
            running += 1

    text = (
        f"👤 <b>User Info</b>\n\n"
        f"🆔 <code>{target_id}</code>\n"
        f"👤 {html_escape(u.first_name or 'N/A')}\n"
        f"📛 @{u.username or 'N/A'}\n"
        f"🎭 Role: {await get_user_status_text(target_id)}\n"
        f"📁 Files: {count}/{limit if limit < settings.owner_limit else '∞'}\n"
        f"🟢 Running: {running}\n"
        f"📤 Uploads: {u.total_uploads}\n"
        f"✅ Approved: {u.total_approved}\n"
        f"❌ Rejected: {u.total_rejected}\n"
        f"🚫 Banned: {'Yes' if u.is_banned else 'No'}\n"
        f"⭐ Subscribed: {'Yes' if u.is_subscribed else 'No'}\n"
    )
    if u.subscription_expiry:
        text += f"⏳ Sub expires: {u.subscription_expiry.strftime('%Y-%m-%d')}\n"
    text += f"📅 Joined: {u.joined_at.strftime('%Y-%m-%d') if u.joined_at else 'N/A'}"

    await message.answer(text, reply_markup=kb_back("admin:users"))


# ============================================================
# 📢 CHANNEL MANAGEMENT
# ============================================================

@router.callback_query(F.data == "admin:channels")
async def cb_channels_menu(call: CallbackQuery):
    if not await _require_admin(call):
        return
    await call.answer()
    text = "📢 <b>Mandatory Channels</b>\n\nManage channels users must join."
    try:
        await call.message.edit_text(text, reply_markup=kb_channels_management())
    except TelegramBadRequest:
        await call.message.answer(text, reply_markup=kb_channels_management())


@router.callback_query(F.data == "admin:channel_add")
async def cb_channel_add(call: CallbackQuery, state: FSMContext):
    if not await _require_admin(call):
        return
    await call.answer()
    await state.set_state(AdminStates.channel_add)
    await call.message.answer(
        "➕ <b>Add Channel</b>\n\n"
        "Send channel ID or @username.\n"
        "Example: <code>@mychannel</code> or <code>-1001234567890</code>\n\n"
        "⚠️ Bot must be admin in that channel.",
        reply_markup=kb_cancel_only(),
    )


@router.message(AdminStates.channel_add)
async def do_channel_add(message: Message, state: FSMContext):
    await state.clear()
    admin_id = message.from_user.id
    if not is_admin(admin_id):
        return
    if message.text and message.text.strip() == "/cancel":
        await message.answer("🔴 Cancelled.")
        return

    cid_input = (message.text or "").strip()

    try:
        chat = await bot.get_chat(cid_input)
        me = await bot.get_me()
        member = await bot.get_chat_member(chat.id, me.id)

        if member.status not in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR):
            await message.answer("🔴 Bot is not admin in this channel.")
            return

        username = f"@{chat.username}" if chat.username else ""
        invite = None
        try:
            invite = await bot.export_chat_invite_link(chat.id)
        except Exception:
            pass

        await db_add_channel(
            cid=str(chat.id),
            username=username,
            name=chat.title or str(chat.id),
            invite_link=invite,
            added_by=admin_id,
        )
        await db_audit(admin_id, "add_channel", None, str(chat.id))
        invalidate_channel_cache()

        await message.answer(
            f"🟢 <b>Channel added</b>\n\n"
            f"📢 {html_escape(chat.title or str(chat.id))}\n"
            f"🆔 <code>{chat.id}</code>\n"
            f"{invite or username}"
        )
    except Exception as e:
        logger.exception("channel add error")
        await message.answer(f"🔴 Error: {html_escape(str(e))}")


@router.callback_query(F.data == "admin:channel_remove")
async def cb_channel_remove(call: CallbackQuery):
    if not await _require_admin(call):
        return
    channels = await db_get_channels()
    if not channels:
        await call.answer("No channels.", show_alert=True)
        return

    b = InlineKeyboardBuilder()
    for ch in channels:
        b.button(
            text=f"🔴 {ch.channel_name}",
            callback_data=f"admin:chrm:{ch.channel_id}",
        )
    b.button(text=Btn.BACK, callback_data="admin:channels")
    b.adjust(1)

    await call.answer()
    try:
        await call.message.edit_text("🔴 <b>Select channel to remove:</b>", reply_markup=b.as_markup())
    except TelegramBadRequest:
        pass


@router.callback_query(F.data.startswith("admin:chrm:"))
async def cb_channel_remove_confirm(call: CallbackQuery):
    if not await _require_admin(call):
        return
    cid = call.data.split(":", 2)[2]
    await db_remove_channel(cid)
    await db_audit(call.from_user.id, "remove_channel", None, cid)
    invalidate_channel_cache()
    await call.answer("🟢 Removed")
    await cb_channels_menu(call)


@router.callback_query(F.data == "admin:channel_list")
async def cb_channel_list(call: CallbackQuery):
    if not await _require_admin(call):
        return
    channels = await db_get_channels()
    if not channels:
        await call.answer("No channels.", show_alert=True)
        return
    text = "📋 <b>Channels</b>\n\n"
    for ch in channels:
        text += (
            f"📢 <b>{html_escape(ch.channel_name)}</b>\n"
            f"🆔 <code>{ch.channel_id}</code>\n"
            f"🔗 {ch.invite_link or ch.channel_username or 'N/A'}\n\n"
        )
    await call.answer()
    try:
        await call.message.edit_text(text, reply_markup=kb_channels_management())
    except TelegramBadRequest:
        pass


# ============================================================
# 💳 SUBSCRIPTIONS
# ============================================================

@router.callback_query(F.data == "admin:subs")
async def cb_subs_menu(call: CallbackQuery):
    if not await _require_admin(call):
        return
    await call.answer()
    text = "💳 <b>Subscription Management</b>"
    try:
        await call.message.edit_text(text, reply_markup=kb_subscription_management())
    except TelegramBadRequest:
        await call.message.answer(text, reply_markup=kb_subscription_management())


@router.callback_query(F.data == "admin:sub_add")
async def cb_sub_add(call: CallbackQuery, state: FSMContext):
    if not await _require_admin(call):
        return
    await call.answer()
    await state.set_state(AdminStates.sub_add)
    await call.message.answer(
        "➕ <b>Add Subscription</b>\n\n"
        "Send: <code>user_id days</code>\n"
        "Example: <code>123456789 30</code>",
        reply_markup=kb_cancel_only(),
    )


@router.message(AdminStates.sub_add)
async def do_sub_add(message: Message, state: FSMContext):
    await state.clear()
    admin_id = message.from_user.id
    if not is_admin(admin_id):
        return
    if message.text and message.text.strip() == "/cancel":
        await message.answer("🔴 Cancelled.")
        return

    parts = (message.text or "").split()
    if len(parts) != 2 or not all(p.isdigit() for p in parts):
        await message.answer("⚠️ Format: <code>user_id days</code>")
        return

    target_id, days = int(parts[0]), int(parts[1])
    if days < 1:
        await message.answer("⚠️ Days must be > 0.")
        return

    expiry = await db_set_subscription(target_id, days, admin_id)
    await db_audit(admin_id, "add_sub", target_id, f"days={days}")
    await message.answer(
        f"🟢 Sub added for <code>{target_id}</code>\n"
        f"Expires: {expiry.strftime('%Y-%m-%d %H:%M')}"
    )
    try:
        await bot.send_message(
            target_id,
            f"⭐ <b>Subscription activated</b> for {days} days!\n"
            f"Expires: {expiry.strftime('%Y-%m-%d')}"
        )
    except Exception:
        pass


@router.callback_query(F.data == "admin:sub_remove")
async def cb_sub_remove(call: CallbackQuery, state: FSMContext):
    if not await _require_admin(call):
        return
    await call.answer()
    await state.set_state(AdminStates.sub_remove)
    await call.message.answer(
        "🔴 <b>Remove Subscription</b>\n\nSend the user ID.",
        reply_markup=kb_cancel_only(),
    )


@router.message(AdminStates.sub_remove)
async def do_sub_remove(message: Message, state: FSMContext):
    await state.clear()
    admin_id = message.from_user.id
    if not is_admin(admin_id):
        return
    if message.text and message.text.strip() == "/cancel":
        await message.answer("🔴 Cancelled.")
        return
    try:
        target_id = int((message.text or "").strip())
    except ValueError:
        await message.answer("⚠️ Invalid ID.")
        return

    ok = await db_remove_subscription(target_id)
    if ok:
        await db_audit(admin_id, "remove_sub", target_id)
        await message.answer(f"🟢 Sub removed for <code>{target_id}</code>")
        try:
            await bot.send_message(target_id, "ℹ️ Your subscription was removed.")
        except Exception:
            pass
    else:
        await message.answer("⚠️ Failed.")


@router.callback_query(F.data == "admin:sub_check")
async def cb_sub_check(call: CallbackQuery, state: FSMContext):
    if not await _require_admin(call):
        return
    await call.answer()
    await state.set_state(AdminStates.sub_check)
    await call.message.answer(
        "🔍 <b>Check Subscription</b>\n\nSend the user ID.",
        reply_markup=kb_cancel_only(),
    )


@router.message(AdminStates.sub_check)
async def do_sub_check(message: Message, state: FSMContext):
    await state.clear()
    if not is_admin(message.from_user.id):
        return
    if message.text and message.text.strip() == "/cancel":
        await message.answer("🔴 Cancelled.")
        return
    try:
        target_id = int((message.text or "").strip())
    except ValueError:
        await message.answer("⚠️ Invalid ID.")
        return

    u = await db_get_user(target_id)
    if u and u.is_subscribed and u.subscription_expiry and u.subscription_expiry > datetime.utcnow():
        days = (u.subscription_expiry - datetime.utcnow()).days
        await message.answer(
            f"🟢 <code>{target_id}</code> has active sub.\n"
            f"Expires: {u.subscription_expiry.strftime('%Y-%m-%d %H:%M')}\n"
            f"Days left: {days}"
        )
    else:
        await message.answer(f"🔴 <code>{target_id}</code> has no active sub.")


# ============================================================
# ➕ ADD / REMOVE ADMIN
# ============================================================

@router.callback_query(F.data == "admin:add_admin")
async def cb_add_admin_init(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != settings.owner_id:
        await call.answer("👑 Owner only.", show_alert=True)
        return
    await call.answer()
    await state.set_state(AdminStates.add_admin_id)
    await call.message.answer(
        "➕ <b>Add Admin</b>\n\nSend the user ID.",
        reply_markup=kb_cancel_only(),
    )


@router.message(AdminStates.add_admin_id)
async def do_add_admin(message: Message, state: FSMContext):
    await state.clear()
    uid = message.from_user.id
    if uid != settings.owner_id:
        return
    if message.text and message.text.strip() == "/cancel":
        await message.answer("🔴 Cancelled.")
        return

    try:
        target_id = int((message.text or "").strip())
    except ValueError:
        await message.answer("⚠️ Invalid ID.")
        return

    if target_id == settings.owner_id:
        await message.answer("⚠️ Already owner.")
        return

    ok = await db_add_admin(target_id, uid)
    if ok:
        _admin_cache.add(target_id)
        await db_audit(uid, "add_admin", target_id)
        await message.answer(f"🟢 Admin added: <code>{target_id}</code>")
        try:
            await bot.send_message(target_id, "🎉 You are now an admin!")
        except Exception:
            pass
    else:
        await message.answer("⚠️ Already admin.")


@router.callback_query(F.data == "admin:remove_admin")
async def cb_remove_admin_init(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != settings.owner_id:
        await call.answer("👑 Owner only.", show_alert=True)
        return
    await call.answer()
    await state.set_state(AdminStates.remove_admin_id)
    await call.message.answer(
        "🔴 <b>Remove Admin</b>\n\nSend the user ID.",
        reply_markup=kb_cancel_only(),
    )


@router.message(AdminStates.remove_admin_id)
async def do_remove_admin(message: Message, state: FSMContext):
    await state.clear()
    uid = message.from_user.id
    if uid != settings.owner_id:
        return
    if message.text and message.text.strip() == "/cancel":
        await message.answer("🔴 Cancelled.")
        return

    try:
        target_id = int((message.text or "").strip())
    except ValueError:
        await message.answer("⚠️ Invalid ID.")
        return

    if target_id == settings.owner_id:
        await message.answer("⚠️ Cannot remove owner.")
        return

    ok = await db_remove_admin(target_id)
    if ok:
        _admin_cache.discard(target_id)
        await db_audit(uid, "remove_admin", target_id)
        await message.answer(f"🟢 Admin removed: <code>{target_id}</code>")
    else:
        await message.answer("⚠️ Not an admin.")


@router.callback_query(F.data == "admin:list_admins")
async def cb_list_admins(call: CallbackQuery):
    if not await _require_admin(call):
        return
    admins = await db_get_all_admins()
    text = "📋 <b>Admins</b>\n\n"
    text += f"👑 <code>{settings.owner_id}</code> (Owner)\n"
    for a in admins:
        if a != settings.owner_id:
            text += f"🛡️ <code>{a}</code>\n"
    await call.answer()
    try:
        await call.message.edit_text(text, reply_markup=kb_admin_panel())
    except TelegramBadRequest:
        pass


# ============================================================
# 📊 STATS
# ============================================================

@router.callback_query(F.data == "admin:stats")
async def cb_admin_stats(call: CallbackQuery):
    if not await _require_admin(call):
        return
    await call.answer()
    await cmd_stats(call)


# ============================================================
# 📋 INSTALL LOGS
# ============================================================

@router.callback_query(F.data == "admin:install_logs")
async def cb_install_logs(call: CallbackQuery):
    if not await _require_admin(call):
        return    logs = await db_get_recent_installs(20)
    if not logs:
        await call.answer("No install logs.", show_alert=True)
        return
    text = "📋 <b>Recent Installs (last 20)</b>\n\n"
    for log in logs:
        icon = "🟢" if log.status == "success" else ("🔴" if log.status == "failed" else "⚠️")
        text += (
            f"{icon} <code>{log.user_id}</code> · {log.installer}\n"
            f"   {html_escape(log.module_name)} → {html_escape(log.package_name)}\n"
            f"   {log.installed_at.strftime('%m-%d %H:%M')}\n\n"
        )
    await call.answer()
    try:
        await call.message.edit_text(text[:4000], reply_markup=kb_admin_settings())
    except TelegramBadRequest:
        await call.message.answer(text[:4000], reply_markup=kb_admin_settings())


# ============================================================
# 📜 AUDIT LOGS
# ============================================================

@router.callback_query(F.data == "admin:audit_logs")
async def cb_audit_logs(call: CallbackQuery):
    if not await _require_admin(call):
        return
    logs = await db_get_recent_audits(30)
    if not logs:
        await call.answer("No audit logs.", show_alert=True)
        return
    text = "📜 <b>Recent Audits (last 30)</b>\n\n"
    for l in logs:
        text += (
            f"• <code>{l.admin_id}</code> · <b>{html_escape(l.action)}</b>\n"
            f"   target: <code>{l.target_id or '-'}</code>\n"
            f"   {l.details or ''}\n"
            f"   {l.created_at.strftime('%m-%d %H:%M')}\n\n"
        )
    await call.answer()
    try:
        await call.message.edit_text(text[:4000], reply_markup=kb_admin_settings())
    except TelegramBadRequest:
        await call.message.answer(text[:4000], reply_markup=kb_admin_settings())


# ============================================================
# 🧹 CLEANUP
# ============================================================

@router.callback_query(F.data == "admin:cleanup")
async def cb_cleanup(call: CallbackQuery):
    if not await _require_admin(call):
        return
    await call.answer("🧹 Cleaning...")

    removed_folders = 0
    removed_logs = 0

    for entry in settings.upload_path.iterdir():
        if not entry.is_dir():
            continue
        try:
            # Delete old logs (>7 days)
            for log in entry.glob("*.log"):
                if time.time() - log.stat().st_mtime > 7 * 86400:
                    try:
                        log.unlink()
                        removed_logs += 1
                    except Exception:
                        pass
            # Remove empty folders
            if not any(entry.iterdir()):
                entry.rmdir()
                removed_folders += 1
        except Exception:
            pass

    await db_cleanup_old_approvals(7)

    try:
        await call.message.edit_text(
            f"🧹 <b>Cleanup done</b>\n\n"
            f"🗑️ Empty folders: <b>{removed_folders}</b>\n"
            f"📜 Old logs: <b>{removed_logs}</b>",
            reply_markup=kb_admin_settings(),
        )
    except TelegramBadRequest:
        pass


# ============================================================
# ⚙️ SETTINGS
# ============================================================

@router.callback_query(F.data == "admin:settings")
async def cb_admin_settings(call: CallbackQuery):
    if not await _require_admin(call):
        return
    await call.answer()
    text = (
        "⚙️ <b>Admin Settings</b>\n\n"
        f"🤖 Bot version: <b>3.x</b>\n"
        f"🐍 Python: <b>{platform.python_version()}</b>\n"
        f"💻 OS: <b>{platform.system()} {platform.release()}</b>\n"
        f"👥 Admin count: <b>{len(_admin_cache)}</b>\n"
        f"📁 Upload dir: <code>{settings.upload_dir}</code>\n"
    )
    try:
        await call.message.edit_text(text, reply_markup=kb_admin_settings())
    except TelegramBadRequest:
        await call.message.answer(text, reply_markup=kb_admin_settings())


# ============================================================
# 🎬 WEB KEEP-ALIVE SERVER
# ============================================================

async def _web_index(request):
    return web.Response(
        text="🤖 Hosting Bot is running",
        content_type="text/plain",
    )


async def _web_health(request):
    st = await db_stats()
    return web.json_response({
        "status": "ok",
        "running_scripts": sum(
            1 for k, v in running_processes.items()
            if v['process'].returncode is None
        ),
        "bot_locked": st.is_locked,
        "time": datetime.utcnow().isoformat(),
    })


async def start_web_server():
    """aiohttp keep-alive for Render / Koyeb / Railway"""
    if not settings.enable_web_keepalive:
        logger.info("Web keep-alive disabled")
        return None

    app = web.Application()
    app.router.add_get("/", _web_index)
    app.router.add_get("/health", _web_health)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, settings.host, settings.port)
    await site.start()

    logger.success(f"🌐 Web server live on {settings.host}:{settings.port}")
    return runner


# ============================================================
# 🎯 STARTUP / SHUTDOWN
# ============================================================

_bg_tasks: list[asyncio.Task] = []


async def set_bot_commands():
    """Register commands in Telegram menu"""
    user_cmds = [
        BotCommand(command="start",  description="Start the bot"),
        BotCommand(command="help",   description="Help & commands"),
        BotCommand(command="upload", description="Upload a bot file"),
        BotCommand(command="files",  description="Manage my files"),
        BotCommand(command="install", description="Install a module"),
        BotCommand(command="myinfo", description="My account info"),
        BotCommand(command="speed",  description="Bot speed test"),
        BotCommand(command="stats",  description="Bot statistics"),
        BotCommand(command="cancel", description="Cancel current action"),
    ]
    await bot.set_my_commands(user_cmds, scope=BotCommandScopeDefault())

    admin_cmds = [
        BotCommand(command="admin",     description="Admin panel"),
        BotCommand(command="stats",     description="Bot statistics"),
        BotCommand(command="broadcast", description="Broadcast message"),
        BotCommand(command="lock",      description="Lock bot"),
        BotCommand(command="unlock",    description="Unlock bot"),
        BotCommand(command="runall",    description="Run all scripts"),
    ]

    from aiogram.types import BotCommandScopeChat
    admin_scopes = {settings.owner_id} | _admin_cache
    for admin_id in admin_scopes:
        try:
            await bot.set_my_commands(admin_cmds, scope=BotCommandScopeChat(chat_id=admin_id))
        except Exception:
            pass


async def on_startup():
    """Called once bot starts"""
    logger.info("=" * 60)
    logger.info("🚀 Bot starting up")

    # Init DB
    await init_db()

    # Load admins
    await refresh_admin_cache()

    # Cleanup old approvals on boot
    await db_cleanup_old_approvals(7)

    # Web keep-alive
    global _web_runner
    _web_runner = await start_web_server()

    # Register commands
    await set_bot_commands()

    # Background periodic task
    _bg_tasks.append(asyncio.create_task(periodic_cleanup_task()))

    me = await bot.get_me()
    logger.success(f"✅ Ready: @{me.username}")
    logger.info(f"👑 Owner: {settings.owner_id}")
    logger.info(f"🛡️ Admins: {len(_admin_cache)}")
    logger.info("=" * 60)


async def on_shutdown():
    """Called once bot is stopping"""
    logger.warning("🛑 Shutting down...")

    # Stop background tasks
    for t in _bg_tasks:
        t.cancel()
        try:
            await t
        except (asyncio.CancelledError, Exception):
            pass

    # Kill running scripts
    await cleanup_all_processes()

    # Close web server
    global _web_runner
    if _web_runner:
        try:
            await _web_runner.cleanup()
        except Exception:
            pass

    # Close DB
    await close_db()

    logger.warning("👋 Shutdown complete")


# ============================================================
# 🎬 MAIN
# ============================================================

def _install_signal_handlers():
    """Graceful shutdown on SIGTERM / SIGINT"""
    loop = asyncio.get_event_loop()

    def _handler():
        logger.warning("Received shutdown signal")
        for task in asyncio.all_tasks(loop):
            task.cancel()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handler)
        except NotImplementedError:
            # Windows
            pass


async def _main_async():
    """Main async entry"""
    _install_signal_handlers()

    # Startup hook
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)

    # Clean webhook (safety)
    try:
        await bot.delete_webhook(drop_pending_updates=True)
    except Exception as e:
        logger.warning(f"delete_webhook: {e}")

    # Start polling
    logger.info("🎯 Starting polling...")
    try:
        await dp.start_polling(
            bot,
            allowed_updates=dp.resolve_used_update_types(),
        )
    except asyncio.CancelledError:
        logger.warning("Polling cancelled")
    finally:
        await on_shutdown()


def main():
    try:
        asyncio.run(_main_async())
    except KeyboardInterrupt:
        logger.warning("Interrupted by user")
    except Exception as e:
        logger.critical(f"💥 Fatal error: {e}")
        logger.exception(e)
        sys.exit(1)


if __name__ == "__main__":
    main()


# ============================================================
# 🗄️ END PART 5  —  FILE COMPLETE ✅
# ============================================================