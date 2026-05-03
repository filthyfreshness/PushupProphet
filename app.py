# main.py
# Burpee Bitch — Sassy, Foul-Mouthed, Zero-Excuses Edition
# ------------------------------------------------------

import os
import re
import ssl
import html
import asyncio
import logging
import random
import datetime as dt
from pathlib import Path
from typing import Dict, Optional, List, Tuple
from collections import deque
from contextlib import asynccontextmanager

import httpx
import uvicorn
from fastapi import FastAPI, Response
from dotenv import load_dotenv
from pytz import timezone
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, PollAnswer
from aiogram.filters import Command, CommandStart
from aiogram.enums.parse_mode import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import TelegramBadRequest, TelegramNetworkError

from sqlalchemy import (
    Column, BigInteger, String, Integer, Boolean, DateTime, Index, select, update, func, event
)
from sqlalchemy.orm import declarative_base
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

# ------------------------ App & config ------------------------

logger = logging.getLogger("burpee-bitch")
logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")

DOTENV_PATH = Path(__file__).with_name(".env")
load_dotenv(dotenv_path=DOTENV_PATH)

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
if not BOT_TOKEN:
    raise SystemExit("ERROR: BOT_TOKEN missing (env / .env).")

# Provider Agnostic AI Config (Optimized for Venice AI)
AI_API_KEY = os.getenv("AI_API_KEY", os.getenv("OPENAI_API_KEY", "")).strip()
AI_MODEL = os.getenv("AI_MODEL", os.getenv("OPENAI_MODEL", "gpt-4o-mini")).strip()
AI_TEMPERATURE = float(os.getenv("AI_TEMPERATURE", os.getenv("OPENAI_TEMPERATURE", "0.7")))
AI_USE_RESPONSES = os.getenv("AI_USE_RESPONSES", os.getenv("OPENAI_USE_RESPONSES", "0")).strip() == "1"
AI_BASE_URL = os.getenv("AI_BASE_URL", os.getenv("OPENAI_BASE_URL", "https://api.openai.com")).strip()

logger.info("[AI] Using key suffix: %s", (AI_API_KEY or "")[-8:])
logger.info("[AI] Using model: %s", AI_MODEL)

TZ = timezone("Europe/Stockholm")
_sysrand = random.SystemRandom()

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
scheduler = AsyncIOScheduler(timezone=TZ)

# Admins & defaults
_ADMIN_IDS = {int(x) for x in os.getenv("ADMIN_USER_IDS", "").split(",") if x.strip().isdigit()}
_DEFAULT_TARGET_CHAT = int(os.getenv("ADMIN_DEFAULT_CHAT_ID", "0") or "0")

DAILY_TEXT = "WAKE THE FUCK UP. The chain starts now. Drop your pathetic excuses and get to the floor. Don't make me come over there."
WINDOW_START = 7
WINDOW_END = 22

BOT_ID: Optional[int] = None
BOT_USERNAME: Optional[str] = None

CHAT_LAST_BOT_TS: Dict[int, float] = {}
CHAT_CONVO_USER: Dict[int, int] = {}
CONVO_WINDOW_S = 30


# ------------------------ Database ---------------------------

DB_URL = os.getenv("DATABASE_URL", "").strip()

def _to_async_url(url: str) -> str:
    if not url:
        return "sqlite+aiosqlite:///./local-dev.db"
    if url.startswith("postgres://"):
        url = "postgresql+asyncpg://" + url[len("postgres://"):]
    elif url.startswith("postgresql://"):
        url = "postgresql+asyncpg://" + url[len("postgresql://"):]
    if "?" in url:
        url = url.split("?", 1)[0]
    return url

ASYNC_DB_URL = _to_async_url(DB_URL)
Base = declarative_base()

class Counter(Base):
    __tablename__ = "counters"
    chat_id = Column(BigInteger, primary_key=True)
    user_id = Column(BigInteger, primary_key=True)
    metric  = Column(String(32), primary_key=True)
    count   = Column(Integer, nullable=False, default=0)
    __table_args__ = (Index("ix_counters_chat_metric_count", "chat_id", "metric", "count"),)

class UserName(Base):
    __tablename__ = "user_names"
    chat_id    = Column(BigInteger, primary_key=True)
    user_id    = Column(BigInteger, primary_key=True)
    first_name = Column(String(128), nullable=True)
    username   = Column(String(128), nullable=True)
    last_seen  = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

class ChatSettings(Base):
    __tablename__ = "chat_settings"
    chat_id       = Column(BigInteger, primary_key=True)
    ai_enabled    = Column(Boolean, nullable=False, default=False)
    chain_enabled = Column(Boolean, nullable=False, default=True)
    changed_at    = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    __table_args__ = (Index("ix_chatsettings_chain_enabled", "chain_enabled"),)

class ProgramSettings(Base):
    __tablename__ = "program_settings"
    id          = Column(Integer, primary_key=True, default=1)
    start_date  = Column(DateTime(timezone=True), nullable=False, default=func.now())
    total_days  = Column(Integer, nullable=False, default=14)
    updated_at  = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

class UserDailyStats(Base):
    __tablename__ = "user_daily_stats"
    id                = Column(Integer, primary_key=True, autoincrement=True)
