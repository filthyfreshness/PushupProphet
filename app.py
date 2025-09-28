# main.py
# Pushup Prophet — clean, single-source-of-truth version
# ------------------------------------------------------
# Fixes implemented:
# 1) DB model == code (Option A: status column only: pending|sent|canceled)
# 2) DB-backed scheduler only (removed all in-memory duplicates)
# 3) No undefined helpers; job IDs are consistent (sch_{id})
# 4) should_ai_reply() is synchronous (and not awaited)
# 5) Single on_startup/on_shutdown; consistent imports; extra comments
# 6) Safer OpenAI call with retries; Responses API by default

import os
import re
import ssl
import html
import asyncio
import logging
import random
import datetime as dt
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, List, Tuple
from collections import deque

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

app = FastAPI()
logger = logging.getLogger("pushup-prophet")
logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")

DOTENV_PATH = Path(__file__).with_name(".env")
load_dotenv(dotenv_path=DOTENV_PATH)

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
if not BOT_TOKEN:
    raise SystemExit("ERROR: BOT_TOKEN missing (env / .env).")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip()
OPENAI_TEMPERATURE = float(os.getenv("OPENAI_TEMPERATURE", "0.6"))
OPENAI_USE_RESPONSES = os.getenv("OPENAI_USE_RESPONSES", "1").strip() == "1"
OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "").strip() or "https://api.openai.com"

logger.info("[AI] Using key suffix: %s", (OPENAI_API_KEY or "")[-8:])
logger.info("[AI] Responses enabled: %s", OPENAI_USE_RESPONSES)


logger.info("[AI] Using key suffix: %s", (OPENAI_API_KEY or "")[-8:])
logger.info("[AI] Using model: %s", OPENAI_MODEL)
logger.info("[AI] Responses enabled: %s", os.getenv("OPENAI_USE_RESPONSES","1"))



TZ = timezone("Europe/Stockholm")
_sysrand = random.SystemRandom()

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
scheduler = AsyncIOScheduler(timezone=TZ)

# Admins & defaults
_ADMIN_IDS = {int(x) for x in os.getenv("ADMIN_USER_IDS", "").split(",") if x.strip().isdigit()}
_DEFAULT_TARGET_CHAT = int(os.getenv("ADMIN_DEFAULT_CHAT_ID", "0") or "0")

# Random daily message window (for your chain message feature)
DAILY_TEXT = "THE FORGIVENESS CHAIN BEGINS NOW. Lay down excuses and ascend. May the power of Push be with you."
WINDOW_START = 7
WINDOW_END = 22

# Bot ID cache
BOT_ID: Optional[int] = None

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
    metric  = Column(String(32), primary_key=True)  # "thanks" | "apology" | "insult" | "mention"
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
    chat_id    = Column(BigInteger, primary_key=True)
    ai_enabled = Column(Boolean, nullable=False, default=False)
    changed_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

class ScheduledMessage(Base):
    __tablename__ = "scheduled_messages"
    id         = Column(Integer, primary_key=True, autoincrement=True)
    chat_id    = Column(BigInteger, nullable=False)                # where to post
    user_id    = Column(BigInteger, nullable=False)                # who scheduled it
    text       = Column(String(4096), nullable=False)
    run_at     = Column(DateTime(timezone=True), nullable=False)   # when to send (UTC or aware)
    status     = Column(String(16), nullable=False, default="pending")  # pending|sent|canceled
    created_at = Column(DateTime(timezone=True), server_default=func.now())

# ------------ Program / Stats models ------------
class ProgramSettings(Base):
    """
    Singleton (id=1) storing the start date and total length of the challenge.
    """
    __tablename__ = "program_settings"
    id          = Column(Integer, primary_key=True, default=1)
    start_date  = Column(DateTime(timezone=True), nullable=False, default=func.now())
    total_days  = Column(Integer, nullable=False, default=100)
    updated_at  = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class UserDailyStats(Base):
    """
    Per-user, per-day counters with a persisted secret insult threshold (2..6).
    date = Stockholm day at local midnight, stored as UTC midnight for stability.
    """
    __tablename__ = "user_daily_stats"

    id                = Column(Integer, primary_key=True, autoincrement=True)
    chat_id           = Column(BigInteger, nullable=False)
    user_id           = Column(BigInteger, nullable=False)
    date              = Column(DateTime(timezone=True), nullable=False)
    thanks_count      = Column(Integer, nullable=False, default=0)
    apology_count     = Column(Integer, nullable=False, default=0)
    insult_count      = Column(Integer, nullable=False, default=0)
    insult_threshold  = Column(Integer, nullable=True)  # 2..6 set on first insult of the day
    mercy_used        = Column(Boolean, nullable=False, default=False)
    updated_at        = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        Index("ux_user_daily_unique", "chat_id", "user_id", "date", unique=True),
        Index("ix_user_daily_chat_date", "chat_id", "date"),
    )


class PenaltyLog(Base):
    """
    Audit log of penalties/rewards the Prophet declares.
    """
    __tablename__ = "penalty_log"

    id         = Column(Integer, primary_key=True, autoincrement=True)
    chat_id    = Column(BigInteger, nullable=False)
    user_id    = Column(BigInteger, nullable=False)
    kind       = Column(String(32), nullable=False)   # 'thanks_overuse' | 'insult' | 'fake_apology' | 'gratitude_boon'
    amount_kr  = Column(Integer, nullable=False)      # +15, -10, etc.
    text       = Column(String(512), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        Index("ix_penalty_chat_date", "chat_id", "created_at"),
        Index("ix_penalty_user_date", "user_id", "created_at"),
    )

def _is_pg_url(async_url: str) -> bool:
    return async_url.startswith("postgresql+asyncpg://")

ssl_ctx = ssl.create_default_context() if _is_pg_url(ASYNC_DB_URL) else None
engine = create_async_engine(
    ASYNC_DB_URL,
    pool_pre_ping=True,
    connect_args={"ssl": ssl_ctx} if ssl_ctx else {},
)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

# SQLite: enable WAL for better concurrency
if ASYNC_DB_URL.startswith("sqlite+aiosqlite"):
    @event.listens_for(engine.sync_engine, "connect")
    def _set_sqlite_pragma(dbapi_conn, _):
        try:
            dbapi_conn.execute("PRAGMA journal_mode=WAL;")
        except Exception:
            pass

def _dialect_insert():
    return pg_insert if _is_pg_url(ASYNC_DB_URL) else sqlite_insert

async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)



# ------------------------ DB helpers -------------------------

async def incr_counter(chat_id: int, user_id: int, metric: str, delta: int = 1) -> None:
    ins = _dialect_insert()(Counter).values(
        chat_id=chat_id, user_id=user_id, metric=metric, count=delta
    )
    stmt = ins.on_conflict_do_update(
        index_elements=[Counter.chat_id.name, Counter.user_id.name, Counter.metric.name],
        set_={"count": Counter.count + delta},
    )
    async with AsyncSessionLocal() as s:
        await s.execute(stmt)
        await s.commit()

async def upsert_username(chat_id: int, user_id: int, first_name: Optional[str], username: Optional[str]) -> None:
    ins = _dialect_insert()(UserName).values(
        chat_id=chat_id, user_id=user_id,
        first_name=first_name or None, username=username or None
    )
    stmt = ins.on_conflict_do_update(
        index_elements=[UserName.chat_id.name, UserName.user_id.name],
        set_={"first_name": ins.excluded.first_name, "username": ins.excluded.username, "last_seen": func.now()},
    )
    async with AsyncSessionLocal() as s:
        await s.execute(stmt)
        await s.commit()

async def get_ai_enabled(chat_id: int) -> bool:
    async with AsyncSessionLocal() as s:
        res = await s.execute(select(ChatSettings.ai_enabled).where(ChatSettings.chat_id == chat_id))
        row = res.first()
        return bool(row[0]) if row else False

async def set_ai_enabled(chat_id: int, enabled: bool) -> None:
    ins = _dialect_insert()(ChatSettings).values(chat_id=chat_id, ai_enabled=enabled)
    stmt = ins.on_conflict_do_update(index_elements=[ChatSettings.chat_id.name], set_={"ai_enabled": enabled})
    async with AsyncSessionLocal() as s:
        await s.execute(stmt)
        await s.commit()

# ---------- Program day helpers ----------
def _local_midnight_utc(today_local: Optional[dt.date] = None) -> dt.datetime:
    """UTC instant corresponding to Stockholm local midnight."""
    if today_local is None:
        today_local = dt.datetime.now(TZ).date()
    local_midnight = dt.datetime(today_local.year, today_local.month, today_local.day, 0, 0, tzinfo=TZ)
    return local_midnight.astimezone(dt.timezone.utc)

async def ensure_program_settings():
    """Create/refresh singleton from env if missing."""
    start_env = os.getenv("PROGRAM_START_DATE", "").strip()
    total_env = int(os.getenv("PROGRAM_TOTAL_DAYS", "100") or "100")

    if start_env:
        try:
            y, m, d = [int(x) for x in start_env.split("-")]
            start_local = dt.datetime(y, m, d, 0, 0, tzinfo=TZ)
        except Exception:
            start_local = dt.datetime.now(TZ).replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        start_local = dt.datetime.now(TZ).replace(hour=0, minute=0, second=0, microsecond=0)

    start_utc = start_local.astimezone(dt.timezone.utc)

    async with AsyncSessionLocal() as s:
        row = await s.get(ProgramSettings, 1)
        if not row:
            s.add(ProgramSettings(id=1, start_date=start_utc, total_days=total_env))
        else:
            if start_env:
                row.start_date = start_utc
            row.total_days = total_env
        await s.commit()

async def get_program_day() -> Tuple[int, int]:
    """Return (current_day, total_days) based on Stockholm local time."""
    async with AsyncSessionLocal() as s:
        row = await s.get(ProgramSettings, 1)
        if not row:
            await ensure_program_settings()
            row = await s.get(ProgramSettings, 1)

        start_local = row.start_date.astimezone(TZ)
        today_local = dt.datetime.now(TZ)
        day_index = (today_local.date() - start_local.date()).days + 1
        return max(1, day_index), (row.total_days or 100)


# ---------- Per-user daily stats (persisted) ----------
async def _get_or_create_user_daily(session: AsyncSession, chat_id: int, user_id: int) -> UserDailyStats:
    day_utc = _local_midnight_utc()
    res = await session.execute(
        select(UserDailyStats)
        .where(UserDailyStats.chat_id == chat_id,
               UserDailyStats.user_id == user_id,
               UserDailyStats.date == day_utc)
        .limit(1)
    )
    row = res.scalar_one_or_none()
    if row:
        return row
    row = UserDailyStats(chat_id=chat_id, user_id=user_id, date=day_utc)
    session.add(row)
    await session.flush()
    return row

async def bump_thanks_db(chat_id: int, user_id: int) -> int:
    async with AsyncSessionLocal() as s:
        row = await _get_or_create_user_daily(s, chat_id, user_id)
        row.thanks_count += 1
        await s.commit()
        return row.thanks_count

async def bump_apology_db(chat_id: int, user_id: int) -> int:
    async with AsyncSessionLocal() as s:
        row = await _get_or_create_user_daily(s, chat_id, user_id)
        row.apology_count += 1
        await s.commit()
        return row.apology_count

async def bump_insult_db(chat_id: int, user_id: int) -> Tuple[int, int]:
    """Returns (insult_count_today, hidden_threshold_2_to_6)."""
    async with AsyncSessionLocal() as s:
        row = await _get_or_create_user_daily(s, chat_id, user_id)
        if row.insult_threshold is None:
            row.insult_threshold = _sysrand.randint(2, 6)
        row.insult_count += 1
        await s.commit()
        return row.insult_count, row.insult_threshold

async def set_mercy_used(chat_id: int, user_id: int) -> None:
    async with AsyncSessionLocal() as s:
        row = await _get_or_create_user_daily(s, chat_id, user_id)
        row.mercy_used = True
        await s.commit()

async def log_penalty(chat_id: int, user_id: int, kind: str, amount_kr: int, text: Optional[str] = None) -> None:
    async with AsyncSessionLocal() as s:
        s.add(PenaltyLog(chat_id=chat_id, user_id=user_id, kind=kind, amount_kr=amount_kr, text=text or ""))
        await s.commit()


# ------------------------ Scheduler (DB-backed) ---------------

def _job_id_for(msg_id: int) -> str:
    return f"sch_{msg_id}"

async def _deliver_scheduled_message(msg_id: int):
    """Send one scheduled message and mark it sent (idempotent)."""
    async with AsyncSessionLocal() as s:
        row = await s.get(ScheduledMessage, msg_id)
        if not row or row.status != "pending":
            return
        try:
            await bot.send_message(row.chat_id, row.text)
            row.status = "sent"
            await s.commit()
        except Exception:
            logger.exception("Failed sending scheduled message id=%s", msg_id)

async def load_and_schedule_pending():
    """Reschedule all pending future jobs from DB (on startup)."""
    now_utc = dt.datetime.now(dt.timezone.utc)
    lookback = now_utc - dt.timedelta(minutes=10)  # safety cushion

    async with AsyncSessionLocal() as s:
        res = await s.execute(
            select(ScheduledMessage.id, ScheduledMessage.run_at)
            .where(ScheduledMessage.status == "pending", ScheduledMessage.run_at >= lookback)
        )
        rows = res.all()

    count = 0
    for msg_id, run_at in rows:
        try:
            scheduler.add_job(
                _deliver_scheduled_message, "date",
                run_date=run_at, args=[msg_id], id=_job_id_for(msg_id), replace_existing=True
            )
            count += 1
        except Exception:
            logger.exception("Failed to schedule message id=%s", msg_id)

    logger.info("Scheduled %d pending messages from DB.", count)

# ------------------------ Utilities --------------------------

def _is_admin(user_id: int) -> bool: return user_id in _ADMIN_IDS
def _is_private_chat(msg: Message) -> bool: return getattr(msg.chat, "type", "") == "private"

async def _delete_soon(chat_id: int, message_id: int, delay: float = 2.0):
    try:
        await asyncio.sleep(delay)
        await bot.delete_message(chat_id, message_id)
    except Exception:
        pass

# ------------------------ AI layer ---------------------------

DEFAULT_PROPHET_SYSTEM = (
    "You are the Pushup Prophet: wise, concise, kind but stern, poetic but practical. "
    "Keep replies short for group chat. Offer form cues, consistency rituals, and supportive accountability. "
    "Avoid medical claims. Stay on-topic; if off-topic, gently steer back to training, habits, or group rituals."
)
PROPHET_SYSTEM = os.getenv("OPENAI_SYSTEM_PROMPT", DEFAULT_PROPHET_SYSTEM).replace("\\n", "\n")
ROAST_BLOCK = os.getenv("OPENAI_ROAST_BLOCK", "").replace("\\n", "\n")
if ROAST_BLOCK:
    PROPHET_SYSTEM = f"{PROPHET_SYSTEM}\n\n{ROAST_BLOCK}"

# === Strict community policy for AI (conceal thresholds & amounts where required) ===
POLICY_BLOCK = """
Enforce these rules strictly. Do not reveal hidden thresholds or remaining allowances. Never write “no penalties today.”

— Gratitude (“thanks”, “tack”, etc.)
  • Always reply to thanks.
  • If thanks are overused today (as indicated by the context flags), DO NOT reveal any penalty details or amounts.
    Instead, give a stern verbal warning like “Enough praise—let deeds speak.” Do not mention numbers, thresholds,
    or remaining strikes. Do not say “you have X left” or similar.
  • A gratitude boon (−10 kr) is allowed only if the context flag `gratitude.reward_minus_10` is true.
  • Never grant blanket exemptions or say “no penalties today.”

— Apologies
  • Accept sincere apologies briefly.
  • If an apology includes insults/sarcasm, treat as a fake apology: follow the insult rules below.

— Insults / slurs
  • The first offense today must be a warning (no amounts disclosed).
  • When the context indicates punishment is due, apply +15 kr to the pot. Do not reveal how many strikes triggered it.
  • Do not disclose thresholds or remaining strikes. Do not hint at when penalties will occur.

— Prophet summon
  • If they summon the Prophet (mention “prophet” etc.), answer briefly and on-topic.

— Pushup phrasing
  • Users already have a fixed daily baseline. When referencing additional work or relief,
    say “N extra pushups” or “N less pushups,” not “you owe N pushups.”

General style:
  • Keep replies compact for group chats.
  • When a penalty or reward must be stated (e.g., insult punishment, or a gratitude boon),
    include it clearly (“+15 kr to the pot”, “−10 kr boon”), but NEVER reveal hidden thresholds.
    
"""
PROPHET_SYSTEM = f"{PROPHET_SYSTEM}\n\n{POLICY_BLOCK}"


PROPHET_SYSTEM = f"{PROPHET_SYSTEM}\n\n{POLICY_BLOCK}"

_AI_COOLDOWN_S = int(os.getenv("AI_COOLDOWN_S", "15"))
_last_ai_reply_at: Dict[int, float] = {}

def _cooldown_ok(user_id: int) -> bool:
    now = dt.datetime.now().timestamp()
    last = _last_ai_reply_at.get(user_id, 0.0)
    if now - last >= _AI_COOLDOWN_S:
        _last_ai_reply_at[user_id] = now
        return True
    return False

async def ai_reply(system: str, messages: List[dict], model: str = OPENAI_MODEL) -> str:
    """OpenAI Responses API with simple retry; returns text ('' on failure)."""
    if not OPENAI_API_KEY:
        return ""
    delays = [0, 0.8, 2.0]
    for i, delay in enumerate(delays):
        if delay:
            await asyncio.sleep(delay)
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                r = await client.post(
                    "https://api.openai.com/v1/responses",
                    headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
                    json={
                        "model": model,
                        "input": [{"role": "system", "content": system}] + messages,
                        "temperature": OPENAI_TEMPERATURE,
                        "max_output_tokens": 180,
                    },
                )
                r.raise_for_status()
                data = r.json()
                # Extract first text chunk
                for item in data.get("output", []):
                    if item.get("type") == "message":
                        for p in item.get("content", []):
                            if p.get("type") == "output_text":
                                txt = (p.get("text") or "").strip()
                                if txt:
                                    return txt
                # Fallback for alternate shapes
                txt = ""
                if isinstance(data.get("text"), dict):
                    txt = (data.get("text", {}).get("value") or "").strip()
                elif isinstance(data.get("text"), str):
                    txt = data["text"].strip()
                return txt
        except Exception as e:
            logger.warning(f"[AI] attempt {i+1} failed: {e}")
    return ""

# Triggers and patterns
def _normalize_text(t: str) -> str:
    t = re.sub(r"[\u200b-\u200f\u202a-\u202e]", "", t or "")
    return t.lower()

# -------- Gratitude & Apology patterns + helpers (REPLACE your current ones) --------
THANKS_RE = re.compile(
    r"\b(thank(?:\s*you)?|thanks|thx|ty|tack(?:\s*så\s*mycket)?)\b",
    re.IGNORECASE
)

APOLOGY_RE = re.compile(
    r"\b("
    r"sorry|i\s*(?:am|’m|'m)\s*sorry|i\s*apolog(?:ise|ize)|apologies|apology|"
    r"my\s*bad|my\s*fault|i\s*was\s*wrong|didn'?t\s*mean|forgive\s*me|"
    r"förlåt|ursäkta|jag\s*är\s*ledsen|ber\s*om\s*ursäkt|mitt\s*fel"
    r")\b",
    re.IGNORECASE
)

# ---- Blessings & Absolution text ----
BLESSINGS = [
    "Your thanks is heard, {name}. May your shoulders carry light burdens and your will grow heavy with resolve.",
    "Gratitude received, {name}. Walk with steady breath; strength will meet you there.",
    "I accept your thanks, {name}. May your last rep be your cleanest.",
    "Thanks noted, {name}. Keep the vow; the vow will keep you.",
]
APOLOGY_RESPONSES = [
    "Your apology is received, {name}. Mercy given; standard unchanged—meet it.",
    "I accept your apology, {name}. Make it right in form and in habit.",
    "Apology taken in, {name}. Rise cleaner; let your next set speak.",
    "Grace extends to you, {name}. Guard your form; guard your word.",
]

def _compose_blessing(user_name: Optional[str]) -> str:
    safe = html.escape(user_name or "friend")
    return _sysrand.choice(BLESSINGS).format(name=safe)

def _compose_absolution(user_name: Optional[str]) -> str:
    safe = html.escape(user_name or "friend")
    return _sysrand.choice(APOLOGY_RESPONSES).format(name=safe)

# ---- Gratitude daily limit + reward/penalty state ----
_gratitude_uses: Dict[int, tuple[dt.date, int]] = {}  # user_id -> (date, count)

def _today_stockholm_date() -> dt.date:
    return dt.datetime.now(TZ).date()

def _gratitude_inc_and_get(user_id: int) -> int:
    today = _today_stockholm_date()
    state = _gratitude_uses.get(user_id)
    if not state or state[0] != today:
        _gratitude_uses[user_id] = (today, 0)
    count = _gratitude_uses[user_id][1] + 1
    _gratitude_uses[user_id] = (today, count)
    return count

def _compose_gratitude_penalty(user_name: Optional[str]) -> str:
    safe = html.escape(user_name or "friend")
    return (
        f"Your gratitude pours too freely today, {safe}. The floor is not fooled by sugar on the tongue.\n"
        f"Let your deeds do the thanking.\n\n"
        f"<b>Edict:</b> Lay <b>10 kr</b> in the pot and return with a steadier heart."
    )

def _compose_gratitude_reward() -> str:
    # 5% chance message
    return "🪙 <b>Favor of Gratitude</b> — Deduct <b>20 kr</b> from your debt for your loyalty."

MENTION_RE = r"(?:\bpush\s*up\s*prophet\b|\bpushup\s*prophet\b|\bprophet\b|\bbot\b)"
INSULT_WORDS = (
    r"(?:fuck(?:ing|er|ed)?|f\*+ck|shit|crap|trash|garbage|bs|sucks?|stupid|idiot|moron|dumb(?:ass)?|"
    r"loser|pathetic|awful|terrible|useless|worthless|annoying|cringe|fraud|fake|clown|"
    r"bitch|ass(?:hole|hat|clown)?|dick(?:head)?|prick|jerk|wank(?:er)?|twat|tosser|dipshit|jackass|motherfucker|mf)"
)
DIRECT_2P = r"(?:fuck\s*(?:you|u|ya)|screw\s*you|stfu|shut\s*up|you\s*(?:suck|are\s*(?:stupid|dumb|useless)))"
INSULT_RE = re.compile(
    rf"(?:(?:{MENTION_RE}).*?(?:{INSULT_WORDS})|(?:{INSULT_WORDS}).*?(?:{MENTION_RE}))|(?:{DIRECT_2P})",
    re.IGNORECASE
)
SUMMON_PATTERN = re.compile(r"\b(pushup\s*prophet|prophet)\b", re.IGNORECASE)

def _matches_wisdom_nat(t: str) -> bool:
    patterns = [
        r"\bshare\s+wisdom\b", r"\bgive\s+(?:me\s+)?wisdom\b", r"\bsay\s+(?:something\s+)?wise\b",
        r"\bwisdom\s+please\b", r"\bteach\s+me\b", r"\bi\s+seek\s+wisdom\b",
        r"\bprophet[,!\s]*\s*(?:share|give|drop)\s+(?:some\s+)?wisdom\b", r"\bdrop\s+(?:some\s+)?wisdom\b",
    ]
    return any(re.search(p, t, re.IGNORECASE) for p in patterns)

def should_ai_reply(msg: Message) -> bool:
    """Pure sync heuristic; DO NOT await this."""
    t = (msg.text or "").strip()
    if not t or t.startswith("/"):
        return False
    if THANKS_RE.search(t) or APOLOGY_RE.search(t) or INSULT_RE.search(_normalize_text(t)) \
       or _matches_wisdom_nat(t):
        return False
    if SUMMON_PATTERN.search(t):
        return True
    if msg.reply_to_message and msg.reply_to_message.from_user and BOT_ID \
       and msg.reply_to_message.from_user.id == BOT_ID:
        return True
    if re.search(r"\b(help|advice|how do i|what should i)\b", t, re.IGNORECASE) and _sysrand.random() < 0.07:
        return True
    return False

# ------------------------ Handlers ---------------------------

@dp.message(CommandStart())
async def start_cmd(msg: Message):
    await msg.answer(
        "I am the Pushup Prophet.\n\n"
        "AI controls:\n"
        "• /enable_ai — allow AI replies in this chat\n"
        "• /disable_ai — stop AI replies in this chat\n"
        "• /ai_status — show AI status\n\n"
        "Scheduling (admin, DM):\n"
        "• /schedule_once 2025-10-01 18:30 | Text\n"
        "• /schedule_once -1001234567890 2025-10-01 18:30 | Text\n"
        "• /schedule_many (then paste multiple lines)\n"
        "• /schedule_list [optional_chat_id]\n"
        "• /schedule_cancel <id>\n"
    )

@dp.message(Command("help"))
async def help_cmd(msg: Message): await start_cmd(msg)

@dp.message(Command("ai_diag"))
async def ai_diag_cmd(msg: Message):
    enabled = await get_ai_enabled(msg.chat.id)
    await msg.answer(
        "AI diagnostics\n"
        f"chat_id: {msg.chat.id}\n"
        f"enabled: {enabled}\n"
        f"model: {OPENAI_MODEL}\n"
        f"OPENAI_API_KEY set: {bool(OPENAI_API_KEY)}\n"
        f"OPENAI_USE_RESPONSES: {OPENAI_USE_RESPONSES}\n"
        f"OPENAI_BASE_URL: {OPENAI_BASE_URL}"
    )

@dp.message(Command("ai_ping"))
async def ai_ping_cmd(msg: Message):
    reply = await ai_reply(PROPHET_SYSTEM, [
        {"role": "user", "content": "Give me one crisp pushup cue."}
    ])
    if reply:
        await msg.answer(f"AI OK:\n{reply}", disable_web_page_preview=True, parse_mode=None)
    else:
        await msg.answer("AI call failed. Check logs and OPENAI_API_KEY / OPENAI_MODEL.")

@dp.message(Command("ai_raw"))
async def ai_raw_cmd(msg: Message):
    base = os.getenv("OPENAI_BASE_URL", "").strip() or "https://api.openai.com"
    key = os.getenv("OPENAI_API_KEY", "").strip()
    model = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip()
    if not key:
        await msg.answer("No OPENAI_API_KEY set.")
        return
    try:
        async with httpx.AsyncClient(timeout=25) as client:
            r = await client.post(
                f"{base}/v1/chat/completions",
                headers={"Authorization": f"Bearer {key}"},
                json={
                    "model": model,
                    "messages": [{"role": "user", "content": "hi"}],
                    "max_tokens": 5,
                },
            )
        body = r.text
        if len(body) > 800: body = body[:800] + "…"
        await msg.answer(f"Status: {r.status_code}\nBody:\n<pre>{html.escape(body)}</pre>")
    except Exception as e:
        await msg.answer(f"Request error: {e!r}")


@dp.message(Command("ai_status"))
async def ai_status_cmd(msg: Message):
    enabled = await get_ai_enabled(msg.chat.id)
    await msg.answer(f"AI status: {'Enabled ✅' if enabled else 'Disabled 🛑'}")

@dp.message(Command("enable_ai"))
async def enable_ai_cmd(msg: Message):
    await set_ai_enabled(msg.chat.id, True)
    await msg.answer("🤖 AI replies enabled for this chat.")

@dp.message(Command("disable_ai"))
async def disable_ai_cmd(msg: Message):
    await set_ai_enabled(msg.chat.id, False)
    await msg.answer("🛑 AI replies disabled for this chat.")

@dp.message(Command("chatid"))
async def chatid_cmd(msg: Message):
    await msg.answer(f"Chat ID: <code>{msg.chat.id}</code>")

# --- Scheduling (DB-backed, admin-only, use in private chat) ---

def _parse_when_and_target(raw: str) -> Tuple[Optional[int], Optional[dt.datetime], str]:
    """Parses '[-100... ]YYYY-MM-DD HH:MM | text' → (chat_id or None, aware_local_dt or None, text)."""
    if "|" not in raw:
        return None, None, ""
    left, text = [p.strip() for p in raw.split("|", 1)]
    parts = left.split()
    if parts and parts[0].lstrip("-").isdigit() and len(parts) >= 3:
        target_chat_id = int(parts[0])
        when_part = " ".join(parts[1:3])
    else:
        target_chat_id = _DEFAULT_TARGET_CHAT or None
        when_part = " ".join(parts[:2]) if len(parts) >= 2 else ""
    try:
        dt_local = dt.datetime.strptime(when_part, "%Y-%m-%d %H:%M")
        run_at_local = TZ.localize(dt_local)
    except Exception:
        return target_chat_id, None, text
    return target_chat_id, run_at_local, text

@dp.message(Command("schedule_once"))
async def schedule_once_cmd(msg: Message):
    if not _is_admin(msg.from_user.id):
        return
    if not _is_private_chat(msg):
        asyncio.create_task(_delete_soon(msg.chat.id, msg.message_id, 0.1))
        m = await msg.answer("Use this command in a private chat with me.")
        asyncio.create_task(_delete_soon(m.chat.id, m.message_id, 1.5))
        return

    raw = re.sub(r"^/schedule_once(@\w+)?\s*", "", (msg.text or ""), flags=re.IGNORECASE).strip()
    chat_id, run_at_local, text = _parse_when_and_target(raw)

    if not chat_id:
        return await msg.answer("No target chat set. Define ADMIN_DEFAULT_CHAT_ID or include chat id.")
    if not run_at_local:
        return await msg.answer("Time must be YYYY-MM-DD HH:MM (Stockholm).")
    if not text:
        return await msg.answer("Message text cannot be empty.")

    run_at_utc = run_at_local.astimezone(dt.timezone.utc)

    # Create DB row
    async with AsyncSessionLocal() as s:
        row = ScheduledMessage(
            chat_id=chat_id,
            user_id=msg.from_user.id,
            text=text,
            run_at=run_at_utc,
            status="pending",
        )
        s.add(row)
        await s.commit()
        await s.refresh(row)

    # Schedule job
    scheduler.add_job(
        _deliver_scheduled_message, "date",
        run_date=run_at_utc, args=[row.id], id=_job_id_for(row.id), replace_existing=True
    )

    await msg.answer(f"✅ Scheduled #{row.id} → {chat_id} at {run_at_local.strftime('%Y-%m-%d %H:%M %Z')}.")

@dp.message(Command("schedule_many"))
async def schedule_many_cmd(msg: Message):
    if not _is_admin(msg.from_user.id):
        return
    if not _is_private_chat(msg):
        asyncio.create_task(_delete_soon(msg.chat.id, msg.message_id, 0.1))
        m = await msg.answer("Use this command in a private chat with me.")
        asyncio.create_task(_delete_soon(m.chat.id, m.message_id, 1.5))
        return

    body = re.sub(r"^/schedule_many(@\w+)?\s*", "", (msg.text or ""), flags=re.IGNORECASE).strip()
    if not body:
        return await msg.answer(
            "Paste multiple lines after the command, e.g.:\n"
            "2025-10-02 07:00 | Morning truth.\n"
            "-1001234567890 2025-10-05 20:30 | Night check-in."
        )

    ok_lines, bad_lines = [], []
    rows_to_add: List[ScheduledMessage] = []

    for line in body.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        chat_id, run_at_local, text = _parse_when_and_target(line)
        if not chat_id:
            bad_lines.append((line, "No default/explicit chat id"))
            continue
        if not run_at_local:
            bad_lines.append((line, "Bad time (YYYY-MM-DD HH:MM)"))
            continue
        if not text:
            bad_lines.append((line, "Empty message"))
            continue
        rows_to_add.append(ScheduledMessage(
            chat_id=chat_id,
            user_id=msg.from_user.id,
            text=text,
            run_at=run_at_local.astimezone(dt.timezone.utc),
            status="pending",
        ))
        ok_lines.append((chat_id, run_at_local, text))

    created_ids: List[int] = []
    if rows_to_add:
        async with AsyncSessionLocal() as s:
            s.add_all(rows_to_add)
            await s.commit()
            for r in rows_to_add:
                await s.refresh(r)
                created_ids.append(r.id)

    for r in rows_to_add:
        scheduler.add_job(
            _deliver_scheduled_message, "date",
            run_date=r.run_at, args=[r.id], id=_job_id_for(r.id), replace_existing=True
        )

    reply = []
    if ok_lines:
        reply.append("✅ Scheduled:")
        for (chat, when_local, txt), sid in zip(ok_lines, created_ids):
            reply.append(f"  • #{sid} → {chat} — {when_local.strftime('%Y-%m-%d %H:%M %Z')} — {txt[:60]}")
    if bad_lines:
        reply.append("\n⚠️ Skipped:")
        for line, reason in bad_lines:
            reply.append(f"  • {line}  ← {reason}")

    await msg.answer("\n".join(reply) if reply else "Nothing parsed.")

@dp.message(Command("schedule_list"))
async def schedule_list_cmd(msg: Message):
    if not _is_admin(msg.from_user.id):
        return
    if not _is_private_chat(msg):
        asyncio.create_task(_delete_soon(msg.chat.id, msg.message_id, 0.1))
        m = await msg.answer("Use in private chat.")
        asyncio.create_task(_delete_soon(m.chat.id, m.message_id, 1.5))
        return

    parts = (msg.text or "").split()
    chat_filter = int(parts[1]) if len(parts) >= 2 and parts[1].lstrip("-").isdigit() else None

    async with AsyncSessionLocal() as s:
        q = select(ScheduledMessage).where(ScheduledMessage.status == "pending")
        if chat_filter is not None:
            q = q.where(ScheduledMessage.chat_id == chat_filter)
        q = q.order_by(ScheduledMessage.run_at.asc())
        res = await s.execute(q)
        rows = res.scalars().all()

    if not rows:
        return await msg.answer("No pending schedules." + (f" (filtered by {chat_filter})" if chat_filter else ""))

    lines = ["Pending schedules:"]
    for r in rows:
        when_local = r.run_at.astimezone(TZ)
        lines.append(f"  • #{r.id} → {r.chat_id} — {when_local.strftime('%Y-%m-%d %H:%M %Z')} — {r.text[:70]}")
    await msg.answer("\n".join(lines))

@dp.message(Command("schedule_cancel"))
async def schedule_cancel_cmd(msg: Message):
    if not _is_admin(msg.from_user.id):
        return
    if not _is_private_chat(msg):
        asyncio.create_task(_delete_soon(msg.chat.id, msg.message_id, 0.1))
        m = await msg.answer("Use in private chat.")
        asyncio.create_task(_delete_soon(m.chat.id, m.message_id, 1.5))
        return

    parts = (msg.text or "").split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        return await msg.answer("Usage: /schedule_cancel <id>")

    try:
        sid = int(parts[1].strip())
    except ValueError:
        return await msg.answer("ID must be an integer.")

    async with AsyncSessionLocal() as s:
        r = await s.execute(select(ScheduledMessage).where(ScheduledMessage.id == sid))
        row = r.scalar_one_or_none()
        if not row or row.status != "pending":
            return await msg.answer("No such pending id (maybe sent/canceled).")
        await s.execute(update(ScheduledMessage).where(ScheduledMessage.id == sid).values(status="canceled"))
        await s.commit()

    try:
        scheduler.remove_job(_job_id_for(sid))
    except Exception:
        pass

    await msg.answer(f"🗑️ Canceled #{sid}.")

# ------------------------ Lightweight content handlers -------

#BLESSINGS = [
#    "Your thanks is heard, {name}. May your shoulders carry light burdens and your will grow heavy with resolve.",
#    "Gratitude received, {name}. Walk with steady breath; strength will meet you there.",
#]
#REBUKES = [
#    "I hear your anger, {name}. I receive it—and I answer with steadiness.",
#    "Your words land, {name}. I take them in, and I remain your witness.",
#]
#SUMMON_RESPONSES = [
#    "Did someone summon me?",
#    "A whisper reaches the floor—speak, seeker.",
#]
#
#def _compose_blessing(user_name: Optional[str]) -> str:
#    safe = html.escape(user_name or "friend")
#    return _sysrand.choice(BLESSINGS).format(name=safe)
#
#def _compose_rebuke(user_name: Optional[str]) -> str:
#    safe = html.escape(user_name or "traveler")
#    return _sysrand.choice(REBUKES).format(name=safe)
#
#@dp.message(F.text.func(lambda t: isinstance(t, str) and not t.strip().startswith("/") and THANKS_RE.search(t)))
#async def thanks_plain(msg: Message):
#    try:
#        await upsert_username(msg.chat.id, msg.from_user.id,
#                              getattr(msg.from_user, "first_name", None),
#                              getattr(msg.from_user, "username", None))
#        await incr_counter(msg.chat.id, msg.from_user.id, "thanks", 1)
#    except Exception:
#        logger.exception("Failed to log 'thanks'")
#    await msg.answer(_compose_blessing(getattr(msg.from_user, "first_name", None)))

# @dp.message(F.text.func(lambda t: isinstance(t, str) and not t.strip().startswith("/") and APOLOGY_RE.search(t)))
# async def apology_reply(msg: Message):
#    try:
#        await upsert_username(msg.chat.id, msg.from_user.id,
#                              getattr(msg.from_user, "first_name", None),
#                              getattr(msg.from_user, "username", None))
#        await incr_counter(msg.chat.id, msg.from_user.id, "apology", 1)
#    except Exception:
#        logger.exception("Failed to log 'apology'")
#    await msg.answer(_compose_blessing(getattr(msg.from_user, "first_name", None)))

#@dp.message(F.text.func(lambda t: isinstance(t, str) and INSULT_RE.search(_normalize_text(t)) and not APOLOGY_RE.search(t)))
#async def prophet_insult_rebuke(msg: Message):
#    try:
#        await upsert_username(msg.chat.id, msg.from_user.id,
#                              getattr(msg.from_user, "first_name", None),
#                              getattr(msg.from_user, "username", None))
#        await incr_counter(msg.chat.id, msg.from_user.id, "insult", 1)
#    except Exception:
#        logger.exception("Failed to log 'insult'")
#    await msg.answer(_compose_rebuke(getattr(msg.from_user, "first_name", None)))

#@dp.message(F.text.func(lambda t: isinstance(t, str)
#                        and not t.strip().startswith("/")
#                        and SUMMON_PATTERN.search(t)
#                        and not THANKS_RE.search(t)
#                        and not APOLOGY_RE.search(t)))
#async def summon_reply(msg: Message):
#    if await get_ai_enabled(msg.chat.id):
#        name = getattr(msg.from_user, "first_name", "") or (msg.from_user.username or "friend")
#        user_text = msg.text or ""
#        messages = [{"role": "user", "content": f"{name}: {user_text}"}]
#        reply = await ai_reply(PROPHET_SYSTEM, messages)
#        if reply:
#            await msg.answer(reply, parse_mode=None, disable_web_page_preview=True)
#            return
#    await msg.answer(_sysrand.choice(SUMMON_RESPONSES))

# ================== Daily Quotes (rotation + /share_wisdom) ==================
from collections import deque

QUOTES = [
    "“Gravity is my quill; with each rep I write strength upon your bones.”",
    "“Do not count your pushups—make your pushups count, and the numbers will fear you.”",
    "“Raise your standards before you raise your reps.”",
]

_quote_rotation: Dict[int, deque] = {}

def _init_quote_rotation(chat_id: int) -> None:
    if not QUOTES:
        _quote_rotation[chat_id] = deque()
        return
    order = list(range(len(QUOTES)))
    _sysrand.shuffle(order)
    _quote_rotation[chat_id] = deque(order)

def _next_quote(chat_id: int) -> Optional[str]:
    if not QUOTES:
        return None
    dq = _quote_rotation.get(chat_id)
    if dq is None or not dq:
        _init_quote_rotation(chat_id)
        dq = _quote_rotation[chat_id]
    idx = dq.popleft()
    return QUOTES[idx]

async def send_daily_quote(chat_id: int):
    q = _next_quote(chat_id)
    if q is None:
        return
    safe = html.escape(q)
    await bot.send_message(chat_id, f"🕖 Daily Wisdom\n{safe}")

@dp.message(Command("share_wisdom"))
async def share_wisdom_cmd(msg: Message):
    q = _next_quote(msg.chat.id)
    if not q:
        await msg.answer("No quotes configured yet.")
        return
    await msg.answer(html.escape(q))


# ================== Weekly Votes (polls on Sundays) ==================
PLAYERS = ["Fresh", "Momo", "Valle", "Tän", "Hampa"]  # ← edit to your roster

# Store vote state in-memory (per chat, per ISO week)
weakest_votes: Dict[int, Dict[str, Dict[int, str]]] = {}
inspiration_votes: Dict[int, Dict[str, Dict[int, str]]] = {}

# Poll ID → { kind: 'weakest'|'inspiration', chat_id: int, options: [players] }
POLL_META: Dict[str, Dict[str, object]] = {}

def _week_key_now() -> str:
    now = dt.datetime.now(TZ)
    iso = now.isocalendar()
    return f"{iso.year}-W{iso.week:02d}"

def _ensure_vote_map(bucket: Dict[int, Dict[str, Dict[int, str]]], chat_id: int, week_key: str) -> Dict[int, str]:
    by_chat = bucket.setdefault(chat_id, {})
    return by_chat.setdefault(week_key, {})

async def send_weekly_vote_prompts(chat_id: int):
    if not PLAYERS:
        await bot.send_message(chat_id, "No players configured for weekly votes.")
        return
    options = list(PLAYERS)

    msg_w = await bot.send_poll(
        chat_id=chat_id,
        question="🏷️ The Weakest Link — Who struggled the most this week?",
        options=options,
        is_anonymous=False,
        allows_multiple_answers=False,
    )
    if msg_w.poll:
        POLL_META[msg_w.poll.id] = {"kind": "weakest", "chat_id": chat_id, "options": options}

    msg_i = await bot.send_poll(
        chat_id=chat_id,
        question="🌟 The Inspiration — Who inspired the circle this week?",
        options=options,
        is_anonymous=False,
        allows_multiple_answers=False,
    )
    if msg_i.poll:
        POLL_META[msg_i.poll.id] = {"kind": "inspiration", "chat_id": chat_id, "options": options}

@dp.message(Command("vote_now", "weekly_votes", "votes_now"))
async def vote_now_cmd(msg: Message):
    await send_weekly_vote_prompts(msg.chat.id)
    await msg.answer("🗳️ The weekly vote prompts have been posted.")

@dp.poll_answer()
async def handle_poll_vote(pa: PollAnswer):
    poll_id = pa.poll_id
    meta = POLL_META.get(poll_id)
    if not meta:
        return
    option_ids = pa.option_ids or []
    if not option_ids:
        return
    idx = option_ids[0]
    options: List[str] = meta["options"]  # type: ignore
    if idx < 0 or idx >= len(options):
        return
    player = options[idx]
    kind = meta["kind"]            # 'weakest' or 'inspiration'
    chat_id = meta["chat_id"]
    week_key = _week_key_now()
    user_id = pa.user.id
    bucket = weakest_votes if kind == "weakest" else inspiration_votes
    votes_map = _ensure_vote_map(bucket, chat_id, week_key)
    votes_map[user_id] = player

    voter_name = (pa.user.full_name or pa.user.first_name or pa.user.username or "Someone").strip()
    safe_voter = html.escape(voter_name)
    safe_player = html.escape(player)
    label = "The Weakest Link" if kind == "weakest" else "The Inspiration"
    await bot.send_message(chat_id, f"🗳️ <b>{safe_voter}</b> voted <b>{safe_player}</b> as <i>{label}</i>.")

# ==== Daily state (Stockholm-local day) for thanks & offenses ====

# If you already have _today_stockholm_date() from Dice of Fate, reuse it:
def _today_stockholm() -> dt.date:
    try:
        return _today_stockholm_date()
    except NameError:
        return dt.datetime.now(TZ).date()

# per-user “thanks” count per day (for >5 rule)
_daily_thanks: Dict[int, tuple[dt.date, int]] = {}

def _bump_thanks(user_id: int) -> int:
    today = _today_stockholm()
    d = _daily_thanks.get(user_id)
    if not d or d[0] != today:
        _daily_thanks[user_id] = (today, 0)
    n = _daily_thanks[user_id][1] + 1
    _daily_thanks[user_id] = (today, n)
    return n

# per-user offense count per day (insults/fake-apology) — warn → punish
_daily_offense: Dict[int, tuple[dt.date, int]] = {}

def _bump_offense(user_id: int) -> int:
    today = _today_stockholm()
    d = _daily_offense.get(user_id)
    if not d or d[0] != today:
        _daily_offense[user_id] = (today, 0)
    n = _daily_offense[user_id][1] + 1
    _daily_offense[user_id] = (today, n)
    return n

# === Secret insult threshold per user per day (random 2..6) ===
_insult_threshold: Dict[int, tuple[dt.date, int]] = {}

def _get_insult_threshold(user_id: int) -> int:
    """
    Pick a hidden daily threshold for this user (2..6 inclusive).
    The first insult today assigns it; we then keep it secret and stable for the rest of the day.
    """
    today = _today_stockholm_date()
    stored = _insult_threshold.get(user_id)
    if not stored or stored[0] != today:
        threshold = _sysrand.randint(2, 6)  # at least one warning; penalty sometime between 2 and 6 insults
        _insult_threshold[user_id] = (today, threshold)
        return threshold
    return stored[1]


# ================== Dice of Fate ==================
FATE_WEIGHTS = [
    ("miracle",         3),
    ("mercy_coin",      6),
    ("trial_form",     10),
    ("command_prophet",10),
    ("giver",          12),
    ("hurricane",      10),
    ("oath_dawn",      16),
    ("trial_flesh",    15),
    ("tribute_blood",  15),
    ("wrath",           3),
]

def _pick_fate_key() -> str:
    keys = [k for k, _ in FATE_WEIGHTS]
    weights = [w for _, w in FATE_WEIGHTS]
    return _sysrand.choices(keys, weights=weights, k=1)[0]

FATE_RULES_TEXT = (
    "<b>Dice of Fate</b>\n\n"
    "(3%) — ✨ <b>The Miracle</b> — Halve your debt\n"
    "(6%) — 🪙 <b>Mercy Coin</b> — Skip one regular pushup day\n"
    "(10%) — ⚔️ <b>Trial of Form</b> — Do 10 perfect pushups → erase 20 kr of debt\n"
    "(10%) — 👑 <b>Command of the Prophet</b> — Pick a player: He does 30 pushups or 30 kr\n"
    "(12%) — 🤝 <b>The Giver</b> — Give away 40 of your daily pushups to a random player\n"
    "\n"
    "(10%) — 🌪️ <b>Hurricane of Chaos</b> — Pay 10 kr; shift 10% of your debt to random player\n"
    "(16%) — 🌅 <b>Oath of Dawn</b> — Be first tomorrow or pay 30 kr\n"
    "(15%) — 🔥 <b>Trial of Flesh</b> — 100 pushups today or +45 kr\n"
    "(15%) — 🩸 <b>Tribute of Blood</b> — Pay 50 kr\n"
    "(3%) — ⚡ <b>Prophet’s Wrath</b> — Double your debt"
)

_fate_rolls: Dict[int, tuple[dt.date, set[int]]] = {}

def _today_stockholm_date() -> dt.date:
    return dt.datetime.now(TZ).date()

def _fate_reset_if_new_day(chat_id: int):
    today = _today_stockholm_date()
    state = _fate_rolls.get(chat_id)
    if not state or state[0] != today:
        _fate_rolls[chat_id] = (today, set())

def _fate_has_rolled_today(chat_id: int, user_id: int) -> bool:
    _fate_reset_if_new_day(chat_id)
    return user_id in _fate_rolls[chat_id][1]

def _fate_mark_rolled(chat_id: int, user_id: int):
    _fate_reset_if_new_day(chat_id)
    _fate_rolls[chat_id][1].add(user_id)

def _fate_epic_text(key: str, target_name: Optional[str] = None) -> str:
    closers = [
        "Thus it is spoken—walk wisely.",
        "So decrees the Prophet—bear the mark with honor.",
        "The die grows silent; let your deeds answer.",
        "The seal is set; may your will not waver.",
        "The wind keeps the tally; choose well.",
    ]
    end = _sysrand.choice(closers)
    texts = {
        "miracle": "✨ <b>The Miracle</b>\nThe scales tilt toward mercy. Your burden is cleaved in half.",
        "giver": (
            "🤝 <b>The Giver</b>\n" +
            (f"Give away <b>40</b> of your daily pushups to <b>{html.escape(target_name)}</b>. Strength shared is strength multiplied."
             if target_name else
             "Give away <b>40</b> of your daily pushups to a random player. Strength shared is strength multiplied.")
        ),
        "trial_form": "⚔️ <b>Trial of Form</b>\nOffer <b>10</b> perfect pushups—tempo true, depth honest—and erase <b>20 kr</b> of debt.",
        "command_prophet": "👑 <b>Command of the Prophet</b>\nPick a player: he does <b>30</b> pushups or pays <b>30 kr</b>. Authority tests friendship.",
        "mercy_coin": "🪙 <b>Mercy Coin</b>\nOne regular day is pardoned. Do not spend it cheaply.",
        "hurricane": "🌪️ <b>Hurricane of Chaos</b>\nPay <b>10 kr</b>; then shift <b>10%</b> of your debt to a random player.",
        "oath_dawn": "🌅 <b>Oath of Dawn</b>\nBe first to rise tomorrow or pay <b>30 kr</b>. Dawn reveals the faithful.",
        "trial_flesh": "🔥 <b>Trial of Flesh</b>\nChoose today: <b>100</b> pushups—or lay <b>45 kr</b> upon the altar.",
        "tribute_blood": "🩸 <b>Tribute of Blood</b>\nThe pot demands <b>50 kr</b>. Pay without grudge, learn without delay.",
        "wrath": "⚡ <b>Prophet’s Wrath</b>\nYour debt is doubled. Pride withers; discipline takes its seat.",
    }
    return texts.get(key, "The die rolls into shadow.") + f"\n\n<i>{end}</i>"

@dp.message(Command("fate", "dice", "dice_of_fate"))
async def fate_cmd(msg: Message):
    await msg.answer("“You dare summon the Dice of Fate. The air trembles with judgment.”")
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="Roll the Dice 🎲", callback_data="fate:roll"),
        InlineKeyboardButton(text="Cancel", callback_data="fate:cancel"),
    ]])
    await msg.answer(FATE_RULES_TEXT, reply_markup=kb)

@dp.callback_query(F.data == "fate:cancel")
async def fate_cancel(cb: CallbackQuery):
    await cb.answer("The Dice return to their slumber.")
    await cb.message.answer("The Dice close their eyes. Another day, perhaps.")

@dp.callback_query(F.data == "fate:roll")
async def fate_roll(cb: CallbackQuery):
    chat_id = cb.message.chat.id
    user_id = cb.from_user.id

    if _fate_has_rolled_today(chat_id, user_id):
        return await cb.answer("You have already rolled today. Return with the next dawn.", show_alert=True)

    _fate_mark_rolled(chat_id, user_id)
    fate_key = _pick_fate_key()

    target = None
    if fate_key == "giver":
        target = _sysrand.choice(PLAYERS) if PLAYERS else None

    epic = _fate_epic_text(fate_key, target_name=target)
    await cb.answer()

    if fate_key == "hurricane":
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🎯 Select random player", callback_data="hurricane:spin")
        ]])
        await cb.message.answer(epic, reply_markup=kb)
    else:
        await cb.message.answer(epic)

@dp.callback_query(F.data == "hurricane:spin")
async def hurricane_spin(cb: CallbackQuery):
    invoker = (cb.from_user.first_name or "").strip()
    candidates = [p for p in PLAYERS if p.lower() != invoker.lower()] or PLAYERS
    if not candidates:
        await cb.answer("No players configured.", show_alert=True)
        return
    target = _sysrand.choice(candidates)
    await cb.answer()
    await cb.message.answer(
        f"🎯 The storm chooses: <b>{html.escape(target)}</b>.\n"
        f"Shift <b>10%</b> of your debt to them. Order is restored."
    )

# Natural-language summon for Dice of Fate
FATE_SUMMON_RE = re.compile(
    r"\b(?:dice\s+of\s+fate|summon(?:\s+the)?\s+dice(?:\s+of\s+fate)?|fate\s+dice|roll\s+the\s+dice\s+of\s+fate)\b",
    re.IGNORECASE
)

@dp.message(F.text.func(lambda t: isinstance(t, str)
                        and not t.strip().startswith("/")
                        and FATE_SUMMON_RE.search(t)))
async def fate_natural(msg: Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="Roll the Dice 🎲", callback_data="fate:roll"),
        InlineKeyboardButton(text="Cancel", callback_data="fate:cancel"),
    ]])
    await msg.answer("“You dare summon the Dice of Fate. The air trembles with judgment.”")
    await msg.answer(FATE_RULES_TEXT, reply_markup=kb)

@dp.message(Command("fate_stats"))
async def fate_stats_cmd(msg: Message):
    """
    Simulate N rolls to sanity-check distribution.
    Usage: /fate_stats [N]  (default N=10000, min 1000, max 200000)
    """
    parts = (msg.text or "").split()
    try:
        N = int(parts[1]) if len(parts) >= 2 else 10000
        N = max(1000, min(N, 200000))
    except Exception:
        N = 10000

    keys = [k for k, _ in FATE_WEIGHTS]
    weights = [w for _, w in FATE_WEIGHTS]

    counts = {k: 0 for k in keys}
    for _ in range(N):
        k = _sysrand.choices(keys, weights=weights, k=1)[0]
        counts[k] += 1

    lines = [f"🎲 Fate stats (N={N}):"]
    total_w = sum(weights)
    for k in keys:
        pct = 100.0 * counts[k] / N
        target = next(w for kk, w in FATE_WEIGHTS if kk == k)
        lines.append(f"• {k:15s}: {counts[k]:6d}  ({pct:5.2f}% vs target {target/total_w*100:5.2f}%)")



# ================== Forgiveness Chain (random daily + 1h follow-up) ==================
# Uses global: DAILY_TEXT, TZ, WINDOW_START, WINDOW_END, scheduler, bot
# One job per chat in `random_jobs`. Cancels/reschedules cleanly.

random_jobs: Dict[int, object] = {}  # chat_id -> APScheduler Job

def _next_random_time(now: dt.datetime) -> dt.datetime:
    """Pick a random time today within [WINDOW_START, WINDOW_END]. If already past, roll to tomorrow."""
    hour = _sysrand.randint(WINDOW_START, WINDOW_END)
    minute = _sysrand.randint(0, 59)
    run_at = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if run_at <= now:
        run_at += dt.timedelta(days=1)
    return run_at

def schedule_random_daily(chat_id: int) -> None:
    """Start (or restart) the daily random announcement loop for a chat."""
    # If a job exists, remove it first
    old = random_jobs.get(chat_id)
    if old:
        try:
            old.remove()
        except Exception:
            pass

    now = dt.datetime.now(TZ)
    run_at = _next_random_time(now)

    async def send_and_reschedule():
        try:
            # 1) Send the main daily message
            await bot.send_message(chat_id, DAILY_TEXT)

            # 2) Schedule the exact 1-hour follow-up (no long sleep)
            scheduler.add_job(
                bot.send_message,
                "date",
                run_date=dt.datetime.now(TZ) + dt.timedelta(hours=1),
                args=[chat_id,
                      ("The hour has passed, the covenant stands. No debt weighs upon those who rise in unison. "
                       "The choice has always been yours. I hope you made the right one.")]
            )
        finally:
            # 3) Immediately schedule tomorrow's random time
            tomorrow = dt.datetime.now(TZ) + dt.timedelta(days=1)
            next_run = _next_random_time(tomorrow)
            new_job = scheduler.add_job(send_and_reschedule, "date", run_date=next_run)
            random_jobs[chat_id] = new_job

    job = scheduler.add_job(send_and_reschedule, "date", run_date=run_at)
    random_jobs[chat_id] = job

@dp.message(Command("enable_random"))
async def enable_random_cmd(msg: Message):
    schedule_random_daily(msg.chat.id)
    await msg.answer("✅ Forgiveness Chain enabled for this chat. I will announce once per day between "
                     f"{WINDOW_START:02d}:00–{WINDOW_END:02d}:00 Stockholm, then follow up 1 hour later.")

@dp.message(Command("disable_random"))
async def disable_random_cmd(msg: Message):
    job = random_jobs.pop(msg.chat.id, None)
    if job:
        try:
            job.remove()
        except Exception:
            pass
        await msg.answer("🛑 Forgiveness Chain disabled for this chat.")
    else:
        await msg.answer("It wasn’t enabled for this chat.")

@dp.message(Command("status_random"))
async def status_random_cmd(msg: Message):
    enabled = msg.chat.id in random_jobs
    await msg.answer(f"Forgiveness Chain status: {'Enabled ✅' if enabled else 'Disabled 🛑'}")

@dp.message(Command("day"))
async def program_day_cmd(msg: Message):
    day, total = await get_program_day()
    await msg.answer(f"📅 Program day: <b>{day}</b> / {total}", parse_mode=ParseMode.HTML)

@dp.message(Command("mystats"))
async def my_stats_cmd(msg: Message):
    # Lifetime (from Counter)
    totals = await user_totals(msg.chat.id, msg.from_user.id)  # you already have this helper
    thanks = totals.get("thanks", 0)
    apologies = totals.get("apology", 0)
    insults = totals.get("insult", 0)
    penalties = totals.get("penalty", 0)

    # Today (from UserDailyStats)
    async with AsyncSessionLocal() as s:
        res = await s.execute(
            select(UserDailyStats.thanks_count, UserDailyStats.apology_count, UserDailyStats.insult_count)
            .where(
                UserDailyStats.chat_id == msg.chat.id,
                UserDailyStats.user_id == msg.from_user.id,
                UserDailyStats.date == _local_midnight_utc()
            )
        )
        row = res.first()
    today_line = "Today — thanks: 0, apologies: 0, insults: 0"
    if row:
        t,a,i = row
        today_line = f"Today — thanks: {t}, apologies: {a}, insults: {i}"

    await msg.answer(
        "📊 Your stats\n"
        f"Lifetime — thanks: {thanks}, apologies: {apologies}, insults: {insults}, penalties: {penalties}\n"
        f"{today_line}"
    )



# Catch-all AI (AI enforces thanks/apology/insult/summon policy; compact group replies)
@dp.message(F.text.func(lambda t: isinstance(t, str) and not t.startswith("/")))
async def ai_catchall(msg: Message):
    try:
        if not await get_ai_enabled(msg.chat.id):
            return

        text = msg.text or ""
        norm = _normalize_text(text)

        # Classify using your existing regexes
        is_thanks  = bool(THANKS_RE.search(text))
        is_apology = bool(APOLOGY_RE.search(text))
        is_insult  = bool(INSULT_RE.search(norm))
        is_summon  = bool(SUMMON_PATTERN.search(text))

        # Persist lightweight lifetime stats (useful for /mystats etc.)
        try:
            await upsert_username(
                msg.chat.id, msg.from_user.id,
                getattr(msg.from_user, "first_name", None),
                getattr(msg.from_user, "username", None),
            )
            if is_thanks:  await incr_counter(msg.chat.id, msg.from_user.id, "thanks", 1)
            if is_apology: await incr_counter(msg.chat.id, msg.from_user.id, "apology", 1)
            if is_insult:  await incr_counter(msg.chat.id, msg.from_user.id, "insult", 1)
            if is_summon:  await incr_counter(msg.chat.id, msg.from_user.id, "mention", 1)
        except Exception:
            logger.exception("counter/name upsert failed")

        user_id = msg.from_user.id

        # === Gratitude rules (DB-backed) ===
        # Overuse => stern warning ONLY (do not disclose thresholds or amounts)
        thanks_count_today = 0
        gratitude_overused = False           # -> AI should warn only (no +kr mentioned)
        gratitude_reward   = False           # 5% boon (−10 kr) ONLY if not overused

        if is_thanks:
            thanks_count_today = await bump_thanks_db(msg.chat.id, user_id)
            if thanks_count_today > 5:
                gratitude_overused = True
                gratitude_reward = False
                # (No penalty applied here—only a stern warning via AI response)
            else:
                gratitude_reward = (_sysrand.random() < 0.05)
                if gratitude_reward:
                    # Record boon in audit log (not counted as a "penalty")
                    await log_penalty(msg.chat.id, user_id, "gratitude_boon", -10, "Random gratitude boon (≤5 thanks)")

        # === Insults / fake apologies-or-thanks (DB-backed) ===
        # First offense today -> warning; after a secret threshold (2..6) -> +15 kr (threshold never revealed)
        fake_or_insult = (is_insult and (is_thanks or is_apology)) or is_insult
        offense_count_today = 0
        insult_punish_due = False

        if fake_or_insult:
            offense_count_today, threshold = await bump_insult_db(msg.chat.id, user_id)
            insult_punish_due = (offense_count_today >= threshold)
            if insult_punish_due:
                await log_penalty(msg.chat.id, user_id, "insult", +15, "Hidden daily threshold reached")
                # Also track lifetime penalty count
                try:
                    await incr_counter(msg.chat.id, user_id, "penalty", 1)
                except Exception:
                    logger.exception("failed to increment lifetime penalty counter")

        # Always reply for these four categories; otherwise use heuristic + cooldown
        force_reply = is_thanks or is_apology or is_insult or is_summon
        if not force_reply:
            if not should_ai_reply(msg):
                return
            if not _cooldown_ok(user_id):
                return
        else:
            # Bypass cooldown for rule-triggered categories
            _last_ai_reply_at[user_id] = dt.datetime.now().timestamp()

        name = (msg.from_user.first_name or msg.from_user.username or "friend")

        # Add program day (e.g., Day 73/100) so AI can reference it
        try:
            day_idx, day_total = await get_program_day()
        except Exception:
            logger.exception("get_program_day failed")
            day_idx, day_total = (None, None)

        # Build strict context for AI
        ai_context = {
            "chat_id": msg.chat.id,
            "user_id": user_id,
            "user_name": name,
            "program_day": {"day": day_idx, "total": day_total},  # may be None/None if lookup failed
            "categories": {
                "thanks": is_thanks,
                "apology": is_apology,
                "insult": is_insult,
                "summon": is_summon,
            },
            "gratitude": {
                "count_today": thanks_count_today,
                "overused": gratitude_overused,                 # => stern warning only; no amounts/thresholds
                "reward_minus_10": (gratitude_reward and not gratitude_overused),
            },
            "insults": {
                "is_fake_or_insult": fake_or_insult,
                "count_today": offense_count_today,
                "punish_plus_15": insult_punish_due,            # when True, Prophet must say “+15 kr to the pot”
                # NEVER reveal or hint at the hidden threshold
            },
            "pushup_wording": "Say 'extra pushups' or 'less pushups', never 'you owe N pushups'.",
            "no_pre_warnings": True,  # do not say “you have X left” or disclose thresholds
            "guidance": "Keep replies short for group chat. Conceal thresholds. State amounts only when required.",
        }

        prompt = f"{name}: {text}\n\n[context-json]\n{ai_context}"
        reply = await ai_reply(PROPHET_SYSTEM, [{"role": "user", "content": prompt}])
        if reply:
            await msg.answer(reply, parse_mode=None, disable_web_page_preview=True)
    except Exception:
        logger.exception("policy-aware AI reply failed")




# ------------------------ Health endpoints -------------------

@app.get("/")
def health(): return {"ok": True, "service": "pushup-prophet"}

@app.head("/")
def health_head(): return Response(status_code=200)

# ------------------------ Startup / Shutdown -----------------

@app.on_event("startup")
async def on_startup():
    global BOT_ID
    # 1) Bot identity
    me = await bot.get_me()
    BOT_ID = me.id
    logger.info(f"Bot authorized: @{me.username} (id={me.id})")

    # 2) Ensure no webhook conflict with polling
    for attempt in range(1, 6):
        try:
            await bot.delete_webhook(drop_pending_updates=True, request_timeout=30)
            info = await bot.get_webhook_info()
            if not info.url:
                break
        except Exception as e:
            logger.warning(f"delete_webhook attempt {attempt} failed: {e}")
        await asyncio.sleep(min(2 ** attempt, 10))

    # 3) DB init & resume pending jobs
    await init_db()
    await load_and_schedule_pending()

    # 4) Schedule daily quotes, weekly votes, and Forgiveness Chain
    ids = os.getenv("GROUP_CHAT_IDS", "").strip()
    if ids:
        for raw in ids.split(","):
            raw = raw.strip()
            if not raw:
                continue
            try:
                chat_id = int(raw)

                # Daily quote at 07:00 Stockholm
                scheduler.add_job(
                    send_daily_quote, "cron",
                    hour=7, minute=0, args=[chat_id],
                    id=f"daily_quote_{chat_id}", replace_existing=True,
                )
                logger.info(f"Scheduled daily quote (07:00) for chat {chat_id}")

                # Weekly votes every Sunday at 11:00 Stockholm
                scheduler.add_job(
                    send_weekly_vote_prompts, "cron",
                    day_of_week="sun", hour=11, minute=0, args=[chat_id],
                    id=f"weekly_votes_{chat_id}", replace_existing=True,
                )
                logger.info(f"Scheduled weekly votes (Sun 11:00) for chat {chat_id}")

                # Forgiveness Chain: random daily + 1h follow-up
                schedule_random_daily(chat_id)
                logger.info(f"Auto-enabled Forgiveness Chain for chat {chat_id}")

            except Exception as e:
                logger.exception(f"Startup scheduling failed for chat {raw}: {e}")

    # 5) Start scheduler + bot polling
    scheduler.start()
    asyncio.create_task(run_bot_polling())



async def run_bot_polling():
    delays = [0, 1, 3, 5, 10]
    for i, delay in enumerate(delays, start=1):
        if delay: await asyncio.sleep(delay)
        try:
            logger.info(f"Starting polling (attempt {i})…")
            await dp.start_polling(bot, polling_timeout=30)
            break
        except TelegramBadRequest as e:
            msg = str(e)
            logger.warning(f"Polling TelegramBadRequest: {msg}")
            try:
                await bot.delete_webhook(drop_pending_updates=True, request_timeout=30)
            except Exception as e2:
                logger.warning(f"delete_webhook after conflict failed: {e2}")
            continue
        except TelegramNetworkError as e:
            logger.warning(f"Network error on polling attempt {i}: {e}")
            continue
        except Exception as e:
            logger.exception(f"Unexpected polling error attempt {i}: {e}")
            continue

@app.on_event("shutdown")
async def on_shutdown():
    try:
        await scheduler.shutdown(wait=False)
    except Exception:
        pass
    try:
        await bot.session.close()
    except Exception:
        pass
    try:
        await engine.dispose()
    except Exception:
        pass

# ------------------------ Entrypoint -------------------------

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port, reload=False, workers=1)










