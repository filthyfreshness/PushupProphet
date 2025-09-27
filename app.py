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

THANKS_RE = re.compile(r"\b(thank(?:\s*you)?|thanks|thx|ty|tack(?:\s*så\s*mycket)?)\b", re.IGNORECASE)
APOLOGY_RE = re.compile(
    r"\b("
    r"sorry|i\s*(?:am|’m|'m)\s*sorry|i\s*apolog(?:ise|ize)|apologies|apology|"
    r"my\s*bad|my\s*fault|i\s*was\s*wrong|didn'?t\s*mean|forgive\s*me|"
    r"förlåt|ursäkta|jag\s*är\s*ledsen|ber\s*om\s*ursäkt|mitt\s*fel"
    r")\b", re.IGNORECASE
)
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

BLESSINGS = [
    "Your thanks is heard, {name}. May your shoulders carry light burdens and your will grow heavy with resolve.",
    "Gratitude received, {name}. Walk with steady breath; strength will meet you there.",
]
REBUKES = [
    "I hear your anger, {name}. I receive it—and I answer with steadiness.",
    "Your words land, {name}. I take them in, and I remain your witness.",
]
SUMMON_RESPONSES = [
    "Did someone summon me?",
    "A whisper reaches the floor—speak, seeker.",
]

def _compose_blessing(user_name: Optional[str]) -> str:
    safe = html.escape(user_name or "friend")
    return _sysrand.choice(BLESSINGS).format(name=safe)

def _compose_rebuke(user_name: Optional[str]) -> str:
    safe = html.escape(user_name or "traveler")
    return _sysrand.choice(REBUKES).format(name=safe)

@dp.message(F.text.func(lambda t: isinstance(t, str) and not t.strip().startswith("/") and THANKS_RE.search(t)))
async def thanks_plain(msg: Message):
    try:
        await upsert_username(msg.chat.id, msg.from_user.id,
                              getattr(msg.from_user, "first_name", None),
                              getattr(msg.from_user, "username", None))
        await incr_counter(msg.chat.id, msg.from_user.id, "thanks", 1)
    except Exception:
        logger.exception("Failed to log 'thanks'")
    await msg.answer(_compose_blessing(getattr(msg.from_user, "first_name", None)))

@dp.message(F.text.func(lambda t: isinstance(t, str) and not t.strip().startswith("/") and APOLOGY_RE.search(t)))
async def apology_reply(msg: Message):
    try:
        await upsert_username(msg.chat.id, msg.from_user.id,
                              getattr(msg.from_user, "first_name", None),
                              getattr(msg.from_user, "username", None))
        await incr_counter(msg.chat.id, msg.from_user.id, "apology", 1)
    except Exception:
        logger.exception("Failed to log 'apology'")
    await msg.answer(_compose_blessing(getattr(msg.from_user, "first_name", None)))

@dp.message(F.text.func(lambda t: isinstance(t, str) and INSULT_RE.search(_normalize_text(t)) and not APOLOGY_RE.search(t)))
async def prophet_insult_rebuke(msg: Message):
    try:
        await upsert_username(msg.chat.id, msg.from_user.id,
                              getattr(msg.from_user, "first_name", None),
                              getattr(msg.from_user, "username", None))
        await incr_counter(msg.chat.id, msg.from_user.id, "insult", 1)
    except Exception:
        logger.exception("Failed to log 'insult'")
    await msg.answer(_compose_rebuke(getattr(msg.from_user, "first_name", None)))

@dp.message(F.text.func(lambda t: isinstance(t, str)
                        and not t.strip().startswith("/")
                        and SUMMON_PATTERN.search(t)
                        and not THANKS_RE.search(t)
                        and not APOLOGY_RE.search(t)))
async def summon_reply(msg: Message):
    if await get_ai_enabled(msg.chat.id):
        name = getattr(msg.from_user, "first_name", "") or (msg.from_user.username or "friend")
        user_text = msg.text or ""
        messages = [{"role": "user", "content": f"{name}: {user_text}"}]
        reply = await ai_reply(PROPHET_SYSTEM, messages)
        if reply:
            await msg.answer(reply, parse_mode=None, disable_web_page_preview=True)
            return
    await msg.answer(_sysrand.choice(SUMMON_RESPONSES))

# Catch-all AI (only if enabled; obey cooldown; sync trigger)
@dp.message(F.text.func(lambda t: isinstance(t, str) and not t.startswith("/")))
async def ai_catchall(msg: Message):
    try:
        if not await get_ai_enabled(msg.chat.id):
            return
        if not should_ai_reply(msg):          # <-- not awaited (fixed)
            return
        if not _cooldown_ok(msg.from_user.id):
            return

        name = getattr(msg.from_user, "first_name", "") or (msg.from_user.username or "friend")
        user_text = msg.text or ""
        messages = [{"role": "user", "content": f"{name}: {user_text}"}]
        reply = await ai_reply(PROPHET_SYSTEM, messages)
        if reply:
            await msg.answer(reply, parse_mode=None, disable_web_page_preview=True)
    except Exception:
        logger.exception("AI catchall failed")

# ------------------------ Health endpoints -------------------

@app.get("/")
def health(): return {"ok": True, "service": "pushup-prophet"}

@app.head("/")
def health_head(): return Response(status_code=200)

# ------------------------ Startup / Shutdown -----------------

@app.on_event("startup")
async def on_startup():
    global BOT_ID
    # Bot identity
    me = await bot.get_me()
    BOT_ID = me.id
    logger.info(f"Bot authorized: @{me.username} (id={me.id})")

    # Ensure no webhook conflict with polling
    for attempt in range(1, 6):
        try:
            await bot.delete_webhook(drop_pending_updates=True, request_timeout=30)
            info = await bot.get_webhook_info()
            if not info.url:
                break
        except Exception as e:
            logger.warning(f"delete_webhook attempt {attempt} failed: {e}")
        await asyncio.sleep(min(2 ** attempt, 10))

    # DB init & schedule resume
    await init_db()
    await load_and_schedule_pending()

    # Start APScheduler and polling
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
