import os, re, asyncio, logging, datetime as dt, random, html
from typing import Dict, Optional, List, Tuple
from pathlib import Path
from collections import deque

import uvicorn
from sqlalchemy import Column, BigInteger, String, Integer, Boolean, DateTime, Index, select, update, func, event
from sqlalchemy.orm import declarative_base
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy import select, update

from aiogram import Bot, Dispatcher
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, PollAnswer
from aiogram.filters import Command, CommandStart
from aiogram import F
from aiogram.enums.parse_mode import ParseMode
from aiogram.client.default import DefaultBotProperties

from aiogram.exceptions import TelegramBadRequest, TelegramNetworkError


from dotenv import load_dotenv
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.job import Job
from pytz import timezone

import httpx

from fastapi import FastAPI, Response
app = FastAPI()

# near the top of the file, after imports
_SINGLETON_LOCK = Path(".bot-lock")

def _acquire_singleton_lock() -> bool:
    try:
        # O_EXCL fails if the file already exists
        fd = os.open(_SINGLETON_LOCK, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.close(fd)
        return True
    except FileExistsError:
        return False

@app.on_event("startup")
async def on_startup():
    if not _acquire_singleton_lock():
        # Another instance already running in this folder
        logger.error("Another bot instance seems to be running (.bot-lock exists). Exiting.")
        raise SystemExit(1)
    ...

@app.on_event("shutdown")
async def on_shutdown():
    ...
    try:
        if _SINGLETON_LOCK.exists():
            _SINGLETON_LOCK.unlink(missing_ok=True)
    except Exception:
        pass


# Prevent accidental double-start
_started_polling = False
BOT_ID: Optional[int] = None

# --------- Load config ----------
DOTENV_PATH = Path(__file__).with_name(".env")
load_dotenv(dotenv_path=DOTENV_PATH)

BOT_TOKEN: Optional[str] = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise SystemExit(
        f"ERROR: BOT_TOKEN missing. Add it as an environment variable on your host "
        f"or to {DOTENV_PATH} locally for testing."
    )

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip()

TZ = timezone("Europe/Stockholm")
DAILY_TEXT = "THE FORGIVENESS CHAIN BEGINS NOW. Lay down excuses and ascend. May the power of Push be with you."
WINDOW_START = 7   # 07:00
WINDOW_END = 22    # 22:00 (inclusive)

# --------- Bot setup ----------
logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
logger = logging.getLogger("pushup-prophet")

_sysrand = random.SystemRandom()

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
scheduler = AsyncIOScheduler(timezone=TZ)

# ----------------- Database (Postgres/SQLite) -----------------
DB_URL = os.getenv("DATABASE_URL", "").strip()


def _to_async_url(url: str) -> str:
    if not url:
        return "sqlite+aiosqlite:///./local-dev.db"  # local dev fallback
    # Normalize scheme to the async driver
    if url.startswith("postgres://"):
        url = "postgresql+asyncpg://" + url[len("postgres://"):]
    elif url.startswith("postgresql://"):
        url = "postgresql+asyncpg://" + url[len("postgresql://"):]
    # Strip any accidental query like ?sslmode=require (we set SSL via connect_args)
    if "?" in url:
        url = url.split("?", 1)[0]
    return url


ASYNC_DB_URL = _to_async_url(DB_URL)

# ========== MODELS ==========
from sqlalchemy import Column, BigInteger, String, Integer, Boolean, DateTime, Index
from sqlalchemy import func

Base = declarative_base()

class Counter(Base):
    __tablename__ = "counters"
    chat_id = Column(BigInteger, primary_key=True)
    user_id = Column(BigInteger, primary_key=True)
    metric  = Column(String(32), primary_key=True)  # "thanks" | "apology" | "insult" | "mention"
    count   = Column(Integer, nullable=False, default=0)

    __table_args__ = (
        Index("ix_counters_chat_metric_count", "chat_id", "metric", "count"),
    )

class UserName(Base):
    __tablename__ = "user_names"
    chat_id    = Column(BigInteger, primary_key=True)
    user_id    = Column(BigInteger, primary_key=True)
    first_name = Column(String(128), nullable=True)
    username   = Column(String(128), nullable=True)  # without '@'
    last_seen  = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

class ChatSettings(Base):
    __tablename__ = "chat_settings"
    chat_id    = Column(BigInteger, primary_key=True)
    ai_enabled = Column(Boolean, nullable=False, default=False)
    changed_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
# ============================

from sqlalchemy import event  # ⬅️ add this import if not already present

import ssl
ssl_ctx = None
if ASYNC_DB_URL.startswith("postgresql+asyncpg://"):
    # Neon requires TLS — create a default SSL context
    ssl_ctx = ssl.create_default_context()

engine = create_async_engine(
    ASYNC_DB_URL,
    pool_pre_ping=True,
    connect_args={"ssl": ssl_ctx} if ssl_ctx else {},
)

AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

# SQLite WAL for better local dev concurrency
if ASYNC_DB_URL.startswith("sqlite+aiosqlite"):
    @event.listens_for(engine.sync_engine, "connect")
    def _set_sqlite_pragma(dbapi_conn, _):
        try:
            dbapi_conn.execute("PRAGMA journal_mode=WAL;")
        except Exception:
            pass

async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

def _is_pg_url(async_url: str) -> bool:
    return async_url.startswith("postgresql+asyncpg://")

async def incr_counter(chat_id: int, user_id: int, metric: str, delta: int = 1) -> None:
    """Atomic upsert: increase a user metric in a chat."""
    async with AsyncSessionLocal() as session:
        dialect_insert = pg_insert if _is_pg_url(ASYNC_DB_URL) else sqlite_insert
        stmt = dialect_insert(Counter).values(
            chat_id=chat_id, user_id=user_id, metric=metric, count=delta
        ).on_conflict_do_update(
            # ✅ use column names for portability
            index_elements=[Counter.chat_id.name, Counter.user_id.name, Counter.metric.name],
            set_={"count": Counter.count + delta},
        )
        await session.execute(stmt)
        await session.commit()


# === Helpers (single, canonical versions) =======================

def _dialect_insert():
    """Pick the right INSERT helper for Postgres vs SQLite."""
    return pg_insert if _is_pg_url(ASYNC_DB_URL) else sqlite_insert

# ---- user name upsert / lookup ----
async def upsert_username(chat_id: int, user_id: int, first_name: Optional[str], username: Optional[str]) -> None:
    """Idempotently save/update a user's display info for this chat."""
    ins = _dialect_insert()(UserName).values(
        chat_id=chat_id,
        user_id=user_id,
        first_name=first_name or None,
        username=username or None,
    )
    stmt = ins.on_conflict_do_update(
        index_elements=[UserName.chat_id.name, UserName.user_id.name],  # composite PK
        set_={
            "first_name": ins.excluded.first_name,
            "username":   ins.excluded.username,
            "last_seen":  func.now(),
        },
    )
    async with AsyncSessionLocal() as s:
        await s.execute(stmt)
        await s.commit()

async def name_for(chat_id: int, user_id: int) -> str:
    """Return a nice display name for a user in a chat."""
    async with AsyncSessionLocal() as s:
        r = await s.execute(
            select(UserName.first_name, UserName.username)
            .where(UserName.chat_id == chat_id, UserName.user_id == user_id)
        )
        row = r.first()
        if row:
            first_name, username = row
            if first_name:
                return first_name
            if username:
                return f"@{username}"
    return f"user {user_id}"

# ---- per-chat AI toggle (uses portable UPSERT) ----
async def get_ai_enabled(chat_id: int) -> bool:
    async with AsyncSessionLocal() as s:
        res = await s.execute(
            select(ChatSettings.ai_enabled).where(ChatSettings.chat_id == chat_id)
        )
        row = res.first()
        return bool(row[0]) if row else False

async def set_ai_enabled(chat_id: int, enabled: bool) -> None:
    """Set AI on/off for a chat (UPSERT so it works whether row exists or not)."""
    ins = _dialect_insert()(ChatSettings).values(chat_id=chat_id, ai_enabled=enabled)
    stmt = ins.on_conflict_do_update(
        index_elements=[ChatSettings.chat_id.name],
        set_={"ai_enabled": enabled},
    )
    async with AsyncSessionLocal() as s:
        await s.execute(stmt)
        await s.commit()

# ---- handy read helpers ----
async def top_n(chat_id: int, metric: str, n: int = 10) -> List[Tuple[int, int]]:
    async with AsyncSessionLocal() as s:
        rows = await s.execute(
            select(Counter.user_id, Counter.count)
            .where(Counter.chat_id == chat_id, Counter.metric == metric)
            .order_by(Counter.count.desc())
            .limit(n)
        )
        return rows.all()

async def user_totals(chat_id: int, user_id: int) -> Dict[str, int]:
    async with AsyncSessionLocal() as s:
        rows = await s.execute(
            select(Counter.metric, Counter.count)
            .where(Counter.chat_id == chat_id, Counter.user_id == user_id)
        )
        return {m: c for (m, c) in rows.all()}

# ===============================================================


random_jobs: Dict[int, Job] = {}

def next_random_time(now: dt.datetime) -> dt.datetime:
    hour = _sysrand.randint(WINDOW_START, WINDOW_END)
    minute = _sysrand.randint(0, 59)
    run_at = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if run_at <= now:
        run_at += dt.timedelta(days=1)
    return run_at

def schedule_random_daily(chat_id: int) -> None:
    old = random_jobs.get(chat_id)
    if old:
        try:
            old.remove()
        except Exception:
            pass

    now = dt.datetime.now(TZ)
    run_at = next_random_time(now)

    async def send_and_reschedule():
        try:
            await bot.send_message(chat_id, DAILY_TEXT)
            # Schedule follow-up exactly 1 hour later (no long sleeps)
            scheduler.add_job(
                bot.send_message,
                "date",
                run_date=dt.datetime.now(TZ) + dt.timedelta(hours=1),
                args=[chat_id,
                      "The hour has passed, the covenant stands. No debt weighs upon those who rise in unison. "
                      "The choice has always been yours. I hope you made the right one."]
            )
        finally:
            tomorrow = dt.datetime.now(TZ) + dt.timedelta(days=1)
            next_run = next_random_time(tomorrow)
            new_job = scheduler.add_job(send_and_reschedule, "date", run_date=next_run)
            random_jobs[chat_id] = new_job

    job = scheduler.add_job(send_and_reschedule, "date", run_date=run_at)
    random_jobs[chat_id] = job

# --- Player roster used by Weekly Prophecy AND Dice of Fate ---
PLAYERS = ["Fresh", "Momo", "Valle", "Tän", "Hampa"]

# ===== Weekly votes (non-anonymous) =====
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

# ================== Quotes rotation (daily 07:00 + /share_wisdom) ==================
QUOTES = [
    "“Gravity is my quill; with each rep I write strength upon your bones.”",
    "“Do not count your pushups—make your pushups count, and the numbers will fear you.”",
    # ... (keep the rest of your QUOTES list unchanged)
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

# ================== DICE OF FATE ==================

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
        "miracle": (
            "✨ <b>The Miracle</b>\n"
            "The scales tilt toward mercy. Your burden is cleaved in half."
        ),
        "giver": (
            "🤝 <b>The Giver</b>\n"
            + (
                f"Give away <b>40</b> of your daily pushups to <b>{html.escape(target_name)}</b>. Strength shared is strength multiplied."
                if target_name else
                "Give away <b>40</b> of your daily pushups to a random player. Strength shared is strength multiplied."
            )
        ),
        "trial_form": (
            "⚔️ <b>Trial of Form</b>\n"
            "Offer <b>10</b> perfect pushups—tempo true, depth honest—and erase <b>20 kr</b> of debt."
        ),
        "command_prophet": (
            "👑 <b>Command of the Prophet</b>\n"
            "Pick a player: he does <b>30</b> pushups or pays <b>30 kr</b>. Authority tests friendship."
        ),
        "mercy_coin": (
            "🪙 <b>Mercy Coin</b>\n"
            "One regular day is pardoned. Do not spend it cheaply."
        ),
        "hurricane": (
            "🌪️ <b>Hurricane of Chaos</b>\n"
            "Pay <b>10 kr</b>; then shift <b>10%</b> of your debt to a random player."
        ),
        "oath_dawn": (
            "🌅 <b>Oath of Dawn</b>\n"
            "Be first to rise tomorrow or pay <b>30 kr</b>. Dawn reveals the faithful."
        ),
        "trial_flesh": (
            "🔥 <b>Trial of Flesh</b>\n"
            "Choose today: <b>100</b> pushups—or lay <b>45 kr</b> upon the altar."
        ),
        "tribute_blood": (
            "🩸 <b>Tribute of Blood</b>\n"
            "The pot demands <b>50 kr</b>. Pay without grudge, learn without delay."
        ),
        "wrath": (
            "⚡ <b>Prophet’s Wrath</b>\n"
            "Your debt is doubled. Pride withers; discipline takes its seat."
        ),
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

# ================== Gratitude / Blessings ==================
BLESSINGS = [
    "Your thanks is heard, {name}. May your shoulders carry light burdens and your will grow heavy with resolve.",
    "Gratitude received, {name}. Walk with steady breath; strength will meet you there.",
    "I accept your thanks, {name}. May your form be honest and your progress inevitable.",
    "Your gratitude is a good omen, {name}. Rise clean, descend wiser.",
    "I hear you, {name}. Let patience be your spotter and discipline your crown.",
    "Thanks received, {name}. May the floor count only truths from your chest.",
    "Your words land true, {name}. Let calm lead effort and effort shape destiny.",
    "I accept this tribute of thanks, {name}. May your last rep be your cleanest.",
    "Gratitude noted, {name}. Keep the vow; the vow will keep you.",
    "I receive your thanks, {name}. Temper the ego, sharpen the technique.",
    "Your thanks rings clear, {name}. May rest write tomorrow’s strength into your bones.",
    "Heard and held, {name}. Let the first push greet the day, and the last bless your night.",
    "I take your thanks, {name}. Let honesty be your range and courage your tempo.",
    "Your gratitude strengthens the circle, {name}. Share cadence; arrive together.",
    "Acknowledged, {name}. May your spine stay straight and your standard even straighter.",
]

def _compose_blessing(user_name: Optional[str]) -> str:
    safe = html.escape(user_name or "friend")
    base = _sysrand.choice(BLESSINGS).format(name=safe)
    if _sysrand.random() < 0.05:
        base += "\n\n🪙 <b>Favor of Gratitude</b> — Deduct <b>20 kr</b> from your debt for your loyalty."
    return base

_gratitude_uses: Dict[int, tuple[dt.date, int]] = {}

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

THANKS_RE = re.compile(r"\b(thank(?:\s*you)?|thanks|thx|ty|tack(?:\s*så\s*mycket)?)\b", re.IGNORECASE)

@dp.message(F.text.func(lambda t: isinstance(t, str) and not t.strip().startswith("/") and THANKS_RE.search(t)))
async def thanks_plain(msg: Message):
    try:
        # NEW: store/display name for this user in this chat
        await upsert_username(
            msg.chat.id,
            msg.from_user.id,
            getattr(msg.from_user, "first_name", None),
            getattr(msg.from_user, "username", None),
        )
        # existing: increment the counter
        await incr_counter(msg.chat.id, msg.from_user.id, "thanks", 1)
    except Exception:
        logger.exception("Failed to log 'thanks' counter")

    attempts = _gratitude_inc_and_get(msg.from_user.id)
    name = getattr(msg.from_user, "first_name", None)
    if attempts > 5:
        await msg.answer(_compose_gratitude_penalty(name))
    else:
        await msg.answer(_compose_blessing(name))


# ================== Apologies / Absolution ==================
APOLOGY_RE = re.compile(
    r"\b("
    r"sorry|i\s*(?:am|’m|'m)\s*sorry|i\s*apolog(?:ise|ize)|apologies|apology|"
    r"my\s*bad|my\s*fault|i\s*was\s*wrong|didn'?t\s*mean|forgive\s*me|"
    r"förlåt|ursäkta|jag\s*är\s*ledsen|ber\s*om\s*ursäkt|mitt\s*fel"
    r")\b",
    re.IGNORECASE
)

APOLOGY_RESPONSES = [
    "Your apology is received, {name}. Mercy given; standard unchanged—meet it.",
    "I accept your apology, {name}. Make it right in form and in habit.",
    "Apology taken in, {name}. Rise cleaner; let your next set speak.",
    "Heard and accepted, {name}. Forgiveness is a door—walk through with discipline.",
    "I receive this apology, {name}. No excuses; only better repetitions.",
    "Acknowledged, {name}. The slate is lighter; the bar is not.",
    "Your apology lands true, {name}. Now let consistency seal it.",
    "Accepted, {name}. Pay with honesty at the bottom and patience at the top.",
    "I grant you grace, {name}. Earn it in the quiet work.",
    "Apology noted, {name}. The floor keeps score—answer it.",
    "I hear contrition, {name}. Return to the standard; leave the drama.",
    "Received, {name}. Forgiveness is given; trust is trained.",
    "Your apology is accounted for, {name}. Now do the next right rep.",
    "I accept and remember, {name}. Let this be a turn, not a tale.",
    "Grace extends to you, {name}. Guard your form; guard your word.",
    "Apology accepted, {name}. Show me steadiness—louder than talk.",
    "Your regret is clear, {name}. Set your spine; set your course.",
    "I receive your apology, {name}. Be exact; be early; be better.",
    "Pardon granted, {name}. Debt remains to effort—pay it daily.",
    "Your apology is welcomed, {name}. Let discipline finish what remorse began.",
]

def _compose_absolution(user_name: Optional[str]) -> str:
    safe = html.escape(user_name or "friend")
    return _sysrand.choice(APOLOGY_RESPONSES).format(name=safe)

@dp.message(F.text.func(lambda t: isinstance(t, str)
                        and not t.strip().startswith("/")
                        and APOLOGY_RE.search(t)))


async def apology_reply(msg: Message):
    try:
        # NEW: store/display name
        await upsert_username(
            msg.chat.id,
            msg.from_user.id,
            getattr(msg.from_user, "first_name", None),
            getattr(msg.from_user, "username", None),
        )
        # existing: increment counter
        await incr_counter(msg.chat.id, msg.from_user.id, "apology", 1)
    except Exception:
        logger.exception("Failed to log 'apology' counter")

    text = _compose_absolution(getattr(msg.from_user, "first_name", None))
    await msg.answer(text)


# === Negativity / insult watcher ===
def _normalize_text(t: str) -> str:
    t = re.sub(r"[\u200b-\u200f\u202a-\u202e]", "", t or "")
    return t.lower()

MENTION_RE = r"(?:\bpush\s*up\s*prophet\b|\bpushup\s*prophet\b|\bprophet\b|\bbot\b)"

INSULT_WORDS = (
    r"(?:"
    r"fuck(?:ing|er|ed)?|f\*+ck|f\W*u\W*c\W*k|"
    r"shit|bull\W*sh(?:it|\*?t)|crap|trash|garbage|bs|"
    r"sucks?|stupid|idiot|moron|dumb(?:ass)?|loser|pathetic|awful|terrible|useless|worthless|annoying|cringe|fraud|fake|clown|nonsense|"
    r"bitch|ass(?:hole|hat|clown)?|dick(?:head)?|prick|jerk|"
    r"wank(?:er)?|twat|tosser|dipshit|jackass|motherfucker|mf"
    r")"
)

DIRECT_2P = (
    r"(?:"
    r"fuck\s*(?:you|u|ya)|"
    r"screw\s*you|stfu|shut\s*up|"
    r"you\s*(?:suck|are\s*(?:stupid|dumb|useless|worthless|annoying|terrible|awful)|"
    r"idiot|moron|loser|clown|bitch|asshole|prick|jerk|dickhead|wanker|twat)"
    r")"
)

INSULT_RE = re.compile(
    rf"""
    (?:
        (?:{MENTION_RE}).*?(?:{INSULT_WORDS})
        |
        (?:{INSULT_WORDS}).*?(?:{MENTION_RE})
    )
    |
    (?:{DIRECT_2P})
    |
    (?:
        fuck\s*this\s*shit
        | fuck(?:ing)?\s+bull\W*shit
        | (?:bull\W*shit|bullsh\*?t)\s*(?:prophet|bot|push\s*up\s*prophet)
        | (?:garbage|trash|worst)\s*bot
        | fuck\s*off
        | go\s*to\s*hell
    )
    """,
    re.IGNORECASE | re.VERBOSE,
)

REBUKES = [
    "I hear your anger, {name}. I receive it—and I answer with steadiness.",
    "Your words land, {name}. I take them in, and I remain your witness.",
    "I receive your sting, {name}. May your breath be longer than your temper.",
    "Noted, {name}. Your voice is heard; let your form speak next.",
    "Heard, {name}. I accept your heat—discipline will cool it.",
    "I accept your edge, {name}. The floor counts truth; so do I.",
    "Taken in, {name}. Let us turn sharp words into clean reps.",
    "Received, {name}. I stand; may your resolve stand with me.",
    "Your message is clear, {name}. Let patience be clearer.",
    "Understood, {name}. I acknowledge you—and call you higher.",
    "I hear you, {name}. I do not flinch; I invite you back to the work.",
    "Acknowledged, {name}. Strength listens, then answers with action.",
    "Your frustration is seen, {name}. I hold it, and I hold the standard.",
    "I take your words, {name}. I will still meet you at the floor.",
    "Message received, {name}. Let your next rep say more than this one.",
]

def _compose_rebuke(user_name: Optional[str]) -> str:
    safe = html.escape(user_name or "traveler")
    base = _sysrand.choice(REBUKES).format(name=safe)
    if _sysrand.random() < 0.10:
        base += "\n\n<b>Edict:</b> Lay <b>20 kr</b> in the pot as penance for disrespect."
    return base

@dp.message(F.text.func(lambda t: isinstance(t, str)
                        and INSULT_RE.search(_normalize_text(t))
                        and not APOLOGY_RE.search(t)))
async def prophet_insult_rebuke(msg: Message):
    try:
        # store/display name
        await upsert_username(
            msg.chat.id,
            msg.from_user.id,
            getattr(msg.from_user, "first_name", None),
            getattr(msg.from_user, "username", None),
        )
        # increment the 'insult' metric
        await incr_counter(msg.chat.id, msg.from_user.id, "insult", 1)
    except Exception:
        logger.exception("Failed to log 'insult' counter")

    text = _compose_rebuke(getattr(msg.from_user, "first_name", None))
    await msg.answer(text)

# --- Helpers for sending the next quote ---
async def _send_next_quote_to_chat(chat_id: int):
    q = _next_quote(chat_id)
    if not q:
        await bot.send_message(chat_id, "No quotes configured yet.")
        return
    await bot.send_message(chat_id, html.escape(q))

def _build_vote_kb(kind: str) -> InlineKeyboardMarkup:
    buttons, row = [], []
    for i, p in enumerate(PLAYERS, 1):
        row.append(InlineKeyboardButton(text=p, callback_data=f"vote:{kind}:{p}"))
        if i % 3 == 0:
            buttons.append(row); row = []
    if row: buttons.append(row)
    return InlineKeyboardMarkup(inline_keyboard=buttons)

async def send_weekly_vote_prompts(chat_id: int):
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

@dp.message(Command("share_wisdom"))
async def share_wisdom_cmd(msg: Message):
    await _send_next_quote_to_chat(msg.chat.id)

_WISDOM_PATTERNS = [
    r"\bshare\s+wisdom\b",
    r"\bgive\s+(?:me\s+)?wisdom\b",
    r"\bsay\s+(?:something\s+)?wise\b",
    r"\bwisdom\s+please\b",
    r"\bteach\s+me\b",
    r"\bi\s+seek\s+wisdom\b",
    r"\bprophet[,!\s]*\s*(?:share|give|drop)\s+(?:some\s+)?wisdom\b",
    r"\bdrop\s+(?:some\s+)?wisdom\b",
]
_WISDOM_RES = [re.compile(p, re.IGNORECASE) for p in _WISDOM_PATTERNS]
def _matches_wisdom_nat(t: str) -> bool:
    return any(rx.search(t) for rx in _WISDOM_RES)

@dp.message(F.text.func(lambda t: isinstance(t, str)
                        and not t.strip().startswith("/")
                        and _matches_wisdom_nat(t)))
async def share_wisdom_natural(msg: Message):
    await _send_next_quote_to_chat(msg.chat.id)

@dp.message(F.text.func(lambda t: isinstance(t, str) and t.strip().lower().startswith("/share wisdom")))
async def share_wisdom_space_alias(msg: Message):
    await _send_next_quote_to_chat(msg.chat.id)

# ==== Prophet Summon Reactions ====
SUMMON_RESPONSES = [
    "Did someone summon me?",
    "A whisper reaches the floor—speak, seeker.",
    "The air stirs; the Prophet listens.",
    "You called; discipline answers.",
    "The Pushup Prophet hears. State your petition.",
    "The floor remembers every name. What do you ask?",
    "I rise where I’m named. What truth do you seek?",
]
SUMMON_PATTERN = re.compile(r"\b(pushup\s*prophet|prophet)\b", re.IGNORECASE)

@dp.message(F.text.func(lambda t: isinstance(t, str)
                        and not t.strip().startswith("/")
                        and SUMMON_PATTERN.search(t)
                        and not THANKS_RE.search(t)
                        and not _matches_wisdom_nat(t)
                        and not APOLOGY_RE.search(t)))
async def summon_reply(msg: Message):
    logger.info(f"[SUMMON] got mention in chat={msg.chat.id} text={msg.text!r}")
    try:
        await upsert_username(
            msg.chat.id, msg.from_user.id,
            getattr(msg.from_user, "first_name", None),
            getattr(msg.from_user, "username", None),
        )
        await incr_counter(msg.chat.id, msg.from_user.id, "mention", 1)
    except Exception:
        logger.exception("Failed to log 'mention' counter")

    if await get_ai_enabled(msg.chat.id):
        logger.info(f"[SUMMON] AI enabled -> letting catchall handle")
        return

    await msg.answer(_sysrand.choice(SUMMON_RESPONSES))



# --------- Other Handlers ----------

@dp.message(Command("ai_ping"))
async def ai_ping_cmd(msg: Message):
    reply = await ai_reply(PROPHET_SYSTEM, [
        {"role": "user", "content": "Give me one crisp pushup cue."}
    ])
    if reply:
        await msg.answer(f"AI OK:\n{reply}", disable_web_page_preview=True, parse_mode=None)
    else:
        await msg.answer("AI call failed. Check server logs and OPENAI_API_KEY/OPENAI_MODEL.")


@dp.message(Command("chatid"))
async def chatid_cmd(msg: Message):
    await msg.answer(f"Chat ID: <code>{msg.chat.id}</code>")

@dp.message(F.text.func(lambda t: isinstance(t, str) and t.strip().lower().startswith(("/chatid", "/chatid@"))))
async def chatid_fallback(msg: Message):
    await msg.answer(f"Chat ID: <code>{msg.chat.id}</code>")

@dp.message(CommandStart())
async def start_cmd(msg: Message):
    await msg.answer(
        "I am the Pushup Prophet.\n\n"
        "What I can do for you:\n"
        "• Ask for wisdom and I shall give.\n"
        "• Summon the Dice of Fate (one roll per person per day).\n\n"
        "Be aware of the Forgiveness Chain:\n"
        "• /enable_random — enable the Forgiveness Chain for this chat.\n"
        "• /disable_random — disable the Forgiveness Chain for this chat.\n"
        "• /status_random — check whether the Forgiveness Chain is enabled.\n\n"
        "AI controls:\n"
        "• /enable_ai — allow AI replies in this chat\n"
        "• /disable_ai — stop AI replies in this chat\n"
        "• /status_ai — show AI status\n\n"
        "You shall see me at every dawn. May the power of the Push be with you."
    )

@dp.message(Command("help"))
async def help_cmd(msg: Message):
    await start_cmd(msg)

# === NEW: AI control commands ===
@dp.message(Command("enable_ai"))
async def enable_ai_cmd(msg: Message):
    await set_ai_enabled(msg.chat.id, True)
    await msg.answer("🤖 AI replies enabled for this chat.")

@dp.message(Command("disable_ai"))
async def disable_ai_cmd(msg: Message):
    await set_ai_enabled(msg.chat.id, False)
    await msg.answer("🛑 AI replies disabled for this chat.")

@dp.message(Command("ai_status"))
async def ai_status_cmd(msg: Message):
    enabled = await get_ai_enabled(msg.chat.id)
    await msg.answer(f"AI status: {'Enabled ✅' if enabled else 'Disabled 🛑'}")

@dp.message(F.text.func(lambda t: isinstance(t, str) and t.strip().lower().startswith(("/ai_status", "/ai_status@"))))
async def ai_status_fallback(msg: Message):
    enabled = await get_ai_enabled(msg.chat.id)
    await msg.answer(f"AI status: {'Enabled ✅' if enabled else 'Disabled 🛑'}")

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

@dp.message(Command("enable_random"))
async def enable_random_cmd(msg: Message):
    schedule_random_daily(msg.chat.id)
    await msg.answer("✅ Forgiveness Chain enabled for this chat. I will announce once per day between 07:00–22:00 Stockholm, then follow up 1 hour later.")

@dp.message(Command("disable_random"))
async def disable_random_cmd(msg: Message):
    job = random_jobs.pop(msg.chat.id, None)
    if job:
        try: job.remove()
        except Exception: pass
        await msg.answer("🛑 Forgiveness Chain disabled for this chat.")
    else:
        await msg.answer("It wasn’t enabled for this chat.")

@dp.message(Command("status_random"))
async def status_random_cmd(msg: Message):
    enabled = msg.chat.id in random_jobs
    await msg.answer(f"Forgiveness Chain status: {'Enabled ✅' if enabled else 'Disabled 🛑'}")

# --------- AI Layer (with toggles) ----------
PROPHET_SYSTEM = (
    "You are the Pushup Prophet: wise, concise, kind but stern, poetic but practical. "
    "Keep replies short for group chat. Offer form cues, consistency rituals, and supportive accountability. "
    "Avoid medical claims. Stay on-topic; if off-topic, gently steer back to training, habits, or group rituals."
)

_AI_COOLDOWN_S = int(os.getenv("AI_COOLDOWN_S", "15"))
_last_ai_reply_at: Dict[int, float] = {}
_ai_enabled_for_chat: Dict[int, bool] = {}  # default off until enabled

def _cooldown_ok(user_id: int) -> bool:
    now = dt.datetime.now().timestamp()
    last = _last_ai_reply_at.get(user_id, 0.0)
    if now - last >= _AI_COOLDOWN_S:
        _last_ai_reply_at[user_id] = now
        return True
    return False

@dp.message(Command("enable_ai"))
async def enable_ai_cmd(msg: Message):
    await set_ai_enabled(msg.chat.id, True)
    enabled = await get_ai_enabled(msg.chat.id)
    logger.info(f"[AI TOGGLE] chat={msg.chat.id} set True -> now {enabled}")
    await msg.answer("🤖 AI replies enabled for this chat.")

@dp.message(Command("disable_ai"))
async def disable_ai_cmd(msg: Message):
    await set_ai_enabled(msg.chat.id, False)
    enabled = await get_ai_enabled(msg.chat.id)
    logger.info(f"[AI TOGGLE] chat={msg.chat.id} set False -> now {enabled}")
    await msg.answer("🛑 AI replies disabled for this chat.")


@dp.message(Command("status_ai"))
async def status_ai_cmd(msg: Message):
    state = _ai_enabled_for_chat.get(msg.chat.id, False)
    await msg.answer(f"AI status: {'Enabled ✅' if state else 'Disabled 🛑'}")

async def ai_reply(system: str, messages: List[dict], model: str = OPENAI_MODEL) -> str:
    if not OPENAI_API_KEY:
        return ""
    # modest backoff retries to avoid going silent on transient errors
    delays = [0, 0.8, 2.0]
    for i, delay in enumerate(delays):
        if delay:
            await asyncio.sleep(delay)
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                r = await client.post(
                    "https://api.openai.com/v1/chat/completions",
                    headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
                    json={
                        "model": model,
                        "messages": [{"role": "system", "content": system}] + messages,
                        "temperature": 0.6,
                        "max_tokens": 180,
                    },
                )
                r.raise_for_status()
                data = r.json()
                return (data["choices"][0]["message"]["content"] or "").strip()
        except Exception as e:
            logger.warning(f"AI call attempt {i+1} failed: {e}")
    return ""

def should_ai_reply(msg: Message) -> bool:
    if not _ai_enabled_for_chat.get(msg.chat.id, False):
        return False
    t = (msg.text or "").strip()
    if not t:
        return False
    if t.startswith("/"):
        return False
    if THANKS_RE.search(t) or APOLOGY_RE.search(t) or INSULT_RE.search(_normalize_text(t)) \
       or FATE_SUMMON_RE.search(t) or _matches_wisdom_nat(t):
        return False
    if SUMMON_PATTERN.search(t):
        return True
    if msg.reply_to_message and msg.reply_to_message.from_user and BOT_ID and msg.reply_to_message.from_user.id == BOT_ID:
        return True
    if re.search(r"\b(help|advice|how do i|what should i)\b", t, re.IGNORECASE) and _sysrand.random() < 0.07:
        return True
    return False

@dp.message(F.text)
async def ai_catchall(msg: Message):
    try:
        logger.info(f"[AI] incoming text in chat={msg.chat.id}: {msg.text!r}")

        if not await get_ai_enabled(msg.chat.id):
            logger.info("[AI] disabled for this chat")
            return

        if not should_ai_reply(msg):
            logger.info("[AI] should_ai_reply = False (not a trigger)")
            return

        if not _cooldown_ok(msg.from_user.id):
            logger.info("[AI] cooldown blocked")
            return

        name = getattr(msg.from_user, "first_name", "") or (msg.from_user.username or "friend")
        user_text = msg.text or ""
        logger.info("[AI] calling OpenAI…")
        messages = [{"role": "user", "content": f"{name}: {user_text}"}]
        reply = await ai_reply(PROPHET_SYSTEM, messages)
        if reply:
            logger.info("[AI] got reply, sending to chat")
            await msg.answer(reply, parse_mode=None, disable_web_page_preview=True)
        else:
            logger.warning("[AI] ai_reply returned empty string")
    except Exception:
        logger.exception("AI reply failed")



# --------- Run bot + web server together ----------

@app.get("/")
def health():
    return {"ok": True, "service": "pushup-prophet"}

@app.head("/")
def health_head():
    return Response(status_code=200)


async def run_bot():
    global _started_polling
    if _started_polling:
        logger.warning("Polling already started; skipping second start.")
        return
    _started_polling = True

    scheduler.start()
    logger.info("Scheduler started")

    # Defensive: ensure webhook really gone right before polling
    try:
        await bot.delete_webhook(drop_pending_updates=True, request_timeout=30)
    except Exception as e:
        logger.warning(f"delete_webhook (pre-poll) failed (continuing): {e}")

    # Retry loop around start_polling to survive transient conflicts
    delays = [0, 1, 3, 5, 10]  # seconds
    for i, delay in enumerate(delays, start=1):
        if delay:
            await asyncio.sleep(delay)
        try:
            logger.info(f"Starting polling (attempt {i})...")
            # polling_timeout keeps long-polls short so shutdowns are snappier
            await dp.start_polling(bot, polling_timeout=30)
            logger.info("Polling finished cleanly.")
            break
        except TelegramBadRequest as e:
            # Typical conflict/error from Bot API
            msg = str(e)
            logger.warning(f"start_polling TelegramBadRequest on attempt {i}: {msg}")
            if "terminated by other getUpdates request" in msg.lower() \
               or "can't use getupdates method while webhook is active" in msg.lower():
                # Try to clear webhook and retry
                try:
                    await bot.delete_webhook(drop_pending_updates=True, request_timeout=30)
                except Exception as e2:
                    logger.warning(f"delete_webhook after conflict failed: {e2}")
                continue
            # other 400-level errors: let it bubble to next retry
            continue
        except TelegramNetworkError as e:
            logger.warning(f"Network error on polling attempt {i}: {e}")
            continue
        except Exception as e:
            logger.exception(f"Unexpected polling error on attempt {i}: {e}")
            continue


@app.on_event("startup")
async def on_startup():
    global BOT_ID
    try:
        me = await bot.get_me()
        BOT_ID = me.id
        logger.info(f"Bot authorized: @{me.username} (id={me.id})")
    except Exception:
        logger.exception("get_me failed. Is BOT_TOKEN correct?")
        raise

    # Ensure webhook is removed before polling (with retries/timeouts)
    for attempt in range(1, 6):
        try:
            await bot.delete_webhook(drop_pending_updates=True, request_timeout=30)
            info = await bot.get_webhook_info()
            if not info.url:
                logger.info("Webhook cleared successfully.")
                break
            logger.warning(f"Webhook still set to: {info.url!r} (attempt {attempt})")
        except Exception as e:
            logger.warning(f"delete_webhook attempt {attempt}) failed: {e}")
        await asyncio.sleep(min(2 ** attempt, 10))
    else:
        logger.error("Could not clear webhook after multiple attempts. Polling may not receive updates.")

    # --- DB init ---
    try:
        await init_db()
        logger.info("Database initialized and ready.")
    except Exception:
        logger.exception("Database init failed.")
        raise

    asyncio.create_task(run_bot())  # start Telegram bot loop

    # Auto-enable schedules for groups
    ids = os.getenv("GROUP_CHAT_IDS", "").strip()
    if ids:
        for raw in ids.split(","):
            raw = raw.strip()
            if not raw:
                continue
            try:
                chat_id = int(raw)
                schedule_random_daily(chat_id)
                logger.info(f"Auto-enabled Forgiveness Chain for chat {chat_id}")

                scheduler.add_job(
                    send_daily_quote, "cron",
                    hour=7, minute=0, args=[chat_id],
                    id=f"daily_quote_{chat_id}", replace_existing=True,
                )
                logger.info(f"Scheduled daily quote (07:00) for chat {chat_id}")

                scheduler.add_job(
                    send_weekly_vote_prompts, "cron",
                    day_of_week="sun", hour=11, minute=0, args=[chat_id],
                    id=f"weekly_votes_{chat_id}", replace_existing=True,
                )
                logger.info(f"Scheduled weekly votes (Sun 11:00) for chat {chat_id}")
            except Exception as e:
                logger.exception(f"Startup scheduling failed for chat {raw}: {e}")

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


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    # workers=1 guarantees single process (important for polling)
    uvicorn.run(app, host="0.0.0.0", port=port, reload=False, workers=1)








