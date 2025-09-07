import os, re, asyncio, logging, datetime as dt, random
from typing import Dict, Optional
from pathlib import Path

from fastapi import FastAPI, Response
import uvicorn

from aiogram import Bot, Dispatcher
from aiogram.types import Message
from aiogram.filters import Command, CommandStart
from aiogram import F
from aiogram.enums.parse_mode import ParseMode
from aiogram.client.default import DefaultBotProperties
from dotenv import load_dotenv
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.job import Job
from pytz import timezone

# --------- Load config ----------
DOTENV_PATH = Path(__file__).with_name(".env")
load_dotenv(dotenv_path=DOTENV_PATH)

BOT_TOKEN: Optional[str] = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise SystemExit(
        f"ERROR: BOT_TOKEN missing. Add it as an environment variable on your host "
        f"or to {DOTENV_PATH} locally for testing."
    )

TZ = timezone("Europe/Stockholm")
DAILY_TEXT = "The Forgiveness Chain has begun! Best of luck!"
WINDOW_START = 7   # 07:00
WINDOW_END = 22    # 22:00 (inclusive)

# ---- Name mapping for Prophecy (1..5) ----
PEOPLE: Dict[int, str] = {
    1: "Fresh",
    2: "Momo",
    3: "Valle",
    4: "Tän",
    5: "Hampa",
}
# True = the two picks may be the same person; False = must be different
ALLOW_REPEAT = True

# --------- Bot setup ----------
logging.basicConfig(level=logging.INFO)
_sysrand = random.SystemRandom()

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
scheduler = AsyncIOScheduler(timezone=TZ)

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
        finally:
            tomorrow = dt.datetime.now(TZ) + dt.timedelta(days=1)
            next_run = next_random_time(tomorrow)
            new_job = scheduler.add_job(send_and_reschedule, "date", run_date=next_run)
            random_jobs[chat_id] = new_job

    job = scheduler.add_job(send_and_reschedule, "date", run_date=run_at)
    random_jobs[chat_id] = job

# --------- Prophecy sequence helper ----------
async def start_prophecy_sequence(chat_id: int):
    """
    t+0:  announce
    t+3m: reveal MERCY (name from PEOPLE)
    t+4m: warn that punishment is in 1 minute
    t+5m: reveal PUNISHED (name from PEOPLE) + final line
    """
    # 0) ANNOUNCE NOW
    announce = (
        "ATTENTION EVERYONE\n"
        "The Prophecy has been revealed! In 3 minutes, it you will hear WHO WILL RECEIVE MERCY tomorrow."
    )
    await bot.send_message(chat_id, announce)

    now = dt.datetime.now(TZ)
    t_mercy = now + dt.timedelta(minutes=3)
    t_warn  = t_mercy + dt.timedelta(minutes=1)
    t_pun   = t_warn  + dt.timedelta(minutes=1)

    first_pick_num: Optional[int] = None

    async def reveal_mercy():
        nonlocal first_pick_num
        n = _sysrand.randint(1, 5)
        first_pick_num = n
        name = PEOPLE[n]
        await bot.send_message(
            chat_id,
            f"🔮 The Prophecy speaks...\nWHO WILL RECEIVE MERCY tomorrow: <b>{name}</b>"
        )

    async def warn_punishment():
        await bot.send_message(
            chat_id,
            "Of course, we cannot have mercy without punishment. In one minute, you will find out WHO WILL BE THE PUNISHED ONE. May the fortunes be with you."
        )

    async def reveal_punishment_and_final():
        if ALLOW_REPEAT:
            n = _sysrand.randint(1, 5)
        else:
            while True:
                n = _sysrand.randint(1, 5)
                if n != first_pick_num:
                    break
        name = PEOPLE[n]
        await bot.send_message(
            chat_id,
            f"⚖️ The verdict is in.\nWHO WILL BE THE PUNISHED ONE: <b>{name}</b>"
        )
        await bot.send_message(
            chat_id,
            "The weekly prophecies have been spoken. May the force of the Pushup Prophet be with you!"
        )

    # Schedule the 3 timed steps
    scheduler.add_job(reveal_mercy, "date", run_date=t_mercy)
    scheduler.add_job(warn_punishment, "date", run_date=t_warn)
    scheduler.add_job(reveal_punishment_and_final, "date", run_date=t_pun)

# --------- Handlers ----------
# Primary: works for /chatid and /chatid@pushupprophetbot
@dp.message(Command("chatid"))
async def chatid_cmd(msg: Message):
    await msg.answer(f"Chat ID: <code>{msg.chat.id}</code>")

# Fallback: catches odd variations like trailing spaces, mentions, etc.
@dp.message(F.text.func(lambda t: isinstance(t, str) and t.strip().lower().startswith(("/chatid", "/chatid@"))))
async def chatid_fallback(msg: Message):
    await msg.answer(f"Chat ID: <code>{msg.chat.id}</code>")

@dp.message(CommandStart())
async def start_cmd(msg: Message):
    await msg.answer(
        "Hi! I can:\n"
        "• Post 1 time per day at a random time (07:00–22:00 Stockholm) with our Forgiveness Chain message.\n"
        "• Roll dice with /roll (e.g., /roll 1d5 → 1..5).\n\n"
        "Commands:\n"
        "/enable_random – start daily random message\n"
        "/disable_random – stop daily message\n"
        "/status_random – show whether daily post is enabled\n"
        "/prophecy – announce now, reveal MERCY in 3 min, warn, then reveal PUNISHED at 5 min total\n"
        "/roll &lt;pattern&gt; – roll dice (examples: /roll 1d5, /roll 6, /roll 3d6)\n"
    )

@dp.message(Command("help"))
async def help_cmd(msg: Message):
    await start_cmd(msg)

@dp.message(Command("enable_random"))
async def enable_random_cmd(msg: Message):
    schedule_random_daily(msg.chat.id)
    await msg.answer("✅ Daily random post enabled for this chat.")

@dp.message(Command("disable_random"))
async def disable_random_cmd(msg: Message):
    job = random_jobs.pop(msg.chat.id, None)
    if job:
        try:
            job.remove()
        except Exception:
            pass
        await msg.answer("🛑 Daily random post disabled for this chat.")
    else:
        await msg.answer("It wasn’t enabled for this chat.")

@dp.message(Command("status_random"))
async def status_random_cmd(msg: Message):
    enabled = msg.chat.id in random_jobs
    await msg.answer(f"Status: {'Enabled ✅' if enabled else 'Disabled 🛑'}")

DICE_RE = re.compile(r"^\s*(\d+)\s*[dD]\s*(\d+)\s*$")
@dp.message(Command("roll"))
async def roll_cmd(msg: Message):
    text = msg.text or ""
    parts = text.strip().split(maxsplit=1)
    arg = parts[1] if len(parts) > 1 else "1d6"

    if arg.isdigit():
        sides = int(arg)
        if sides < 1:
            return await msg.answer("Sides must be ≥ 1.")
        result = _sysrand.randint(1, sides)
        return await msg.answer(f"🎲 1d{sides} → <b>{result}</b>")

    m = DICE_RE.match(arg)
    if m:
        count = int(m.group(1))
        sides = int(m.group(2))
        if count < 1 or sides < 1:
            return await msg.answer("Use positive integers, e.g., /roll 1d5")
        if count == 1:
            result = _sysrand.randint(1, sides)
            return await msg.answer(f"🎲 1d{sides} → <b>{result}</b>")
        rolls = [_sysrand.randint(1, sides) for _ in range(count)]
        total = sum(rolls)
        rolls_str = ", ".join(map(str, rolls))
        return await msg.answer(f"🎲 {count}d{sides} → [{rolls_str}]  |  Sum: <b>{total}</b>")

    return await msg.answer("Usage:\n/roll 1d5  (→ 1..5)\n/roll 6    (→ 1..6)\n/roll 3d6  (→ three 1..6 rolls + sum)")

# Manual trigger for the full prophecy sequence
@dp.message(Command("prophecy"))
async def prophecy_cmd(msg: Message):
    await start_prophecy_sequence(msg.chat.id)

# --------- Run bot + web server together ----------
app = FastAPI()

@app.get("/")
def health():
    return {"ok": True, "service": "pushup-prophet"}

@app.head("/")
def health_head():
    return Response(status_code=200)

async def run_bot():
    scheduler.start()
    await dp.start_polling(bot)

@app.on_event("startup")
async def on_startup():
    # makes sure no old webhook is set; polling will be the only mode
    await bot.delete_webhook(drop_pending_updates=True)

    asyncio.create_task(run_bot())  # start Telegram bot loop

    # Auto-enable daily schedule for groups listed in env var
    ids = os.getenv("GROUP_CHAT_IDS", "").strip()
    if ids:
        for raw in ids.split(","):
            raw = raw.strip()
            if not raw:
                continue
            try:
                chat_id = int(raw)
                schedule_random_daily(chat_id)
                logging.info(f"Auto-enabled daily random post for chat {chat_id}")
            except Exception as e:
                logging.exception(f"Failed to auto-enable for chat {raw}: {e}")

        # Auto-schedule weekly Prophecy every Sunday at 11:00 (Stockholm) for each chat
        for raw in ids.split(","):
            raw = raw.strip()
            if not raw:
                continue
            try:
                chat_id = int(raw)
                scheduler.add_job(
                    start_prophecy_sequence,
                    "cron",
                    day_of_week="sun",
                    hour=11,
                    minute=0,
                    args=[chat_id],
                    id=f"weekly_prophecy_{chat_id}",
                    replace_existing=True,
                )
                logging.info(f"Scheduled weekly prophecy (Sun 11:00) for chat {chat_id}")
            except Exception as e:
                logging.exception(f"Failed to schedule weekly prophecy for chat {raw}: {e}")

# If you want to run locally:
if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=False)
