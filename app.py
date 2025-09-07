import os, re, asyncio, logging, datetime as dt, random
from typing import Dict, Optional
from pathlib import Path

from fastapi import FastAPI
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

# --------- Run bot + web server together ----------
async def run_bot():
    scheduler.start()
    await dp.start_polling(bot)

app = FastAPI()

@app.get("/")
def health():
    return {"ok": True, "service": "pushup-prophet"}

@app.on_event("startup")
async def on_startup():
    # Start the Telegram bot loop in the background
    asyncio.create_task(run_bot())

# If you want to run locally:
if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=False)


