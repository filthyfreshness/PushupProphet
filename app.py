import os, re, asyncio, logging, datetime as dt, random, html
from typing import Dict, Optional
from pathlib import Path
from collections import deque

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
DAILY_TEXT = "THE FORGIVENESS CHAIN BEGINS NOW. Lay down excuses and ascend. May the power of Push be with you all."
WINDOW_START = 7   # 07:00
WINDOW_END = 22    # 22:00 (inclusive)

FOLLOWUP_TEXT = (
    "The hour has passed, the covenant stands. No debt weighs upon those who rise in unison. "
    "The choice has always been yours. I hope you made the right one."
)


# ---- Weekly Prophecy config ----
PEOPLE = {1: "Fresh", 2: "Momo", 3: "Valle", 4: "Tän", 5: "Hampa"}
ALLOW_REPEAT = True  # False => mercy & punishment must be different

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
            # 1) Send the Forgiveness Chain announcement
            await bot.send_message(chat_id, DAILY_TEXT)

            # 2) Schedule the 1-hour follow-up message
            follow_time = dt.datetime.now(TZ) + dt.timedelta(hours=1)

            async def send_followup():
                await bot.send_message(chat_id, FOLLOWUP_TEXT)

            scheduler.add_job(send_followup, "date", run_date=follow_time)

        finally:
            # 3) Schedule tomorrow's random-time post
            tomorrow = dt.datetime.now(TZ) + dt.timedelta(days=1)
            next_run = next_random_time(tomorrow)
            new_job = scheduler.add_job(send_and_reschedule, "date", run_date=next_run)
            random_jobs[chat_id] = new_job

    job = scheduler.add_job(send_and_reschedule, "date", run_date=run_at)
    random_jobs[chat_id] = job


# ================== Quotes rotation (daily 07:00 + /share_wisdom) ==================

QUOTES = [
    "“Gravity is my quill; with each rep I write strength upon your bones.”",
    "“Do not count your pushups—make your pushups count, and the numbers will fear you.”",
    "“Form is truth. Without truth, repetitions are only noise.”",
    "“When your arms tremble, listen—this is your weakness volunteering to leave.”",
    "“The floor is not your enemy; it is the altar where you lay down excuses.”",
    "“Consistency is the spell that turns effort into destiny.”",
    "“Breathe like a tide, move like a vow, rise like a promise kept.”",
    "“Rest is the secret rep—unseen, but written in tomorrow’s power.”",
    "“Progress bows to patience; ego bows to technique.”",
    "“Kiss the earth with your chest and return wiser—every descent is a teacher, every ascent a testimony.”",
    "“The plank is the parent of the pushup; honor the parent, and the child grows mighty.”",
    "“Do not bargain with depth—the earth hears every half-truth.”",
    "“Ego loads the shoulders; wisdom loads the calendar.”",
    "“Reps are a language: tension the grammar, breath the punctuation.”",
    "“A straight spine tells a straight story—lie not to your lower back.”",
    "“When progress stalls, change the question: narrower hands, slower descent, truer form.”",
    "“Tempo reveals character—pause at the bottom and meet yourself.”",
    "“If wrists protest, rotate the world: fists, handles, or incline—wisdom bends, not breaks.”",
    "“Your first clean pushup is a door; your thousandth, a road.”",
    "“On doubtful days, do one honest rep—prophecy begins with a single truth.”",
    "“On heavy days, shorten the sets, never the standard.”",
    "“A missed day costs coin, not destiny—pay, confess, continue.”",
    "“When the Forgiveness Chain is cast, move as one; shared discipline lightens every debt.”",
    "“Day 75 tests the mind—slow the tempo, breathe the count, and the wall becomes a doorway.”",
    "“Day 100 is not an ending but an inheritance—keep a daily tithe: one perfect pushup to remember who you became.”",
    "“Mid-journey math: divide the mountain into honest tens and climb.”",
    "“Protect the wrists, warm the shoulders—oil the hinges before opening the heavy door.”",
    "“Let the last set be the cleanest; finish with dignity, not desperation.”",
    "“Debt may weigh your coin; poor form will tax your future—pay the pot, not your joints.”",
    "“Schedule is a silent spotter—set alarms, stack habits, keep promises.”",
    "“When doubt visits, breathe a three-count descent and meet yourself at the bottom.”",
    "“On day 100, do not stop—carry a legacy forward: one perfect pushup, every day, forever.”",
    "“The floor keeps perfect score; it only counts the truth.”",
    "“Brace the core, squeeze the glutes—make your body one unbroken vow.”",
    "“Elbows close like gates at forty-five; open wider and the storm will enter.”",
    "“Touch the ground with your chest, not your pride.”",
    "“The lockout is a promise; break it and the next rep breaks you.”",
    "“Incline is a bridge, not an excuse—cross it to reach mastery.”",
    "“Grease the groove: many doors open to those who knock lightly and often.”",
    "“Slow negatives carve strength into stone.”",
    "“He who rushes the bottom dodges the lesson.”",
    "“Your scapulae are wings; spread at the top, glide to the next ascent.”",
    "“Hydrate your discipline; dry resolve cracks.”",
    "“Sleep is the smithy where today’s efforts become tomorrow’s iron.”",
    "“Warm-up is the toll you pay to cross into heavy work.”",
    "“If pain speaks in joints, listen with humility and change the path.”",
    "“Count sets by breaths: three in descent, three out to rise—let calm lead effort.”",
    "“Do fewer with honor rather than many with alibis.”",
    "“A straight gaze steadies the spine; look where you wish to go.”",
    "“When companions falter, lend cadence not judgment.”",
    "“Deload to reload—the bow that never slackens cannot fire true.”",
    "“Technique first, volume second, vanity never.”",
    "“Hands beneath shoulders—foundations belong under walls.”",
    "“On the hardest days, move at the speed of honesty.”",
    "“Record your reps; memory flatters, ink does not.”",
    "“Make the last two centimeters your signature.”",
    "“Strength grows in quiet places—between sets, between days.”",
    "“Let discipline be boring and results be loud.”",
    "“Finish your promise on the floor, then carry it into your life.”",
    "“Treat the first set like a greeting and the last like a goodbye—both deserve respect.”",
    "“Diamond hands belong under your heart—narrow the base to widen your courage.”",
    "“Archer pushups teach patience; strength favors those who learn to lean.”",
    "“Decline is not defeat; it is ascent by another name.”",
    "“Between rep and rep lives posture; guard it like a secret.”",
    "“Fatigue is honest; negotiate with sets, not standards.”",
    "“Train the serratus—protract at the top and you shall press with the whole ribcage.”",
    "“Your breath is a metronome; let it set the pace your pride cannot.”",
    "“A century of days is built from minutes; put them where your mouth is.”",
    "“The floor is a mirror—approach it with the face you want to wear.”",
    "“Do not chase burn; chase precision—the fire will follow.”",
    "“Strength is a quiet harvest; sow today, reap when no one claps.”",
    "“Every rep has a birthplace: the brace.”",
    "“If shoulders roll forward, call the scapula home.”",
    "“The first rep proves your readiness; the last rep proves your character.”",
    "“Hard sets whisper lessons that easy sets never learn.”",
    "“Make your warm-up a love letter to your joints.”",
    "“Skill is the savings account of effort; deposit daily.”",
    "“Pushups do not make you humble; poor form should.”",
    "“Depth is democratic—everyone can afford the truth.”",
    "“Chase mastery like a shadow; it stays with those who move in light.”",
    "“When numbers rise, range must not fall.”",
    "“Control the descent, own the ascent.”",
    "“Rotate your variations; monotony is the rust of progress.”",
    "“Incline for learning, decline for earning, standard for judgment.”",
    "“Let soreness be a storyteller, not a jailer.”",
    "“If the floor is far, stack books—build knowledge and height together.”",
    "“Five clean now beats fifty crooked later.”",
    "“Reset your hands, reset your mind.”",
    "“Pauses forge honesty at the bottom; lockouts stamp the seal at the top.”",
    "“Count integrity, then reps.”",
    "“The day you don’t want to is the day you must.”",
    "“Community multiplies resolve; match your cadence to the slowest and bring them home.”",
    "“A single crooked rep teaches more than a hundred excuses.”",
    "“Your chest meets the earth; your spirit meets its standard.”",
    "“Make failure a data point, not a destiny.”",
    "“Recovery writes the chapter your training begins.”",
    "“Calories are ink; protein is the bold font.”",
    "“Stretch the pecs, open the T-spine—unlock the door you keep pushing.”",
    "“Keep elbows soft at the top; locked is lawful, jammed is foolish.”",
    "“Raise your standards before you raise your reps.”",
]

# Per-chat rotation state: each chat_id has a deque of randomized quote indices.
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
    # Escape for HTML mode to avoid parse errors if quote has <, >, &
    safe = html.escape(q)
    await bot.send_message(chat_id, f"🕖 Daily Wisdom\n{safe}")

# ================== End Quotes section ==================

# ================== Weekly Prophecy (Sun 11:00 + /prophecy) ==================

async def start_prophecy_sequence(chat_id: int):
    """
    t+0:  announce
    t+3m: reveal MERCY (name from PEOPLE)
    t+4m: warn punishment in 1 minute
    t+5m: reveal PUNISHED + final line
    """
    announce = (
        "ATTENTION EVERYONE\n"
        "The Prophecy has been revealed! In 3 minutes, you will hear WHO WILL RECEIVE MERCY tomorrow.\n"
        "@Wabbadabbadubdub @Hampuz @FilthyFresh @ThugnificentMomo @Bowlcut00 "
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
        await bot.send_message(
            chat_id,
            f"🔮 The Prophecy speaks...\nWHO WILL RECEIVE MERCY tomorrow: <b>{PEOPLE[n]}</b>"
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
        await bot.send_message(
            chat_id,
            f"⚖️ The verdict is in.\nWHO WILL BE THE PUNISHED ONE: <b>{PEOPLE[n]}</b>"
        )
        await bot.send_message(
            chat_id,
            "The weekly prophecies have been spoken. May the force of the Pushup Prophet be with you!"
        )

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
        "• Share a daily quote at 07:00 Stockholm (per group) and rotate through your list randomly without repeats.\n"
        "• Roll dice with /roll (e.g., /roll 1d5 → 1..5).\n\n"
        "Commands:\n"
        "/share_wisdom – send the next quote now\n"
        "/wisdom – same as /share_wisdom\n"
        "/quote – same as /share_wisdom\n"
        "/prophecy – announce now, reveal MERCY in 3m, warn at 4m, reveal PUNISHED at 5m\n"
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

# --- Helpers for sending the next quote ---
async def _send_next_quote_to_chat(chat_id: int):
    q = _next_quote(chat_id)
    if not q:
        await bot.send_message(chat_id, "No quotes configured yet.")
        return
    await bot.send_message(chat_id, html.escape(q))

# Official command with aliases: /share_wisdom, /wisdom, /quote
@dp.message(Command("share_wisdom", "wisdom", "quote"))
async def share_wisdom_cmd(msg: Message):
    await _send_next_quote_to_chat(msg.chat.id)

# Fallback for people who type `/share wisdom`
@dp.message(F.text.func(lambda t: isinstance(t, str) and t.strip().lower().startswith("/share wisdom")))
async def share_wisdom_space_alias(msg: Message):
    await _send_next_quote_to_chat(msg.chat.id)

# Manual trigger for the prophecy sequence
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
                # Forgiveness Chain daily random-time window
                schedule_random_daily(chat_id)
                logging.info(f"Auto-enabled daily random post for chat {chat_id}")

                # Daily quotes at 07:00 Stockholm
                scheduler.add_job(
                    send_daily_quote,
                    "cron",
                    hour=7,
                    minute=0,
                    args=[chat_id],
                    id=f"daily_quote_{chat_id}",
                    replace_existing=True,
                )
                logging.info(f"Scheduled daily quote (07:00) for chat {chat_id}")

                # Weekly Prophecy every Sunday at 11:00 Stockholm
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
                logging.exception(f"Startup scheduling failed for chat {raw}: {e}")

# If you want to run locally:
if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=False)

