import os, re, asyncio, logging, datetime as dt, random, html
from typing import Dict, Optional
from pathlib import Path
from collections import deque

from fastapi import FastAPI, Response
import uvicorn

from aiogram import Bot, Dispatcher
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
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
            # follow-up exactly 1 hour later
            await asyncio.sleep(60 * 60)
            await bot.send_message(
                chat_id,
                "The hour has passed, the covenant stands. No debt weighs upon those who rise in unison. "
                "The choice has always been yours. I hope you made the right one."
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
    safe = html.escape(q)
    await bot.send_message(chat_id, f"🕖 Daily Wisdom\n{safe}")

# ================== DICE OF FATE ==================

# New weights (sum to 100)
FATE_WEIGHTS = [
    ("miracle",         3),  # 3%
    ("mercy_coin",      6),  # 6%
    ("trial_form",     10),  # 10%
    ("command_prophet",10),  # 10%
    ("giver",          12),  # 12%  (renamed from shared_burden)
    ("hurricane",      10),  # 10%
    ("oath_dawn",      16),  # 16%
    ("trial_flesh",    15),  # 15%
    ("tribute_blood",  15),  # 15%
    ("wrath",           3),  # 3%
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

# Per-chat in-memory limiter: {chat_id: (date, set(user_id))}
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

# --- /fate command & flow ---
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

    # If the fate is The Giver, pick a random player from roster
    target = None
    if fate_key == "giver":
        target = _sysrand.choice(PLAYERS) if PLAYERS else None

    epic = _fate_epic_text(fate_key, target_name=target)

    await cb.answer()  # stop the inline-button spinner

    if fate_key == "hurricane":
        # Add a button to select a random player for the 10% shift
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🎯 Select random player", callback_data="hurricane:spin")
        ]])
        await cb.message.answer(epic, reply_markup=kb)
    else:
        await cb.message.answer(epic)

# Handle the Hurricane random player selection
@dp.callback_query(F.data == "hurricane:spin")
async def hurricane_spin(cb: CallbackQuery):
    # Try to avoid picking the invoker themself if their first_name matches a PLAYERS entry.
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

# Natural-language trigger to summon the Dice of Fate (no slash)
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

# ================== Gratitude / Blessings (5% favor, anti-abuse) ==================
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
    # 5% chance of a small gift
    if _sysrand.random() < 0.05:
        base += "\n\n🪙 <b>Favor of Gratitude</b> — Deduct <b>20 kr</b> from your debt for your loyalty."
    return base

# --- Anti-abuse limiter for gratitude (per-user, per-day) ---
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

# Natural-language thanks (thanks/thank you/thx/ty/tack/tack så mycket)
THANKS_RE = re.compile(r"\b(thank(?:\s*you)?|thanks|thx|ty|tack(?:\s*så\s*mycket)?)\b", re.IGNORECASE)
@dp.message(F.text.func(lambda t: isinstance(t, str) and not t.strip().startswith("/") and THANKS_RE.search(t)))
async def thanks_plain(msg: Message):
    attempts = _gratitude_inc_and_get(msg.from_user.id)
    name = getattr(msg.from_user, "first_name", None)
    if attempts > 5:
        await msg.answer(_compose_gratitude_penalty(name))
    else:
        await msg.answer(_compose_blessing(name))

# ================== Apologies / Absolution (kind but stern) ==================
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
    text = _compose_absolution(getattr(msg.from_user, "first_name", None))
    await msg.answer(text)

# === Negativity / insult watcher (EXHAUSTIVE & robust) ===
# Covers English + Swedish, obfuscations (f*u*c*k / f.u.c.k / fucc / fuxk), and common phrases.
# We intentionally exclude hate slurs toward protected classes.

# Core fuzzy building blocks
FUQ = r"f[\W_]*u[\W_]*c[\W_]*k"
FUCKISH = rf"(?:{FUQ}(?:ing|er|ed)?|f\*+ck|fu+ck|fucc|fuxk|fck)"
SHIT = r"s[\W_]*h[\W_]*i[\W_]*t"
BITCH = r"b[\W_]*i[\W_]*t[\W_]*c[\W_]*h"
ASSHOLE = r"a[\W_]*s[\W_]*s[\W_]*h[\W_]*o[\W_]*l[\W_]*e"
CUNT = r"c[\W_]*u[\W_]*n[\W_]*t"
DICK = r"d[\W_]*i[\W_]*c[\W_]*k"
PUSSY = r"p[\W_]*u[\W_]*s[\W_]*s[\W_]*y"
BS = rf"(?:bull[\W_]*{SHIT}|b[\W_]*s\b|bs\b)"
SUCKS = r"suck(?:s|ing)?"

# Mentions of the bot (to pair with insults either order)
MENTION = r"(?:\bpush\s*up\s*prophet\b|\bpushup\s*prophet\b|\bprophet\b|\bbot\b|@pushupprophetbot)"

# Strong standalone phrases (always trigger)
STRONG = r"(?:"
STRONG += rf"{FUCKISH}\s*(?:you|u)\b|"                              # fuck you / f u
STRONG += rf"go[\W_]*{FUQ}[\W_]*yourself|"                          # go fuck yourself
STRONG += rf"{FUQ}[\W_]*off\b|"                                     # fuck off
STRONG += rf"shut[\W_]*up\b|"                                       # shut up
STRONG += rf"\bstfu\b|"                                             # stfu
STRONG += rf"you[\W_]*{SUCKS}\b|"                                   # you suck / you sucking
STRONG += rf"piece[\W_]*of[\W_]*{SHIT}\b|"                          # piece of shit
STRONG += rf"{BS}\b|"                                               # bullshit / b.s. / bs
STRONG += rf"go[\W_]*to[\W_]*hell\b|"                               # go to hell
STRONG += rf"drop[\W_]*dead\b|"                                     # drop dead
STRONG += rf"kill[\W_]*yourself\b|"                                 # kill yourself
STRONG += r"\bkys\b"                                                # kys
STRONG += r")"

# Single-word/short insults (English + Swedish)
WORDS = r"(?:"
WORDS += rf"{FUCKISH}|{SHIT}|{BITCH}|{ASSHOLE}|{CUNT}|{DICK}|{PUSSY}|"
WORDS += r"crap|trash|garbage|lame|awful|terrible|useless|worthless|annoying|fraud|fake|clown|nonsense|"
WORDS += r"stupid|idiot|moron|dumb(?:ass)?|loser|sucks?|"
# Swedish (avoid protected-class slurs)
WORDS += r"skit|skr(?:a|ä)p|suger|j(?:a|ä)vla|helvete|kuk|fitta|idiot|dum(?:\s*i\s*huvudet)?"
WORDS += r")"

# Swedish strong phrases (standalone)
SWE_STRONG = r"(?:"
SWE_STRONG += r"dra[\W_]*a(?:̊|a)t[\W_]*helvete\b|"                 # dra åt helvete
SWE_STRONG += r"h(?:a|å)ll[\W_]*k(?:a|ä)ft(?:en)?\b|"               # håll käften / hall kaften
SWE_STRONG += r"h(?:a|å)ll[\W_]*truten\b|"                          # håll truten
SWE_STRONG += r"stick\b|"                                           # stick
SWE_STRONG += r"f(?:o|ö)rsvinn\b|"                                  # försvinn / forsvinn
SWE_STRONG += r"dra[\W_]*(?:h(?:a|ä)rifr(?:a|å)n)\b"                # dra härifrån
SWE_STRONG += r")"

# Final detector:
#  - mention ± insult within 0–60 chars (either order)
#  - or any strong phrase (EN/SV) by itself
INSULT_RE = re.compile(
    rf"""
    (
        (?:{MENTION}[\s\S]{{0,60}}(?:{STRONG}|{WORDS}))
        |
        (?:(?:{STRONG}|{WORDS})[\s\S]{{0,60}}{MENTION})
        |
        {STRONG}
        |
        {SWE_STRONG}
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
    # 10% chance of a 20 kr penance
    if _sysrand.random() < 0.10:
        base += "\n\n<b>Edict:</b> Lay <b>20 kr</b> in the pot as penance for disrespect."
    return base

@dp.message(F.text.func(lambda t: isinstance(t, str)
                        and INSULT_RE.search(t)
                        and not APOLOGY_RE.search(t)))
async def prophet_insult_rebuke(msg: Message):
    text = _compose_rebuke(getattr(msg.from_user, "first_name", None))
    await msg.answer(text)
# === end insult watcher ===

# --- Helpers for sending the next quote ---
async def _send_next_quote_to_chat(chat_id: int):
    q = _next_quote(chat_id)
    if not q:
        await bot.send_message(chat_id, "No quotes configured yet.")
        return
    await bot.send_message(chat_id, html.escape(q))

# Official command: ONLY /share_wisdom
@dp.message(Command("share_wisdom"))
async def share_wisdom_cmd(msg: Message):
    await _send_next_quote_to_chat(msg.chat.id)

# Natural-language trigger for asking wisdom (no slash)
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

# Fallback for people who type `/share wisdom`
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
    await msg.answer(_sysrand.choice(SUMMON_RESPONSES))

# --------- Other Handlers ----------
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
        "I am the Pushup Prophet.\n\n"
        "What I can do for you:\n"
        "• Ask for wisdom and I shall give.\n"
        "• Summon the Dice of Fate (one roll per person per day).\n\n"
        "Be aware of the Forgiveness Chain:\n"
        "• /enable_random — enable the Forgiveness Chain for this chat.\n"
        "• /disable_random — disable the Forgiveness Chain for this chat.\n"
        "• /status_random — check whether the Forgiveness Chain is enabled.\n\n"
        "You shall see me at every dawn. May the power of the Push be with you."
    )

@dp.message(Command("help"))
async def help_cmd(msg: Message):
    await start_cmd(msg)

@dp.message(Command("enable_random"))
async def enable_random_cmd(msg: Message):
    schedule_random_daily(msg.chat.id)
    await msg.answer("✅ Forgiveness Chain enabled for this chat. I will announce once per day between 07:00–22:00 Stockholm, then follow up 1 hour later.")

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
app = FastAPI()

@app.get("/")
def health():
    return {"ok": True, "service": "pushup-prophet"}

@app.head("/")
def health_head():
    return Response(status_code=200)

async def run_bot():
    scheduler.start()
    logger.info("Scheduler started")
    await dp.start_polling(bot)

@app.on_event("startup")
async def on_startup():
    # Identify the bot and log it early
    try:
        me = await bot.get_me()
        logger.info(f"Bot authorized: @{me.username} (id={me.id})")
    except Exception as e:
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
            logger.warning(f"delete_webhook attempt {attempt} failed: {e}")
        await asyncio.sleep(min(2 ** attempt, 10))
    else:
        logger.error("Could not clear webhook after multiple attempts. Polling may not receive updates.")

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
                # Forgiveness Chain daily random-time window (+1h follow-up)
                schedule_random_daily(chat_id)
                logger.info(f"Auto-enabled Forgiveness Chain for chat {chat_id}")

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
                logger.info(f"Scheduled daily quote (07:00) for chat {chat_id}")

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

# If you want to run locally:
if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=False)

