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

# ================== DICE OF FATE (no DB, one roll/day in-memory) ==================

# Weighted ranges on 1..100
FATE_BUCKETS = [
    (1, 3,   "miracle"),          # 3%
    (4, 15,  "shared_burden"),    # 12%
    (16, 25, "trial_form"),       # 10%
    (26, 35, "command_prophet"),  # 9%
    (36, 40, "mercy_coin"),       # 6%
    (41, 50, "hurricane"),        # 10%
    (51, 65, "oath_dawn"),        # 15%
    (66, 80, "trial_flesh"),      # 15%
    (81, 95, "tribute_blood"),    # 15%
    (96, 100,"wrath"),            # 5%
]

FATE_RULES_TEXT = (
    "<b>Dice of Fate – Outcomes</b>\n"
    "1–3 (3%) — ✨ <b>The Miracle</b> — Halve your debt\n"
    "4–15 (12%) — 🤝 <b>Shared Burden</b> — Give away 30 pushups to a random player\n"
    "16–25 (10%) — ⚔️ <b>Trial of Form</b> — 10 perfect pushups → erase 20 kr of debt\n"
    "26–35 (9%) — 👑 <b>Command of the Prophet</b> — Choose a player: 30 pushups or 30 kr\n"
    "36–40 (6%) — 🪙 <b>Mercy Coin</b> — Skip one regular pushup day\n"
    "\n"
    "41–50 (10%) — 🌪️ <b>Hurricane of Chaos</b> — +10 kr; shift 10% to another\n"
    "51–65 (15%) — 🌅 <b>Oath of Dawn</b> — Be first tomorrow or pay 30 kr\n"
    "66–80 (15%) — 🔥 <b>Trial of Flesh</b> — 100 pushups today or +45 kr\n"
    "81–95 (15%) — 🩸 <b>Tribute of Blood</b> — Pay 50 kr\n"
    "96–100 (5%) — ⚡ <b>Prophet’s Wrath</b> — Double your debt"
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

def _pick_fate_key() -> str:
    roll = _sysrand.randint(1, 100)
    for lo, hi, key in FATE_BUCKETS:
        if lo <= roll <= hi:
            return key
    return "wrath"

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
        "shared_burden": (
            "🤝 <b>Shared Burden</b>\n"
            + (
                f"Gift 30 pushups to <b>{html.escape(target_name)}</b>; let strength travel from hand to hand."
                if target_name else
                "Gift 30 pushups to a random player; let strength travel from hand to hand."
            )
        ),
        "trial_form": (
            "⚔️ <b>Trial of Form</b>\n"
            "Offer 10 perfect pushups—tempo true, depth honest—and erase 20 kr of debt."
        ),
        "command_prophet": (
            "👑 <b>Command of the Prophet</b>\n"
            "Name a player. They must choose: 30 pushups or 30 kr. Authority tests friendship."
        ),
        "mercy_coin": (
            "🪙 <b>Mercy Coin</b>\n"
            "One regular day is pardoned. Do not spend it cheaply."
        ),
        "hurricane": (
            "🌪️ <b>Hurricane of Chaos</b>\n"
            "Fortune stings and swirls: +10 kr, and a tithe of your weight shifts to another."
        ),
        "oath_dawn": (
            "🌅 <b>Oath of Dawn</b>\n"
            "Be first to rise tomorrow or pay 30 kr. Dawn reveals the faithful."
        ),
        "trial_flesh": (
            "🔥 <b>Trial of Flesh</b>\n"
            "Choose today: 100 pushups—or lay 45 kr upon the altar."
        ),
        "tribute_blood": (
            "🩸 <b>Tribute of Blood</b>\n"
            "The pot demands 50 kr. Pay without grudge, learn without delay."
        ),
        "wrath": (
            "⚡ <b>Prophet’s Wrath</b>\n"
            "Your debt is doubled. Pride withers; discipline takes its seat."
        ),
    }
    return texts.get(key, "The die rolls into shadow.") + f"\n\n<i>{end}</i>"

# Command to summon the Dice of Fate
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

    # If the fate is Shared Burden, pick a random player from roster
    target = None
    if fate_key == "shared_burden":
        target = _sysrand.choice(PLAYERS) if PLAYERS else None

    epic = _fate_epic_text(fate_key, target_name=target)

    await cb.answer()  # stop the inline-button spinner
    await cb.message.answer(epic)

# ================== End Dice section ==================

# ================== Gratitude / Blessings (single unified block, 5% favor) ==================
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

# /thanks command
@dp.message(Command("thanks"))
async def thanks_cmd(msg: Message):
    text = _compose_blessing(getattr(msg.from_user, "first_name", None))
    await msg.answer(text)

# Natural-language thanks (thanks/thank you/thx/ty/tack/tack så mycket)
THANKS_RE = re.compile(r"\b(thank(?:\s*you)?|thanks|thx|ty|tack(?:\s*så\s*mycket)?)\b", re.IGNORECASE)
@dp.message(F.text.func(lambda t: isinstance(t, str) and not t.strip().startswith("/") and THANKS_RE.search(t)))
async def thanks_plain(msg: Message):
    text = _compose_blessing(getattr(msg.from_user, "first_name", None))
    await msg.answer(text)
# ================== End Gratitude section ==================

# === Negativity / insult watcher (fixed compile flags) ===
INSULT_RE = re.compile(r"""
(
    # ---- A) Mention + insult (either order) ----
    (?:
        (?:\bpush\s*up\s*prophet\b|\bpushup\s*prophet\b|\bprophet\b|\bbot\b)
        .*?
        (?:\bf\W*u\W*c\W*k\b|\bf\*+ck\b|\bfuck(?:ing|er|ed)?\b|\bbull\W*sh(?:it|\*?t)\b|\bshit\b|\bcrap\b|\btrash\b|\bgarbage\b|\bsucks?\b|\bstupid\b|\bidiot\b|\bmoron\b|\bdumb\b|\bloser\b|\bpathetic\b|\blame\b|\bawful\b|\bterrible\b|\buseless\b|\bworthless\b|\bannoying\b|\bcringe\b|\bfraud\b|\bfake\b|\bclown\b|\bnonsense\b|\bbs\b|\bstfu\b|\bshut\s*up\b|\bgo\s*away\b|\bget\s*lost\b|\bscrew\s*you\b|\b(?:hate|hating)\s+(?:you|this)\b)
    )
    |
    (?:
        (?:\bf\W*u\W*c\W*k\b|\bf\*+ck\b|\bfuck(?:ing|er|ed)?\b|\bbull\W*sh(?:it|\*?t)\b|\bshit\b|\bcrap\b|\btrash\b|\bgarbage\b|\bsucks?\b|\bstupid\b|\bidiot\b|\bmoron\b|\bdumb\b|\bloser\b|\bpathetic\b|\blame\b|\bawful\b|\bterrible\b|\buseless\b|\bworthless\b|\bannoying\b|\bcringe\b|\bfraud\b|\bfake\b|\bclown\b|\bnonsense\b|\bbs\b|\bstfu\b|\bshut\s*up\b|\bgo\s*away\b|\bget\s*lost\b|\bscrew\s*you\b|\b(?:hate|hating)\s+(?:you|this)\b)
        .*?
        (?:\bpush\s*up\s*prophet\b|\bpushup\s*prophet\b|\bprophet\b|\bbot\b)
    )
    |
    # ---- B) Strong general phrases (catch-alls) ----
    \bfuck\s*this\s*shit\b
    |
    \bfuck(?:ing)?\s+bull\W*shit\b
    |
    \b(?:bull\W*shit|bullsh\*?t)\s*(?:prophet|bot|push\s*up\s*prophet)\b
    |
    \b(?:garbage\s*bot|trash\s*bot|worst\s*bot)\b
    |
    \b(?:fuck\s*off|go\s*to\s*hell)\b
)
""", re.IGNORECASE | re.VERBOSE)

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
    # 10% chance of a 10 kr/20 kr penance (your code currently uses 20 kr)
    if _sysrand.random() < 0.10:
        base += "\n\n<b>Edict:</b> Lay <b>20 kr</b> in the pot as penance for disrespect."
    return base

@dp.message(F.text.func(lambda t: isinstance(t, str) and INSULT_RE.search(t)))
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

# Official command: ONLY /share_wisdom (aliases removed)
@dp.message(Command("share_wisdom"))
async def share_wisdom_cmd(msg: Message):
    await _send_next_quote_to_chat(msg.chat.id)

# Natural-language trigger for "share wisdom" (no slash)
SHARE_WISDOM_NAT = re.compile(r"\bshare\s+wisdom\b", re.IGNORECASE)
@dp.message(F.text.func(lambda t: isinstance(t, str)
                        and not t.strip().startswith("/")
                        and SHARE_WISDOM_NAT.search(t)))
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
                        and not SHARE_WISDOM_NAT.search(t)))
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
        "I am the Pushup Prophet! For my loyal subjects, I can:\n"
        "• /share_wisdom for those who seek knowledge and guidance.\n"
        "• Roll dice (e.g., /roll 1d5 → 1..5).\n"
        "• Summon the Dice of Fate with /fate (one roll per person per day).\n\n"
        "Commands:\n"
        "/enable_random – start daily random message\n"
        "/disable_random – stop daily message\n"
        "/status_random – show whether daily post is enabled\n"
        "/thanks – receive a blessing (5% favor)\n"
        "\n"
        "Remember to always show your gratitude to the Pushup Prophet.\n"
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
app = FastAPI()

@app.get("/")
def health():
    return {"ok": True, "service": "pushup-prophet"}

@app.head("/")
def health_head():
    return Response(status_code=200)

async def run_bot():
    # Start scheduler first, then polling
    scheduler.start()
    await dp.start_polling(bot)

@app.on_event("startup")
async def on_startup():
    # Try to remove any webhook; don't crash the app if Telegram is slow/unreachable
    try:
        await bot.delete_webhook(drop_pending_updates=True, request_timeout=10)
    except Exception as e:
        logging.warning(f"delete_webhook failed ({e!r}); continuing with polling.")

    # Start the bot loop in the background
    asyncio.create_task(run_bot())

    # Pre-schedule jobs for listed group chats
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
            except Exception as e:
                logging.exception(f"Startup scheduling failed for chat {raw}: {e}")

# If you want to run locally:
if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=False)
