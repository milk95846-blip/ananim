import logging
import datetime
import asyncio
import uuid
import os
import psycopg2
from psycopg2.extras import DictCursor

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, ReplyKeyboardRemove, BotCommand, InputMediaPhoto
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
    ConversationHandler,
    CallbackQueryHandler
)
from telegram.constants import ParseMode, ChatAction
from telegram.error import Forbidden, BadRequest
from functools import wraps

# --- НАСТРОЙКИ ИЗ ПЕРЕМЕННЫХ ОКРУЖЕНИЯ ХОСТИНГА ---
BOT_TOKEN = os.getenv("BOT_TOKEN", "8284261615:AAEFCwzGn1c-WuR1otmpwO39zc5W0npEo_4")
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID", "5364433992")
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("КРИТИЧЕСКАЯ ОШИБКА: Не найдена переменная окружения DATABASE_URL. Добавьте аддон PostgreSQL на Scalingo.")

# --- Настройки системы предупреждений ---
FORBIDDEN_CHARS = {'и', 'щ', 'ъ', 'И', 'Щ', 'Ъ'}
WARNING_LIMIT = 3
AMNESTY_CODE = "АДРАДЖЭННЕ"

# Состояния для ConversationHandlers
(AWAITING_BROADCAST_MESSAGE, AWAITING_INFO_ID, AWAITING_SENDTO_IDS, AWAITING_SENDTO_MESSAGE, AWAITING_REPORT_SCREENSHOTS) = range(5)
CHAT_STATUS_IDLE, CHAT_STATUS_WAITING, CHAT_STATUS_CHATTING = "idle", "waiting", "chatting"

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# --- КЛАВИАТУРЫ ---
ADMIN_MAIN_MENU_KEYBOARD = ReplyKeyboardMarkup([["📊 Статыстыка", "👥 Карыстальнікі"],["📣 Рассылкі", "💬 Выпадковы чат"],["🆘 SOS-чаты", "⚙️ Сыстэма"]], resize_keyboard=True)
ADMIN_USERS_MENU_KEYBOARD = ReplyKeyboardMarkup([["ℹ️ Інфо пра юзэра", "📋 Сьпіс усіх"],["🔙 Галоўнае мэню"]], resize_keyboard=True)
ADMIN_BROADCAST_MENU_KEYBOARD = ReplyKeyboardMarkup([["📣 Усім", "🎯 Выбраным"],["🔙 Галоўнае мэню"]], resize_keyboard=True)
ADMIN_SYSTEM_MENU_KEYBOARD = ReplyKeyboardMarkup([["🗑️ Ачысьціць гісторыю чатаў"],["🔙 Галоўнае мэню"]], resize_keyboard=True)


# --- ФУНКЦИИ РАБОТЫ С БАЗОЙ ДАННЫХ POSTGRESQL ---

def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

def initialize_databases():
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY, first_name TEXT, username TEXT,
                    start_time TIMESTAMPTZ DEFAULT NOW(), last_active_time TIMESTAMPTZ DEFAULT NOW(),
                    chat_status TEXT DEFAULT 'idle', current_chat_partner BIGINT, current_chat_session TEXT,
                    is_banned BOOLEAN DEFAULT FALSE, warnings INTEGER DEFAULT 0, has_blocked_bot BOOLEAN DEFAULT FALSE
                )""")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS chat_logs (
                    log_id SERIAL PRIMARY KEY, session_id TEXT NOT NULL, timestamp TIMESTAMPTZ DEFAULT NOW(),
                    sender_id BIGINT NOT NULL, partner_id BIGINT NOT NULL, message_id BIGINT NOT NULL,
                    message_type TEXT NOT NULL, message_text TEXT, file_id TEXT
                )""")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS message_links (
                    source_chat_id BIGINT NOT NULL, source_message_id BIGINT NOT NULL, dest_chat_id BIGINT NOT NULL,
                    dest_message_id BIGINT NOT NULL, timestamp TIMESTAMPTZ DEFAULT NOW(),
                    PRIMARY KEY (source_chat_id, source_message_id)
                )""")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_dest_message_psql ON message_links (dest_chat_id, dest_message_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_session_id_psql ON chat_logs (session_id)")
    logger.info("Проверка/инициализация таблиц в базе данных завершена.")

def get_user(user_id):
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("SELECT * FROM users WHERE user_id = %s", (user_id,))
            user = cur.fetchone()
            return dict(user) if user else None

def get_all_users():
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("SELECT user_id, first_name, username, is_banned, has_blocked_bot, warnings FROM users")
            return {str(row['user_id']): dict(row) for row in cur.fetchall()}

def update_user(user_data):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO users (user_id, first_name, username, start_time, last_active_time, chat_status, current_chat_partner, current_chat_session, is_banned, warnings, has_blocked_bot)
                VALUES (%(user_id)s, %(first_name)s, %(username)s, %(start_time)s, %(last_active_time)s, %(chat_status)s, %(current_chat_partner)s, %(current_chat_session)s, %(is_banned)s, %(warnings)s, %(has_blocked_bot)s)
                ON CONFLICT (user_id) DO UPDATE SET
                    first_name = EXCLUDED.first_name, username = EXCLUDED.username, last_active_time = EXCLUDED.last_active_time,
                    chat_status = EXCLUDED.chat_status, current_chat_partner = EXCLUDED.current_chat_partner,
                    current_chat_session = EXCLUDED.current_chat_session, is_banned = EXCLUDED.is_banned,
                    warnings = EXCLUDED.warnings, has_blocked_bot = EXCLUDED.has_blocked_bot;
            """, user_data)

def reset_all_user_statuses_on_startup():
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE users SET chat_status = %s, current_chat_partner = NULL, current_chat_session = NULL WHERE chat_status IN (%s, %s)",
                            (CHAT_STATUS_IDLE, CHAT_STATUS_CHATTING, CHAT_STATUS_WAITING))
        logger.info("Статусы пользователей в БД были сброшены после перезапуска.")
    except Exception as e:
        logger.error(f"Не удалось сбросить статусы пользователей в БД при запуске: {e}")

def log_chat_message(sender_id: str, partner_id: str, message: Update.message, session_id: str):
    data = {'type': 'unknown', 'text': None, 'file_id': None}
    if message.text: data.update({'type': 'text', 'text': message.text})
    elif message.sticker: data.update({'type': 'sticker', 'file_id': message.sticker.file_id})
    elif message.photo: data.update({'type': 'photo', 'file_id': message.photo[-1].file_id, 'text': message.caption})
    elif message.video: data.update({'type': 'video', 'file_id': message.video.file_id, 'text': message.caption})
    elif message.voice: data.update({'type': 'voice', 'file_id': message.voice.file_id, 'text': message.caption})
    elif message.audio: data.update({'type': 'audio', 'file_id': message.audio.file_id, 'text': message.caption})
    elif message.document: data.update({'type': 'document', 'file_id': message.document.file_id, 'text': message.caption})
    elif message.video_note: data.update({'type': 'video_note', 'file_id': message.video_note.file_id})
    message_id = message.message_id if hasattr(message, 'message_id') else 0
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO chat_logs (session_id, sender_id, partner_id, message_id, message_type, message_text, file_id) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (session_id, int(sender_id), int(partner_id), message_id, data['type'], data['text'], data['file_id']))

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---

def check_if_banned(func):
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        user_telegram = update.effective_user
        if not user_telegram:
            if hasattr(update, 'callback_query'): user_telegram = update.callback_query.from_user
        if not user_telegram: return

        user_id = user_telegram.id
        user_data = get_user(user_id)

        is_new_user = not user_data
        if is_new_user:
            user_data = {
                "user_id": user_id, "first_name": user_telegram.first_name, "username": user_telegram.username or "няма",
                "start_time": datetime.datetime.now(datetime.timezone.utc), "last_active_time": datetime.datetime.now(datetime.timezone.utc),
                "chat_status": CHAT_STATUS_IDLE, "current_chat_partner": None, "current_chat_session": None,
                "is_banned": False, "warnings": 0, "has_blocked_bot": False
            }
            logger.info(f"Зарегистрирован новый пользователь: {user_id} ({user_telegram.first_name})")
        
        context.user_data['is_new_user'] = is_new_user
        
        if user_data.get('is_banned', False):
            if update.message and update.message.text == AMNESTY_CODE:
                await handle_amnesty_code(update, context)
            return

        user_data['last_active_time'] = datetime.datetime.now(datetime.timezone.utc)
        if user_data.get('has_blocked_bot'):
             user_data['has_blocked_bot'] = False
        
        if user_data.get("username") != (user_telegram.username or "няма") or user_data.get("first_name") != user_telegram.first_name:
            user_data["username"] = user_telegram.username or "няма"
            user_data["first_name"] = user_telegram.first_name

        update_user(user_data)
        return await func(update, context, *args, **kwargs)
    return wrapper

def is_admin(user_id: str) -> bool: return user_id == ADMIN_CHAT_ID

def find_user_id_by_identifier(identifier: str) -> int | None:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            try:
                if identifier.isdigit():
                    cur.execute("SELECT user_id FROM users WHERE user_id = %s", (int(identifier),))
                elif identifier.startswith('@'):
                    cur.execute("SELECT user_id FROM users WHERE username = %s", (identifier[1:],))
                else: return None
                result = cur.fetchone()
                return result[0] if result else None
            except Exception:
                return None

def get_user_display_name(user_id: str, users_db: dict) -> str:
    user_info = users_db.get(str(user_id), {})
    return user_info.get("first_name") or f"User {user_id}"

def mark_user_as_bot_blocker(user_id: str):
    user_data = get_user(int(user_id))
    if user_data and not user_data.get('has_blocked_bot'):
        user_data['has_blocked_bot'] = True
        update_user(user_data)
        logger.info(f"User {user_id} has likely blocked the bot. Marked.")

# --- ОСНОВНЫЕ ФУНКЦИИ БОТА ---

@check_if_banned
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    is_new_user = context.user_data.get('is_new_user', False)
    if is_admin(str(user.id)):
        welcome_text = "Вітаю, Адміністратар! Вашая адмысловая клявіятура актываваная."
        reply_markup = ADMIN_MAIN_MENU_KEYBOARD
    else:
        if is_new_user:
            welcome_text = (f"Прывітаньне, {user.first_name}! 👋\n\nГэта ананімны чат. "
                            f"Каб пачаць, выкарыстоўвайце каманду /search.\n"
                            f"Таксама раім азнаёміцца з /rules.\n\n"
                            f"Каб убачыць усе даступныя дзеяньні, націсьніце на кнопку '☰' або '/' побач з полем уводу тэксту.")
        else:
            welcome_text = f"Зь вяртаннем, {user.first_name}! Карыстайцеся мэню камандаў для навігацыі."
        reply_markup = ReplyKeyboardRemove()
    await update.message.reply_text(welcome_text, reply_markup=reply_markup)

@check_if_banned
async def rules_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    rules_text = ("**Да ўвагі ўдзельнікаў гэтага чату!**\n\n"
                  "Сябры, варта разумець фундаментальную рэч: бясьпека ананімнага чату звычайна ўбачаная ў абароне вашай асобы ад іншых удзельнікаў. "
                  "Аднак з пункту гледжання канфідэнцыяльнасьці перапіска ня можа быць больш бясьпечнай, чым асабісты чат у Тэлеграме.\n\n"
                  "*Калі вам неабходна абмяркоўваць сакрэтныя тэмы, якія патрабуюць максымальнай абароны, нашая рэкамендацыя простая і надзейная:*\n"
                  "*Выкарыстоўвайце сэкрэтныя чаты ў Тэлеграме ці іншыя сродкі сувязі.*\n\n"
                  "Беражыце сябе і сваю прыватнасьць.\n\n"
                  "**Правілы ананімнага чату:**\n\n"
                  "1.  Мова чату — **беларуская**.\n"
                  "2.  Шануйце суразмоўцаў.\n"
                  "3.  Забаронены непрыстойны кантэнт.\n"
                  "4.  Без спаму.\n"
                  "5.  Не раскрывайце асабістую інфармацыю.")
    await update.message.reply_text(rules_text, parse_mode=ParseMode.MARKDOWN)

async def connect_users(user1_id_str: str, user2_id_str: str, context: ContextTypes.DEFAULT_TYPE):
    user1_data = get_user(int(user1_id_str))
    user2_data = get_user(int(user2_id_str))
    session_id = f"session_{uuid.uuid4().hex[:12]}"
    user1_data.update({'chat_status': CHAT_STATUS_CHATTING, 'current_chat_partner': int(user2_id_str), 'current_chat_session': session_id})
    user2_data.update({'chat_status': CHAT_STATUS_CHATTING, 'current_chat_partner': int(user1_id_str), 'current_chat_session': session_id})
    update_user(user1_data)
    update_user(user2_data)
    connect_message = "✅ Суразмоўца знойдзены! Можаце пачынаць зносіны."
    for uid in [user1_id_str, user2_id_str]:
        try:
            await context.bot.send_message(uid, connect_message)
        except Exception as e: logger.error(f"Не атрымалася апавясьціць {uid}: {e}")

@check_if_banned
async def start_chat_logic(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id_str = str(update.effective_user.id)
    user_data = get_user(int(user_id_str))
    if user_data.get('chat_status') in [CHAT_STATUS_CHATTING, CHAT_STATUS_WAITING]:
        await update.message.reply_text("Вы ўжо ў працэсе. Каб спыніць, выкарыстоўвайце /stop.")
        return
    lock = context.bot_data['chat_search_lock']
    async with lock:
        user_data = get_user(int(user_id_str))
        user_data['chat_status'] = CHAT_STATUS_WAITING
        update_user(user_data)
        waiting_queue = context.bot_data.setdefault('waiting_queue', [])
        if user_id_str in waiting_queue:
            waiting_queue.remove(user_id_str)
        if len(waiting_queue) > 0:
            partner_id = waiting_queue.pop(0)
            asyncio.create_task(connect_users(partner_id, user_id_str, context))
            return
        if user_id_str not in waiting_queue:
            waiting_queue.append(user_id_str)
    await update.message.reply_text("🔎 Шукаем суразмоўцу...")

@check_if_banned
async def search_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_data = get_user(update.effective_user.id)
    if user_data.get('chat_status') == CHAT_STATUS_CHATTING:
        await stop_command(update, context, is_part_of_search=True)
        await asyncio.sleep(0.5)
        await start_chat_logic(update, context)
    else:
        await start_chat_logic(update, context)

async def process_post_chat_warnings(user_id_str: str, context: ContextTypes.DEFAULT_TYPE):
    chat_flags = context.bot_data.setdefault('chat_flags', {})
    if chat_flags.get(user_id_str):
        user_data = get_user(int(user_id_str))
        if user_data:
            current_warnings = user_data.get('warnings', 0) + 1
            user_data['warnings'] = current_warnings
            if current_warnings >= WARNING_LIMIT:
                user_data['is_banned'] = True
                ban_message = (f"❗️Вы атрымалі {current_warnings}/{WARNING_LIMIT} папярэджаньняў за выкарыстаньне літараў небеларускага альфабэту. Ваш доступ да чата заблякаваны.")
                await context.bot.send_message(user_id_str, ban_message, parse_mode=ParseMode.MARKDOWN)
            else:
                await context.bot.send_message(user_id_str, f"⚠️ Папярэджаньне ({current_warnings}/{WARNING_LIMIT}): калі ласка, выкарыстоўвайце толькі літары беларускага альфабэту ('і' замест 'и', 'шч' замест 'щ' і г.д.).")
            update_user(user_data)
        if user_id_str in chat_flags: del chat_flags[user_id_str]

async def end_chat_session(user_id1_str: str, user_id2_str: str | None, context: ContextTypes.DEFAULT_TYPE, initiator_id_str: str, is_part_of_search: bool = False) -> None:
    await process_post_chat_warnings(user_id1_str, context)
    if user_id2_str:
        await process_post_chat_warnings(user_id2_str, context)
    
    for uid_str in [user_id1_str, user_id2_str]:
        if uid_str:
            user_data = get_user(int(uid_str))
            if user_data:
                user_data.update({'chat_status': CHAT_STATUS_IDLE, 'current_chat_partner': None, 'current_chat_session': None})
                update_user(user_data)
    
    if is_part_of_search:
        partner_id_str = user_id2_str if initiator_id_str == user_id1_str else user_id1_str
        if partner_id_str:
             try:
                await context.bot.send_message(partner_id_str, "Суразмоўца пакінуў чат, каб знайсьці новага.\n\nКаб пачаць новы пошук, выкарыстоўвайце /search.")
             except Forbidden: mark_user_as_bot_blocker(partner_id_str)
             except Exception as e: logger.error(f"Не атрымалася адправіць паведамленьне пра завяршэньне чата для {partner_id_str}: {e}")
        return

    for uid_str, partner_id_str in [(user_id1_str, user_id2_str), (user_id2_str, user_id1_str)]:
        if uid_str:
            try:
                message_text = "Чат завершаны." if initiator_id_str == uid_str else "Суразмоўца завяршыў чат."
                reply_markup = ADMIN_MAIN_MENU_KEYBOARD if is_admin(uid_str) else ReplyKeyboardRemove()
                final_message_text = f"{message_text}\n\nКаб пачаць новы пошук, выкарыстоўвайце /search."
                final_reply_markup = None
                if partner_id_str and not is_admin(partner_id_str):
                    final_reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("Паскардзіцца на суразмоўцу", callback_data=f"report_{partner_id_str}")]])
                await context.bot.send_message(uid_str, final_message_text, reply_markup=reply_markup)
                if final_reply_markup:
                    await context.bot.send_message(uid_str, "Калі суразмоўца парушаў правілы, вы можаце паскардзіцца.", reply_markup=final_reply_markup)
            except Forbidden: mark_user_as_bot_blocker(uid_str)
            except Exception as e: logger.error(f"Не атрымалася адправіць паведамленьне пра завяршэньне чата для {uid_str}: {e}")

@check_if_banned
async def stop_command(update: Update, context: ContextTypes.DEFAULT_TYPE, is_part_of_search: bool = False) -> None:
    user_id_str = str(update.effective_user.id)
    user_data = get_user(int(user_id_str))
    status = user_data.get('chat_status')
    reply_markup = ADMIN_MAIN_MENU_KEYBOARD if is_admin(user_id_str) else ReplyKeyboardRemove()

    if status == CHAT_STATUS_WAITING:
        if user_id_str in context.bot_data.get('waiting_queue', []):
            context.bot_data['waiting_queue'].remove(user_id_str)
        user_data['chat_status'] = CHAT_STATUS_IDLE
        update_user(user_data)
        if not is_part_of_search: await update.message.reply_text("Пошук скасаваны.", reply_markup=reply_markup)
    elif status == CHAT_STATUS_CHATTING:
        partner_id = user_data.get('current_chat_partner')
        partner_id_str = str(partner_id) if partner_id else None
        await end_chat_session(user_id_str, partner_id_str, context, initiator_id_str=user_id_str, is_part_of_search=is_part_of_search)
        if not partner_id and not is_part_of_search:
             await update.message.reply_text("Вы выйшлі з чата.", reply_markup=reply_markup)
    else:
        if not is_part_of_search: await update.message.reply_text("Вы не ў чаце і не ў пошуку.", reply_markup=reply_markup)

@check_if_banned
async def chat_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message: return
    user_id_str = str(update.effective_user.id)
    user_data = get_user(int(user_id_str))
    partner_id = user_data.get('current_chat_partner')
    session_id = user_data.get('current_chat_session')
    
    if not (partner_id and session_id): return
    partner_id_str = str(partner_id)

    log_chat_message(user_id_str, partner_id_str, update.message, session_id)
    if update.message.text and any(char in FORBIDDEN_CHARS for char in update.message.text):
        context.bot_data.setdefault('chat_flags', {})[user_id_str] = True
    
    try:
        sent_message = await forward_message_with_reply(context, user_id_str, partner_id_str, update.message)
        if sent_message:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("INSERT INTO message_links (source_chat_id, source_message_id, dest_chat_id, dest_message_id) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
                                (int(user_id_str), update.message.message_id, int(partner_id_str), sent_message.message_id))
                    cur.execute("INSERT INTO message_links (source_chat_id, source_message_id, dest_chat_id, dest_message_id) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
                                (int(partner_id_str), sent_message.message_id, int(user_id_str), update.message.message_id))
    except Forbidden:
        mark_user_as_bot_blocker(partner_id_str)
        reply_markup_after_error = ADMIN_MAIN_MENU_KEYBOARD if is_admin(user_id_str) else ReplyKeyboardRemove()
        await end_chat_session(user_id_str, partner_id_str, context, initiator_id_str=user_id_str)
        await update.message.reply_text("❌ Не атрымалася даставіць паведамленьне. Суразмоўца, магчыма, заблякаваў бота. Чат завершаны.", reply_markup=reply_markup_after_error)
    except Exception as e: logger.error(f"Памылка перасылкі: {e}")

async def forward_message_with_reply(context: ContextTypes.DEFAULT_TYPE, from_id_str: str, to_id_str: str, message: Update.message):
    reply_to_dest_id = None
    if message.reply_to_message:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT dest_message_id FROM message_links WHERE source_chat_id = %s AND source_message_id = %s",
                                      (int(from_id_str), message.reply_to_message.message_id))
                result = cur.fetchone()
                if result: reply_to_dest_id = result[0]
    kwargs = {'chat_id': to_id_str, 'reply_to_message_id': reply_to_dest_id}
    if message.text: return await context.bot.send_message(text=message.text, entities=message.entities, **kwargs)
    elif message.photo: return await context.bot.send_photo(photo=message.photo[-1].file_id, caption=message.caption, caption_entities=message.caption_entities, **kwargs)
    elif message.sticker: return await context.bot.send_sticker(sticker=message.sticker.file_id, **kwargs)
    elif message.voice: return await context.bot.send_voice(voice=message.voice.file_id, caption=message.caption, caption_entities=message.caption_entities, **kwargs)
    elif message.video: return await context.bot.send_video(video=message.video.file_id, caption=message.caption, caption_entities=message.caption_entities, **kwargs)
    elif message.video_note: return await context.bot.send_video_note(video_note=message.video_note.file_id, **kwargs)
    elif message.document: return await context.bot.send_document(document=message.document.file_id, caption=message.caption, caption_entities=message.caption_entities, **kwargs)
    elif message.audio: return await context.bot.send_audio(audio=message.audio.file_id, caption=message.caption, caption_entities=message.caption_entities, **kwargs)
    else: return await message.copy(chat_id=to_id_str, reply_to_message_id=reply_to_dest_id)

@check_if_banned
async def edited_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    edited_message = update.edited_message
    if not edited_message: return

    user_id = edited_message.chat_id
    user_data = get_user(user_id)
    if not user_data or not (partner_id := user_data.get('current_chat_partner')): return

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT dest_message_id FROM message_links WHERE source_chat_id = %s AND source_message_id = %s",
                          (user_id, edited_message.message_id))
            result = cur.fetchone()
            if result:
                dest_message_id = result[0]
                try:
                    if edited_message.text:
                        await context.bot.edit_message_text(chat_id=partner_id, message_id=dest_message_id, text=edited_message.text, entities=edited_message.entities)
                        cur.execute("UPDATE chat_logs SET message_text = %s WHERE sender_id = %s AND message_id = %s",
                                     (edited_message.text, user_id, edited_message.message_id))
                    elif edited_message.caption is not None:
                        await context.bot.edit_message_caption(chat_id=partner_id, message_id=dest_message_id, caption=edited_message.caption, caption_entities=edited_message.caption_entities)
                        cur.execute("UPDATE chat_logs SET message_text = %s WHERE sender_id = %s AND message_id = %s",
                                     (edited_message.caption, user_id, edited_message.message_id))
                except BadRequest as e:
                    if "message is not modified" not in str(e).lower(): logger.error(f"Памылка рэдагаваньня (BadRequest): {e}")
                except Exception as e: logger.error(f"Невядомая памылка рэдагаваньня: {e}")

# --- АДМІНІСТРАВАНЬНЕ ---

@check_if_banned
async def admin_enter_random_chat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Вы перайшлі ў рэжым выпадковага чата.\n\n"
                                    "Карыстайцеся камандамі /search і /stop для кіраваньня.\n\n"
                                    "Каб вярнуцца ў мэню адміністратара, выкарыстоўвайце /stop, а затым /start.",
                                    reply_markup=ReplyKeyboardRemove())

async def admin_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Галоўнае мэню:", reply_markup=ADMIN_MAIN_MENU_KEYBOARD)
async def admin_users_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Кіраваньне карыстальнікамі:", reply_markup=ADMIN_USERS_MENU_KEYBOARD)
async def admin_broadcast_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Рассылкі:", reply_markup=ADMIN_BROADCAST_MENU_KEYBOARD)
async def admin_system_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Сыстэмныя наладкі:", reply_markup=ADMIN_SYSTEM_MENU_KEYBOARD)

@check_if_banned
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    help_text = ("<b>Дапамога па мэню адміністратара:</b>\n\n"
                 "<b>📊 Статыстыка</b> - паказвае пашыраную статыстыку па боту.\n"
                 "<b>👥 Карыстальнікі</b> - мэню для працы з карыстальнікамі (прагляд інфо, сьпісы, бан/разбан).\n"
                 "<b>📣 Рассылкі</b> - адпраўка паведамленьняў усім ці выбраным карыстальнікам.\n"
                 "<b>🆘 SOS-чаты</b> - пачаць чат з карыстальнікам з чаргі SOS.\n"
                 "<b>💬 Выпадковы чат</b> - увайсьці ў ананімны чат як звычайны карыстальнік.\n"
                 "<b>⚙️ Сыстэма</b> - дадатковыя наладкі, напрыклад, ачыстка базы зьвестак.")
    await update.message.reply_text(help_text, parse_mode=ParseMode.HTML)

@check_if_banned
async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=DictCursor) as cur:
                cur.execute("SELECT COUNT(DISTINCT session_id) as total_sessions, COUNT(log_id) as total_messages FROM chat_logs")
                chat_stats = cur.fetchone()
                cur.execute("""
                    SELECT 
                        COUNT(user_id) as total_users,
                        COUNT(CASE WHEN last_active_time > NOW() - INTERVAL '24 hours' THEN 1 END) as active_today,
                        COUNT(CASE WHEN is_banned = TRUE THEN 1 END) as banned,
                        COUNT(CASE WHEN has_blocked_bot = TRUE THEN 1 END) as blocked_bot,
                        COUNT(CASE WHEN chat_status = %s THEN 1 END) as chatting
                    FROM users
                """, (CHAT_STATUS_CHATTING,))
                user_stats = cur.fetchone()
        
        waiting_now = len(context.bot_data.get('waiting_queue', []))
        sos_queue_len = len(context.bot_data.get('sos_queue', []))
        
        uptime = datetime.datetime.utcnow() - context.application.bot_data['start_time']
        d, r = divmod(int(uptime.total_seconds()), 86400)
        h, r = divmod(r, 3600)
        m, _ = divmod(r, 60)
        
        stats_text = (f"📊 **Падрабязная статыстыка**\n\n"
                      f"👥 **Карыстальнікі:**\n"
                      f"  - Усяго зарэгістравана: `{user_stats['total_users']}`\n"
                      f"  - Актыўных за 24г: `{user_stats['active_today']}`\n"
                      f"  - Заблякавана адміністратарам: `{user_stats['banned']}`\n"
                      f"  - Імаверна, заблакавалі бота: `{user_stats['blocked_bot']}`\n\n"
                      f"🗣️ **Актыўнасьць:**\n"
                      f"  - Зараз у чаце (пары): `{user_stats['chatting'] // 2}`\n"
                      f"  - Чакаюць суразмоўцу: `{waiting_now}`\n"
                      f"  - У чарзе SOS: `{sos_queue_len}`\n\n"
                      f"🗂 **Гісторыя:**\n"
                      f"  - Усяго праведзена дыялёгаў: `{chat_stats['total_sessions']}`\n"
                      f"  - Усяго адпраўлена паведамленьняў: `{chat_stats['total_messages']}`\n\n"
                      f"⚙️ **Сыстэма:**\n"
                      f"  - Uptime: `{d}д {h}г {m}хв`")
        await update.message.reply_text(stats_text, parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        logger.error(f"Памылка пры атрыманьні статыстыкі: {e}")
        await update.message.reply_text(f"❌ Адбылася памылка пры атрыманьні статыстыкі: {e}")

@check_if_banned
async def users_list(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    users_data = get_all_users()
    await update.message.reply_text(f"👥 Усяго карыстальнікаў: {len(users_data)}", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Паказаць поўны сьпіс", callback_data='show_full_user_list')]]))

async def show_full_user_list_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    users_data = get_all_users()
    user_list = [f"• `{get_user_display_name(uid, users_data)}` (@`{info.get('username', 'N/A')}`){' (ЗАБАНЕНЫ)' if info.get('is_banned') else ''}{' (ЗАБЛАКАВАЎ БОТА)' if info.get('has_blocked_bot') else ''} [Папярэджаньні: {info.get('warnings', 0)}]\n  ID: `{uid}`" for uid, info in users_data.items()]
    full_text = f"👥 **Усяго карыстальнікаў: {len(users_data)}**\n\n" + "\n\n".join(user_list)
    try: await query.edit_message_text(full_text, parse_mode=ParseMode.MARKDOWN)
    except BadRequest as e:
        if "Message is not modified" in str(e): return
        await query.edit_message_text(f"👥 **Усяго:** {len(users_data)}\n\nСьпіс занадта доўгі, адпраўляю па частках:")
        for i in range(0, len(user_list), 50):
            await context.bot.send_message(query.message.chat_id, "\n\n".join(user_list[i:i+50]), parse_mode=ParseMode.MARKDOWN)
            await asyncio.sleep(0.5)

async def get_user_info_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text("Увядзіце ID або @username карыстальніка. /cancel для скасаваньня.")
    return AWAITING_INFO_ID

async def get_user_info_receive(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id_to_get: str = None) -> int:
    is_callback = update.callback_query is not None
    if is_callback:
        await update.callback_query.answer()
        message = update.callback_query.message
        user_id_to_get = query.data.split('_')[-1]
    else:
        message = update.message
        user_id_found = find_user_id_by_identifier(message.text.strip())
        user_id_to_get = str(user_id_found) if user_id_found else None

    if user_id_to_get and (user_data := get_user(int(user_id_to_get))):
        reg_date = user_data.get('start_time').strftime('%Y-%m-%d %H:%M') if user_data.get('start_time') else 'N/A'
        last_active = user_data.get('last_active_time').strftime('%Y-%m-%d %H:%M') if user_data.get('last_active_time') else 'N/A'
        status_list = []
        if user_data.get('is_banned'): status_list.append('ЗАБАНЕНЫ АДМІНАМ')
        else: status_list.append('Актыўны')
        if user_data.get('has_blocked_bot'): status_list.append('ЗАБЛАКАВАЎ БОТА')
        info_text = (f"📄 **Інфармацыя пра карыстальніка**\n**ID**: `{user_id_to_get}`\n**Імя**: `{user_data.get('first_name', 'N/A')}`\n**Username**: @`{user_data.get('username', 'N/A')}`\n\n"
                     f"**Статус**: `{', '.join(status_list)}`\n**Папярэджаньні**: `{user_data.get('warnings', 0)}/{WARNING_LIMIT}`\n"
                     f"**Статус чата**: `{user_data.get('chat_status', 'N/A')}`\n\n"
                     f"**Зарэгістраваны**: `{reg_date} UTC`\n**Апошняя актыўнасьць**: `{last_active} UTC`")
        buttons = [[InlineKeyboardButton("📜 Гісторыя чатаў", callback_data=f"history_list_{user_id_to_get}")]]
        if user_data.get('is_banned'): buttons.append([InlineKeyboardButton("✅ Разбаніць", callback_data=f"unban_{user_id_to_get}")])
        else: buttons.append([InlineKeyboardButton("🚫 Забаніць", callback_data=f"ban_{user_id_to_get}")])
        reply_markup = InlineKeyboardMarkup(buttons)
        target_message = message.edit_text if is_callback else message.reply_text
        await target_message(info_text, reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN)
    else:
        await (message.edit_text if is_callback else message.reply_text)("❌ Карыстальнік ня знойдзены.")

    if not is_callback:
        await admin_users_menu(update, context)
        return ConversationHandler.END
    return ConversationHandler.END

async def admin_ban_unban_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    action, user_id_str = query.data.split('_')
    user_data = get_user(int(user_id_str))
    if user_data:
        user_data['is_banned'] = (action == 'ban')
        if action == 'unban': user_data['warnings'] = 0
        update_user(user_data)
        await get_user_info_receive(update, context, user_id_to_get=user_id_str)

async def admin_show_chat_partners(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    user_id_str = query.data.split('_')[-1]
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT partner_id FROM chat_logs WHERE sender_id = %s
                UNION
                SELECT DISTINCT sender_id FROM chat_logs WHERE partner_id = %s
            """, (int(user_id_str), int(user_id_str)))
            partners = [p[0] for p in cur.fetchall()]
    
    if not partners:
        await query.edit_message_text(f"У карыстальніка ID `{user_id_str}` няма гісторыі чатаў.", parse_mode=ParseMode.MARKDOWN,
                                      reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("<< Назад да інфо", callback_data=f"back_to_user_info_{user_id_str}")]]))
        return
    
    users_db = get_all_users()
    buttons = [[InlineKeyboardButton(f"💬 {get_user_display_name(str(p), users_db)} (`{p}`)", callback_data=f"list_sessions_{user_id_str}_{p}")] for p in partners]
    buttons.append([InlineKeyboardButton("<< Назад да інфо", callback_data=f"back_to_user_info_{user_id_str}")])
    await query.edit_message_text(f"Выберыце суразмоўцу для прагляду гісторыі (карыстальнік ID `{user_id_str}`):", reply_markup=InlineKeyboardMarkup(buttons), parse_mode=ParseMode.MARKDOWN)

async def admin_list_sessions(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    _, _, user1_id, user2_id = query.data.split('_')
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT session_id, MIN(timestamp) as start_time FROM chat_logs
                WHERE (sender_id = %s AND partner_id = %s) OR (sender_id = %s AND partner_id = %s)
                GROUP BY session_id ORDER BY start_time DESC
            """, (int(user1_id), int(user2_id), int(user2_id), int(user1_id)))
            sessions = cur.fetchall()

    if not sessions:
        await query.edit_message_text("Ня знойдзена сэсіяў паміж гэтымі карыстальнікамі.",
                                      reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("<< Назад да партнэраў", callback_data=f"history_list_{user1_id}")]]))
        return

    users_db = get_all_users()
    user2_name = get_user_display_name(user2_id, users_db)
    buttons = []
    for session_id, start_time_dt in sessions:
        start_time_formatted = start_time_dt.strftime('%Y-%m-%d %H:%M')
        buttons.append([InlineKeyboardButton(f"Сэсія ад {start_time_formatted}", callback_data=f"view_session_{session_id}")])

    buttons.append([InlineKeyboardButton("<< Назад да партнэраў", callback_data=f"history_list_{user1_id}")])
    await query.edit_message_text(f"Выберыце сэсію з **{user2_name}** (`{user2_id}`):",
                                  reply_markup=InlineKeyboardMarkup(buttons), parse_mode=ParseMode.MARKDOWN)

async def admin_view_specific_chat(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer("Загружаю гісторыю...")
    session_id = query.data.removeprefix('view_session_')
    
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("SELECT * FROM chat_logs WHERE session_id = %s ORDER BY log_id ASC", (session_id,))
            chat_history = cur.fetchall()

    if not chat_history:
        await context.bot.send_message(ADMIN_CHAT_ID, f"Паведамленьняў у гэтай сэсіі няма. (ID сэсіі: `{session_id}`)", parse_mode=ParseMode.MARKDOWN)
        return

    users_db = get_all_users()
    participant_ids = list(set([str(row['sender_id']) for row in chat_history] + [str(row['partner_id']) for row in chat_history]))
    participant_names = [f"`{get_user_display_name(uid, users_db)}` (`{uid}`)" for uid in participant_ids]
    start_time_formatted = chat_history[0]['timestamp'].strftime('%Y-%m-%d %H:%M')
    
    header_text = f"--- Пачатак перапіскі ({start_time_formatted}): {' і '.join(participant_names)} ---"
    await context.bot.send_message(ADMIN_CHAT_ID, header_text, parse_mode=ParseMode.MARKDOWN)

    for row in chat_history:
        sender_name = get_user_display_name(str(row['sender_id']), users_db)
        sender_header = f"**`{sender_name}`**:"
        try:
            if row['message_type'] == 'text':
                await context.bot.send_message(ADMIN_CHAT_ID, f"{sender_header} {row['message_text']}", parse_mode=ParseMode.MARKDOWN)
            elif row['file_id']:
                caption = f"{sender_header}\n\n{row['message_text']}" if row['message_text'] else sender_header
                if len(caption) > 1024: caption = caption[:1021] + "..."
                kwargs = {'chat_id': ADMIN_CHAT_ID, 'caption': caption, 'parse_mode': ParseMode.MARKDOWN}
                
                msg_type = row['message_type']
                file_id = row['file_id']
                if msg_type == 'photo': await context.bot.send_photo(photo=file_id, **kwargs)
                elif msg_type == 'video': await context.bot.send_video(video=file_id, **kwargs)
                elif msg_type == 'voice': await context.bot.send_voice(voice=file_id, **kwargs)
                elif msg_type == 'document': await context.bot.send_document(document=file_id, **kwargs)
                elif msg_type == 'audio': await context.bot.send_audio(audio=file_id, **kwargs)
                elif msg_type == 'sticker':
                    await context.bot.send_message(ADMIN_CHAT_ID, sender_header, parse_mode=ParseMode.MARKDOWN)
                    await context.bot.send_sticker(ADMIN_CHAT_ID, file_id)
                elif msg_type == 'video_note':
                    await context.bot.send_message(ADMIN_CHAT_ID, sender_header, parse_mode=ParseMode.MARKDOWN)
                    await context.bot.send_video_note(ADMIN_CHAT_ID, file_id)
            await asyncio.sleep(0.3)
        except Exception as e:
            logger.error(f"Немагчыма адправіць паведамленьне з гісторыі: {e}")
            await context.bot.send_message(ADMIN_CHAT_ID, f"{sender_header} [Паведамленьне ня можа быць паказана. Памылка: {e}]")

    await context.bot.send_message(ADMIN_CHAT_ID, "--- Канец перапіскі ---")

# --- ЛОГИКА СКАРГАЎ ---

async def report_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    reporter_id = str(query.from_user.id)
    reported_id = query.data.split('_')[1]
    buttons = [[InlineKeyboardButton("Так, паскардзіцца", callback_data=f"confirm_report_{reporter_id}_{reported_id}")],
               [InlineKeyboardButton("Не, скасаваць", callback_data="cancel_report")]]
    await query.answer()
    await query.edit_message_text("Вы ўпэўненыя, што хочаце паскардзіцца на гэтага суразмоўцу?", reply_markup=InlineKeyboardMarkup(buttons))

async def confirm_report_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    _, _, reporter_id, reported_id = query.data.split('_')
    context.user_data['report_data'] = {'reporter_id': reporter_id, 'reported_id': reported_id, 'screenshots': []}
    await query.edit_message_text("Калі ласка, дашліце адно або некалькі фота (скрыншотаў) зь перапіскі, якія пацьвярджаюць парушэньне.\n\n"
                                  "Пасьля таго, як дашлеце ўсе файлы, націсьніце кнопку **'Гатова'**.",
                                  reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✅ Гатова", callback_data="finish_report")]]))
    return AWAITING_REPORT_SCREENSHOTS

async def receive_report_screenshot(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.message.photo:
        await update.message.reply_text("Калі ласка, дасылайце толькі фота (скрыншоты).")
        return
    photo_id = update.message.photo[-1].file_id
    context.user_data.get('report_data', {}).get('screenshots', []).append(photo_id)
    await update.message.reply_text("Скрыншот прыняты. Дашліце яшчэ або націсьніце 'Гатова'.")

async def finish_report_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    report_data = context.user_data.get('report_data', {})
    reporter_id = report_data.get('reporter_id')
    reported_id = report_data.get('reported_id')
    screenshots = report_data.get('screenshots', [])
    if not (reporter_id and reported_id and screenshots):
        await query.edit_message_text("❌ Памылка. Скарга ня будзе адпраўленая. Паспрабуйце зноў.")
        context.user_data.pop('report_data', None)
        return ConversationHandler.END
    
    users_db = get_all_users()
    reporter_name = get_user_display_name(reporter_id, users_db)
    reported_name = get_user_display_name(reported_id, users_db)
    report_text = (f"❗️ **Новая скарга!**\n\n"
                   f"**Ад:** `{reporter_name}` (ID: `{reporter_id}`)\n"
                   f"**На:** `{reported_name}` (ID: `{reported_id}`)\n\n"
                   f"Адміністратар, праверце прыкладзеныя доказы.")
    await context.bot.send_message(ADMIN_CHAT_ID, report_text, parse_mode=ParseMode.MARKDOWN)
    media_group = [InputMediaPhoto(media=ss) for ss in screenshots]
    if media_group:
        await context.bot.send_media_group(ADMIN_CHAT_ID, media=media_group)
    await query.edit_message_text("✅ Дзякуй! Вашая скарга адпраўлена адміністратару.")
    context.user_data.pop('report_data', None)
    return ConversationHandler.END

async def cancel_report_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    await query.edit_message_text("Адпраўка скаргі скасаваная.")
    if 'report_data' in context.user_data: context.user_data.pop('report_data')
    return ConversationHandler.END

# --- ОСТАЛЬНЫЕ ФУНКЦИИ ---

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text("Дзеяньне скасаванае.", reply_markup=ADMIN_MAIN_MENU_KEYBOARD)
    return ConversationHandler.END

async def sendall_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text("Увядзіце паведамленьне для рассылкі ўсім. /cancel для скасаваньня.")
    return AWAITING_BROADCAST_MESSAGE

async def broadcast_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    sent, failed = 0, 0
    users_to_send = get_all_users()
    await update.message.reply_text(f"Пачынаю рассылку для {len(users_to_send)} карыстальнікаў...")
    for user_id_str in users_to_send.keys():
        try:
            await update.message.copy(chat_id=user_id_str)
            sent += 1
            await asyncio.sleep(0.05)
        except Forbidden:
            mark_user_as_bot_blocker(user_id_str)
            failed += 1
        except Exception as e:
            logger.error(f"Не атрымалася адправіць паведамленьне {user_id_str} падчас рассылкі: {e}")
            failed += 1
    await update.message.reply_text(f"✅ Рассылка завершаная!\n👍 Адпраўлена: {sent}\n👎 Не атрымалася: {failed}", reply_markup=ADMIN_BROADCAST_MENU_KEYBOARD)
    return ConversationHandler.END

async def sendto_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text("Увядзіце ID або @username атрымальнікаў праз коску. /cancel для скасаваньня")
    return AWAITING_SENDTO_IDS

async def sendto_receive_ids(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    found_ids = {find_user_id_by_identifier(r.strip()) for r in update.message.text.split(',') if find_user_id_by_identifier(r.strip())}
    if not found_ids:
        await update.message.reply_text("⚠️ Ня знойдзены ніводзін карыстальнік. Паспрабуйце яшчэ раз. /cancel для скасаваньня")
        return AWAITING_SENDTO_IDS
    context.user_data['sendto_ids'] = list(found_ids)
    await update.message.reply_text(f"✅ Знойдзена: {len(found_ids)}. Увядзіце паведамленьне:")
    return AWAITING_SENDTO_MESSAGE

async def sendto_receive_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    sent, failed = 0, 0
    for user_id in context.user_data.get('sendto_ids', []):
        try:
            await update.message.copy(chat_id=user_id)
            sent += 1
            await asyncio.sleep(0.05)
        except Forbidden:
            mark_user_as_bot_blocker(str(user_id))
            failed += 1
        except Exception: failed += 1
    await update.message.reply_text(f"🎯 Рассылка завершаная:\n👍 Адпраўлена: {sent}\n👎 Не атрымалася: {failed}", reply_markup=ADMIN_BROADCAST_MENU_KEYBOARD)
    context.user_data.pop('sendto_ids', None)
    return ConversationHandler.END

@check_if_banned
async def contact_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_data = get_user(user_id)
    if user_data.get('chat_status') != CHAT_STATUS_IDLE:
        await update.message.reply_text("Вы не можаце зьвязацца з адміністратарам, пакуль знаходзіцеся ў чаце. Спачатку выкарыстоўвайце /stop.")
        return
    sos_queue = context.bot_data.setdefault('sos_queue', [])
    if user_id in sos_queue:
        await update.message.reply_text("Ваш запыт ужо ў чарзе. Калі ласка, чакайце.")
        return
    sos_queue.append(user_id)
    users_db = get_all_users()
    user_display_name = get_user_display_name(str(user_id), users_db)
    await update.message.reply_text("Ваш запыт дададзены ў чаргу. Адміністратар хутка з вамі зьвяжацца.")
    try:
        await context.bot.send_message(ADMIN_CHAT_ID,
            f"❗️ Новы запыт у чарзе SOS ад **{user_display_name}** (`{user_id}`).\n"
            f"Усяго ў чарзе: **{len(sos_queue)}**.\n\n"
            f"Націсьніце '🆘 SOS-чаты', каб пачаць.", parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        logger.error(f"Немагчыма адправіць SOS апавяшчэньне адміну: {e}")

@check_if_banned
async def admin_sos_chat_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    admin_id = update.effective_user.id
    sos_queue = context.bot_data.setdefault('sos_queue', [])
    if not sos_queue:
        await update.message.reply_text("Чарга SOS-запытаў пустая.", reply_markup=ADMIN_MAIN_MENU_KEYBOARD)
        return
    user_id_to_connect = sos_queue.pop(0)
    user_to_connect_data = get_user(user_id_to_connect)
    if not user_to_connect_data or user_to_connect_data.get('chat_status') != CHAT_STATUS_IDLE:
        await update.message.reply_text(f"❌ Карыстальнік {user_id_to_connect} ужо заняты. Шукаю наступнага...")
        await admin_sos_chat_start(update, context)
        return
    
    admin_data = get_user(admin_id)
    if admin_data.get('chat_status') == CHAT_STATUS_CHATTING:
        admin_partner_id = admin_data.get('current_chat_partner')
        await end_chat_session(str(admin_id), str(admin_partner_id) if admin_partner_id else None, context, initiator_id_str=str(admin_id))
        await asyncio.sleep(0.5)
    
    users_db = get_all_users()
    user_display_name = get_user_display_name(str(user_id_to_connect), users_db)
    await update.message.reply_text(f"⏳ Падключаю вас да {user_display_name} (`{user_id_to_connect}`)...")
    try:
        await context.bot.send_message(user_id_to_connect, "Адміністратар падключаецца да вас...")
    except Exception as e:
        logger.error(f"Немагчыма апавясьціць {user_id_to_connect} пра падключэньне адміна: {e}")
    await connect_users(str(admin_id), str(user_id_to_connect), context)

async def handle_amnesty_code(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    user_data = get_user(user_id)
    if user_data and user_data.get('is_banned'):
        user_data['is_banned'] = False
        user_data['warnings'] = 0
        update_user(user_data)
        await update.message.reply_text("✅ Ваш доступ адноўлены. Калі ласка, надалей карыстайцеся выключна літарамі беларускага альфабэту.", reply_markup=ReplyKeyboardRemove())
        logger.info(f"Карыстальнік {user_id} выкарыстаў код амністыі і быў разбанены.")

async def clear_chat_history(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    buttons = [[InlineKeyboardButton("Так, я ўпэўнены, выдаліць усё", callback_data="confirm_clear_history")],
               [InlineKeyboardButton("Не, скасаваць", callback_data="cancel_clear_history")]]
    await update.message.reply_text("🗑️ **Папярэджаньне!**\n\nВы зьбіраецеся **БЕСПАВАРОТНА ВЫДАЛІЦЬ** усю гісторыю перапісак усіх карыстальнікаў.\n\n"
                                    "Гэтае дзеяньне нельга скасаваць. Вы ўпэўненыя?",
                                    reply_markup=InlineKeyboardMarkup(buttons), parse_mode=ParseMode.MARKDOWN)

async def confirm_clear_history_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer("Ачышчаю гісторыю...")
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE chat_logs, message_links;")
        logger.warning("Адміністратар ачысьціў усю гісторыю чатаў.")
        await query.edit_message_text("✅ Уся гісторыя перапісак пасьпяхова выдаленая.")
    except Exception as e:
        logger.error(f"Памылка пры ачыстцы гісторыі: {e}")
        await query.edit_message_text(f"❌ Адбылася памылка пры ачыстцы гісторыі: {e}")

async def cancel_clear_history_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    await query.edit_message_text("Ачыстка гісторыі скасаваная.")

async def post_init(application: Application):
    await application.bot.set_my_commands([
        BotCommand("search", "🔎 Пачаць/наступны ананімны чат"),
        BotCommand("stop", "⏹️ Спыніць бягучы дыялёг"),
        BotCommand("sos", "🆘 Зьвязацца з адміністратарам"),
        BotCommand("rules", "📜 Правілы чату"),
    ])

def main() -> None:
    initialize_databases()
    reset_all_user_statuses_on_startup()

    application = Application.builder().token(BOT_TOKEN).post_init(post_init).build()

    application.bot_data['start_time'] = datetime.datetime.utcnow()
    application.bot_data['sos_queue'] = []
    application.bot_data['waiting_queue'] = []
    application.bot_data['chat_search_lock'] = asyncio.Lock()

    admin_filter = filters.User(user_id=int(ADMIN_CHAT_ID))
    conv_fallbacks = [CommandHandler("cancel", cancel, filters=admin_filter)]

    report_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(confirm_report_callback, pattern=r'^confirm_report_')],
        states={
            AWAITING_REPORT_SCREENSHOTS: [
                MessageHandler(filters.PHOTO, receive_report_screenshot),
                CallbackQueryHandler(finish_report_callback, pattern=r'^finish_report$')
            ]
        },
        fallbacks=[CallbackQueryHandler(cancel_report_callback, pattern=r'^cancel_report$')],
        per_message=False
    )
    application.add_handler(report_handler)

    application.add_handler(ConversationHandler(
        entry_points=[MessageHandler(filters.Regex('^📣 Усім$') & admin_filter, sendall_start)],
        states={AWAITING_BROADCAST_MESSAGE: [MessageHandler(filters.ALL & ~filters.COMMAND, broadcast_message)]},
        fallbacks=conv_fallbacks))
    application.add_handler(ConversationHandler(
        entry_points=[MessageHandler(filters.Regex('^🎯 Выбраным$') & admin_filter, sendto_start)],
        states={
            AWAITING_SENDTO_IDS: [MessageHandler(filters.TEXT & ~filters.COMMAND, sendto_receive_ids)],
            AWAITING_SENDTO_MESSAGE: [MessageHandler(filters.ALL & ~filters.COMMAND, sendto_receive_message)]
        },
        fallbacks=conv_fallbacks))
    application.add_handler(ConversationHandler(
        entry_points=[MessageHandler(filters.Regex('^ℹ️ Інфо пра юзэра$') & admin_filter, get_user_info_start)],
        states={AWAITING_INFO_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_user_info_receive)]},
        fallbacks=conv_fallbacks))

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("rules", rules_command))
    application.add_handler(CommandHandler("sos", contact_admin))
    application.add_handler(CommandHandler("search", search_command))
    application.add_handler(CommandHandler("stop", stop_command))
    application.add_handler(CommandHandler("help", help_command, filters=admin_filter))

    application.add_handler(MessageHandler(filters.Regex('^📊 Статыстыка$') & admin_filter, stats))
    application.add_handler(MessageHandler(filters.Regex('^👥 Карыстальнікі$') & admin_filter, admin_users_menu))
    application.add_handler(MessageHandler(filters.Regex('^📣 Рассылкі$') & admin_filter, admin_broadcast_menu))
    application.add_handler(MessageHandler(filters.Regex('^⚙️ Сыстэма$') & admin_filter, admin_system_menu))
    application.add_handler(MessageHandler(filters.Regex('^🔙 Галоўнае мэню$') & admin_filter, admin_main_menu))
    application.add_handler(MessageHandler(filters.Regex('^📋 Сьпіс усіх$') & admin_filter, users_list))
    application.add_handler(MessageHandler(filters.Regex('^🆘 SOS-чаты$') & admin_filter, admin_sos_chat_start))
    application.add_handler(MessageHandler(filters.Regex('^🗑️ Ачысьціць гісторыю чатаў$') & admin_filter, clear_chat_history))
    application.add_handler(MessageHandler(filters.Regex('^💬 Выпадковы чат$') & admin_filter, admin_enter_random_chat))

    application.add_handler(CallbackQueryHandler(show_full_user_list_callback, pattern='^show_full_user_list$'))
    application.add_handler(CallbackQueryHandler(admin_show_chat_partners, pattern=r'^history_list_'))
    application.add_handler(CallbackQueryHandler(get_user_info_receive, pattern=r'^back_to_user_info_'))
    application.add_handler(CallbackQueryHandler(admin_list_sessions, pattern=r'^list_sessions_'))
    application.add_handler(CallbackQueryHandler(admin_view_specific_chat, pattern=r'^view_session_'))
    application.add_handler(CallbackQueryHandler(admin_ban_unban_user, pattern=r'^(un)?ban_'))
    application.add_handler(CallbackQueryHandler(report_callback, pattern=r'^report_'))
    application.add_handler(CallbackQueryHandler(cancel_report_callback, pattern=r'^cancel_report$'))
    application.add_handler(CallbackQueryHandler(confirm_clear_history_callback, pattern=r'^confirm_clear_history$'))
    application.add_handler(CallbackQueryHandler(cancel_clear_history_callback, pattern=r'^cancel_clear_history$'))

    application.add_handler(MessageHandler(filters.UpdateType.EDITED_MESSAGE & filters.ChatType.PRIVATE & ~filters.COMMAND, edited_message_handler), group=1)
    application.add_handler(MessageHandler(filters.ChatType.PRIVATE & ~filters.COMMAND, chat_message_handler), group=1)

    print("Бот пасьпяхова запушчаны...")
    application.run_polling()

if __name__ == "__main__":
    main()