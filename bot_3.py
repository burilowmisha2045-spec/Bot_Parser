"""
Telegram Business Monitor — bot_3.py
Полная версия: aiogram 3.x + FastAPI веб-сервер + SQLite с историей правок
"""

import asyncio
import logging
import os
import sqlite3
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path

import aiofiles
import uvicorn
from aiogram import Bot, Dispatcher, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import (
    BufferedInputFile,
    BusinessMessagesDeleted,
    Message,
)
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

# ─── Конфигурация ─────────────────────────────────────────────────────────────

BIZ_TOKEN = "8709635645:AAHV4GqkGO8sD5OWdcfXebM9c5aqGwRCqaw"
LOG_TOKEN  = "8443752656:AAEKeDM61WDxW0EJTVEstP4r6ZP23AK6AQc"
MY_ID      = 7557612980

if not BIZ_TOKEN or not LOG_TOKEN or not MY_ID:
    raise RuntimeError(
        "Необходимо задать переменные окружения: BIZ_TOKEN, LOG_TOKEN, ADMIN_ID"
    )

MEDIA_DIR  = Path("media_vault")
DB_PATH    = Path("business_archive.db")

API_HOST   = os.getenv("API_HOST", "0.0.0.0")
API_PORT   = int(os.getenv("API_PORT", "8000"))

# ─── Логирование ──────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("biz_monitor")

# ─── База данных ──────────────────────────────────────────────────────────────

def init_db() -> None:
    """
    Создаём таблицу msgs с расширенной схемой:
      - rowid      : автоинкремент SQLite — используется как cursor для инкрементальных обновлений
      - status     : 'normal' | 'edited' | 'deleted'
      - old_text   : текст до последнего редактирования (история)
      - is_edited  : флаг редактирования (0/1)
      - is_deleted : флаг удаления (0/1)
      - ts         : временная метка сохранения (ISO 8601)
    Если таблица уже существует — мигрируем: добавляем недостающие колонки.
    """
    MEDIA_DIR.mkdir(exist_ok=True)
    con = sqlite3.connect(DB_PATH)
    con.execute("""
        CREATE TABLE IF NOT EXISTS msgs (
            rowid      INTEGER PRIMARY KEY AUTOINCREMENT,
            m_id       INTEGER,
            biz_id     TEXT,
            user_info  TEXT,
            text       TEXT,
            old_text   TEXT    DEFAULT '',
            file_path  TEXT,
            m_type     TEXT,
            status     TEXT    DEFAULT 'normal',
            is_edited  INTEGER DEFAULT 0,
            is_deleted INTEGER DEFAULT 0,
            ts         TEXT    DEFAULT '',
            UNIQUE(m_id, biz_id)
        )
    """)
    # Миграция: добавляем колонки, если таблица уже существовала в старом формате
    existing_cols = {
        row[1]
        for row in con.execute("PRAGMA table_info(msgs)").fetchall()
    }
    migrations = [
        ("old_text",   "TEXT    DEFAULT ''"),
        ("status",     "TEXT    DEFAULT 'normal'"),
        ("is_edited",  "INTEGER DEFAULT 0"),
        ("is_deleted", "INTEGER DEFAULT 0"),
        ("ts",         "TEXT    DEFAULT ''"),
    ]
    for col, definition in migrations:
        if col not in existing_cols:
            con.execute(f"ALTER TABLE msgs ADD COLUMN {col} {definition}")
            log.info("Миграция БД: добавлена колонка '%s'", col)
    con.commit()
    con.close()
    log.info("БД инициализирована: %s", DB_PATH)


def _now_iso() -> str:
    return datetime.now().isoformat(sep=" ", timespec="seconds")


def _now_str() -> str:
    return datetime.now().strftime("%d.%m.%Y %H:%M:%S")


def db_save(
    m_id: int,
    biz_id: str,
    user_info: str,
    text: str,
    file_path: str,
    m_type: str,
    status: str = "normal",
    old_text: str = "",
    is_edited: int = 0,
    is_deleted: int = 0,
) -> None:
    try:
        con = sqlite3.connect(DB_PATH)
        con.execute(
            """
            INSERT INTO msgs
                (m_id, biz_id, user_info, text, old_text, file_path, m_type,
                 status, is_edited, is_deleted, ts)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(m_id, biz_id) DO UPDATE SET
                user_info  = excluded.user_info,
                text       = excluded.text,
                old_text   = excluded.old_text,
                file_path  = excluded.file_path,
                m_type     = excluded.m_type,
                status     = excluded.status,
                is_edited  = excluded.is_edited,
                is_deleted = excluded.is_deleted,
                ts         = excluded.ts
            """,
            (m_id, biz_id, user_info, text, old_text, file_path, m_type,
             status, is_edited, is_deleted, _now_iso()),
        )
        con.commit()
        con.close()
        log.debug("db_save: m_id=%s biz_id=%s status=%s", m_id, biz_id, status)
    except sqlite3.Error as e:
        log.error("db_save ОШИБКА: m_id=%s biz_id=%s — %s", m_id, biz_id, e)
        raise


def db_get(m_id: int, biz_id: str) -> dict | None:
    try:
        con = sqlite3.connect(DB_PATH)
        row = con.execute(
            """
            SELECT rowid, m_id, biz_id, user_info, text, old_text,
                   file_path, m_type, status, is_edited, is_deleted, ts
            FROM msgs WHERE m_id=? AND biz_id=?
            """,
            (m_id, biz_id),
        ).fetchone()
        con.close()
        if row:
            return dict(zip(
                ["rowid", "m_id", "biz_id", "user_info", "text", "old_text",
                 "file_path", "m_type", "status", "is_edited", "is_deleted", "ts"],
                row,
            ))
        return None
    except sqlite3.Error as e:
        log.error("db_get ОШИБКА: m_id=%s biz_id=%s — %s", m_id, biz_id, e)
        return None


def db_mark_deleted(m_id: int, biz_id: str) -> None:
    try:
        con = sqlite3.connect(DB_PATH)
        con.execute(
            """
            UPDATE msgs
            SET status='deleted', is_deleted=1, ts=?
            WHERE m_id=? AND biz_id=?
            """,
            (_now_iso(), m_id, biz_id),
        )
        con.commit()
        con.close()
        log.info("db_mark_deleted: m_id=%s biz_id=%s", m_id, biz_id)
    except sqlite3.Error as e:
        log.error("db_mark_deleted ОШИБКА: m_id=%s biz_id=%s — %s", m_id, biz_id, e)
        raise


def db_mark_edited(m_id: int, biz_id: str, new_text: str, old_text: str) -> None:
    try:
        con = sqlite3.connect(DB_PATH)
        con.execute(
            """
            UPDATE msgs
            SET status='edited', is_edited=1, text=?, old_text=?, ts=?
            WHERE m_id=? AND biz_id=?
            """,
            (new_text, old_text, _now_iso(), m_id, biz_id),
        )
        con.commit()
        con.close()
        log.info("db_mark_edited: m_id=%s biz_id=%s", m_id, biz_id)
    except sqlite3.Error as e:
        log.error("db_mark_edited ОШИБКА: m_id=%s biz_id=%s — %s", m_id, biz_id, e)
        raise


# ─── API helpers для FastAPI ───────────────────────────────────────────────────

def db_get_users() -> list[dict]:
    """
    Возвращает уникальных пользователей, сгруппированных по biz_id (аккаунту).
    Структура ответа:
    [
      {
        "id": "<biz_id>",
        "name": "<biz_id или имя аккаунта>",
        "initials": "XX",
        "users": [
          { "id": "<biz_id>::<user_info>", "biz_id": "...", "name": "...",
            "initials": "XX", "raw": "<user_info_raw>", "last_ts": "..." }
        ]
      }, ...
    ]
    """
    try:
        con = sqlite3.connect(DB_PATH)
        rows = con.execute(
            """
            SELECT biz_id, user_info, MAX(ts) as last_ts
            FROM msgs
            GROUP BY biz_id, user_info
            ORDER BY last_ts DESC
            """
        ).fetchall()
        con.close()
    except sqlite3.Error as e:
        log.error("db_get_users ОШИБКА: %s", e)
        return []

    # Группируем по biz_id
    accounts: dict[str, dict] = {}
    for biz_id, user_info, last_ts in rows:
        clean = user_info.split(" (в чате:")[0].strip()
        parts = clean.replace("(@", "").replace(")", "").split()
        initials = "".join(
            p[0].upper() for p in parts[:2] if p and p[0].isalpha()
        ) or "??"

        user_entry = {
            "id":       f"{biz_id}::{user_info}",
            "biz_id":   biz_id,
            "name":     clean,
            "initials": initials,
            "raw":      user_info,
            "last_ts":  last_ts or "",
        }

        if biz_id not in accounts:
            acc_initials = biz_id[:2].upper() if biz_id else "??"
            accounts[biz_id] = {
                "id":       biz_id,
                "name":     f"Аккаунт {biz_id[:8]}",
                "initials": acc_initials,
                "users":    [],
            }
        accounts[biz_id]["users"].append(user_entry)

    return list(accounts.values())


def db_get_messages(biz_id: str, user_info_raw: str) -> list[dict]:
    """
    Возвращает все сообщения конкретного пользователя (biz_id + user_info).
    """
    try:
        con = sqlite3.connect(DB_PATH)
        rows = con.execute(
            """
            SELECT rowid, m_id, text, old_text, m_type, file_path,
                   status, is_edited, is_deleted, ts
            FROM msgs
            WHERE biz_id=? AND user_info=?
            ORDER BY m_id ASC
            """,
            (biz_id, user_info_raw),
        ).fetchall()
        con.close()
    except sqlite3.Error as e:
        log.error("db_get_messages ОШИБКА: biz_id=%s — %s", biz_id, e)
        return []

    result = []
    for row in rows:
        rowid, m_id, text, old_text, m_type, file_path, status, is_edited, is_deleted, ts = row
        result.append({
            "rowid":      rowid,
            "m_id":       m_id,
            "text":       text or "",
            "old_text":   old_text or "",
            "m_type":     m_type,
            "file_path":  file_path or "",
            "status":     status or "normal",
            "is_edited":  bool(is_edited),
            "is_deleted": bool(is_deleted),
            "ts":         ts or "",
        })
    return result


def db_get_updates(since_rowid: int) -> dict:
    """
    Инкрементальные обновления: возвращает все записи с rowid > since_rowid.
    Используется фронтом для polling-обновления без полной перезагрузки.
    Ответ: { "messages": [...], "max_rowid": N, "users_changed": bool }
    """
    try:
        con = sqlite3.connect(DB_PATH)
        rows = con.execute(
            """
            SELECT rowid, m_id, biz_id, user_info, text, old_text, m_type,
                   file_path, status, is_edited, is_deleted, ts
            FROM msgs
            WHERE rowid > ?
            ORDER BY rowid ASC
            LIMIT 200
            """,
            (since_rowid,),
        ).fetchall()
        max_rowid_row = con.execute("SELECT MAX(rowid) FROM msgs").fetchone()
        con.close()
    except sqlite3.Error as e:
        log.error("db_get_updates ОШИБКА: since_rowid=%s — %s", since_rowid, e)
        return {"messages": [], "max_rowid": since_rowid, "users_changed": False}

    max_rowid = max_rowid_row[0] or 0
    messages = []
    new_users = set()

    for row in rows:
        rowid, m_id, biz_id, user_info, text, old_text, m_type, file_path, status, is_edited, is_deleted, ts = row
        messages.append({
            "rowid":      rowid,
            "m_id":       m_id,
            "biz_id":     biz_id,
            "user_info":  user_info,
            "text":       text or "",
            "old_text":   old_text or "",
            "m_type":     m_type,
            "file_path":  file_path or "",
            "status":     status or "normal",
            "is_edited":  bool(is_edited),
            "is_deleted": bool(is_deleted),
            "ts":         ts or "",
        })
        new_users.add(f"{biz_id}::{user_info}")

    # users_changed = True если появились новые пользователи в апдейтах
    users_changed = len(new_users) > 0 and since_rowid == 0 or bool(
        new_users and rows
    )

    log.debug(
        "db_get_updates: since_rowid=%s, found=%s, max_rowid=%s",
        since_rowid, len(messages), max_rowid
    )
    return {
        "messages":     messages,
        "max_rowid":    max_rowid,
        "users_changed": users_changed,
    }


# ─── Вспомогательные функции бота ─────────────────────────────────────────────

def _user_info(msg: Message) -> str:
    u = msg.from_user
    if not u:
        return "Unknown"
    name = " ".join(filter(None, [u.first_name, u.last_name]))
    return f"{name} (@{u.username})" if u.username else name


def _ext_from_mime(mime: str | None, default: str = "bin") -> str:
    if not mime:
        return default
    mapping = {
        "audio/ogg": "ogg", "audio/mpeg": "mp3", "video/mp4": "mp4",
        "video/webm": "webm", "image/jpeg": "jpg", "image/png": "png",
        "image/gif": "gif", "application/pdf": "pdf",
    }
    return mapping.get(mime, mime.split("/")[-1])


async def _download_file(bot: Bot, file_id: str, dest: Path) -> None:
    try:
        tg_file = await bot.get_file(file_id)
        await bot.download_file(tg_file.file_path, destination=str(dest))
        log.info("Файл скачан: %s", dest)
    except Exception as e:
        log.error("_download_file ОШИБКА: file_id=%s dest=%s — %s", file_id, dest, e)
        raise


async def _detect_and_download(msg: Message, biz_bot: Bot) -> tuple[str, str | None]:
    m_type = "text"
    file_path: str | None = None
    try:
        if msg.photo:
            photo = msg.photo[-1]
            dest = MEDIA_DIR / f"{photo.file_id}.jpg"
            await _download_file(biz_bot, photo.file_id, dest)
            m_type, file_path = "photo", str(dest)
        elif msg.voice:
            v = msg.voice
            dest = MEDIA_DIR / f"{v.file_id}.ogg"
            await _download_file(biz_bot, v.file_id, dest)
            m_type, file_path = "voice", str(dest)
        elif msg.video:
            vid = msg.video
            ext = _ext_from_mime(vid.mime_type, "mp4")
            dest = MEDIA_DIR / f"{vid.file_id}.{ext}"
            await _download_file(biz_bot, vid.file_id, dest)
            m_type, file_path = "video", str(dest)
        elif msg.document:
            doc = msg.document
            ext = (Path(doc.file_name).suffix.lstrip(".") if doc.file_name
                   else _ext_from_mime(doc.mime_type))
            dest = MEDIA_DIR / f"{doc.file_id}.{ext}"
            await _download_file(biz_bot, doc.file_id, dest)
            m_type, file_path = "document", str(dest)
    except Exception as e:
        log.warning("_detect_and_download: не удалось скачать медиа для msg_id=%s — %s",
                    msg.message_id, e)
    return m_type, file_path


async def _read_file(path: str) -> bytes | None:
    try:
        async with aiofiles.open(path, "rb") as f:
            return await f.read()
    except FileNotFoundError:
        log.warning("_read_file: файл не найден %s", path)
        return None
    except Exception as e:
        log.error("_read_file ОШИБКА: %s — %s", path, e)
        return None


async def _send_media_to_admin(
    log_bot: Bot, file_path: str, m_type: str, caption: str = ""
) -> None:
    data = await _read_file(file_path)
    if not data:
        log.warning("_send_media_to_admin: нет данных для %s", file_path)
        return
    fname = Path(file_path).name
    buf   = BufferedInputFile(data, filename=fname)
    try:
        if m_type == "photo":
            await log_bot.send_photo(MY_ID, buf, caption=caption, parse_mode=ParseMode.HTML)
        elif m_type == "voice":
            await log_bot.send_voice(MY_ID, buf, caption=caption, parse_mode=ParseMode.HTML)
        elif m_type == "video":
            await log_bot.send_video(MY_ID, buf, caption=caption, parse_mode=ParseMode.HTML)
        else:
            await log_bot.send_document(MY_ID, buf, caption=caption, parse_mode=ParseMode.HTML)
        log.info("_send_media_to_admin: отправлено %s (%s)", m_type, fname)
    except Exception as e:
        log.error("_send_media_to_admin ОШИБКА: %s — %s", fname, e)
        raise


# ─── Роутер ───────────────────────────────────────────────────────────────────

router = Router()

_biz_bot: Bot | None = None
_log_bot: Bot | None = None


# ─── Хэндлер: входящее бизнес-сообщение ──────────────────────────────────────

@router.business_message()
async def on_business_message(msg: Message, biz_bot: Bot, log_bot: Bot) -> None:
    biz_id     = msg.business_connection_id or "unknown"
    user_info  = _user_info(msg)
    text       = msg.text or msg.caption or ""
    chat_title = msg.chat.full_name or msg.chat.title or "Private Chat"

    log.info("📥 Бизнес-сообщение от %s | msg_id=%s | biz_id=%s",
             user_info, msg.message_id, biz_id)

    try:
        m_type, file_path = await _detect_and_download(msg, biz_bot)
    except Exception as e:
        log.error("on_business_message: ошибка при обработке медиа — %s", e)
        m_type, file_path = "text", None

    full_user_entry = f"{user_info} (в чате: {chat_title})"

    try:
        db_save(
            m_id=msg.message_id,
            biz_id=biz_id,
            user_info=full_user_entry,
            text=text,
            file_path=file_path or "",
            m_type=m_type,
            status="normal",
        )
    except Exception as e:
        log.error("on_business_message: ошибка db_save msg_id=%s — %s",
                  msg.message_id, e)

    type_emoji = {"text": "💬", "photo": "🖼", "voice": "🎙", "video": "🎥", "document": "📄"}
    emoji      = type_emoji.get(m_type, "📎")
    caption    = (
        f"👥 <b>Чат:</b> {chat_title}\n"
        f"👤 <b>От кого:</b> {user_info}\n"
        f"{emoji} <b>{m_type.upper()}</b>\n"
        f"💬 {text or '—'}"
    )

    try:
        if file_path and m_type != "text":
            await _send_media_to_admin(log_bot, file_path, m_type, caption)
        else:
            await log_bot.send_message(MY_ID, caption, parse_mode=ParseMode.HTML)
    except Exception as e:
        log.error("on_business_message: ошибка отправки лога — %s", e)


# ─── Хэндлер: редактирование бизнес-сообщения ────────────────────────────────

@router.edited_business_message()
async def on_edited_business_message(msg: Message, log_bot: Bot) -> None:
    biz_id     = msg.business_connection_id or "unknown"
    user_info  = _user_info(msg)
    new_text   = msg.text or msg.caption or "—"
    chat_title = msg.chat.full_name or msg.chat.title or "Private Chat"

    log.info("✏️ Отредактировано сообщение от %s | msg_id=%s | biz_id=%s",
             user_info, msg.message_id, biz_id)

    row      = db_get(msg.message_id, biz_id)
    old_text = row["text"] if row else "—"

    full_user_entry = f"{user_info} (в чате: {chat_title})"

    try:
        if row:
            db_mark_edited(msg.message_id, biz_id, new_text, old_text)
        else:
            db_save(
                m_id=msg.message_id,
                biz_id=biz_id,
                user_info=full_user_entry,
                text=new_text,
                old_text=old_text,
                file_path="",
                m_type="text",
                status="edited",
                is_edited=1,
            )
    except Exception as e:
        log.error("on_edited_business_message: ошибка записи в БД msg_id=%s — %s",
                  msg.message_id, e)

    detail = (
        f"✏️ <b>ОТРЕДАКТИРОВАНО</b>\n"
        f"👥 <b>Чат:</b> {chat_title}\n"
        f"👤 <b>Автор:</b> {user_info}\n"
        f"🕒 <i>{_now_str()}</i>\n\n"
        f"<b>Было:</b>\n{old_text}\n\n"
        f"<b>Стало:</b>\n{new_text}"
    )

    try:
        await log_bot.send_message(MY_ID, detail, parse_mode=ParseMode.HTML)
    except Exception as e:
        log.error("on_edited_business_message: ошибка отправки уведомления — %s", e)


# ─── Хэндлер: удалённые бизнес-сообщения ─────────────────────────────────────

async def on_deleted_business_messages(
    deleted: BusinessMessagesDeleted,
    log_bot: Bot,
    **kwargs,
) -> None:
    biz_id     = deleted.business_connection_id
    chat_id    = deleted.chat.id
    ids        = deleted.message_ids
    chat_title = deleted.chat.full_name or deleted.chat.title or f"ID: {chat_id}"

    log.info("🗑 Удалено %d сообщений из чата %s (biz_id=%s): %s",
             len(ids), chat_id, biz_id, ids)

    for m_id in ids:
        row = db_get(m_id, biz_id)

        if not row:
            log.warning("on_deleted: сообщение msg_id=%s не найдено в архиве", m_id)
            preview = (
                f"🗑 <b>УДАЛЕНО</b> — сообщение не найдено в архиве\n"
                f"👥 <b>Чат:</b> {chat_title}\n"
                f"<code>msg_id: {m_id}</code>  ·  <i>{_now_str()}</i>"
            )
            try:
                await log_bot.send_message(MY_ID, preview, parse_mode=ParseMode.HTML)
            except Exception as e:
                log.error("on_deleted: ошибка уведомления об удалении — %s", e)
            continue

        try:
            db_mark_deleted(m_id, biz_id)
        except Exception as e:
            log.error("on_deleted: ошибка db_mark_deleted msg_id=%s — %s", m_id, e)

        user_info  = row["user_info"]
        text       = row["text"]
        m_type     = row["m_type"]
        file_path  = row["file_path"]
        clean_author = user_info.split(" (в чате:")[0]
        type_label = {
            "text": "текст", "photo": "фото", "voice": "голосовое",
            "video": "видео", "document": "файл",
        }.get(m_type, m_type)

        caption = (
            f"🗑 <b>УДАЛЕНО</b>\n"
            f"👥 <b>Чат:</b> {chat_title}\n"
            f"👤 <b>Автор:</b> {clean_author}\n"
            f"📎 тип: {type_label}  ·  <i>{_now_str()}</i>\n\n"
            f"💬 {text or '—'}"
        )

        log.info("🗑 Отправляю информацию об удалении msg_id=%s", m_id)

        try:
            if file_path and os.path.exists(file_path) and m_type != "text":
                await _send_media_to_admin(log_bot, file_path, m_type, caption)
            else:
                await log_bot.send_message(MY_ID, caption, parse_mode=ParseMode.HTML)
        except Exception as e:
            log.error("on_deleted: ошибка отправки msg_id=%s — %s", m_id, e)


# ─── FastAPI веб-сервер ───────────────────────────────────────────────────────

app = FastAPI(title="Business Monitor API", version="3.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],          # В продакшене заменить на конкретный origin TMA
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/users")
async def api_users() -> JSONResponse:
    """
    Возвращает список аккаунтов с пользователями.
    Формат:
    [
      {
        "id": "<biz_id>",
        "name": "Аккаунт XXXXXXXX",
        "initials": "АК",
        "users": [
          { "id", "biz_id", "name", "initials", "raw", "last_ts" }
        ]
      }
    ]
    """
    try:
        accounts = db_get_users()
        log.info("API /api/users → %d аккаунтов", len(accounts))
        return JSONResponse(content=accounts)
    except Exception as e:
        log.error("API /api/users ОШИБКА: %s", e, exc_info=True)
        return JSONResponse(content={"error": str(e)}, status_code=500)


@app.get("/api/messages/{biz_id}/{user_key:path}")
async def api_messages(biz_id: str, user_key: str) -> JSONResponse:
    """
    Возвращает все сообщения для пользователя.
    user_key — URL-encoded строка user_info_raw из /api/users.
    Каждое сообщение содержит поле rowid для инкрементальных запросов.
    """
    try:
        msgs = db_get_messages(biz_id, user_key)
        log.info("API /api/messages/%s → %d сообщений", biz_id, len(msgs))
        return JSONResponse(content=msgs)
    except Exception as e:
        log.error("API /api/messages ОШИБКА: biz_id=%s — %s", biz_id, e, exc_info=True)
        return JSONResponse(content={"error": str(e)}, status_code=500)


@app.get("/api/updates")
async def api_updates(since: int = Query(default=0, ge=0)) -> JSONResponse:
    """
    Инкрементальные обновления — основной эндпоинт для авто-обновления фронта.
    Параметр: ?since=<rowid>  (фронт передаёт последний известный rowid)
    Ответ:
    {
      "messages":      [...],   // только новые/изменённые записи с rowid > since
      "max_rowid":     N,       // текущий максимальный rowid в БД
      "users_changed": bool     // true — нужно перезагрузить /api/users
    }
    Фронт вызывает этот эндпоинт каждые N секунд (polling).
    """
    try:
        result = db_get_updates(since)
        if result["messages"]:
            log.info(
                "API /api/updates since=%s → %d записей, max_rowid=%s",
                since, len(result["messages"]), result["max_rowid"]
            )
        return JSONResponse(content=result)
    except Exception as e:
        log.error("API /api/updates ОШИБКА: since=%s — %s", since, e, exc_info=True)
        return JSONResponse(content={"error": str(e)}, status_code=500)


@app.get("/api/health")
async def api_health() -> JSONResponse:
    """Проверка состояния сервера."""
    try:
        con = sqlite3.connect(DB_PATH)
        count = con.execute("SELECT COUNT(*) FROM msgs").fetchone()[0]
        con.close()
        return JSONResponse(content={
            "status": "ok",
            "ts": _now_iso(),
            "total_messages": count,
        })
    except Exception as e:
        log.error("API /api/health ОШИБКА: %s", e)
        return JSONResponse(content={"status": "error", "error": str(e)}, status_code=500)


# ─── Запуск ───────────────────────────────────────────────────────────────────

async def run_web_server() -> None:
    config = uvicorn.Config(
        app,
        host=API_HOST,
        port=API_PORT,
        log_level="warning",
        loop="none",
    )
    server = uvicorn.Server(config)
    await server.serve()


async def main() -> None:
    global _biz_bot, _log_bot

    init_db()

    _biz_bot = Bot(
        token=BIZ_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    _log_bot = Bot(
        token=LOG_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )

    dp = Dispatcher()
    dp.include_router(router)
    dp.observers["deleted_business_messages"].register(on_deleted_business_messages)

    try:
        await _biz_bot.delete_webhook(drop_pending_updates=True)
        await _log_bot.delete_webhook(drop_pending_updates=True)
        log.info("Вебхуки очищены. Стартуем поллинг + API сервер…")
    except Exception as e:
        log.error("Ошибка очистки вебхуков: %s", e)

    try:
        await _log_bot.send_message(
            MY_ID,
            f"🟢 <b>Business Monitor v3 запущен</b>\n"
            f"📡 API доступен на <code>http://{API_HOST}:{API_PORT}</code>\n"
            f"🔄 Авто-обновление фронта: /api/updates\n"
            f"Отслеживаю бизнес-чаты…",
        )
    except Exception as e:
        log.warning("Не удалось отправить стартовое уведомление: %s", e)

    await asyncio.gather(
        dp.start_polling(
            _biz_bot,
            _log_bot,
            allowed_updates=[
                "message",
                "business_message",
                "edited_business_message",
                "deleted_business_messages",
            ],
            biz_bot=_biz_bot,
            log_bot=_log_bot,
        ),
        run_web_server(),
    )


if __name__ == "__main__":
    asyncio.run(main())
