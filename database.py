# database.py — Работа с базой данных SQLite

import aiosqlite
import json
from datetime import datetime

DB_PATH = 'chatbot.db'

# =============================================
# ИНИЦИАЛИЗАЦИЯ БАЗЫ ДАННЫХ
# =============================================

async def init_db():
    """Создаём таблицы если их нет"""
    async with aiosqlite.connect(DB_PATH) as db:

        # Таблица заявок
        await db.execute('''
            CREATE TABLE IF NOT EXISTS applications (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                name        TEXT    NOT NULL,
                phone       TEXT    NOT NULL,
                comment     TEXT    DEFAULT '',
                answers     TEXT    DEFAULT '{}',
                branch      TEXT    DEFAULT '',
                ip_address  TEXT    DEFAULT '',
                created_at  TEXT    DEFAULT (datetime('now', 'localtime'))
            )
        ''')

        # Таблица сессий
        await db.execute('''
            CREATE TABLE IF NOT EXISTS sessions (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id  TEXT    UNIQUE NOT NULL,
                ip_address  TEXT    DEFAULT '',
                started_at  TEXT    DEFAULT (datetime('now', 'localtime')),
                finished_at TEXT,
                status      TEXT    DEFAULT 'active'
            )
        ''')

        # Таблица сообщений
        await db.execute('''
            CREATE TABLE IF NOT EXISTS messages (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id  TEXT    NOT NULL,
                sender      TEXT    NOT NULL,
                message     TEXT    NOT NULL,
                step        TEXT    DEFAULT '',
                created_at  TEXT    DEFAULT (datetime('now', 'localtime'))
            )
        ''')

        await db.commit()
        print('✅ База данных SQLite инициализирована')


# =============================================
# ФУНКЦИИ ДЛЯ ЗАЯВОК
# =============================================

async def save_application(name, phone, comment, answers, branch, ip=''):
    """Сохранить заявку"""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('''
            INSERT INTO applications
                (name, phone, comment, answers, branch, ip_address)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (name, phone, comment, json.dumps(answers, ensure_ascii=False), branch, ip))
        await db.commit()
        print(f'💾 Заявка сохранена в БД: #{cursor.lastrowid}')
        return cursor.lastrowid


async def get_all_applications():
    """Получить все заявки"""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('''
            SELECT * FROM applications ORDER BY created_at DESC
        ''')
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]


async def get_application_by_id(app_id):
    """Получить заявку по ID"""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            'SELECT * FROM applications WHERE id = ?', (app_id,)
        )
        row = await cursor.fetchone()
        return dict(row) if row else None


# =============================================
# ФУНКЦИИ ДЛЯ СЕССИЙ
# =============================================

async def create_session(session_id, ip=''):
    """Создать новую сессию"""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('''
            INSERT OR IGNORE INTO sessions (session_id, ip_address)
            VALUES (?, ?)
        ''', (session_id, ip))
        await db.commit()


async def finish_session(session_id):
    """Завершить сессию"""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('''
            UPDATE sessions
            SET status = 'finished',
                finished_at = datetime('now', 'localtime')
            WHERE session_id = ?
        ''', (session_id,))
        await db.commit()


async def get_all_sessions():
    """Получить все сессии"""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('''
            SELECT * FROM sessions ORDER BY started_at DESC
        ''')
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]


# =============================================
# ФУНКЦИИ ДЛЯ СООБЩЕНИЙ
# =============================================

async def save_message(session_id, sender, message, step=''):
    """Сохранить сообщение"""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('''
            INSERT INTO messages (session_id, sender, message, step)
            VALUES (?, ?, ?, ?)
        ''', (session_id, sender, message, step))
        await db.commit()


async def get_session_messages(session_id):
    """Получить сообщения сессии"""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('''
            SELECT * FROM messages
            WHERE session_id = ?
            ORDER BY created_at ASC
        ''', (session_id,))
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]


# =============================================
# СТАТИСТИКА
# =============================================

async def get_stats():
    """Статистика для админки"""
    async with aiosqlite.connect(DB_PATH) as db:

        # Всего заявок
        cursor = await db.execute('SELECT COUNT(*) FROM applications')
        total = (await cursor.fetchone())[0]

        # Сегодня
        cursor = await db.execute('''
            SELECT COUNT(*) FROM applications
            WHERE date(created_at) = date('now', 'localtime')
        ''')
        today = (await cursor.fetchone())[0]

        # Всего сессий
        cursor = await db.execute('SELECT COUNT(*) FROM sessions')
        sessions_count = (await cursor.fetchone())[0]

        # По веткам
        cursor = await db.execute('''
            SELECT branch, COUNT(*) as count
            FROM applications
            GROUP BY branch
            ORDER BY count DESC
        ''')
        branches_rows = await cursor.fetchall()
        branches = [{'branch': row[0], 'count': row[1]} for row in branches_rows]

        return {
            'total': total,
            'today': today,
            'sessions': sessions_count,
            'branches': branches
        }