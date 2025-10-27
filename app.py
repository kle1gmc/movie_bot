from datetime import datetime
import asyncio
import os
import random
import requests
from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, BotCommand
from aiogram.filters import Command
from aiogram.fsm.storage.memory import MemoryStorage
from dotenv import load_dotenv
import asyncpg
from reportlab.lib.units import inch
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.lib.utils import ImageReader
import io
from urllib.parse import urlparse

load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
TMDB_TOKEN = os.getenv("API_TMDB")
DATABASE_URL = os.getenv("DATABASE_URL")


bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
db: asyncpg.Pool = None

# Персистентные структуры в памяти
user_sessions = {}
user_filters = {}
user_input_waiting = {}

# ЖАНРЫ TMDB
GENRES_MOVIE = {
    "🔫 Боевик": 28, "🏹 Приключения": 12, "🎨 Мультфильм": 16, "😂 Комедия": 35,
    "🕵️ Криминал": 80, "🎬 Документальный": 99, "😢 Драма": 18,
    "👨‍👩‍👧‍👦 Семейный": 10751, "🧙‍♂️ Фэнтези": 14, "📜 История": 36,
    "👻 Ужасы": 27, "🎵 Музыка": 10402, "🕵️‍♂️ Детектив": 9648,
    "❤️ Мелодрама": 10749, "🚀 Фантастика": 878, "📺 ТВ-фильм": 10770,
    "😱 Триллер": 53, "⚔️ Военный": 10752, "🤠 Вестерн": 37
}

GENRES_TV = {
    "🔫🏹 Боевик и приключения": 10759, "🎨 Мультфильм": 16, "😂 Комедия": 35,
    "🕵️ Криминал": 80, "🎬 Документальный": 99, "😢 Драма": 18,
    "👨‍👩‍👧‍👦 Семейный": 10751, "👶 Детский": 10762, "🕵️‍♂️ Детектив": 9648,
    "📰 Новости": 10763, "📺 Реалити-шоу": 10764, "🧙‍♂️🚀 НФ и Фэнтези": 10765,
    "🎭 Мыльная опера": 10766, "🎤 Ток-шоу": 10767, "⚔️ Политика и война": 10768,
    "🤠 Вестерн": 37
}

COUNTRY_FLAGS = {
    "RU": "🇷🇺", "US": "🇺🇸", "GB": "🇬🇧", "FR": "🇫🇷", "DE": "🇩🇪", "IT": "🇮🇹", "ES": "🇪🇸", "JP": "🇯🇵", "KR": "🇰🇷", "CN": "🇨🇳",
    "IN": "🇮🇳", "BR": "🇧🇷", "CA": "🇨🇦", "AU": "🇦🇺", "UA": "🇺🇦", "PL": "🇵🇱", "TR": "🇹🇷", "SE": "🇸🇪", "NO": "🇳🇴", "DK": "🇩🇰"
}


# -------------------- DB INIT --------------------
async def init_db():
    global db
    db = await asyncpg.create_pool(DATABASE_URL)
    async with db.acquire() as conn:
        await conn.execute("""
                           CREATE TABLE IF NOT EXISTS users
                           (
                            user_id SERIAL PRIMARY KEY,
                            tg_id BIGINT NOT NULL UNIQUE,
                            username TEXT,
                            disable_anime BOOLEAN DEFAULT FALSE,
                            disable_cartoons BOOLEAN DEFAULT FALSE,
                            hide_watched boolean DEFAULT false
                           );
                           """)
        await conn.execute("""
                           CREATE TABLE IF NOT EXISTS collection
                           (
                            collection_id SERIAL PRIMARY KEY,
                            user_id INT REFERENCES users(user_id) ON DELETE CASCADE,
                            tmdb_id INT NOT NULL,
                            type TEXT NOT NULL,
                            title TEXT,
                            year TEXT,
                            poster_path TEXT,
                            added_at timestamp DEFAULT now()
                               );
                           """)
        await conn.execute("""
                           CREATE TABLE IF NOT EXISTS ratings
                           (
                            rating_id SERIAL PRIMARY KEY,
                            tmdb_id INT NOT NULL,
                            type TEXT NOT NULL,
                            title TEXT,
                            user_id INT REFERENCES users(user_id) ON DELETE CASCADE,
                            liked BOOLEAN DEFAULT FALSE,
                            disliked BOOLEAN DEFAULT FALSE,
                            watched BOOLEAN DEFAULT FALSE,
                            is_hidden BOOLEAN DEFAULT FALSE,
                            CONSTRAINT unique_user_rating UNIQUE (user_id, tmdb_id, type)
                               );
                           """)
        await conn.execute("""
                           CREATE TABLE IF NOT EXISTS user_filters
                           (
                            filter_id SERIAL PRIMARY KEY,
                            user_id INT REFERENCES users(user_id) ON DELETE CASCADE,
                            start_year INT,
                            end_year INT,
                            country_code VARCHAR(10),
                            min_rating DECIMAL(3,1),
                            created_at TIMESTAMP DEFAULT NOW(),
                            updated_at TIMESTAMP DEFAULT NOW(),
                            CONSTRAINT unique_user_filter UNIQUE (user_id)
                               );
                           """)
        await conn.execute("""
                           CREATE INDEX IF NOT EXISTS idx_user_filters_user_id ON user_filters(user_id);
                           """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS banned_content (
                ban_id SERIAL PRIMARY KEY,
                tmdb_id INT NOT NULL,
                type TEXT NOT NULL,
                title TEXT NOT NULL,
                banned_by BIGINT,
                banned_at TIMESTAMP DEFAULT NOW(),
                reason TEXT,
                CONSTRAINT unique_banned_item UNIQUE (tmdb_id, type)
            );
        """)
        await conn.execute("""
                    CREATE TABLE IF NOT EXISTS user_friends (
                        friendship_id SERIAL PRIMARY KEY,
                        user_id INT REFERENCES users(user_id) ON DELETE CASCADE,
                        friend_user_id INT REFERENCES users(user_id) ON DELETE CASCADE,
                        created_at TIMESTAMP DEFAULT NOW(),
                        CONSTRAINT unique_friendship UNIQUE (user_id, friend_user_id)
                    );
                """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS friend_requests (
                request_id SERIAL PRIMARY KEY,
                from_user_id INT REFERENCES users(user_id) ON DELETE CASCADE,
                to_user_id INT REFERENCES users(user_id) ON DELETE CASCADE,
                status TEXT DEFAULT 'pending', -- pending, accepted, rejected
                created_at TIMESTAMP DEFAULT NOW(),
                CONSTRAINT unique_friend_request UNIQUE (from_user_id, to_user_id)
            );
        """)


# -------------------- DB HELPERS --------------------
async def get_or_create_user(tg_id: int, username: str | None = None):
    async with db.acquire() as conn:
        user = await conn.fetchrow("SELECT * FROM users WHERE tg_id=$1", tg_id)
        if not user:
            user = await conn.fetchrow(
                "INSERT INTO users (tg_id, username) VALUES ($1, $2) RETURNING *",
                tg_id, username
            )
        return user


async def update_user_filter(tg_id: int, field: str, value: bool):
    async with db.acquire() as conn:
        await conn.execute(f"UPDATE users SET {field}=$1 WHERE tg_id=$2", value, tg_id)


async def get_user_filters(tg_id: int):
    async with db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT disable_anime, disable_cartoons, hide_watched FROM users WHERE tg_id=$1", tg_id
        )
        if row:
            return {
                "exclude_anime": row["disable_anime"],
                "exclude_cartoons": row["disable_cartoons"],
                "exclude_watched": row["hide_watched"]
            }
        return {"exclude_anime": False, "exclude_cartoons": False, "exclude_watched": False}


async def save_search_filters(tg_id: int, filters: dict):
    """Сохраняет фильтры поиска в базу данных"""
    async with db.acquire() as conn:
        user = await conn.fetchrow("SELECT user_id FROM users WHERE tg_id=$1", tg_id)
        if not user:
            return False

        # Проверяем, есть ли уже фильтры для пользователя
        existing = await conn.fetchrow("SELECT * FROM user_filters WHERE user_id=$1", user["user_id"])

        if existing:
            # Обновляем существующие фильтры
            await conn.execute("""
                               UPDATE user_filters
                               SET start_year=$1,
                                   end_year=$2,
                                   country_code=$3,
                                   min_rating=$4,
                                   updated_at=NOW()
                               WHERE user_id = $5
                               """, filters.get('start_year'), filters.get('end_year'), filters.get('country'),
                               filters.get('rating'), user["user_id"])
        else:
            # Создаем новые фильтры
            await conn.execute("""
                               INSERT INTO user_filters (user_id, start_year, end_year, country_code, min_rating)
                               VALUES ($1, $2, $3, $4, $5)
                               """, user["user_id"], filters.get('start_year'), filters.get('end_year'),
                               filters.get('country'), filters.get('rating'))

        return True


async def load_search_filters(tg_id: int):
    """Загружает фильтры поиска из базы данных"""
    async with db.acquire() as conn:
        user = await conn.fetchrow("SELECT user_id FROM users WHERE tg_id=$1", tg_id)
        if not user:
            return {}

        row = await conn.fetchrow("""
                                  SELECT start_year, end_year, country_code, min_rating
                                  FROM user_filters
                                  WHERE user_id = $1
                                  """, user["user_id"])

        if row:
            filters = {}
            if row["start_year"] and row["end_year"]:
                filters["start_year"] = row["start_year"]
                filters["end_year"] = row["end_year"]
            if row["country_code"]:
                filters["country"] = row["country_code"]
            if row["min_rating"]:
                filters["rating"] = float(row["min_rating"])
            return filters

        return {}


async def clear_search_filters(tg_id: int):
    """Очищает фильтры поиска пользователя"""
    async with db.acquire() as conn:
        user = await conn.fetchrow("SELECT user_id FROM users WHERE tg_id=$1", tg_id)
        if not user:
            return False

        await conn.execute("DELETE FROM user_filters WHERE user_id=$1", user["user_id"])
        return True

async def get_current_filters(chat_id: int):
    """Возвращает текущие фильтры пользователя (из сессии или БД)"""
    if chat_id in user_sessions and "filters" in user_sessions[chat_id]:
        return user_sessions[chat_id]["filters"]
    else:
        # Загружаем из БД если нет в сессии
        filters = await load_search_filters(chat_id)
        if chat_id not in user_sessions:
            user_sessions[chat_id] = {}
        user_sessions[chat_id]["filters"] = filters
        return filters

async def add_to_collection(tg_id: int, tmdb_id: int, type_: str, title: str, year: str, poster_path: str):
    async with db.acquire() as conn:
        user = await conn.fetchrow("SELECT user_id FROM users WHERE tg_id=$1", tg_id)
        if not user:
            return False
        await conn.execute("""
                           INSERT INTO collection (user_id, tmdb_id, type, title, year, poster_path)
                           VALUES ($1, $2, $3, $4, $5, $6)
                           """, user["user_id"], tmdb_id, type_, title, year, poster_path)
        return True


async def get_collection(tg_id: int, limit=4, offset=0):
    async with db.acquire() as conn:
        user = await conn.fetchrow("SELECT user_id FROM users WHERE tg_id=$1", tg_id)
        if not user:
            return []
        rows = await conn.fetch("""
                                SELECT *
                                FROM collection
                                WHERE user_id = $1
                                ORDER BY added_at DESC
                                    LIMIT $2
                                OFFSET $3
                                """, user["user_id"], limit, offset)
        return rows


async def get_collection_count(tg_id: int):
    async with db.acquire() as conn:
        user = await conn.fetchrow("SELECT user_id FROM users WHERE tg_id=$1", tg_id)
        if not user:
            return 0
        row = await conn.fetchrow("""
                                  SELECT COUNT(*)
                                  FROM collection
                                  WHERE user_id = $1
                                  """, user["user_id"])
        return row["count"]


async def remove_from_collection(tg_id: int, tmdb_id: int, type_: str):
    async with db.acquire() as conn:
        user = await conn.fetchrow("SELECT user_id FROM users WHERE tg_id=$1", tg_id)
        if not user:
            return False
        await conn.execute("""
                           DELETE
                           FROM collection
                           WHERE user_id = $1
                             AND tmdb_id = $2
                             AND type = $3
                           """, user["user_id"], tmdb_id, type_)
        return True


async def add_friend(user_tg_id: int, friend_tg_id: int):
    """Добавляет друга"""
    async with db.acquire() as conn:
        user = await conn.fetchrow("SELECT user_id FROM users WHERE tg_id=$1", user_tg_id)
        friend = await conn.fetchrow("SELECT user_id FROM users WHERE tg_id=$1", friend_tg_id)

        if not user or not friend or user["user_id"] == friend["user_id"]:
            return False

        # Добавляем взаимную дружбу
        await conn.execute("""
            INSERT INTO user_friends (user_id, friend_user_id)
            VALUES ($1, $2), ($2, $1)
            ON CONFLICT (user_id, friend_user_id) DO NOTHING
        """, user["user_id"], friend["user_id"])

        return True


async def get_user_friends(tg_id: int):
    """Получает список друзей пользователя"""
    async with db.acquire() as conn:
        user = await conn.fetchrow("SELECT user_id FROM users WHERE tg_id=$1", tg_id)
        if not user:
            return []

        rows = await conn.fetch("""
            SELECT u.tg_id, u.username
            FROM user_friends uf
            JOIN users u ON uf.friend_user_id = u.user_id
            WHERE uf.user_id = $1
            ORDER BY uf.created_at DESC
        """, user["user_id"])

        return rows


async def get_friends_likes(tg_id: int, limit: int = 20):
    """Получает лайки друзей для рекомендаций (только не скрытые)"""
    async with db.acquire() as conn:
        user = await conn.fetchrow("SELECT user_id FROM users WHERE tg_id=$1", tg_id)
        if not user:
            return []

        # Получаем все рекомендации (только не скрытые оценки)
        rows = await conn.fetch("""
            SELECT DISTINCT 
                r.tmdb_id, 
                r.type, 
                r.title,
                COUNT(r.liked) as friend_likes_count,
                u.tg_id as friend_tg_id,
                u.username as friend_username
            FROM user_friends uf
            JOIN ratings r ON uf.friend_user_id = r.user_id
            JOIN users u ON r.user_id = u.user_id
            LEFT JOIN ratings user_ratings ON 
                user_ratings.user_id = $1 AND 
                user_ratings.tmdb_id = r.tmdb_id AND 
                user_ratings.type = r.type
            WHERE 
                uf.user_id = $1 AND 
                r.liked = TRUE AND 
                r.watched = TRUE AND
                r.is_hidden = FALSE AND  -- ТОЛЬКО НЕ СКРЫТЫЕ ОЦЕНКИ
                (user_ratings.watched IS NULL OR user_ratings.watched = FALSE)
            GROUP BY r.tmdb_id, r.type, r.title, u.tg_id, u.username
            ORDER BY friend_likes_count DESC
            LIMIT $2
        """, user["user_id"], limit)

        # Фильтруем забаненный контент
        filtered_rows = []
        for row in rows:
            if not await is_banned(row['tmdb_id'], row['type']):
                filtered_rows.append(row)

        return filtered_rows


async def add_rating(user_id, tmdb_id, type_, liked=None, disliked=None, watched=None, is_hidden=None, title=None):
    try:
        async with db.acquire() as conn:
            # Сначала получаем текущие значения
            current = await conn.fetchrow(
                "SELECT liked, disliked, watched, is_hidden FROM ratings WHERE user_id = (SELECT user_id FROM users WHERE tg_id = $1) AND tmdb_id = $2 AND type = $3",
                user_id, tmdb_id, type_
            )

            # Если запись существует, обновляем только переданные поля
            if current:
                # Сохраняем текущие значения для полей, которые не переданы
                update_liked = liked if liked is not None else current['liked']
                update_disliked = disliked if disliked is not None else current['disliked']
                update_watched = watched if watched is not None else current['watched']
                update_hidden = is_hidden if is_hidden is not None else current['is_hidden']

                await conn.execute(
                    "UPDATE ratings SET liked = $1, disliked = $2, watched = $3, is_hidden = $4, title = $5 WHERE user_id = (SELECT user_id FROM users WHERE tg_id = $6) AND tmdb_id = $7 AND type = $8",
                    update_liked, update_disliked, update_watched, update_hidden, title, user_id, tmdb_id, type_
                )
            else:
                # Создаем новую запись
                await conn.execute(
                    "INSERT INTO ratings (user_id, tmdb_id, type, liked, disliked, watched, is_hidden, title) VALUES ((SELECT user_id FROM users WHERE tg_id = $1), $2, $3, $4, $5, $6, $7, $8)",
                    user_id, tmdb_id, type_,
                    liked or False,
                    disliked or False,
                    watched or False,
                    is_hidden or False,
                    title
                )
            return True
    except Exception as e:
        print(f"Error in add_rating: {e}")
        return False


async def get_ratings(tmdb_id: int, type_: str):
    async with db.acquire() as conn:
        row = await conn.fetchrow("""
                                  SELECT COUNT(CASE WHEN liked = TRUE THEN 1 END)    as likes,
                                         COUNT(CASE WHEN disliked = TRUE THEN 1 END) as dislikes,
                                         COUNT(CASE WHEN watched = TRUE THEN 1 END)  as watches
                                  FROM ratings
                                  WHERE tmdb_id = $1
                                    AND type = $2
                                  """, tmdb_id, type_)
        if row:
            return {
                "likes": row["likes"] or 0,
                "dislikes": row["dislikes"] or 0,
                "watches": row["watches"] or 0
            }
        return {"likes": 0, "dislikes": 0, "watches": 0}

async def ban_content(tmdb_id: int, type_: str, title: str, banned_by: int, reason: str = None):
    """Добавляет контент в бан-лист"""
    async with db.acquire() as conn:
        await conn.execute("""
            INSERT INTO banned_content (tmdb_id, type, title, banned_by, reason)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (tmdb_id, type) DO NOTHING
        """, tmdb_id, type_, title, banned_by, reason)

async def unban_content(tmdb_id: int, type_: str):
    """Убирает контент из бан-листа"""
    async with db.acquire() as conn:
        await conn.execute("""
            DELETE FROM banned_content 
            WHERE tmdb_id = $1 AND type = $2
        """, tmdb_id, type_)

async def is_banned(tmdb_id: int, type_: str) -> bool:
    """Проверяет, забанен ли контент"""
    async with db.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT 1 FROM banned_content 
            WHERE tmdb_id = $1 AND type = $2
        """, tmdb_id, type_)
        return bool(row)

async def get_banned_list(limit: int = 50):
    """Возвращает список забаненного контента"""
    async with db.acquire() as conn:
        return await conn.fetch("""
            SELECT * FROM banned_content 
            ORDER BY banned_at DESC 
            LIMIT $1
        """, limit)

# -------------------- TMDB --------------------
def tmdb_get(url: str, params: dict):
    headers = {"accept": "application/json", "Authorization": f"Bearer {TMDB_TOKEN}"}
    return requests.get(url, headers=headers, params=params, timeout=10)


async def discover_tmdb(type_: str, genre_id: int | None = None, vote_count_min: int = 50, filters: dict = None):
    base_url = f"https://api.themoviedb.org/3/discover/{type_}"
    common = {
        "language": "ru-RU",
        "sort_by": random.choice(["popularity.desc", "vote_average.desc", "primary_release_date.desc"]),
        "vote_count.gte": vote_count_min,
        "page": 1,  # Сначала получаем первую страницу чтобы узнать total_pages
        "include_adult": "false",
    }

    if genre_id:
        common["with_genres"] = genre_id

    # Применяем дополнительные фильтры
    if filters:
        if filters.get('start_year') and filters.get('end_year'):
            start_year = filters['start_year']
            end_year = filters['end_year']
            if type_ == "movie":
                common["primary_release_date.gte"] = f"{start_year}-01-01"
                common["primary_release_date.lte"] = f"{end_year}-12-31"
            else:
                common["first_air_date.gte"] = f"{start_year}-01-01"
                common["first_air_date.lte"] = f"{end_year}-12-31"

        if filters.get('country'):
            common["with_origin_country"] = filters['country']

        if filters.get('rating'):
            common["vote_average.gte"] = filters['rating']

    # 🔴 ИСПРАВЛЕНИЕ: Сначала получаем total_pages
    r1 = tmdb_get(base_url, common)
    if r1.status_code != 200:
        return []

    data1 = r1.json()
    results = data1.get("results", [])
    total_pages = min(data1.get("total_pages", 1), 500)  # Ограничиваем 500 страницами

    # 🟢 ТЕПЕРЬ выбираем случайную страницу
    if total_pages > 1:
        random_page = random.randint(1, total_pages)
        if random_page != 1:
            common["page"] = random_page
            r2 = tmdb_get(base_url, common)
            if r2.status_code == 200:
                results = r2.json().get("results", [])

    # Если ничего не нашли, пробуем снизить порог голосов
    if not results and vote_count_min > 10:
        return await discover_tmdb(type_, genre_id=genre_id, vote_count_min=10, filters=filters)

    # Фильтруем забаненный контент
    async def filter_banned_items(items):
        filtered_items = []
        for item in items:
            if not await is_banned(item["id"], type_):
                filtered_items.append(item)
        return filtered_items

    results = await filter_banned_items(results)
    return results


def get_item_details(type_: str, tmdb_id: int):
    url = f"https://api.themoviedb.org/3/{type_}/{tmdb_id}"
    r = tmdb_get(url, {"language": "ru-RU"})
    if r.status_code == 200:
        return r.json()
    return {}


def get_trailer_url(type_, tmdb_id):
    url = f"https://api.themoviedb.org/3/{type_}/{tmdb_id}/videos"
    r = tmdb_get(url, {"language": "ru-RU"})
    if r.status_code == 200:
        for v in r.json().get("results", []):
            if v.get("type") == "Trailer" and v.get("site") == "YouTube":
                return f"https://www.youtube.com/watch?v={v.get('key')}"
    return None


def get_trending(media_type: str, time_window: str = "week"):
    """Получает трендовые фильмы/сериалы за неделю"""
    url = f"https://api.themoviedb.org/3/trending/{media_type}/{time_window}"
    r = tmdb_get(url, {"language": "ru-RU"})
    if r.status_code == 200:
        return r.json().get("results", [])
    return []


def is_anime_by_details(type_: str, details: dict, item: dict) -> bool:
    genre_ids = [g.get("id") for g in details.get("genres", []) if g.get("id")] or item.get("genre_ids", [])
    if 16 not in genre_ids:
        return False
    prod_countries = [c.get("iso_3166_1") for c in details.get("production_countries", []) if c.get("iso_3166_1")]
    origin_country = details.get("origin_country", []) or []
    codes = set(prod_countries + origin_country)
    return "JP" in codes


def is_cartoons_by_details(type_: str, details: dict, item: dict) -> bool:
    genre_ids = [g.get("id") for g in details.get("genres", []) if g.get("id")] or item.get("genre_ids", [])
    return 16 in genre_ids


# -------------------- KEYBOARDS --------------------
def kb_main():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔍 Поиск", callback_data="search_menu")],
        [InlineKeyboardButton(text="⚡ Фильтры поиска", callback_data="search_filters")],
        [InlineKeyboardButton(text="👥 Друзья", callback_data="friends_menu")],  # Новая кнопка
        [InlineKeyboardButton(text="📚 Коллекция", callback_data="show_collection")],
        [InlineKeyboardButton(text="⚙️ Настройки", callback_data="settings")],
    ])


def kb_search_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎲 Случайный поиск", callback_data="random_search")],
        [InlineKeyboardButton(text="🔍 Поиск по названию", callback_data="search_by_title")],
        [InlineKeyboardButton(text="🎭 Поиск по актеру", callback_data="search_by_person")],
        [InlineKeyboardButton(text="🎯 На основе предпочтений", callback_data="preferences")],
        [InlineKeyboardButton(text="🔥 В тренде сейчас", callback_data="trending_menu")],
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="back_to_main")],
    ])


def kb_random_search():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎬 Случайный фильм", callback_data="discover_movie")],
        [InlineKeyboardButton(text="📺 Случайный сериал", callback_data="discover_tv")],
        [InlineKeyboardButton(text="🧭 Случайный поиск по жанрам", callback_data="search_genre")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="search_menu")],
    ])


def kb_settings(filters):
    anime_status = "✅" if filters.get("exclude_anime") else "❌"
    cartoons_status = "✅" if filters.get("exclude_cartoons") else "❌"
    watched_status = "✅" if filters.get("exclude_watched") else "❌"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"{anime_status} Скрывать аниме", callback_data="toggle_anime")],
        [InlineKeyboardButton(text=f"{cartoons_status} Скрывать мультфильмы", callback_data="toggle_cartoons")],
        [InlineKeyboardButton(text=f"{watched_status} Скрывать просмотренное", callback_data="toggle_watched")],
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="back_to_main")],
    ])

def kb_admin_panel():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔍 Поиск для бана", callback_data="admin_search_ban")],
        [InlineKeyboardButton(text="📋 Список банов", callback_data="admin_ban_list")],
        [InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats")],
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="back_to_main")]
    ])


def kb_admin_stats(sort_by: str, page: int, total_pages: int):
    """Клавиатура для панели статистики"""
    sort_buttons = [
        [
            InlineKeyboardButton(
                text=f"🕐 По дате {'✅' if sort_by == 'updated' else ''}",
                callback_data="stats_sort_updated"
            ),
            InlineKeyboardButton(
                text=f"👍 По лайкам {'✅' if sort_by == 'likes' else ''}",
                callback_data="stats_sort_likes"
            )
        ],
        [
            InlineKeyboardButton(
                text=f"👎 По дизлайкам {'✅' if sort_by == 'dislikes' else ''}",
                callback_data="stats_sort_dislikes"
            ),
            InlineKeyboardButton(
                text=f"👀 По просмотрам {'✅' if sort_by == 'watches' else ''}",
                callback_data="stats_sort_watches"
            )
        ]
    ]

    # Кнопки пагинации
    pagination_buttons = []
    if page > 0:
        pagination_buttons.append(
            InlineKeyboardButton(text="⬅️ Назад", callback_data=f"stats_page_{page - 1}_{sort_by}"))

    pagination_buttons.append(InlineKeyboardButton(text=f"{page + 1}/{total_pages}", callback_data="stats_info"))

    if page < total_pages - 1:
        pagination_buttons.append(
            InlineKeyboardButton(text="Вперед ➡️", callback_data=f"stats_page_{page + 1}_{sort_by}"))

    if pagination_buttons:
        sort_buttons.append(pagination_buttons)

    # НОВЫЕ КНОПКИ ЭКСПОРТА
    sort_buttons.append([
        InlineKeyboardButton(text="📄 Выгрузить в PDF", callback_data="stats_export_pdf"),
        InlineKeyboardButton(text="📊 Диаграммы в PDF", callback_data="stats_charts_pdf")
    ])

    sort_buttons.append([
        InlineKeyboardButton(text="⬅️ В админ-панель", callback_data="admin_panel")
    ])

    return InlineKeyboardMarkup(inline_keyboard=sort_buttons)

async def set_bot_commands():
    commands = [
        BotCommand(command="start", description="Запустить бота"),
        BotCommand(command="admin", description="Админ-панель"),
        BotCommand(command="search", description="Поиск по TMDB ID"),
        BotCommand(command="myid", description="Показать ваш ID"),
    ]

    await bot.set_my_commands(commands)
    print("✅ Команды бота установлены")


async def kb_ban_confirmation(tmdb_id: int, type_: str, title: str):
    # Проверяем, забанен ли уже контент
    is_already_banned = await is_banned(tmdb_id, type_)

    if is_already_banned:
        return InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="🔓 Разбанить", callback_data=f"confirm_unban_{tmdb_id}_{type_}"),
                InlineKeyboardButton(text="❌ Отмена", callback_data="delete_message")
            ]
        ])
    else:
        return InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="🚫 Забанить", callback_data=f"confirm_ban_{tmdb_id}_{type_}"),
                InlineKeyboardButton(text="❌ Отмена", callback_data="delete_message")
            ]
        ])


def get_country_flag(country_code: str) -> str:
    """Возвращает флаг страны или код, если флаг не найден"""
    return COUNTRY_FLAGS.get(country_code, country_code)

def kb_filters_menu(current_filters: dict):
    # Отображаем год/диапазон
    if current_filters.get('start_year') and current_filters.get('end_year'):
        start_year = current_filters['start_year']
        end_year = current_filters['end_year']
        if start_year == end_year:
            year_btn = f"📅 Год: {start_year}"
        else:
            year_btn = f"📅 Года: {start_year}-{end_year}"
    else:
        year_btn = "📅 Года: Любые"

    # Отображаем страну с флагом
    country_value = current_filters.get('country')
    if country_value:
        country_flag = get_country_flag(country_value)
        country_btn = f"🌍 Страна: {country_flag}"
    else:
        country_btn = "🌍 Страна: Любая"

    rating_btn = f"⭐ Рейтинг: {current_filters.get('rating', 'Любой')}+"

    filters_active = any(current_filters.values())
    status_btn = "✅ Фильтры активны" if filters_active else "❌ Фильтры не активны"

    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=status_btn, callback_data="filters_status")],
        [InlineKeyboardButton(text=year_btn, callback_data="filter_year")],
        [InlineKeyboardButton(text=country_btn, callback_data="filter_country")],
        [InlineKeyboardButton(text=rating_btn, callback_data="filter_rating")],
        [InlineKeyboardButton(text="🔄 Сбросить все фильтры", callback_data="reset_all_filters")],
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="back_to_main")],
    ])


def kb_rating_selection():
    ratings = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]

    keyboard = []
    row = []
    for rating in ratings:
        row.append(InlineKeyboardButton(text=f"{rating}+", callback_data=f"set_rating_{rating}"))
        if len(row) == 3:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)

    keyboard.append([InlineKeyboardButton(text="❌ Без рейтинга", callback_data="clear_rating")])
    keyboard.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="search_filters")])

    return InlineKeyboardMarkup(inline_keyboard=keyboard)


async def is_in_user_collection(tg_id: int, tmdb_id: int, type_: str) -> bool:
    """Проверяет, находится ли контент в коллекции пользователя"""
    async with db.acquire() as conn:
        user = await conn.fetchrow("SELECT user_id FROM users WHERE tg_id=$1", tg_id)
        if not user:
            return False

        row = await conn.fetchrow("""
            SELECT 1 FROM collection 
            WHERE user_id = $1 AND tmdb_id = $2 AND type = $3
        """, user["user_id"], tmdb_id, type_)

        return bool(row)


async def kb_card(chat_id: int, tmdb_id: int, type_: str, is_genre_search: bool = False, is_trending: bool = False):
    buttons = []
    trailer_url = get_trailer_url(type_, tmdb_id)
    if trailer_url:
        buttons.append([InlineKeyboardButton(text="▶️ Трейлер", url=trailer_url)])

    is_in_collection = await is_in_user_collection(chat_id, tmdb_id, type_)

    if is_in_collection:
        buttons.append([
            InlineKeyboardButton(text="✅ В коллекции", callback_data=f"already_in_collection"),
        ])
    else:
        buttons.append([
            InlineKeyboardButton(text="➕ В коллекцию", callback_data=f"add_{tmdb_id}_{type_}"),
        ])
    buttons.append([InlineKeyboardButton(text="➡️ Следующий", callback_data="next_item")])

    if is_genre_search:
        buttons.append([InlineKeyboardButton(text="⬅️ К жанрам", callback_data=f"back_to_genres_{type_}")])

    if is_trending:
        buttons.append([InlineKeyboardButton(text="⬅️ К трендам", callback_data="trending_menu")])

    buttons.append([InlineKeyboardButton(text="🔍 Меню поиска", callback_data="search_menu")])
    buttons.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="back_to_main")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def kb_collection_item(tmdb_id: int, type_: str, watched: bool = False, liked: bool | None = None,
                       disliked: bool | None = None, is_hidden: bool = False):
    buttons = []
    trailer_url = get_trailer_url(type_, tmdb_id)
    if trailer_url:
        buttons.append([InlineKeyboardButton(text="▶️ Трейлер", url=trailer_url)])

    watched_text = "✅ Просмотрено" if watched else "👀 Отметить просмотр"
    buttons.append([InlineKeyboardButton(text=watched_text, callback_data=f"mark_watched_{tmdb_id}_{type_}")])

    like_text = "👍 Лайк ✅" if liked is True else "👍 Лайк"
    dislike_text = "👎 Дизлайк ✅" if disliked is True else "👎 Дизлайк"

    # НОВАЯ КНОПКА - скрыть оценку от друзей
    hide_text = "🙈 Скрыть от друзей ✅" if is_hidden else "🙈 Скрыть от друзей"

    buttons.append([
        InlineKeyboardButton(text=like_text, callback_data=f"like_{tmdb_id}_{type_}"),
        InlineKeyboardButton(text=dislike_text, callback_data=f"dislike_{tmdb_id}_{type_}")
    ])

    # Показываем кнопку скрытия только если есть оценка
    if liked is True or disliked is True:
        buttons.append([
            InlineKeyboardButton(text=hide_text, callback_data=f"toggle_hide_{tmdb_id}_{type_}")
        ])

    # Добавляем кнопку "Снять оценку", показываем только если была оценка
    if (liked is True) or (disliked is True):
        buttons.append([InlineKeyboardButton(text="🔄 Снять оценку", callback_data=f"reset_rating_{tmdb_id}_{type_}")])

    buttons.append([InlineKeyboardButton(text="❌ Удалить", callback_data=f"remove_{tmdb_id}_{type_}")])
    buttons.append([InlineKeyboardButton(text="⬅️ К коллекции", callback_data="show_collection")])
    buttons.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="back_to_main")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def kb_trending_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎬 Фильмы за неделю", callback_data="trending_movie_week")],
        [InlineKeyboardButton(text="📺 Сериалы за неделю", callback_data="trending_tv_week")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="search_menu")],
    ])


async def filter_watched_items(tg_id: int, items: list, type_: str):
    async with db.acquire() as conn:
        user = await conn.fetchrow("SELECT user_id FROM users WHERE tg_id=$1", tg_id)
        if not user:
            return items
        watched_ids = await conn.fetch(
            "SELECT tmdb_id FROM ratings WHERE user_id=$1 AND type=$2 AND watched = true",
            user["user_id"], type_
        )
        watched_ids = {row["tmdb_id"] for row in watched_ids}
        return [item for item in items if item["id"] not in watched_ids]


async def get_user_likes(tg_id: int):
    async with db.acquire() as conn:
        user = await conn.fetchrow("SELECT user_id FROM users WHERE tg_id=$1", tg_id)
        if not user:
            return []
        rows = await conn.fetch("""
                                SELECT tmdb_id, type
                                FROM ratings
                                WHERE user_id = $1
                                  AND liked = true
                                """, user["user_id"])
        return [{"tmdb_id": row["tmdb_id"], "type": row["type"]} for row in rows]


async def kb_collection(tg_id: int, page: int, total_pages: int):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[])
    collection = await get_collection(tg_id, limit=4, offset=page * 4)

    for item in collection:
        # Используем item['type'] вместо неопределенной переменной type_
        if not await is_banned(item["tmdb_id"], item["type"]):  # ← ИСПРАВЛЕНО ЗДЕСЬ
            keyboard.inline_keyboard.append([
                InlineKeyboardButton(
                    text=f"{item['title']} ({item['year']})",
                    callback_data=f"show_collection_item_{item['tmdb_id']}_{item['type']}"
                )
            ])

    navigation = []
    if page > 0:
        navigation.append(InlineKeyboardButton(text="⬅️ Предыдущая", callback_data=f"collection_page_{page - 1}"))
    if page < total_pages - 1:
        navigation.append(InlineKeyboardButton(text="Следующая ➡️", callback_data=f"collection_page_{page + 1}"))

    if navigation:
        keyboard.inline_keyboard.append(navigation)

    # Добавляем кнопку экспорта в PDF
    keyboard.inline_keyboard.append([
        InlineKeyboardButton(text="📄 Экспорт в PDF", callback_data="export_pdf")
    ])

    keyboard.inline_keyboard.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="back_to_main")])
    return keyboard


def kb_friends_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👥 Мои друзья", callback_data="my_friends")],
        [InlineKeyboardButton(text="➕ Добавить друга", callback_data="add_friend")],
        [InlineKeyboardButton(text="🎯 Рекомендации друзей", callback_data="friends_recommendations")],
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="back_to_main")]
    ])

def kb_friend_profile(friend_tg_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Удалить друга", callback_data=f"remove_friend_{friend_tg_id}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="my_friends")]
    ])

def kb_my_friends(friends_list, page=0, friends_per_page=10):
    keyboard = []

    start_idx = page * friends_per_page
    end_idx = start_idx + friends_per_page
    page_friends = friends_list[start_idx:end_idx]

    for friend in page_friends:
        friend_name = friend['username'] or f"Пользователь {friend['tg_id']}"
        keyboard.append([
            InlineKeyboardButton(text=f"👤 {friend_name}", callback_data=f"friend_{friend['tg_id']}")
        ])

    # Пагинация
    nav_buttons = []
    total_pages = (len(friends_list) + friends_per_page - 1) // friends_per_page

    if page > 0:
        nav_buttons.append(InlineKeyboardButton(text="⬅️", callback_data=f"friends_page_{page - 1}"))

    nav_buttons.append(InlineKeyboardButton(text=f"{page + 1}/{total_pages}", callback_data="friends_info"))

    if page < total_pages - 1:
        nav_buttons.append(InlineKeyboardButton(text="➡️", callback_data=f"friends_page_{page + 1}"))

    if nav_buttons:
        keyboard.append(nav_buttons)

    keyboard.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="friends_menu")])

    return InlineKeyboardMarkup(inline_keyboard=keyboard)

async def remove_friend(user_tg_id: int, friend_tg_id: int):
    """Удаляет друга"""
    async with db.acquire() as conn:
        user = await conn.fetchrow("SELECT user_id FROM users WHERE tg_id=$1", user_tg_id)
        friend = await conn.fetchrow("SELECT user_id FROM users WHERE tg_id=$1", friend_tg_id)

        if not user or not friend:
            return False

        # Удаляем взаимную дружбу
        await conn.execute("""
            DELETE FROM user_friends 
            WHERE (user_id = $1 AND friend_user_id = $2) 
               OR (user_id = $2 AND friend_user_id = $1)
        """, user["user_id"], friend["user_id"])

        return True

def is_admin(chat_id: int) -> bool:
    # Здесь можешь добавить проверку по ID админов
    admin_ids = [950764975]  # Замени на реальные ID админов
    return chat_id in admin_ids


async def generate_stats_pdf(stats_data: dict, sort_by: str):
    """Генерирует PDF со статистикой с эмодзи"""
    has_russian_font = register_russian_font()

    buffer = io.BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4

    if has_russian_font:
        font_normal = "RussianFont"
        font_bold = "RussianFont"
    else:
        font_normal = "Helvetica"
        font_bold = "Helvetica-Bold"

    # Заголовок
    pdf.setFont(font_bold, 16)
    pdf.drawString(50, height - 50, "Статистика контента")
    pdf.setFont(font_normal, 10)

    sort_descriptions = {
        "updated": "по дате обновления",
        "likes": "по лайкам",
        "dislikes": "по дизлайкам",
        "watches": "по просмотрам"
    }
    pdf.drawString(50, height - 70, f"Сортировка: {sort_descriptions.get(sort_by, 'по дате')}")
    pdf.drawString(50, height - 85, f"Всего: {stats_data['total_count']} элементов")
    pdf.drawString(50, height - 100, f"Дата генерации: {datetime.now().strftime('%d.%m.%Y %H:%M')}")

    y_position = height - 130

    for i, item in enumerate(stats_data["items"]):
        if y_position < 120:
            pdf.showPage()
            y_position = height - 50

        item_height = 90
        center_line = y_position - (item_height / 2)

        # ПОСТЕР
        poster_width = 60
        poster_height = 80
        poster_x = width - 80
        poster_y = center_line - (poster_height / 2)

        if item.get('tmdb_id'):
            try:
                details = get_item_details(item['type'], item['tmdb_id'])
                if details and details.get('poster_path') and details['poster_path'] != "/default.jpg":
                    poster_url = f"https://image.tmdb.org/t/p/w154{details['poster_path']}"
                    response = requests.get(poster_url, timeout=10)
                    if response.status_code == 200:
                        img_data = io.BytesIO(response.content)
                        img_reader = ImageReader(img_data)
                        pdf.drawImage(img_reader, poster_x, poster_y,
                                      width=poster_width, height=poster_height,
                                      preserveAspectRatio=True, mask='auto')
            except Exception as e:
                print(f"Poster loading error: {e}")

        # ТЕКСТ
        text_start_y = center_line + 20

        # Название
        pdf.setFont(font_bold, 12)
        title = item['title'] or "Без названия"
        if len(title) > 40:
            title = title[:37] + "..."
        pdf.drawString(50, text_start_y, title)

        # Тип и ID
        pdf.setFont(font_normal, 10)
        type_text = "Фильм" if item['type'] == 'movie' else "Сериал"
        pdf.drawString(50, text_start_y - 15, f"{type_text}, ID: {item['tmdb_id']}")

        # Статистика с эмодзи - используем обычный текст
        # PDF нормально отображает базовые эмодзи
        stats_text = f"Лайки: {item['likes']}   Дизлайки: {item['dislikes']}   Просмотры: {item['watches']}"
        pdf.drawString(50, text_start_y - 30, stats_text)

        y_position -= item_height

        # Разделитель
        if i < len(stats_data["items"]) - 1:
            pdf.line(50, y_position + 5, width - 50, y_position + 5)
            y_position -= 10

    pdf.save()
    buffer.seek(0)
    return buffer


async def generate_stats_charts_pdf(stats_data: dict):
    """Генерирует PDF с диаграммами статистики (2 диаграммы на страницу)"""
    try:
        import matplotlib.pyplot as plt
        from matplotlib.ticker import MaxNLocator
        import numpy as np
    except ImportError:
        return None

    try:
        items = stats_data["items"]

        # Создаем буфер для всех диаграмм
        chart_buffers = []

        # 1. ПЕРВАЯ СТРАНИЦА: Топ-5 по лайкам и просмотрам
        fig1, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 12))

        # 1.1 Топ-5 по лайкам
        top_likes = sorted(items, key=lambda x: x['likes'], reverse=True)[:5]
        titles_likes = [item['title'][:20] + "..." if len(item['title']) > 20 else item['title'] for item in top_likes]
        likes = [item['likes'] for item in top_likes]

        bars1 = ax1.barh(titles_likes, likes, color=['#4CAF50', '#66BB6A', '#81C784', '#A5D6A7', '#C8E6C9'])
        ax1.xaxis.set_major_locator(MaxNLocator(integer=True))
        ax1.set_title('Топ-5 по лайкам', fontsize=14, fontweight='bold', pad=20)
        ax1.set_xlabel('Количество лайков', fontsize=12)
        ax1.tick_params(axis='y', labelsize=10)

        # Добавляем значения на столбцы
        for i, v in enumerate(likes):
            ax1.text(v + max(likes) * 0.01, i, f"{int(v)}", va='center', fontsize=10, fontweight='bold')

        # 1.2 Топ-5 по просмотрам
        top_watches = sorted(items, key=lambda x: x['watches'], reverse=True)[:5]
        titles_watches = [item['title'][:20] + "..." if len(item['title']) > 20 else item['title'] for item in
                          top_watches]
        watches = [item['watches'] for item in top_watches]

        bars2 = ax2.barh(titles_watches, watches, color=['#2196F3', '#42A5F5', '#64B5F6', '#90CAF9', '#BBDEFB'])
        ax2.xaxis.set_major_locator(MaxNLocator(integer=True))
        ax2.set_title('Топ-5 по просмотрам', fontsize=14, fontweight='bold', pad=20)
        ax2.set_xlabel('Количество просмотров', fontsize=12)
        ax2.tick_params(axis='y', labelsize=10)

        # Добавляем значения на столбцы
        for i, v in enumerate(watches):
            ax2.text(v + max(watches) * 0.01, i, f"{int(v)}", va='center', fontsize=10, fontweight='bold')

        plt.tight_layout(pad=4.0)

        # Сохраняем первую страницу
        buffer1 = io.BytesIO()
        plt.savefig(buffer1, format='png', dpi=150, bbox_inches='tight')
        plt.close(fig1)
        buffer1.seek(0)
        chart_buffers.append(buffer1)

        # 2. ВТОРАЯ СТРАНИЦА: Топ-5 по дизлайкам и соотношение фильмов/сериалов
        fig2, (ax3, ax4) = plt.subplots(2, 1, figsize=(10, 12))

        # 2.1 Топ-5 по дизлайкам
        top_dislikes = sorted(items, key=lambda x: x['dislikes'], reverse=True)[:5]
        titles_dislikes = [item['title'][:20] + "..." if len(item['title']) > 20 else item['title'] for item in
                           top_dislikes]
        dislikes = [item['dislikes'] for item in top_dislikes]

        bars3 = ax3.barh(titles_dislikes, dislikes, color=['#F44336', '#EF5350', '#E57373', '#EF9A9A', '#FFCDD2'])
        ax3.xaxis.set_major_locator(MaxNLocator(integer=True))
        ax3.set_title('Топ-5 по дизлайкам', fontsize=14, fontweight='bold', pad=20)
        ax3.set_xlabel('Количество дизлайков', fontsize=12)
        ax3.tick_params(axis='y', labelsize=10)

        # Добавляем значения на столбцы
        for i, v in enumerate(dislikes):
            ax3.text(v + max(dislikes) * 0.01, i, f"{int(v)}", va='center', fontsize=10, fontweight='bold')

        # 2.2 Соотношение фильмов и сериалов (круговая диаграмма)
        movie_count = sum(1 for item in items if item['type'] == 'movie')
        tv_count = sum(1 for item in items if item['type'] == 'tv')

        sizes = [movie_count, tv_count]
        labels = ['Фильмы', 'Сериалы']
        colors = ['#FF9800', '#9C27B0']
        explode = (0.05, 0.05)  # Немного выдвигаем сектора

        # Если есть данные для круговой диаграммы
        if movie_count > 0 or tv_count > 0:
            wedges, texts, autotexts = ax4.pie(sizes, explode=explode, labels=labels, colors=colors,
                                               autopct=lambda p: f'{int(round(p))}%', shadow=True, startangle=90,
                                               textprops={'fontsize': 12})

            # Делаем проценты жирными
            for autotext in autotexts:
                autotext.set_color('white')
                autotext.set_fontweight('bold')
                autotext.set_fontsize(11)

            ax4.set_title('Соотношение фильмов и сериалов', fontsize=14, fontweight='bold', pad=20)

            # Добавляем легенду с количеством
            legend_labels = [f'{label}: {size}' for label, size in zip(labels, sizes)]
            ax4.legend(wedges, legend_labels, title="Количество", loc="center left",
                       bbox_to_anchor=(0.9, 0, 0.5, 1), fontsize=10)
        else:
            # Если нет данных
            ax4.text(0.5, 0.5, 'Нет данных\nо типах контента',
                     horizontalalignment='center', verticalalignment='center',
                     transform=ax4.transAxes, fontsize=14, fontweight='bold')
            ax4.set_title('Соотношение фильмов и сериалов', fontsize=14, fontweight='bold', pad=20)

        plt.tight_layout(pad=4.0)

        # Сохраняем вторую страницу
        buffer2 = io.BytesIO()
        plt.savefig(buffer2, format='png', dpi=150, bbox_inches='tight')
        plt.close(fig2)
        buffer2.seek(0)
        chart_buffers.append(buffer2)

        # 3. ТРЕТЬЯ СТРАНИЦА: Общая статистика и соотношение лайков/дизлайков
        fig3, (ax5, ax6) = plt.subplots(2, 1, figsize=(10, 12))

        # 3.1 Общая статистика в виде таблицы
        ax5.axis('off')

        total_likes = sum(item['likes'] for item in items)
        total_dislikes = sum(item['dislikes'] for item in items)
        total_watches = sum(item['watches'] for item in items)
        total_items = len(items)

        # Создаем красивую таблицу с общей статистикой
        stats_data_table = [
            ['  ОБЩАЯ СТАТИСТИКА', ''],
            ['Всего записей:', f'{total_items}'],
            ['Фильмы:', f'{movie_count}'],
            ['Сериалы:', f'{tv_count}'],
            ['Всего лайков:', f'{total_likes}'],
            ['Всего дизлайков:', f'{total_dislikes}'],
            ['Всего просмотров:', f'{total_watches}'],
        ]

        # Создаем таблицу
        table = ax5.table(cellText=stats_data_table,
                          cellLoc='left',
                          loc='center',
                          bbox=[0.1, 0.2, 0.8, 0.6])

        table.auto_set_font_size(False)
        table.set_fontsize(11)
        table.scale(1, 2)

        # Стилизуем заголовок таблицы
        for i in range(2):
            table[(0, i)].set_facecolor('#4CAF50')
            table[(0, i)].set_text_props(weight='bold', color='white')

        # Стилизуем остальные ячейки
        for i in range(1, len(stats_data_table)):
            for j in range(2):
                if i % 2 == 0:
                    table[(i, j)].set_facecolor('#f5f5f5')

        ax5.set_title('Общая статистика базы данных', fontsize=16, fontweight='bold', pad=30)

        # 3.2 Соотношение лайков и дизлайков (круговая диаграмма)
        if total_likes > 0 or total_dislikes > 0:
            sizes_likes = [total_likes, total_dislikes]
            labels_likes = ['Лайки', 'Дизлайки']
            colors_likes = ['#4CAF50', '#F44336']

            wedges2, texts2, autotexts2 = ax6.pie(sizes_likes, labels=labels_likes, colors=colors_likes,
                                                  autopct=lambda p: f'{int(round(p))}%', shadow=True, startangle=90,
                                                  textprops={'fontsize': 12})

            # Делаем проценты жирными
            for autotext in autotexts2:
                autotext.set_color('white')
                autotext.set_fontweight('bold')
                autotext.set_fontsize(11)

            ax6.set_title('Соотношение лайков и дизлайков', fontsize=14, fontweight='bold', pad=20)

            # Добавляем легенду с количеством
            legend_labels2 = [f'{label}: {size}' for label, size in zip(labels_likes, sizes_likes)]
            ax6.legend(wedges2, legend_labels2, title="Количество", loc="center left",
                       bbox_to_anchor=(0.9, 0, 0.5, 1), fontsize=10)
        else:
            ax6.text(0.5, 0.5, 'Нет данных\nо реакциях',
                     horizontalalignment='center', verticalalignment='center',
                     transform=ax6.transAxes, fontsize=14, fontweight='bold')
            ax6.set_title('Соотношение лайков и дизлайков', fontsize=14, fontweight='bold', pad=20)

        plt.tight_layout(pad=4.0)

        # Сохраняем третью страницу
        buffer3 = io.BytesIO()
        plt.savefig(buffer3, format='png', dpi=150, bbox_inches='tight')
        plt.close(fig3)
        buffer3.seek(0)
        chart_buffers.append(buffer3)

        # Создаем PDF с несколькими страницами
        has_russian_font = register_russian_font()
        pdf_buffer = io.BytesIO()

        if has_russian_font:
            font_normal = "RussianFont"
            font_bold = "RussianFont"
        else:
            font_normal = "Helvetica"
            font_bold = "Helvetica-Bold"

        pdf = canvas.Canvas(pdf_buffer, pagesize=A4)
        width, height = A4

        for i, chart_buffer in enumerate(chart_buffers):
            # Заголовок
            pdf.setFont(font_bold, 16)
            pdf.drawString(50, height - 50, "Диаграммы статистики")
            pdf.setFont(font_normal, 10)
            pdf.drawString(50, height - 70, f"Всего записей: {stats_data['total_count']}")
            pdf.drawString(350, height - 70, f"Страница {i + 1}/{len(chart_buffers)}")
            pdf.drawString(450, height - 70, f"Дата: {datetime.now().strftime('%d.%m.%Y %H:%M')}")

            # Получаем изображение диаграммы
            img = ImageReader(chart_buffer)
            img_width, img_height = img.getSize()

            # Масштабируем, чтобы не сплющить
            max_width = width * 0.85
            max_height = height * 0.7
            scale = min(max_width / img_width, max_height / img_height)
            new_width = img_width * scale
            new_height = img_height * scale

            # Центрируем
            x = (width - new_width) / 2
            y = height - new_height - 120
            pdf.drawImage(img, x, y, width=new_width, height=new_height)

            # Если не последняя страница — новая
            if i < len(chart_buffers) - 1:
                pdf.showPage()

        pdf.save()
        pdf_buffer.seek(0)

        # Закрываем все буферы
        for buffer in chart_buffers:
            buffer.close()

        return pdf_buffer

    except Exception as e:
        print(f"Error generating charts PDF: {e}")
        return None


def register_russian_font():
    try:
        # Пробуем найти русский шрифт в системе
        font_paths = [
            # Windows
            'C:/Windows/Fonts/arial.ttf',
            'C:/Windows/Fonts/times.ttf',
            # Linux
            '/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf',
            '/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf',
            # macOS
            '/Library/Fonts/Arial.ttf',
            '/System/Library/Fonts/Arial.ttf'
        ]

        for font_path in font_paths:
            if os.path.exists(font_path):
                pdfmetrics.registerFont(TTFont('RussianFont', font_path))
                return True
    except Exception as e:
        print(f"Font registration error: {e}")

    return False


def get_recommendations(type_: str, tmdb_id: int):
    url = f"https://api.themoviedb.org/3/{type_}/{tmdb_id}/recommendations"
    r = tmdb_get(url, {"language": "ru-RU", "page": 1})
    if r.status_code == 200:
        return r.json().get("results", [])
    return []


def kb_genres(type_: str):
    genres = GENRES_MOVIE if type_ == "movie" else GENRES_TV
    keyboard = []
    row = []
    for title, gid in genres.items():
        row.append(InlineKeyboardButton(text=title, callback_data=f"genre_{type_}_{gid}"))
        if len(row) == 3:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    keyboard.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="back_to_main")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def search_by_title(title: str, type_: str = None, page: int = 1):
    """Поиск контента по названию через TMDB API с проверкой банов"""
    url = "https://api.themoviedb.org/3/search/multi"
    params = {
        "query": title,
        "language": "ru-RU",
        "page": page,
        "include_adult": "false"
    }

    r = tmdb_get(url, params)
    if r.status_code == 200:
        data = r.json()
        results = data.get("results", [])
        total_pages = data.get("total_pages", 1)

        # Если нужно больше результатов, получаем дополнительные страницы
        if total_pages > 1 and page == 1:
            # Ограничим максимум 3 страницы (60 результатов)
            max_pages = min(total_pages, 3)
            for next_page in range(2, max_pages + 1):
                try:
                    params["page"] = next_page
                    next_r = tmdb_get(url, params)
                    if next_r.status_code == 200:
                        next_data = next_r.json()
                        results.extend(next_data.get("results", []))
                except Exception as e:
                    print(f"Error fetching page {next_page}: {e}")
                    break

        # Фильтруем по типу если указан
        if type_:
            results = [item for item in results if item.get("media_type") == type_]

        return results
    return []


def search_by_person(name: str):
    """Поиск актеров/режиссеров через TMDB API"""
    url = "https://api.themoviedb.org/3/search/person"
    params = {
        "query": name,
        "language": "ru-RU",
        "page": 1,
        "include_adult": "false"
    }

    print(f"DEBUG: Searching for person: {name}")  # Отладка

    r = tmdb_get(url, params)
    if r.status_code == 200:
        data = r.json()
        results = data.get("results", [])

        print(f"DEBUG: Found {len(results)} persons")  # Отладка

        # Получаем дополнительные страницы если есть
        total_pages = data.get("total_pages", 1)
        if total_pages > 1:
            max_pages = min(total_pages, 3)
            for next_page in range(2, max_pages + 1):
                try:
                    params["page"] = next_page
                    next_r = tmdb_get(url, params)
                    if next_r.status_code == 200:
                        next_data = next_r.json()
                        results.extend(next_data.get("results", []))
                except Exception as e:
                    print(f"Error fetching person page {next_page}: {e}")
                    break

        print(f"DEBUG: Total persons after pagination: {len(results)}")  # Отладка
        return results

    print(f"DEBUG: TMDB API error: {r.status_code}")  # Отладка
    return []


async def get_person_filmography(person_id: int):
    """Получает фильмографию актера с режиссерскими работами и умной фильтрацией"""
    url = f"https://api.themoviedb.org/3/person/{person_id}/combined_credits"
    params = {
        "language": "ru-RU"
    }

    print(f"DEBUG: Getting filmography for person_id: {person_id}")

    r = tmdb_get(url, params)
    if r.status_code == 200:
        data = r.json()
        cast = data.get("cast", [])
        crew = data.get("crew", [])

        print(f"DEBUG: Raw cast count: {len(cast)}, crew count: {len(crew)}")

        # Жанры для исключения (телешоу, ток-шоу, новости, реалити)
        EXCLUDED_GENRES = {10764, 10767, 10763, 10764}  # Reality, Talk, News, Reality

        # Ключевые слова в названиях для исключения
        EXCLUDED_KEYWORDS = [
            # Телешоу и программы
            "show", "шоу", "поздней ночью", "night show", "tonight show", "late night",
            "утро", "morning", "вечер", "evening", "talk", "ток-шоу", "интервью",
            "interview", "news", "новости", "wwe", "raw", "snl", "субботним вечером",
            "saturday night live", "джимми", "jimmy", "кimmel", "киммел", "фэллон", "fallon",

            # Церемонии и премии
            "золотой глобус", "golden globe", "церемония вручения премии", "awards",
            "премия", "award", "oscar", "оскар", "grammy", "грэмми", "emmy", "эмми",
            "ceremony", "церемония", "награждение", "red carpet", "красная дорожка",
            "met gala", "мет гала", "bafta", "британская академия", "canne", "канны",
            "venice", "венеция", "berlinale", "берлинале", "sundance", "санденс",
            "mtv movie", "mtv music", "vma", "billboard", "биллборд"
        ]

        filmography_dict = {}

        def should_exclude_item(item):
            """Проверяет, нужно ли исключить элемент из фильмографии"""
            media_type = item.get("media_type")
            title = (item.get("title") or item.get("name") or "").lower()

            # Проверяем по жанрам
            genre_ids = set(item.get("genre_ids", []))
            if genre_ids & EXCLUDED_GENRES:
                return True

            # Проверяем по ключевым словам в названии
            if any(keyword in title for keyword in EXCLUDED_KEYWORDS):
                return True

            return False

        # Обрабатываем актерские работы
        for item in cast:
            media_type = item.get("media_type")

            # Проверяем исключения по жанрам и ключевым словам
            if should_exclude_item(item):
                print(f"DEBUG: Excluding actor item by filter: {item.get('title') or item.get('name')}")
                continue

            # Для сериалов применяем строгую фильтрацию
            if media_type == "tv":
                # ИСКЛЮЧАЕМ сериалы где человек снимался только в 1 эпизоде
                episode_count = item.get("episode_count", 0)
                if episode_count <= 1:
                    print(f"DEBUG: Skipping TV show with only {episode_count} episodes: {item.get('name')}")
                    continue

                # Дополнительная проверка: исключаем эпизодические появления
                character = item.get("character", "").lower()
                if any(keyword in character for keyword in ["himself", "себя", "guest", "эпизод", "cameo", "камео"]):
                    print(f"DEBUG: Skipping guest appearance: {item.get('name')} as {character}")
                    continue

            # Для фильмов берем все роли (даже эпизодические)
            elif media_type == "movie":
                # Все фильмы включаем
                pass
            else:
                continue  # Пропускаем другие типы медиа

            item_id = item.get("id")
            if item_id not in filmography_dict:
                filmography_dict[item_id] = {
                    "id": item_id,
                    "media_type": media_type,
                    "title": item.get("title") or item.get("name"),
                    "release_date": item.get("release_date") or item.get("first_air_date"),
                    "popularity": item.get("popularity", 0),
                    "poster_path": item.get("poster_path"),
                    "episode_count": item.get("episode_count", 0),
                    "character": item.get("character", ""),
                    "genre_ids": item.get("genre_ids", []),
                    "roles": set()
                }

            filmography_dict[item_id]["roles"].add("actor")

        # Обрабатываем режиссерские работы с УМНОЙ ФИЛЬТРАЦИЕЙ
        for item in crew:
            media_type = item.get("media_type")
            job = item.get("job", "").lower()
            department = item.get("department", "").lower()

            # Берем только режиссеров из режиссерского департамента
            if department != "directing":
                continue

            # Только основные режиссерские должности
            if job not in ["director"]:
                continue

            if media_type not in ["movie", "tv"]:
                continue

            # Проверяем исключения по жанрам и ключевым словам
            if should_exclude_item(item):
                print(f"DEBUG: Excluding director item by filter: {item.get('title') or item.get('name')}")
                continue

            # ДОПОЛНИТЕЛЬНАЯ ФИЛЬТРАЦИЯ ДЛЯ РЕЖИССЕРОВ:
            # Для сериалов - только если это не эпизодическая режиссура
            if media_type == "tv":
                # Получаем детали сериала для проверки
                series_details = get_item_details("tv", item.get("id"))
                if series_details:
                    # Проверяем создателей сериала
                    created_by = series_details.get("created_by", [])
                    creator_ids = [creator.get("id") for creator in created_by]

                    # Если человек не создатель и сериал имеет много сезонов - возможно это режиссер эпизода
                    if person_id not in creator_ids:
                        number_of_seasons = series_details.get("number_of_seasons", 0)
                        if number_of_seasons > 3:  # Популярный долгоиграющий сериал
                            print(f"DEBUG: Skipping episode director in popular series: {item.get('name')}")
                            continue

            item_id = item.get("id")
            if item_id not in filmography_dict:
                filmography_dict[item_id] = {
                    "id": item_id,
                    "media_type": media_type,
                    "title": item.get("title") or item.get("name"),
                    "release_date": item.get("release_date") or item.get("first_air_date"),
                    "popularity": item.get("popularity", 0),
                    "poster_path": item.get("poster_path"),
                    "episode_count": item.get("episode_count", 0),
                    "genre_ids": item.get("genre_ids", []),
                    "roles": set()
                }

            filmography_dict[item_id]["roles"].add("director")

        # Преобразуем словарь обратно в список
        filmography = []
        for item_data in filmography_dict.values():
            item_data["person_role"] = list(item_data["roles"])
            filmography.append(item_data)

        print(f"DEBUG: Final filmography count (with directors): {len(filmography)}")

        # Выводим отладочную информацию
        for i, item in enumerate(filmography[:10]):
            title = item.get("title", "No title")
            media_type = item.get("media_type")
            roles = item.get("person_role", [])
            print(f"DEBUG: Filmography item {i}: {title} ({media_type}) - Roles: {roles}")

        # ФИЛЬТРАЦИЯ ЗАБАНЕННОГО КОНТЕНТА
        async def filter_banned_filmography(items):
            filtered_items = []
            for item in items:
                media_type = item.get("media_type")
                if not await is_banned(item["id"], media_type):
                    filtered_items.append(item)
                else:
                    print(f"DEBUG: Excluding banned content from filmography: {item.get('title')} (ID: {item['id']})")
            return filtered_items

        # Применяем фильтрацию банов
        filmography = await filter_banned_filmography(filmography)

        print(f"DEBUG: Final filmography count (with ban filter): {len(filmography)}")

        # Сортируем по популярности
        filmography.sort(key=lambda x: (
            x.get("popularity", 0),
            x.get("release_date") or "0000-00-00"
        ), reverse=True)

        return filmography

    print(f"DEBUG: TMDB API error: {r.status_code}")
    return []  # ВАЖНО: возвращаем пустой список при ошибке

def format_banned_page(banned_list: list, page: int, items_per_page: int = 15):
    """Форматирует страницу списка банов"""
    total_items = len(banned_list)
    total_pages = (total_items + items_per_page - 1) // items_per_page
    start_idx = page * items_per_page
    end_idx = min(start_idx + items_per_page, total_items)

    text = f"📋 Забаненный контент (страница {page + 1}/{total_pages}):\n\n"

    for i in range(start_idx, end_idx):
        item = banned_list[i]
        text += f"• {item['title']} (ID: {item['tmdb_id']}, {item['type']})\n"

    text += f"\nВсего: {total_items}"
    return {"text": text}


def format_stats_page(stats_data: dict, sort_by: str, page: int):
    """Форматирует страницу статистики"""
    items = stats_data["items"]
    total_count = stats_data["total_count"]
    total_pages = stats_data["total_pages"]

    # Описание сортировки
    sort_descriptions = {
        "updated": "🕐 по дате обновления",
        "likes": "👍 по лайкам",
        "dislikes": "👎 по дизлайкам",
        "watches": "👀 по просмотрам"
    }

    text = f"📊 <b>Статистика контента</b>\n"
    text += f"📈 Сортировка: {sort_descriptions.get(sort_by, 'по дате')}\n"
    text += f"📄 Страница: {page + 1}/{total_pages}\n"
    text += f"📋 Всего записей: {total_count}\n\n"

    if not items:
        text += "❌ Нет данных для отображения"
        return text

    for i, item in enumerate(items, start=page * len(items) + 1):
        title = item['title'] or "Без названия"
        media_type = "🎬 Фильм" if item['type'] == 'movie' else '📺 Сериал'

        text += f"<b>{i})</b> \"{title}\" - {media_type}, ID: {item['tmdb_id']}\n"
        text += f"   👍 Лайки: {item['likes']} | 👎 Дизлайки: {item['dislikes']} | 👀 Просмотров: {item['watches']}\n\n"

    return text


def kb_banned_pagination(banned_list: list, page: int, items_per_page: int = 15):
    """Клавиатура с пагинацией для списка банов"""
    total_items = len(banned_list)
    total_pages = (total_items + items_per_page - 1) // items_per_page

    keyboard = []

    # Кнопки пагинации
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"ban_page_{page - 1}"))

    if page < total_pages - 1:
        nav_buttons.append(InlineKeyboardButton(text="Вперед ➡️", callback_data=f"ban_page_{page + 1}"))

    if nav_buttons:
        keyboard.append(nav_buttons)

    keyboard.append([InlineKeyboardButton(text="⬅️ В админ-панель", callback_data="admin_panel")])

    return InlineKeyboardMarkup(inline_keyboard=keyboard)


async def send_banned_page(chat_id: int, banned_list: list, page: int):
    """Отправляет страницу списка банов"""
    await bot.send_message(
        chat_id,
        **format_banned_page(banned_list, page),
        reply_markup=kb_banned_pagination(banned_list, page)
    )


def kb_search_results(results, search_query: str, page: int = 0, results_per_page: int = 10):
    """Клавиатура с результатами поиска с пагинацией"""
    total_results = len(results)
    start_idx = page * results_per_page
    end_idx = start_idx + results_per_page
    page_results = results[start_idx:end_idx]

    keyboard = []

    for item in page_results:
        media_type = item.get("media_type")
        title = item.get("title") or item.get("name")
        year = (item.get("release_date") or item.get("first_air_date") or "")[:4]

        if media_type in ["movie", "tv"]:
            btn_text = f"{'🎬' if media_type == 'movie' else '📺'} {title}"
            if year:
                btn_text += f" ({year})"

            keyboard.append([
                InlineKeyboardButton(
                    text=btn_text,
                    callback_data=f"admin_preban_{item['id']}_{media_type}"
                )
            ])

    # Добавляем пагинацию
    navigation_buttons = []
    if page > 0:
        navigation_buttons.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"admin_search_page_{page - 1}"))

    if end_idx < total_results:
        navigation_buttons.append(InlineKeyboardButton(text="Вперед ➡️", callback_data=f"admin_search_page_{page + 1}"))

    if navigation_buttons:
        keyboard.append(navigation_buttons)

    keyboard.append([InlineKeyboardButton(text="🔍 Новый поиск", callback_data="admin_search_ban")])
    keyboard.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_panel")])

    return InlineKeyboardMarkup(inline_keyboard=keyboard)


async def send_search_results_page(chat_id: int, results: list, search_query: str, page: int,
                                   results_per_page: int = 10):
    total_results = len(results)
    start_idx = page * results_per_page
    end_idx = start_idx + results_per_page
    page_results = results[start_idx:end_idx]

    # СЧИТАЕМ ТОЛЬКО ОТОБРАЖАЕМЫЕ РЕЗУЛЬТАТЫ (movie/tv)
    displayable_results = []
    for item in page_results:
        media_type = item.get("media_type")
        if media_type in ["movie", "tv"]:
            displayable_results.append(item)

    actual_display_count = len(displayable_results)
    total_pages = (actual_display_count + results_per_page - 1) // results_per_page

    text = f"🔍 Найдено {actual_display_count} результатов по запросу: '{search_query}'\nСтраница {page + 1}/{max(total_pages, 1)}\n\nВыберите:"

    keyboard = []

    for item in displayable_results:
        media_type = item.get("media_type")
        title = item.get("title") or item.get("name")
        year = (item.get("release_date") or item.get("first_air_date") or "")[:4]

        btn_text = f"{'🎬' if media_type == 'movie' else '📺'} {title}"
        if year:
            btn_text += f" ({year})"

        keyboard.append([
            InlineKeyboardButton(
                text=btn_text,
                callback_data=f"select_{item['id']}_{media_type}"
            )
        ])

    # Пагинация (если есть больше отображаемых результатов)
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"search_page_{page - 1}"))
    if actual_display_count == results_per_page and (page + 1) * results_per_page < total_results:
        nav_buttons.append(InlineKeyboardButton(text="Вперед ➡️", callback_data=f"search_page_{page + 1}"))

    if nav_buttons:
        keyboard.append(nav_buttons)

    keyboard.append([InlineKeyboardButton(text="🔍 Новый поиск", callback_data="search_by_title")])
    keyboard.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="back_to_main")])

    await bot.send_message(chat_id, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard))

# -------------------- HANDLERS --------------------
@dp.message(Command("search"))
async def search_by_tmdb_id(message: types.Message):
    args = message.text.split()
    if len(args) < 3:
        await message.answer("❌ Укажите тип (movie/tv) и TMDB ID. Пример: /search movie 12345")
        return
    type_, tmdb_id = args[1], args[2]
    try:
        tmdb_id = int(tmdb_id)
    except ValueError:
        await message.answer("❌ Некорректный TMDB ID. Укажите число.")
        return
    if type_ not in ["movie", "tv"]:
        await message.answer("❌ Некорректный тип. Укажите movie или tv.")
        return
    details = get_item_details(type_, tmdb_id)

    if await is_banned(tmdb_id, type_):
        await message.answer("❌ Этот контент заблокирован администратором и недоступен для просмотра.")
        return

    if not details:
        await message.answer("❌ Не удалось найти фильм или сериал с таким TMDB ID.")
        return
    title = details.get("title") or details.get("name") or "Без названия"
    year = (details.get("release_date") or details.get("first_air_date") or "")[:4]
    rating = details.get("vote_average") or "—"
    overview = details.get("overview") or "Описание отсутствует."
    if len(overview) > 2000:
        overview = overview[:2000] + "..."
    poster = f"https://image.tmdb.org/t/p/w500{details.get('poster_path')}" if details.get("poster_path") else None
    avg_ratings = await get_ratings(tmdb_id, type_)
    watched_text = ""
    user_rating = await get_user_rating(message.chat.id, tmdb_id, type_)
    if user_rating and user_rating["watched"]:
        watched_text = "✅ Вы смотрели"
    caption = (
        f"{title} ({year})\n"
        f"Рейтинг: {rating} (TMDB)\n"
        f"👍 Лайки: {avg_ratings['likes']} | 👎 Дизлайки: {avg_ratings['dislikes']} | 👀 Просмотров: {avg_ratings['watches']}\n"
        f"{watched_text}\n\n{overview}"
    )
    if poster:
        await message.answer_photo(photo=poster, caption=caption, reply_markup=kb_card(tmdb_id, type_))
    else:
        await message.answer(text=caption, reply_markup=kb_card(tmdb_id, type_))


@dp.message(Command("unban"))
async def unban_command(message: types.Message):
    if not is_admin(message.chat.id):
        await message.answer("❌ Нет доступа к админ-панели!")
        return

    args = message.text.split()
    if len(args) < 3:
        await message.answer("❌ Укажите тип (movie/tv) и ID. Пример: /unban movie 12345")
        return

    type_ = args[1].lower()
    tmdb_id_str = args[2]

    if type_ not in ["movie", "tv"]:
        await message.answer("❌ Некорректный тип. Укажите movie или tv.")
        return

    try:
        tmdb_id = int(tmdb_id_str)
    except ValueError:
        await message.answer("❌ Некорректный ID. Укажите число.")
        return

    # Проверяем, существует ли такой бан
    if not await is_banned(tmdb_id, type_):
        await message.answer("❌ Этот контент не забанен.")
        return

    # Убираем из бана
    await unban_content(tmdb_id, type_)
    await message.answer(f"✅ Контент {type_} с ID {tmdb_id} разбанен!")

@dp.message(Command("ban"))  # Команда /ban
async def ban_command(message: types.Message):  # Название функции должно быть ban_command
    if not is_admin(message.chat.id):
        await message.answer("❌ Нет доступа к админ-панели!")
        return

    args = message.text.split()
    if len(args) < 3:
        await message.answer("❌ Укажите тип (movie/tv) и ID. Пример: /ban movie 12345")  # Исправил пример
        return

    type_ = args[1].lower()
    tmdb_id_str = args[2]

    if type_ not in ["movie", "tv"]:
        await message.answer("❌ Некорректный тип. Укажите movie или tv.")
        return

    try:
        tmdb_id = int(tmdb_id_str)
    except ValueError:
        await message.answer("❌ Некорректный ID. Укажите число.")
        return

    # Проверяем, не забанен ли уже
    if await is_banned(tmdb_id, type_):
        await message.answer("❌ Этот контент уже забанен.")
        return

    # Получаем название для бана
    details = get_item_details(type_, tmdb_id)
    title = details.get("title") or details.get("name") or "Unknown"

    # Баним контент
    await ban_content(tmdb_id, type_, title, message.chat.id, "Бан через команду")
    await message.answer(f"✅ Контент {type_} с ID {tmdb_id} забанен!")

@dp.message(Command("admin"))
async def admin_command(message: types.Message):
    if not is_admin(message.chat.id):
        await message.answer("❌ Нет доступа к админ-панели!")
        return

    await message.answer("⚙️ Админ-панель:", reply_markup=kb_admin_panel())


@dp.message(Command("start"))
async def start(message: types.Message):
    await get_or_create_user(message.chat.id, message.from_user.username)
    filters = await get_user_filters(message.chat.id)
    user_filters[message.chat.id] = filters

    # Загружаем сохраненные фильтры поиска
    search_filters = await load_search_filters(message.chat.id)
    if message.chat.id not in user_sessions:
        user_sessions[message.chat.id] = {}
    user_sessions[message.chat.id]["filters"] = search_filters

    await message.answer("Выберите, что хотите получить:", reply_markup=kb_main())

@dp.message(Command("myid"))
async def get_my_id(message: types.Message):
    await message.answer(f"🆔 Ваш ID: `{message.chat.id}`\n\nПоделитесь этим ID с друзьями, чтобы они могли добавить вас в друзья!", parse_mode="Markdown")


@dp.message()
async def handle_user_input(message: types.Message):
    chat_id = message.chat.id
    user_input = message.text.strip()

    # Добавь в handle_user_input()
    if chat_id in user_sessions and user_sessions[chat_id].get("waiting_admin_search"):
        if not is_admin(chat_id):
            await message.answer("❌ Нет доступа!")
            return

        user_sessions[chat_id]["waiting_admin_search"] = False

        search_query = user_input
        type_filter = None

        # Парсим тип если указан
        if " movie" in search_query.lower():
            type_filter = "movie"
            search_query = search_query.replace(" movie", "").strip()
        elif " tv" in search_query.lower():
            type_filter = "tv"
            search_query = search_query.replace(" tv", "").strip()

        results = search_by_title(search_query, type_filter)
        user_sessions[chat_id]["search_results"] = results
        user_sessions[chat_id]["search_query"] = search_query

        if not results:
            await message.answer("❌ Ничего не найдено")
            await message.answer("⚙️ Админ-панель:", reply_markup=kb_admin_panel())
            return

        await message.answer(
            f"🔍 Найдено {len(results)} результатов по запросу: '{search_query}'\nСтраница 1",
            reply_markup=kb_search_results(results, search_query, page=0)
        )
        return

    if chat_id in user_sessions and user_sessions[chat_id].get("waiting_title_search"):
        user_sessions[chat_id]["waiting_title_search"] = False

        search_query = user_input
        results = search_by_title(search_query)

        if not results:
            await message.answer("❌ Ничего не найдено")
            await message.answer("Выберите тип поиска:", reply_markup=kb_search_menu())
            return

        # ФИЛЬТРАЦИЯ ЗАБАНЕННОГО КОНТЕНТА
        filtered_results = []
        for item in results:
            media_type = item.get("media_type")
            if media_type in ["movie", "tv"]:
                if not await is_banned(item["id"], media_type):
                    filtered_results.append(item)
            else:
                filtered_results.append(item)  # Для person и других типов не проверяем бан

        if not filtered_results:
            await message.answer("❌ Все найденные результаты заблокированы администратором")
            await message.answer("Выберите тип поиска:", reply_markup=kb_search_menu())
            return

        user_sessions[chat_id]["search_results"] = filtered_results
        user_sessions[chat_id]["search_query"] = search_query
        user_sessions[chat_id]["search_page"] = 0

        await send_search_results_page(chat_id, filtered_results, search_query, 0)
        return

    if chat_id in user_sessions and user_sessions[chat_id].get("waiting_friend_id"):
        user_sessions[chat_id]["waiting_friend_id"] = False

        try:
            friend_tg_id = int(user_input)
            if friend_tg_id == chat_id:
                await message.answer("❌ Нельзя добавить самого себя в друзья!")
            else:
                result = await send_friend_request(chat_id, friend_tg_id)
                if result is True:
                    await message.answer("✅ Заявка в друзья отправлена!")
                    # Уведомляем друга
                    try:
                        await bot.send_message(
                            friend_tg_id,
                            f"👋 Вам пришла заявка в друзья от пользователя!",
                            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                [InlineKeyboardButton(text="📨 Посмотреть заявки", callback_data="friend_requests")],
                            ])
                        )
                    except:
                        pass  # Если бот не может написать пользователю
                elif result == "already_sent":
                    await message.answer("❌ Вы уже отправили заявку этому пользователю")
                else:
                    await message.answer("❌ Пользователь не найден")

        except ValueError:
            await message.answer("❌ Введите корректный числовой ID")

        await message.answer("👥 Система друзей:", reply_markup=kb_friends_menu())
        return

    # Обработка контактов
    if message.contact:
        contact = message.contact
        # Здесь можно обработать контакт, если нужно
        await message.answer("📱 Функция контактов в разработке. Используйте ID для добавления друзей.")
        return

    # Обработка ввода страны
    if chat_id in user_input_waiting and user_input_waiting[chat_id].get("waiting_country"):
        # Удаляем сообщение с инструкцией
        try:
            await bot.delete_message(chat_id, user_input_waiting[chat_id]["message_id"])
        except:
            pass

        if user_input.lower() == 'any':
            if chat_id in user_sessions and "filters" in user_sessions[chat_id]:
                user_sessions[chat_id]["filters"]["country"] = None
                await save_search_filters(chat_id, user_sessions[chat_id]["filters"])
            await message.answer("✅ Фильтр страны убран")
        else:
            if chat_id not in user_sessions:
                user_sessions[chat_id] = {}
            if "filters" not in user_sessions[chat_id]:
                user_sessions[chat_id]["filters"] = {}

            user_sessions[chat_id]["filters"]["country"] = user_input.upper()
            await save_search_filters(chat_id, user_sessions[chat_id]["filters"])
            await message.answer(f"✅ Страна установлена: {user_input}")

        user_input_waiting[chat_id]["waiting_country"] = False
        current_filters = user_sessions.get(chat_id, {}).get("filters", {})
        await message.answer("Настройте фильтры поиска:", reply_markup=kb_filters_menu(current_filters))
        return

    # Обработка ввода года/диапазона
    if chat_id in user_input_waiting and user_input_waiting[chat_id].get("waiting_year"):
        # Удаляем сообщение с инструкцией
        try:
            await bot.delete_message(chat_id, user_input_waiting[chat_id]["message_id"])
        except:
            pass

        if user_input.lower() == 'any':
            # Убираем фильтр года
            if chat_id in user_sessions and "filters" in user_sessions[chat_id]:
                user_sessions[chat_id]["filters"]["year"] = None
                user_sessions[chat_id]["filters"]["start_year"] = None
                user_sessions[chat_id]["filters"]["end_year"] = None
                await save_search_filters(chat_id, user_sessions[chat_id]["filters"])
            await message.answer("✅ Фильтр года убран")
        else:
            # Парсим ввод
            current_year = 2025

            try:
                if '-' in user_input:
                    parts = user_input.split('-')
                    if len(parts) == 2:
                        start_part = parts[0].strip()
                        end_part = parts[1].strip()

                        if start_part and end_part:  # Диапазон: 2010-2020
                            start_year = int(start_part)
                            end_year = int(end_part)
                            if 1920 <= start_year <= current_year and 1920 <= end_year <= current_year and start_year <= end_year:
                                user_sessions[chat_id]["filters"]["start_year"] = start_year
                                user_sessions[chat_id]["filters"]["end_year"] = end_year
                                await save_search_filters(chat_id, user_sessions[chat_id]["filters"])
                                await message.answer(f"✅ Диапазон установлен: {start_year}-{end_year}")
                            else:
                                await message.answer("❌ Некорректный диапазон. Используйте года от 1920 до 2024")
                                return

                        elif start_part and not end_part:  # От года: 2010-
                            start_year = int(start_part)
                            if 1920 <= start_year <= current_year:
                                user_sessions[chat_id]["filters"]["start_year"] = start_year
                                user_sessions[chat_id]["filters"]["end_year"] = current_year
                                await save_search_filters(chat_id, user_sessions[chat_id]["filters"])
                                await message.answer(f"✅ Установлено: с {start_year} года")
                            else:
                                await message.answer("❌ Некорректный год. Используйте года от 1920 до 2024")
                                return

                        elif not start_part and end_part:  # До года: -2020
                            end_year = int(end_part)
                            if 1920 <= end_year <= current_year:
                                user_sessions[chat_id]["filters"]["start_year"] = 1920
                                user_sessions[chat_id]["filters"]["end_year"] = end_year
                                await save_search_filters(chat_id, user_sessions[chat_id]["filters"])
                                await message.answer(f"✅ Установлено: до {end_year} года")
                            else:
                                await message.answer("❌ Некорректный год. Используйте года от 1920 до 2024")
                                return
                        else:
                            await message.answer("❌ Некорректный формат. Примеры: 2010, 2010-2020, 2010-, -2020")
                            return
                    else:
                        await message.answer("❌ Некорректный формат. Используйте один дефис")
                        return
                else:
                    # Один год: 2010
                    year = int(user_input)
                    if 1920 <= year <= current_year:
                        user_sessions[chat_id]["filters"]["start_year"] = year
                        user_sessions[chat_id]["filters"]["end_year"] = year
                        await save_search_filters(chat_id, user_sessions[chat_id]["filters"])
                        await message.answer(f"✅ Год установлен: {year}")
                    else:
                        await message.answer("❌ Некорректный год. Используйте года от 1920 до 2024")
                        return

            except ValueError:
                await message.answer("❌ Введите числа для года. Примеры: 2010, 2010-2020")
                return

        # Убираем флаг ожидания
        user_input_waiting[chat_id]["waiting_year"] = False

        # Показываем обновленное меню фильтров
        current_filters = user_sessions.get(chat_id, {}).get("filters", {})
        await message.answer("Настройте фильтры поиска:", reply_markup=kb_filters_menu(current_filters))
        return

        # В handle_user_input после других обработчиков поиска добавьте:
    if chat_id in user_sessions and user_sessions[chat_id].get("waiting_person_search"):
        user_sessions[chat_id]["waiting_person_search"] = False

        search_query = user_input
        results = search_by_person(search_query)

        if not results:
            await message.answer("❌ Ничего не найдено")
            await message.answer("Выберите тип поиска:", reply_markup=kb_search_menu())
            return

        # Сохраняем результаты в сессию
        user_sessions[chat_id]["person_results"] = results
        user_sessions[chat_id]["person_query"] = search_query
        user_sessions[chat_id]["person_page"] = 0

        await send_person_results_page(chat_id, results, search_query, 0)
        return


@dp.callback_query()
async def handle_callback(callback: types.CallbackQuery):
    chat_id = callback.message.chat.id
    data = callback.data
    old_msg_id = callback.message.message_id

    # В handle_callback добавьте:
    if data == "already_in_collection":
        await callback.answer("✅ Этот контент уже в вашей коллекции!")
        return

    if data == "delete_message":
        try:
            await bot.delete_message(chat_id, old_msg_id)
        except:
            pass
        await callback.answer("❌ Действие отменено")
        return

    # В handle_callback добавьте:
    if data == "friends_menu":
        await navigate_to_menu(chat_id, old_msg_id, "👥 Система друзей:", kb_friends_menu())
        return

    if data == "my_friends":
        friends = await get_user_friends(chat_id)
        if not friends:
            await callback.answer("❌ У вас пока нет друзей")
            await navigate_to_menu(
                chat_id, old_msg_id,
                "📭 У вас пока нет друзей. Добавьте друзей, чтобы вискать их рекомендации!",
                InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="➕ Добавить друга", callback_data="add_friend")],
                    [InlineKeyboardButton(text="⬅️ Назад", callback_data="friends_menu")]
                ])
            )
        else:
            await navigate_to_menu(
                chat_id, old_msg_id,
                f"👥 Ваши друзья ({len(friends)}):",
                kb_my_friends(friends, 0)
            )
        return

    if data == "friends_recommendations":
        # Используем улучшенную версию с тегами
        recommendations = await get_friends_likes(
            chat_id)  # или get_friends_likes_enhanced если выберете второй вариант
        if not recommendations:
            await callback.answer("❌ Нет рекомендаций от друзей")
            await navigate_to_menu(
                chat_id, old_msg_id,
                "📭 Пока нет рекомендаций от друзей.\n\nДобавьте друзей и попросите их ставить лайки фильмам и сериалам!",
                InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="➕ Добавить друга", callback_data="add_friend")],
                    [InlineKeyboardButton(text="⬅️ Назад", callback_data="friends_menu")]
                ])
            )
            return

        # Сохраняем рекомендации в сессию
        if chat_id not in user_sessions:
            user_sessions[chat_id] = {}

        user_sessions[chat_id]["friends_recommendations"] = recommendations
        user_sessions[chat_id]["friends_rec_index"] = 0

        await send_friend_recommendation_card(chat_id, old_msg_id)  # или send_friend_recommendation_card_enhanced
        return

    if data == "add_friend":
        await navigate_to_menu(
            chat_id, old_msg_id,
            "👥 Чтобы добавить друга:\n\n"
            "1. Попросите друга написать боту\n"
            "2. Попросите друга отправить команду /myid\n"
            "3. Отправьте мне ID вашего друга\n\n"
            "Ваш ID для друзей:",
            InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔢 Узнать мой ID", callback_data="get_my_id")],
                [InlineKeyboardButton(text="🔢 Ввести ID друга", callback_data="input_friend_id")],
                [InlineKeyboardButton(text="📨 Мои заявки", callback_data="friend_requests")],
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="friends_menu")]
            ])
        )
        return

    if data == "friend_requests":
        requests = await get_pending_friend_requests(chat_id)
        if not requests:
            await callback.answer("❌ Нет входящих заявок")
            await navigate_to_menu(
                chat_id, old_msg_id,
                "📭 Нет входящих заявок в друзья",
                InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="⬅️ Назад", callback_data="friends_menu")]
                ])
            )
            return

        text = "📨 Входящие заявки в друзья:\n\n"
        keyboard = []

        for req in requests:
            username = req['username'] or f"Пользователь {req['tg_id']}"
            text += f"👤 {username}\n"
            keyboard.append([
                InlineKeyboardButton(text=f"✅ Принять {username}", callback_data=f"accept_request_{req['request_id']}"),
                InlineKeyboardButton(text=f"❌ Отклонить", callback_data=f"reject_request_{req['request_id']}")
            ])

        keyboard.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="friends_menu")])

        await navigate_to_menu(
            chat_id, old_msg_id,
            text,
            InlineKeyboardMarkup(inline_keyboard=keyboard)
        )
        return

    if data.startswith("accept_request_"):
        request_id = int(data.split("_")[2])
        result = await accept_friend_request(request_id)

        if result:
            await callback.answer("✅ Заявка принята!")
            # Обновляем меню
            await navigate_to_menu(
                chat_id, old_msg_id,
                "✅ Заявка в друзья принята!",
                InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="👥 К друзьям", callback_data="friends_menu")]
                ])
            )
        else:
            await callback.answer("❌ Ошибка при принятии заявки")
        return

    if data.startswith("reject_request_"):
        request_id = int(data.split("_")[2])
        # Просто удаляем заявку
        async with db.acquire() as conn:
            await conn.execute("DELETE FROM friend_requests WHERE request_id = $1", request_id)

        await callback.answer("❌ Заявка отклонена")
        await navigate_to_menu(
            chat_id, old_msg_id,
            "❌ Заявка в друзья отклонена",
            InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="👥 К друзьям", callback_data="friends_menu")]
            ])
        )
        return



    # Добавь этот обработчик
    if data == "get_my_id":
        await callback.answer(f"🆔 Ваш ID: {chat_id}\n\nПоделитесь этим ID с друзьями!", show_alert=True)
        return

    if data == "input_friend_id":
        if chat_id not in user_sessions:
            user_sessions[chat_id] = {}
        user_sessions[chat_id]["waiting_friend_id"] = True

        await callback.message.edit_text(  # ← ИСПРАВЛЕНО: edit_text вместо answer
            "🔢 Введите ID вашего друга:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="❌ Отмена", callback_data="friends_menu")]
            ])
        )
        return

    if data == "next_friend_rec":
        await send_friend_recommendation_card(chat_id, old_msg_id)
        return

    if data == "search_filters":
        # Инициализируем фильтры если их нет
        if chat_id not in user_sessions:
            user_sessions[chat_id] = {}
        if "filters" not in user_sessions[chat_id]:
            # Пробуем загрузить сохраненные фильтры
            saved_filters = await load_search_filters(chat_id)
            user_sessions[chat_id]["filters"] = saved_filters

        current_filters = user_sessions[chat_id]["filters"]
        await navigate_to_menu(chat_id, old_msg_id, "Настройте фильтры поиска:", kb_filters_menu(current_filters))
        return

    if data == "search_by_title":
        # Сохраняем состояние ожидания ввода
        if chat_id not in user_sessions:
            user_sessions[chat_id] = {}
        user_sessions[chat_id]["waiting_title_search"] = True

        await navigate_to_menu(
            chat_id,
            old_msg_id,
            "🔍 Введите название фильма или сериала для поиска:",
            InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="search_menu")]
            ])
        )
        return

    # В handle_callback добавьте:
    if data == "search_by_person":
        # Сохраняем состояние ожидания ввода
        if chat_id not in user_sessions:
            user_sessions[chat_id] = {}
        user_sessions[chat_id]["waiting_person_search"] = True

        await navigate_to_menu(
            chat_id,
            old_msg_id,
            "🎭 Введите имя актера или режиссера:",
            InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="search_menu")]
            ])
        )
        return



    if data == "admin_ban_list":
        await callback.answer()
        if not is_admin(chat_id):
            await callback.answer("❌ Нет доступа!")
            return

        try:
            await bot.delete_message(chat_id, old_msg_id)
        except:
            pass

        banned_list = await get_banned_list(100)  # увеличиваем лимит
        if not banned_list:
            await callback.message.answer(
                "📭 Список банов пуст",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_panel")]
                ])
            )
            return

        # Отправляем первую страницу
        await send_banned_page(chat_id, banned_list, 0)
        return

    # Добавляем обработчик для страниц
    if data.startswith("ban_page_"):
        await callback.answer()
        if not is_admin(chat_id):
            await callback.answer("❌ Нет доступа!")
            return

        page = int(data.split("_")[2])
        banned_list = await get_banned_list(100)

        if not banned_list:
            await callback.answer("❌ Список банов пуст")
            return

        await callback.message.edit_text(
            **format_banned_page(banned_list, page),
            reply_markup=kb_banned_pagination(banned_list, page)
        )
        return

    if data.startswith("admin_search_page_"):
        await callback.answer()
        if not is_admin(chat_id):
            await callback.answer("❌ Нет доступа!")
            return

        page = int(data.split("_")[3])

        # Получаем сохраненные результаты поиска
        if "search_results" not in user_sessions.get(chat_id, {}):
            await callback.answer("❌ Результаты поиска устарели")
            return

        search_results = user_sessions[chat_id]["search_results"]
        search_query = user_sessions[chat_id].get("search_query", "")

        await callback.message.edit_text(
            f"🔍 Найдено {len(search_results)} результатов по запросу: '{search_query}'\nСтраница {page + 1}",
            reply_markup=kb_search_results(search_results, search_query, page=page)
        )
        return



    if data.startswith("filmography_page_"):
        page = int(data.split("_")[2])

        if "filmography" not in user_sessions.get(chat_id, {}):
            await callback.answer("❌ Фильмография не найдена")
            return

        filmography = user_sessions[chat_id]["filmography"]
        person_name = user_sessions[chat_id].get("filmography_person_name", "Актер")

        # Обновляем страницу в сессии
        user_sessions[chat_id]["filmography_page"] = page

        text, keyboard = await send_person_filmography_page(
            chat_id, filmography, person_name, page
        )

        await callback.message.edit_text(text, reply_markup=keyboard)
        return

    # В handle_callback добавьте:
    if data.startswith("select_person_"):
        person_id = int(data.split("_")[2])

        # Получаем информацию об актере для отображения имени
        person_results = user_sessions[chat_id]["person_results"]
        person_info = next((p for p in person_results if p["id"] == person_id), None)
        person_name = person_info.get("name", "Актер") if person_info else "Актер"

        # Получаем фильмографию актера
        filmography = await get_person_filmography(person_id)  # ДОБАВЬТЕ await

        if not filmography:
            await callback.answer("❌ Не удалось загрузить фильмографию или все работы заблокированы")
            return

        # Сохраняем фильмографию в сессию
        user_sessions[chat_id]["filmography"] = filmography
        user_sessions[chat_id]["filmography_person_name"] = person_name
        user_sessions[chat_id]["filmography_page"] = 0
        user_sessions[chat_id]["filmography_person_id"] = person_id

        # Отправляем первую страницу фильмографии
        text, keyboard = await send_person_filmography_page(
            chat_id, filmography, person_name, 0
        )

        try:
            await bot.delete_message(chat_id, old_msg_id)
        except:
            pass

        await bot.send_message(chat_id, text, reply_markup=keyboard)
        return

    # Обработчик пагинации для поиска актеров
    # В handle_callback добавьте обработчик пагинации:
    if data.startswith("person_page_"):
        page = int(data.split("_")[2])

        if "person_results" not in user_sessions.get(chat_id, {}):
            await callback.answer("❌ Результаты поиска устарели")
            return

        person_results = user_sessions[chat_id]["person_results"]
        person_query = user_sessions[chat_id].get("person_query", "")

        # Обновляем страницу в сессии
        user_sessions[chat_id]["person_page"] = page

        await send_person_results_page(chat_id, person_results, person_query, page)
        return

    # Пагинация поиска
    if data.startswith("search_page_"):
        page = int(data.split("_")[2])

        print(f"DEBUG: search_page_ called, page={page}, chat_id={chat_id}")
        print(f"DEBUG: user_sessions keys: {list(user_sessions.get(chat_id, {}).keys())}")

        if "search_results" not in user_sessions.get(chat_id, {}):
            print(f"DEBUG: search_results not found in session!")
            await callback.answer("❌ Результаты поиска устарели")
            return

        search_results = user_sessions[chat_id]["search_results"]
        search_query = user_sessions[chat_id].get("search_query", "")

        # Обновляем страницу в сессии
        user_sessions[chat_id]["search_page"] = page

        # ИСПОЛЬЗУЕМ ЛОГИКУ ИЗ send_search_results_page (а не kb_search_results)
        total_results = len(search_results)
        results_per_page = 10
        start_idx = page * results_per_page
        end_idx = start_idx + results_per_page
        page_results = search_results[start_idx:end_idx]

        text = f"🔍 Найдено {total_results} результатов по запросу: '{search_query}'\nСтраница {page + 1}/{(total_results + results_per_page - 1) // results_per_page}\n\nВыберите:"

        keyboard = []

        for item in page_results:
            media_type = item.get("media_type")
            title = item.get("title") or item.get("name")
            year = (item.get("release_date") or item.get("first_air_date") or "")[:4]

            if media_type in ["movie", "tv"]:
                btn_text = f"{'🎬' if media_type == 'movie' else '📺'} {title}"
                if year:
                    btn_text += f" ({year})"

                keyboard.append([
                    InlineKeyboardButton(
                        text=btn_text,
                        callback_data=f"select_{item['id']}_{media_type}"  # ВАЖНО: select_ для обычного поиска
                    )
                ])

        # Пагинация
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"search_page_{page - 1}"))
        if end_idx < total_results:
            nav_buttons.append(InlineKeyboardButton(text="Вперед ➡️", callback_data=f"search_page_{page + 1}"))

        if nav_buttons:
            keyboard.append(nav_buttons)

        keyboard.append([InlineKeyboardButton(text="🔍 Новый поиск", callback_data="search_by_title")])
        keyboard.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="back_to_main")])

        await callback.message.edit_text(
            text,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard)
        )
        return

    # ДОБАВЬТЕ ЭТОТ ОБРАБОТЧИК - ОН ОТСУТСТВУЕТ В ВАШЕМ КОДЕ!
    if data.startswith("select_"):
        print(f"🟢 SELECT handler called: {data}")

        parts = data.split("_")
        if len(parts) < 3:
            await callback.answer("❌ Ошибка: некорректные данные.")
            return

        try:
            tmdb_id = int(parts[1])
            type_ = parts[2]
            print(f"DEBUG: tmdb_id={tmdb_id}, type_={type_}")
        except (ValueError, IndexError) as e:
            await callback.answer("❌ Ошибка данных.")
            return

        try:
            await bot.delete_message(chat_id, old_msg_id)
        except:
            print("Не удалось удалить сообщение с результатами поиска")

        # Получаем детали фильма/сериала
        details = get_item_details(type_, tmdb_id)
        if not details:
            await callback.answer("❌ Не удалось загрузить данные")
            return

        title = details.get("title") or details.get("name") or "Без названия"
        year = (details.get("release_date") or details.get("first_air_date") or "")[:4]
        rating = details.get("vote_average") or "—"
        overview = details.get("overview") or "Описание отсутствует."
        if len(overview) > 2000:
            overview = overview[:2000] + "..."
        poster = f"https://image.tmdb.org/t/p/w500{details.get('poster_path')}" if details.get("poster_path") else None
        avg_ratings = await get_ratings(tmdb_id, type_)

        # Определяем все роли человека в этом проекте
        roles = set()

        # Проверяем, пришли ли мы из поиска по актерам
        if "filmography" in user_sessions.get(chat_id, {}):
            filmography = user_sessions[chat_id]["filmography"]

            # Собираем все роли для этого проекта
            for item in filmography:
                if item.get("id") == tmdb_id and item.get("media_type") == type_:
                    item_roles = item.get("person_role", [])  # Теперь это список ролей
                    for role in item_roles:
                        if role == "director":
                            roles.add("🎬 Режиссер")
                        elif role == "actor":
                            roles.add("🎭 Актер")

        # Формируем информацию о ролях
        role_info = ""
        if roles:
            role_info = ", ".join(sorted(roles)) + "\n"

        # Формируем caption с информацией о ролях
        caption = (
            f"{title} ({year})\n"
            f"{role_info}"
            f"Рейтинг: {rating} (TMDB)\n"
            f"👍 Лайки: {avg_ratings['likes']} | 👎 Дизлайки: {avg_ratings['dislikes']} | 👀 Просмотров: {avg_ratings['watches']}\n\n{overview}"
        )

        # Определяем, откуда пришли - из поиска или из фильмографии
        is_from_filmography = "filmography" in user_sessions.get(chat_id, {})

        if is_from_filmography:
            # Проверяем, находится ли контент в коллекции
            is_in_collection = await is_in_user_collection(chat_id, tmdb_id, type_)

            # Клавиатура для фильмографии
            buttons = []

            if is_in_collection:
                buttons.append([
                    InlineKeyboardButton(text="✅ В коллекции", callback_data=f"already_in_collection"),
                ])
            else:
                buttons.append([
                    InlineKeyboardButton(text="➕ В коллекцию", callback_data=f"add_{tmdb_id}_{type_}"),
                ])

            buttons.append([InlineKeyboardButton(text="🎯 Похожее", callback_data=f"similar_{tmdb_id}_{type_}")])
            buttons.append([InlineKeyboardButton(text="⬅️ К фильмографии", callback_data="back_to_filmography")])
            buttons.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="back_to_main")])

            keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
        else:
            # Проверяем, находится ли контент в коллекции
            is_in_collection = await is_in_user_collection(chat_id, tmdb_id, type_)

            # Клавиатура для обычного поиска
            buttons = []

            if is_in_collection:
                buttons.append([
                    InlineKeyboardButton(text="✅ В коллекции", callback_data=f"already_in_collection"),
                ])
            else:
                buttons.append([
                    InlineKeyboardButton(text="➕ В коллекцию", callback_data=f"add_{tmdb_id}_{type_}"),
                ])

            buttons.append([InlineKeyboardButton(text="🎯 Похожее", callback_data=f"similar_{tmdb_id}_{type_}")])
            buttons.append([InlineKeyboardButton(text="⬅️ К результатам", callback_data="back_to_search_results")])
            buttons.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="back_to_main")])

            keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)

        if poster:
            await callback.message.answer_photo(photo=poster, caption=caption, reply_markup=keyboard)
        else:
            await callback.message.answer(text=caption, reply_markup=keyboard)
        return

    # Кнопка "Похожее"
    if data.startswith("similar_"):
        parts = data.split("_")
        tmdb_id = int(parts[1])
        type_ = parts[2]

        # Сохраняем в сессию для рекомендаций
        if chat_id not in user_sessions:
            user_sessions[chat_id] = {}

        user_sessions[chat_id]["user_likes"] = [{"tmdb_id": tmdb_id, "type": type_}]
        user_sessions[chat_id]["type"] = "preferences"
        user_sessions[chat_id]["shown_recommendations"] = []  # очищаем список показанных

        await send_preference_item(chat_id, callback.message.message_id)
        return

    # Назад к результатам поиска
    # Назад к результатам поиска
    if data == "back_to_search_results":
        if "search_results" not in user_sessions.get(chat_id, {}):
            await callback.answer("❌ Результаты поиска устарели")
            return

        search_results = user_sessions[chat_id]["search_results"]
        search_query = user_sessions[chat_id].get("search_query", "")
        search_page = user_sessions[chat_id].get("search_page", 0)

        # УДАЛЯЕМ сообщение с карточкой фильма
        try:
            await bot.delete_message(chat_id, old_msg_id)
        except:
            pass

        await send_search_results_page(chat_id, search_results, search_query, search_page)
        return

    if data == "back_to_filmography":
        if "filmography" not in user_sessions.get(chat_id, {}):
            await callback.answer("❌ Фильмография не найдена")
            return

        filmography = user_sessions[chat_id]["filmography"]
        person_name = user_sessions[chat_id].get("filmography_person_name", "Актер")
        filmography_page = user_sessions[chat_id].get("filmography_page", 0)

        try:
            await bot.delete_message(chat_id, old_msg_id)
        except:
            pass

        text, keyboard = await send_person_filmography_page(
            chat_id, filmography, person_name, filmography_page
        )

        await bot.send_message(chat_id, text, reply_markup=keyboard)
        return

    # В handle_callback добавьте:
    if data == "back_to_person_results":
        if "person_results" not in user_sessions.get(chat_id, {}):
            await callback.answer("❌ Результаты поиска устарели")
            return

        person_results = user_sessions[chat_id]["person_results"]
        person_query = user_sessions[chat_id].get("person_query", "")
        person_page = user_sessions[chat_id].get("person_page", 0)

        try:
            await bot.delete_message(chat_id, old_msg_id)
        except:
            pass

        await send_person_results_page(chat_id, person_results, person_query, person_page)
        return

    if data == "back_to_person_list":
        if "person_results" not in user_sessions.get(chat_id, {}):
            await callback.answer("❌ Результаты поиска устарели")
            return

        person_results = user_sessions[chat_id]["person_results"]
        person_query = user_sessions[chat_id].get("person_query", "")
        person_page = user_sessions[chat_id].get("person_page", 0)

        try:
            await bot.delete_message(chat_id, old_msg_id)
        except:
            pass

        await send_person_results_page(chat_id, person_results, person_query, person_page)
        return

    # Просмотр профиля друга
    if data.startswith("friend_"):
        try:
            friend_tg_id = int(data.split("_")[1])

            # Получаем информацию о друге
            async with db.acquire() as conn:
                # Получаем основную информацию о друге
                friend = await conn.fetchrow("""
                    SELECT tg_id, username 
                    FROM users 
                    WHERE tg_id = $1
                """, friend_tg_id)

                if not friend:
                    await callback.answer("❌ Друг не найден")
                    return

                friend_name = friend['username'] or f"Пользователь {friend_tg_id}"

                # Получаем дату добавления в друзья
                friendship_data = await conn.fetchrow("""
                    SELECT created_at 
                    FROM user_friends 
                    WHERE user_id = (SELECT user_id FROM users WHERE tg_id = $1)
                    AND friend_user_id = (SELECT user_id FROM users WHERE tg_id = $2)
                """, chat_id, friend_tg_id)

                # Получаем статистику друга
                friend_stats = await conn.fetchrow("""
                    SELECT 
                        COUNT(CASE WHEN liked = TRUE THEN 1 END) as likes_count,
                        COUNT(CASE WHEN watched = TRUE THEN 1 END) as watched_count
                    FROM ratings 
                    WHERE user_id = (SELECT user_id FROM users WHERE tg_id = $1)
                """, friend_tg_id)

                likes_count = friend_stats['likes_count'] if friend_stats else 0
                watched_count = friend_stats['watched_count'] if friend_stats else 0

                # Форматируем время в друзьях
                if friendship_data and friendship_data['created_at']:
                    from datetime import datetime
                    created_at = friendship_data['created_at']
                    now = datetime.now()

                    # Точное вычисление разницы в годах, месяцах и днях
                    def calculate_time_diff(start_date, end_date):
                        years = end_date.year - start_date.year
                        months = end_date.month - start_date.month
                        days = end_date.day - start_date.day

                        # Корректируем отрицательные значения
                        if days < 0:
                            # Занимаем дни из предыдущего месяца
                            months -= 1
                            # Находим сколько дней в предыдущем месяце
                            if start_date.month == 1:
                                prev_month_days = 31  # Декабрь
                            else:
                                import calendar
                                prev_month_days = calendar.monthrange(start_date.year, start_date.month - 1)[1]
                            days += prev_month_days

                        if months < 0:
                            years -= 1
                            months += 12

                        return years, months, days

                    years, months, days = calculate_time_diff(created_at, now)

                    # Функция для правильного склонения дней
                    def format_days(days):
                        if days % 10 == 1 and days % 100 != 11:
                            return f"{days} день"
                        elif 2 <= days % 10 <= 4 and (days % 100 < 10 or days % 100 >= 20):
                            return f"{days} дня"
                        else:
                            return f"{days} дней"

                    # Функция для правильного склонения месяцев
                    def format_months(months):
                        if months % 10 == 1 and months % 100 != 11:
                            return f"{months} месяц"
                        elif 2 <= months % 10 <= 4 and (months % 100 < 10 or months % 100 >= 20):
                            return f"{months} месяца"
                        else:
                            return f"{months} месяцев"

                    # Функция для правильного склонения лет
                    def format_years(years):
                        if years % 10 == 1 and years % 100 != 11:
                            return f"{years} год"
                        elif 2 <= years % 10 <= 4 and (years % 100 < 10 or years % 100 >= 20):
                            return f"{years} года"
                        else:
                            return f"{years} лет"

                    # Форматируем красивый текст
                    if years == 0 and months == 0 and days == 0:
                        # Проверяем разницу в часах для "менее дня"
                        hours_diff = (now - created_at).total_seconds() / 3600
                        if hours_diff < 24:
                            friends_duration = "менее дня"
                        else:
                            friends_duration = format_days(days)
                    elif years == 0 and months == 0:
                        friends_duration = format_days(days)
                    elif years == 0:
                        if days == 0:
                            friends_duration = format_months(months)
                        else:
                            friends_duration = f"{format_months(months)} и {format_days(days)}"
                    else:
                        if months == 0 and days == 0:
                            friends_duration = format_years(years)
                        elif months == 0:
                            friends_duration = f"{format_years(years)} и {format_days(days)}"
                        elif days == 0:
                            friends_duration = f"{format_years(years)} и {format_months(months)}"
                        else:
                            friends_duration = f"{format_years(years)}, {format_months(months)} и {format_days(days)}"
                else:
                    friends_duration = "неизвестно"

                text = (
                    f"👤 Профиль друга\n\n"
                    f"📛 Имя: @{friend_name}\n"
                    f"👍 Лайков: {likes_count}\n"
                    f"🎬 Просмотрено: {watched_count}\n"
                    f"📅 В друзьях: {friends_duration}\n\n"
                )

                await navigate_to_menu(
                    chat_id, old_msg_id,
                    text,
                    kb_friend_profile(friend_tg_id)
                )

        except (ValueError, IndexError) as e:
            await callback.answer("❌ Ошибка при загрузке профиля друга")
            print(f"Error loading friend profile: {e}")
        return

    # Удаление друга
    if data.startswith("remove_friend_"):
        try:
            friend_tg_id = int(data.split("_")[2])

            # Получаем имя друга для сообщения
            async with db.acquire() as conn:
                friend = await conn.fetchrow("""
                    SELECT username 
                    FROM users 
                    WHERE tg_id = $1
                """, friend_tg_id)

                friend_name = friend['username'] or f"Пользователь {friend_tg_id}" if friend else "друг"

            # Удаляем друга
            success = await remove_friend(chat_id, friend_tg_id)

            if success:
                await callback.answer(f"❌ Друг {friend_name} удален")

                # Возвращаемся к списку друзей
                friends = await get_user_friends(chat_id)
                if not friends:
                    await navigate_to_menu(
                        chat_id, old_msg_id,
                        "📭 У вас пока нет друзей. Добавьте друзей, чтобы видеть их рекомендации!",
                        InlineKeyboardMarkup(inline_keyboard=[
                            [InlineKeyboardButton(text="➕ Добавить друга", callback_data="add_friend")],
                            [InlineKeyboardButton(text="⬅️ Назад", callback_data="friends_menu")]
                        ])
                    )
                else:
                    await navigate_to_menu(
                        chat_id, old_msg_id,
                        f"👥 Ваши друзья ({len(friends)}):",
                        kb_my_friends(friends, 0)
                    )
            else:
                await callback.answer("❌ Ошибка при удалении друга")

        except (ValueError, IndexError) as e:
            await callback.answer("❌ Ошибка при удалении друга")
            print(f"Error removing friend: {e}")
        return

    # Статус фильтров
    if data == "filters_status":
        current_filters = user_sessions.get(chat_id, {}).get("filters", {})
        filters_active = any(current_filters.values())

        if filters_active:
            filter_text = "Активные фильтры:\n"
            if current_filters.get('start_year') and current_filters.get('end_year'):
                if current_filters['start_year'] == current_filters['end_year']:
                    filter_text += f"• Год: {current_filters['start_year']}\n"
                else:
                    filter_text += f"• Года: {current_filters['start_year']}-{current_filters['end_year']}\n"
            if current_filters.get('country'):
                filter_text += f"• Страна: {current_filters['country']}\n"
            if current_filters.get('rating'):
                filter_text += f"• Рейтинг: {current_filters['rating']}+\n"
            filter_text += "\nФильтры применяются к:\n• Случайный поиск\n• Поиск по жанрам"
        else:
            filter_text = "❌ Фильтры не активны"

        await callback.answer(filter_text, show_alert=True)
        return

    # Обработчик кнопки года
    if data == "filter_year":
        # Устанавливаем флаг ожидания ввода диапазона
        if chat_id not in user_input_waiting:
            user_input_waiting[chat_id] = {}
        user_input_waiting[chat_id]["waiting_year"] = True

        # Удаляем текущее сообщение с меню
        try:
            await bot.delete_message(chat_id, old_msg_id)
        except:
            pass

        # Отправляем сообщение с инструкцией и сохраняем его ID
        msg = await callback.message.answer(
            "📅 Введите год или диапазон годов:\n\n"
            "• Один год: 2010\n"
            "• Диапазон: 2010-2020\n"
            "• От года: 2010-\n"
            "• До года: -2020\n\n"
            "❌ Чтобы убрать фильтр года, введите 'any'"
        )
        user_input_waiting[chat_id]["message_id"] = msg.message_id
        return

    # Добавь в handle_callback()

    # Проверка админских прав (замени на свою логику)


    # Админ-панель
    if data == "admin_panel":
        await callback.answer()
        if not is_admin(chat_id):
            await callback.answer("❌ Нет доступа!")
            return
        await navigate_to_menu(chat_id, old_msg_id, "⚙️ Админ-панель:", kb_admin_panel())
        return

    # Поиск для бана
    if data == "admin_search_ban":
        await callback.answer()
        if not is_admin(chat_id):
            await callback.answer("❌ Нет доступа!")
            return

        try:
            await bot.delete_message(chat_id, old_msg_id)
        except:
            pass

        # Сохраняем состояние ожидания ввода
        if chat_id not in user_sessions:
            user_sessions[chat_id] = {}
        user_sessions[chat_id]["waiting_admin_search"] = True

        await callback.message.answer(
            "🔍 Введите название фильма или сериала для поиска:\n\n"
            "Можно уточнить тип:\n"
            "• 'интерстеллар movie' - только фильмы\n"
            "• 'breaking bad tv' - только сериалы\n"
            "• 'матрица' - все результаты"
        )
        return

    # Предпросмотр перед баном
    if data.startswith("admin_preban_"):
        await callback.answer()
        if not is_admin(chat_id):
            await callback.answer("❌ Нет доступа!")
            return

        parts = data.split("_")
        tmdb_id = int(parts[2])
        type_ = parts[3]

        # Получаем детали
        details = get_item_details(type_, tmdb_id)
        if not details:
            await callback.answer("❌ Не удалось загрузить данные")
            return

        title = details.get("title") or details.get("name")
        year = (details.get("release_date") or details.get("first_air_date") or "")[:4]

        # Проверяем статус бана
        is_already_banned = await is_banned(tmdb_id, type_)

        if is_already_banned:
            caption = f"🎯 Контент ЗАБАНЕН:\n\n{title} ({year})\nID: {tmdb_id} | Тип: {type_}\n\nРазблокировать контент?"
        else:
            caption = f"🎯 Подтвердите бан:\n\n{title} ({year})\nID: {tmdb_id} | Тип: {type_}"

        await callback.message.answer(
            caption,
            reply_markup=await kb_ban_confirmation(tmdb_id, type_, title)  # не забудь await!
        )
        return

    if data.startswith("confirm_unban_"):
        if not is_admin(chat_id):
            await callback.answer("❌ Нет доступа!")
            return

        parts = data.split("_")
        tmdb_id = int(parts[2])
        type_ = parts[3]

        details = get_item_details(type_, tmdb_id)
        title = details.get("title") or details.get("name") or "Unknown"

        await unban_content(tmdb_id, type_)

        if type_ == "movie":
            await callback.answer(f"✅ Фильм {title} разбанен!")
        elif type_ == "tv":
            await callback.answer(f"✅ Сериал {title} разбанен!")
        else:
            await callback.answer(f"❌ Произошла ошибка!")

        try:
            await bot.delete_message(chat_id, old_msg_id)
        except:
            pass
        return

    # Подтверждение бана
    if data.startswith("confirm_ban_"):
        if not is_admin(chat_id):
            await callback.answer("❌ Нет доступа!")
            return

        parts = data.split("_")
        tmdb_id = int(parts[2])
        type_ = parts[3]

        # Получаем детали для названия
        details = get_item_details(type_, tmdb_id)
        title = details.get("title") or details.get("name") or "Unknown"

        await ban_content(tmdb_id, type_, title, chat_id, "Админ-бан")

        if type_ == "movie":
            await callback.answer(f"✅ Фильм {title} забанен!")
        elif type_ == "tv":
            await callback.answer(f"✅ Сериал {title} забанен!")
        else:
            await callback.answer(f"❌ Произошла ошибка!")

        try:
            await bot.delete_message(chat_id, old_msg_id)
        except:
            pass
        return

    # Список банов
    if data == "admin_ban_list":
        await callback.answer()
        if not is_admin(chat_id):
            await callback.answer("❌ Нет доступа!")
            return

        try:
            await bot.delete_message(chat_id, old_msg_id)
        except:
            pass

        banned_list = await get_banned_list(20)
        if not banned_list:
            await callback.message.answer(
                "📭 Список банов пуст",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_panel")]
                ])
            )
            return

        text = "📋 Забаненный контент:\n\n"
        for item in banned_list:
            text += f"• {item['title']} (ID: {item['tmdb_id']})\n"

        text += f"\nВсего: {len(banned_list)}"

        await callback.message.answer(
            text,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_panel")]
            ])
        )
        return

    # Разбан
    if data.startswith("unban_"):
        await callback.answer()
        if not is_admin(chat_id):
            await callback.answer("❌ Нет доступа!")
            return

        parts = data.split("_")
        tmdb_id = int(parts[1])
        type_ = parts[2]

        await unban_content(tmdb_id, type_)
        await callback.answer("✅ Контент разбанен!")
        await navigate_to_menu(chat_id, old_msg_id, "⚙️ Админ-панель:", kb_admin_panel())
        return

    # Статистика админ-панели
    # Статистика админ-панели
    if data == "admin_stats":
        await callback.answer()
        if not is_admin(chat_id):
            await callback.answer("❌ Нет доступа!")
            return

        try:
            await bot.delete_message(chat_id, old_msg_id)
        except:
            pass

        # Сохраняем сортировку по умолчанию
        if chat_id not in user_sessions:
            user_sessions[chat_id] = {}
        user_sessions[chat_id]["current_stats_sort"] = "updated"

        # Загружаем первую страницу статистики
        stats_data = await get_ratings_stats(sort_by="updated", page=0)
        text = format_stats_page(stats_data, "updated", 0)

        await bot.send_message(
            chat_id,
            text,
            reply_markup=kb_admin_stats("updated", 0, stats_data["total_pages"]),
            parse_mode="HTML"
        )
        return

    # Смена сортировки статистики
    if data.startswith("stats_sort_"):
        await callback.answer()
        if not is_admin(chat_id):
            await callback.answer("❌ Нет доступа!")
            return

        sort_by = data.split("_")[2]

        # Сохраняем сортировку в сессии
        if chat_id not in user_sessions:
            user_sessions[chat_id] = {}
        user_sessions[chat_id]["current_stats_sort"] = sort_by

        stats_data = await get_ratings_stats(sort_by=sort_by, page=0)
        text = format_stats_page(stats_data, sort_by, 0)

        await callback.message.edit_text(
            text,
            reply_markup=kb_admin_stats(sort_by, 0, stats_data["total_pages"]),
            parse_mode="HTML"
        )
        return

    # Смена страницы статистики
    if data.startswith("stats_page_"):
        await callback.answer()
        if not is_admin(chat_id):
            await callback.answer("❌ Нет доступа!")
            return

        parts = data.split("_")
        page = int(parts[2])
        sort_by = parts[3]

        # Сохраняем сортировку в сессии
        if chat_id not in user_sessions:
            user_sessions[chat_id] = {}
        user_sessions[chat_id]["current_stats_sort"] = sort_by

        stats_data = await get_ratings_stats(sort_by=sort_by, page=page)
        text = format_stats_page(stats_data, sort_by, page)

        await callback.message.edit_text(
            text,
            reply_markup=kb_admin_stats(sort_by, page, stats_data["total_pages"]),
            parse_mode="HTML"
        )
        return

    # Информация о статистике
    if data == "stats_info":
        await callback.answer("ℹ️ Используйте кнопки для сортировки и навигации", show_alert=True)
        return


    # Переключение страны
    if data == "filter_country":
        # Устанавливаем флаг ожидания ввода
        if chat_id not in user_input_waiting:
            user_input_waiting[chat_id] = {}
        user_input_waiting[chat_id]["waiting_country"] = True

        # Удаляем текущее сообщение с меню
        try:
            await bot.delete_message(chat_id, old_msg_id)
        except:
            pass

        # Отправляем сообщение с инструкцией и сохраняем его ID
        msg = await callback.message.answer(
            "🌍 Введите название страны на английском (например: RU, US, FR):\n\n"
            "❌ Чтобы убрать фильтр страны, введите 'any'"
        )
        user_input_waiting[chat_id]["message_id"] = msg.message_id
        return

    if data == "filter_rating":
        try:
            await bot.delete_message(chat_id, old_msg_id)
        except:
            pass
        await callback.message.answer("Выберите рейтинг:", reply_markup=kb_rating_selection())
        return

    # Установка года
    if data.startswith("set_year_"):
        year = int(data.split("_")[2])

        if chat_id not in user_sessions:
            user_sessions[chat_id] = {}
        if "filters" not in user_sessions[chat_id]:
            user_sessions[chat_id]["filters"] = {}

        user_sessions[chat_id]["filters"]["year"] = year
        await save_search_filters(chat_id, user_sessions[chat_id]["filters"])
        await callback.answer(f"✅ Год установлен: {year}")

        # Возвращаемся к меню фильтров
        current_filters = user_sessions[chat_id]["filters"]
        await navigate_to_menu(chat_id, old_msg_id, "Настройте фильтры поиска:", kb_filters_menu(current_filters))
        return

    # Установка рейтинга
    if data.startswith("set_rating_"):
        rating = float(data.split("_")[2])

        if chat_id not in user_sessions:
            user_sessions[chat_id] = {}
        if "filters" not in user_sessions[chat_id]:
            user_sessions[chat_id]["filters"] = {}

        user_sessions[chat_id]["filters"]["rating"] = rating
        await save_search_filters(chat_id, user_sessions[chat_id]["filters"])
        await callback.answer(f"✅ Рейтинг установлен: {rating}+")

        current_filters = user_sessions[chat_id]["filters"]
        await navigate_to_menu(chat_id, old_msg_id, "Настройте фильтры поиска:", kb_filters_menu(current_filters))
        return

    # Переключение страны
    if data == "filter_country":
        # Устанавливаем флаг ожидания ввода
        if chat_id not in user_input_waiting:
            user_input_waiting[chat_id] = {}
        user_input_waiting[chat_id]["waiting_country"] = True

        # Удаляем текущее сообщение с меню
        try:
            await bot.delete_message(chat_id, old_msg_id)
        except:
            pass

        # Отправляем сообщение с инструкцией и сохраняем его ID
        msg = await callback.message.answer(
            "🌍 Введите название страны на английском (например: RU, US, FR):\n\n"
            "❌ Чтобы убрать фильтр страны, введите 'any'"
        )
        user_input_waiting[chat_id]["message_id"] = msg.message_id
        return

    # Очистка фильтров
    if data == "clear_year":
        if chat_id in user_sessions and "filters" in user_sessions[chat_id]:
            user_sessions[chat_id]["filters"]["year"] = None
            await save_search_filters(chat_id, user_sessions[chat_id]["filters"])
        await callback.answer("✅ Год сброшен")

        current_filters = user_sessions[chat_id]["filters"]
        await navigate_to_menu(chat_id, old_msg_id, "Настройте фильтры поиска:", kb_filters_menu(current_filters))
        return

    if data == "clear_rating":
        if chat_id in user_sessions and "filters" in user_sessions[chat_id]:
            user_sessions[chat_id]["filters"]["rating"] = None
            await save_search_filters(chat_id, user_sessions[chat_id]["filters"])
        await callback.answer("✅ Рейтинг сброшен")

        current_filters = user_sessions[chat_id]["filters"]
        await navigate_to_menu(chat_id, old_msg_id, "Настройте фильтры поиска:", kb_filters_menu(current_filters))
        return

    # Сброс всех фильтров
    if data == "reset_all_filters":
        try:
            await bot.delete_message(chat_id, old_msg_id)
        except:
            pass
        if chat_id in user_sessions and "filters" in user_sessions[chat_id]:
            user_sessions[chat_id]["filters"] = {}
            await clear_search_filters(chat_id)
        await callback.answer("✅ Все фильтры сброшены!")
        current_filters = user_sessions.get(chat_id, {}).get("filters", {})
        await callback.message.answer("Настройте фильтры поиска:", reply_markup=kb_filters_menu(current_filters))
        return

    # Меню трендов
    if data == "trending_menu":
        await navigate_to_menu(chat_id, old_msg_id, "Что интересует?", kb_trending_menu())
        return

    # Трендовые фильмы за неделю
    if data == "trending_movie_week":
        items = get_trending("movie", "week")
        if not items:
            await callback.answer("Не удалось получить трендовые фильмы", show_alert=True)
            return

        user_sessions[chat_id] = {
            "results": items,
            "index": 0,
            "type": "movie",
            "mode": "trending"
        }
        await send_card(chat_id, old_msg_id)
        return

    # Трендовые сериалы за неделю
    if data == "trending_tv_week":
        items = get_trending("tv", "week")
        if not items:
            await callback.answer("Не удалось получить трендовые сериалы", show_alert=True)
            return

        user_sessions[chat_id] = {
            "results": items,
            "index": 0,
            "type": "tv",
            "mode": "trending"
        }
        await send_card(chat_id, old_msg_id)
        return

    # Главное меню поиска
    if data == "search_menu":
        await navigate_to_menu(chat_id, old_msg_id, "Выберите тип поиска:", kb_search_menu())
        return

    # Меню случайного поиска
    if data == "random_search":
        await navigate_to_menu(chat_id, old_msg_id, "Выберите что искать:", kb_random_search())
        return

    # Добавляем в handle_callback
    if data == "export_pdf":
        await callback.answer("🔄 Создаю PDF...")

        # Создаем PDF
        pdf_buffer = await generate_collection_pdf(chat_id)

        if not pdf_buffer:
            await callback.answer("❌ Коллекция пуста!", show_alert=True)
            return

        # Отправляем файл пользователю
        try:
            await bot.send_document(
                chat_id=chat_id,
                document=types.BufferedInputFile(
                    pdf_buffer.getvalue(),
                    filename="my_collection.pdf"
                ),
                caption="📚 Ваша коллекция фильмов и сериалов"
            )
            await callback.answer("✅ PDF готов!")
        except Exception as e:
            await callback.answer("❌ Ошибка при создании PDF", show_alert=True)
            print(f"PDF export error: {e}")

        return

    # Экспорт статистики в PDF
    if data == "stats_export_pdf":
        await callback.answer("🔄 Генерирую PDF...")
        if not is_admin(chat_id):
            await callback.answer("❌ Нет доступа!")
            return

        try:
            # Получаем текущую сортировку из сессии ИЛИ используем ту, что сейчас отображается
            current_sort = user_sessions.get(chat_id, {}).get("current_stats_sort", "updated")

            # Если нет в сессии, пробуем определить из текущего сообщения
            if "current_stats_sort" not in user_sessions.get(chat_id, {}):
                # Парсим текущую сортировку из сообщения
                message_text = callback.message.text
                if "Сортировка: по лайкам" in message_text:
                    current_sort = "likes"
                elif "Сортировка: по дизлайкам" in message_text:
                    current_sort = "dislikes"
                elif "Сортировка: по просмотрам" in message_text:
                    current_sort = "watches"
                else:
                    current_sort = "updated"

            stats_data = await get_ratings_stats(sort_by=current_sort, page=0, limit=1000)

            pdf_buffer = await generate_stats_pdf(stats_data, current_sort)

            if pdf_buffer:
                from datetime import datetime
                await bot.send_document(
                    chat_id=chat_id,
                    document=types.BufferedInputFile(
                        pdf_buffer.getvalue(),
                        filename=f"statistics_{current_sort}_{datetime.now().strftime('%Y%m%d_%H%M')}.pdf"
                    ),
                    caption=f"📊 Статистика контента (сортировка: {current_sort})"
                )
                await callback.answer("✅ PDF готов!")
            else:
                await callback.answer("❌ Ошибка при создании PDF", show_alert=True)

        except Exception as e:
            await callback.answer("❌ Ошибка при создании PDF", show_alert=True)
            print(f"Stats PDF export error: {e}")
        return

    # Диаграммы статистики в PDF
    if data == "stats_charts_pdf":
        await callback.answer("🔄 Генерирую диаграммы...")
        if not is_admin(chat_id):
            await callback.answer("❌ Нет доступа!")
            return

        try:
            stats_data = await get_ratings_stats(sort_by="likes", page=0, limit=1000)

            pdf_buffer = await generate_stats_charts_pdf(stats_data)

            if pdf_buffer:
                from datetime import datetime  # Добавляем импорт здесь
                await bot.send_document(
                    chat_id=chat_id,
                    document=types.BufferedInputFile(
                        pdf_buffer.getvalue(),
                        filename=f"charts_{datetime.now().strftime('%Y%m%d_%H%M')}.pdf"
                    ),
                    caption="📊 Диаграммы статистики"
                )
                await callback.answer("✅ Диаграммы готовы!")
            else:
                await callback.answer("❌ Ошибка при создании диаграмм", show_alert=True)

        except Exception as e:
            await callback.answer("❌ Ошибка при создании диаграмм", show_alert=True)
            print(f"Charts PDF export error: {e}")
        return

    # В handle_callback добавить:
    # В обработчике preferences заменим:
    if data == "preferences":
        user_likes = await get_user_likes(chat_id)
        if not user_likes:
            await callback.answer("❌ Сначала поставьте лайки некоторым фильмам/сериалам!")
            return

        # Сохраняем лайки пользователя в сессию
        user_sessions[chat_id] = {
            "user_likes": user_likes,
            "type": "preferences"
        }
        await send_preference_item(chat_id, old_msg_id)
        return

    # Обработчик кнопки "Следующая рекомендация"
    if data == "next_preference":
        await send_preference_item(chat_id, old_msg_id)
        return

    if data == "toggle_watched":
        filters = await get_user_filters(chat_id)
        new_value = not filters["exclude_watched"]
        await update_user_filter(chat_id, "hide_watched", new_value)
        filters = await get_user_filters(chat_id)
        user_filters[chat_id] = filters
        await callback.message.edit_reply_markup(reply_markup=kb_settings(filters))
        return

    # Назад в главное меню
    if data == "back_to_main":
        try:
            await bot.delete_message(chat_id, old_msg_id)
        except Exception:
            pass
        await bot.send_message(chat_id, "Выберите, что хотите получить:", reply_markup=kb_main())
        return

    # Настройки
    if data == "settings":
        filters = await get_user_filters(chat_id)
        await navigate_to_menu(chat_id, old_msg_id, "Настройки фильтров:", kb_settings(filters))
        return

    if data == "toggle_anime":
        filters = await get_user_filters(chat_id)
        new_value = not filters["exclude_anime"]
        await update_user_filter(chat_id, "disable_anime", new_value)
        filters = await get_user_filters(chat_id)
        user_filters[chat_id] = filters
        await callback.message.edit_reply_markup(reply_markup=kb_settings(filters))
        return

    if data == "toggle_cartoons":
        filters = await get_user_filters(chat_id)
        new_value = not filters["exclude_cartoons"]
        await update_user_filter(chat_id, "disable_cartoons", new_value)
        filters = await get_user_filters(chat_id)
        user_filters[chat_id] = filters
        await callback.message.edit_reply_markup(reply_markup=kb_settings(filters))
        return

    # Случайный фильм/сериал (с применением фильтров)
    if data in ("discover_movie", "discover_tv"):
        type_ = "movie" if data == "discover_movie" else "tv"

        # Получаем активные фильтры пользователя
        current_filters = await get_current_filters(chat_id)

        items = await discover_tmdb(type_, filters=current_filters)  # ДОБАВЬ await
        if user_filters.get(chat_id, {}).get("exclude_watched"):
            items = await filter_watched_items(chat_id, items, type_)
        if not items:
            await callback.message.answer("Не удалось получить данные.")
            return
        user_sessions[chat_id] = {
            "results": items,
            "index": 0,
            "type": type_,
            "mode": "random"
        }
        await send_card(chat_id, old_msg_id)
        return

    # Поиск по жанрам (с применением фильтров)
    if data == "search_genre":
        await navigate_to_menu(chat_id, old_msg_id, "Выберите тип:",
                               InlineKeyboardMarkup(inline_keyboard=[
                                   [InlineKeyboardButton(text="🎬 Фильмы", callback_data="genre_type_movie")],
                                   [InlineKeyboardButton(text="📺 Сериалы", callback_data="genre_type_tv")],
                                   [InlineKeyboardButton(text="⬅️ Назад", callback_data="search_menu")],
                               ]))
        return

    if data in ("genre_type_movie", "genre_type_tv"):
        type_ = "movie" if data == "genre_type_movie" else "tv"
        await navigate_to_menu(chat_id, old_msg_id, "Выберите жанр:", kb_genres(type_))
        return

    # Обновленный обработчик для выбора жанра (с применением фильтров)
    if data.startswith("genre_"):
        parts = data.split("_")
        if len(parts) < 3:
            await callback.answer("Ошибка: некорректные данные.")
            return
        type_ = parts[1]
        gid = int(parts[2])

        # Получаем активные фильтры пользователя
        current_filters = await get_current_filters(chat_id)

        items = await discover_tmdb(type_, genre_id=gid, filters=current_filters)  # ДОБАВЬ await
        if not items:
            await callback.message.answer("По этому жанру ничего не найдено.")
            return
        user_sessions[chat_id] = {
            "results": items,
            "index": 0,
            "type": type_,
            "genre_id": gid,
            "mode": "genre"
        }
        await send_card(chat_id, old_msg_id)
        return

    # Обновленный обработчик для кнопки "Следующий"
    if data == "next_item":
        session = user_sessions.get(chat_id)
        if not session or "results" not in session:
            await callback.answer("Сначала сделайте выбор.")
            return
        session["index"] += 1
        is_genre_search = session.get("mode") == "genre"
        await send_card(chat_id, old_msg_id)
        return

    # Обновленный обработчик для кнопки "Назад к жанрам"
    if data.startswith("back_to_genres_"):
        type_ = data.split("_")[-1]
        try:
            await bot.delete_message(chat_id, old_msg_id)
        except Exception:
            pass
        await bot.send_message(chat_id, "Выберите жанр:", reply_markup=kb_genres(type_))
        return

    # Коллекция
    if data == "show_collection":
        try:
            await bot.delete_message(chat_id, old_msg_id)
        except Exception:
            pass
        total_items = await get_collection_count(chat_id)
        total_pages = (total_items + 3) // 4  # Округление вверх
        keyboard = await kb_collection(chat_id, 0, total_pages)
        if total_pages == 0:
            await bot.send_message(
                chat_id,
                "Коллекция пуста.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🏠 Главное меню", callback_data="back_to_main")]
                ])
            )
        else:
            await bot.send_message(chat_id, "📚 Ваша коллекция:", reply_markup=keyboard)
        return

    if data.startswith("collection_page_"):
        page = int(data.split("_")[2])
        total_items = await get_collection_count(chat_id)
        total_pages = (total_items + 3) // 4  # Округление вверх
        keyboard = await kb_collection(chat_id, page, total_pages)
        try:
            await bot.edit_message_reply_markup(chat_id=chat_id, message_id=old_msg_id, reply_markup=keyboard)
        except Exception:
            pass
        return

    if data.startswith("show_collection_item_"):
        parts = data.split("_")
        if len(parts) < 4:
            await callback.answer("Ошибка: некорректные данные.")
            return
        try:
            tmdb_id = int(parts[3])
            type_ = parts[4]
        except (ValueError, IndexError) as e:
            await callback.answer("Ошибка: некорректный ID.")
            return

        details = get_item_details(type_, tmdb_id)
        if not details:
            await callback.message.answer("Не удалось загрузить данные.")
            return

        title = details.get("title") or details.get("name") or "Без названия"
        year = (details.get("release_date") or details.get("first_air_date") or "")[:4]
        rating = details.get("vote_average") or "—"
        overview = details.get("overview") or "Описание отсутствует."
        poster = f"https://image.tmdb.org/t/p/w500{details.get('poster_path')}" if details.get("poster_path") else None
        avg_ratings = await get_ratings(tmdb_id, type_)

        user_rating = await get_user_rating(chat_id, tmdb_id, type_)
        watched = user_rating["watched"] if user_rating else False
        liked = user_rating["liked"] if user_rating else None
        disliked = user_rating["disliked"] if user_rating else None
        is_hidden = user_rating["is_hidden"] if user_rating else False  # Получаем статус скрытия

        caption = f"{title} ({year})\nРейтинг: {rating} (TMDB) | 👍 Лайки: {avg_ratings['likes']} | 👎 Дизлайки: {avg_ratings['dislikes']} | 👀 Просмотров: {avg_ratings['watches']}"

        if watched:
            caption += "\n✅ Вы смотрели"

        if is_hidden:
            caption += "\n🙈 Скрыто от друзей"

        caption += f"\n\n{overview}"

        try:
            await bot.delete_message(chat_id, old_msg_id)
        except Exception:
            pass

        if poster:
            await bot.send_photo(chat_id, photo=poster, caption=caption,
                                 reply_markup=kb_collection_item(tmdb_id, type_, watched, liked, disliked, is_hidden))
        else:
            await bot.send_message(chat_id, text=caption,
                                   reply_markup=kb_collection_item(tmdb_id, type_, watched, liked, disliked, is_hidden))
        return

    # Добавить в коллекцию
    # Добавить в коллекцию
    if data.startswith("add_"):
        parts = data.split("_")
        if len(parts) < 3:
            await callback.answer("Ошибка: некорректные данные.")
            return

        tmdb_id = int(parts[1])
        type_ = parts[2]

        # НЕ ИСПОЛЬЗУЕМ СЕССИЮ ИЗ ПОИСКА ПО НАЗВАНИЮ
        # Вместо этого получаем детали напрямую по tmdb_id
        details = get_item_details(type_, tmdb_id)
        if not details:
            await callback.answer("Ошибка: не удалось получить данные.")
            return

        title = details.get("title") or details.get("name") or "Без названия"
        year = (details.get("release_date") or details.get("first_air_date") or "")[:4]
        poster_path = details.get("poster_path") or "/default.jpg"

        success = await add_to_collection(chat_id, tmdb_id, type_, title, year, poster_path)
        if success:
            await callback.answer("✅ Добавлено в коллекцию!")
        else:
            await callback.answer("❌ Ошибка при добавлении.")
        return

    # Удалить из коллекции
    if data.startswith("remove_"):
        parts = data.split("_")
        if len(parts) < 3:
            await callback.answer("Ошибка: некорректные данные.")
            return
        tmdb_id = int(parts[1])
        type_ = parts[2]
        success = await remove_from_collection(chat_id, tmdb_id, type_)
        if success:
            await callback.answer("Удалено из коллекции!")
            try:
                await bot.delete_message(chat_id, callback.message.message_id)
            except Exception:
                pass
            total_items = await get_collection_count(chat_id)
            total_pages = (total_items + 3) // 4
            if total_pages == 0:
                await bot.send_message(
                    chat_id,
                    "Коллекция пуста.",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="back_to_main")]
                    ])
                )
            else:
                keyboard = await kb_collection(chat_id, 0, total_pages)
                await bot.send_message(chat_id, "📚 Ваша коллекция:", reply_markup=keyboard)
        else:
            await callback.answer("Ошибка при удалении.")
        return

    # Просмотрено
    if data.startswith("like_"):
        await handle_rating(callback, "like")
        return
    if data.startswith("dislike_"):
        await handle_rating(callback, "dislike")
        return
    if data.startswith("reset_rating_"):
        parts = data.split("_")
        if len(parts) < 4:
            await callback.answer("Ошибка: некорректные данные.")
            return
        try:
            tmdb_id = int(parts[2])
            type_ = parts[3]
            await handle_rating(callback, "reset", tmdb_id, type_)
        except (ValueError, IndexError):
            await callback.answer("Ошибка: некорректные данные.")
        return

    if data.startswith("mark_watched_"):
        await handle_rating(callback, "watch")
        return

    # Обработчик скрытия/показа оценки
    if data.startswith("toggle_hide_"):
        parts = data.split("_")
        if len(parts) < 4:
            await callback.answer("Ошибка: некорректные данные.")
            return

        tmdb_id = int(parts[2])
        type_ = parts[3]

        details = get_item_details(type_, tmdb_id)
        title = details.get("title") or details.get("name") or "Без названия"

        # Получаем текущий статус
        user_rating = await get_user_rating(chat_id, tmdb_id, type_)
        current_hidden = user_rating["is_hidden"] if user_rating else False

        # Переключаем статус
        new_hidden = not current_hidden

        # Обновляем оценку (ТОЛЬКО is_hidden, сохраняем текущие лайки)
        await add_rating(
            chat_id,
            tmdb_id,
            type_,
            is_hidden=new_hidden,
            title=title,
            # ЯВНО передаем текущие значения лайков, чтобы они не сбрасывались
            liked=user_rating.get('liked') if user_rating else None,
            disliked=user_rating.get('disliked') if user_rating else None,
            watched=user_rating.get('watched') if user_rating else None
        )

        if new_hidden:
            await callback.answer("🙈 Оценка скрыта от друзей")
        else:
            await callback.answer("👀 Оценка видна друзьям")

        # Обновляем интерфейс
        user_rating_updated = await get_user_rating(chat_id, tmdb_id, type_)
        keyboard = kb_collection_item(
            tmdb_id,
            type_,
            user_rating_updated.get('watched', False),
            user_rating_updated.get('liked'),
            user_rating_updated.get('disliked'),
            user_rating_updated.get('is_hidden', False)
        )

        await callback.message.edit_reply_markup(reply_markup=keyboard)
        return


async def generate_collection_pdf(tg_id: int):
    has_russian_font = register_russian_font()
    collection = await get_collection(tg_id, limit=1000, offset=0)

    if not collection:
        return None

    buffer = io.BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4

    if has_russian_font:
        font_normal = "RussianFont"
        font_bold = "RussianFont"
    else:
        font_normal = "Helvetica"
        font_bold = "Helvetica-Bold"

    # Заголовок
    pdf.setFont(font_bold, 16)
    pdf.drawString(50, height - 50, "Моя коллекция")
    pdf.setFont(font_normal, 10)

    # Статистика коллекции
    movie_count = sum(1 for item in collection if item['type'] == 'movie')
    tv_count = sum(1 for item in collection if item['type'] == 'tv')

    # Получаем статистику по просмотрам и оценкам
    watched_count = 0
    liked_count = 0
    disliked_count = 0

    for item in collection:
        user_rating = await get_user_rating(tg_id, item['tmdb_id'], item['type'])
        if user_rating:
            if user_rating['watched']:
                watched_count += 1
            if user_rating['liked']:
                liked_count += 1
            if user_rating['disliked']:
                disliked_count += 1

    pdf.drawString(50, height - 70, f"Всего: {len(collection)} элементов")
    pdf.drawString(50, height - 85, f"Дата генерации: {datetime.now().strftime('%d.%m.%Y %H:%M')}")

    y_position = height - 115

    for i, item in enumerate(collection):
        if y_position < 120:
            pdf.showPage()
            # Заголовок на новой странице
            y_position = height - 50

        item_height = 100
        center_line = y_position - (item_height / 2)

        # ПОСТЕР
        poster_width = 60
        poster_height = 80
        poster_x = width - 80
        poster_y = center_line - (poster_height / 2)

        if item['poster_path'] and item['poster_path'] != "/default.jpg":
            try:
                poster_url = f"https://image.tmdb.org/t/p/w154{item['poster_path']}"
                response = requests.get(poster_url, timeout=10)
                if response.status_code == 200:
                    img_data = io.BytesIO(response.content)
                    img_reader = ImageReader(img_data)
                    pdf.drawImage(img_reader, poster_x, poster_y,
                                  width=poster_width, height=poster_height,
                                  preserveAspectRatio=True, mask='auto')
            except Exception as e:
                print(f"Poster loading error: {e}")

        # ТЕКСТ
        text_start_y = center_line + 25

        # Название
        pdf.setFont(font_bold, 12)
        title = item['title']
        if len(title) > 35:
            title = title[:32] + "..."
        pdf.drawString(50, text_start_y, title)

        # Год и тип
        pdf.setFont(font_normal, 10)
        type_text = "Фильм" if item['type'] == 'movie' else "Сериал"
        pdf.drawString(50, text_start_y - 15, f"Год: {item['year']} | {type_text}")

        # Дата добавления
        added_date = item['added_at'].strftime("%d.%m.%Y") if item['added_at'] else "N/A"
        pdf.drawString(50, text_start_y - 30, f"Добавлено: {added_date}")

        # ID элемента
        pdf.drawString(50, text_start_y - 45, f"ID: {item['tmdb_id']}")

        # Информация о просмотре и оценке - используем тот же шрифт что и для основного текста
        user_rating = await get_user_rating(tg_id, item['tmdb_id'], item['type'])

        if user_rating:
            status_parts = []
            if user_rating['watched']:
                status_parts.append("Просмотрено")
            if user_rating['liked']:
                status_parts.append("Понравилось")
            elif user_rating['disliked']:
                status_parts.append("Не понравилось")

            if status_parts:
                status_text = " | ".join(status_parts)
                # Используем тот же шрифт что работает для русского текста
                pdf.setFont(font_normal, 9)
                pdf.drawString(50, text_start_y - 60, status_text)

        y_position -= item_height

        # Разделитель
        if i < len(collection) - 1:
            pdf.line(50, y_position + 5, width - 50, y_position + 5)
            y_position -= 10

    pdf.save()
    buffer.seek(0)
    return buffer

async def navigate_to_menu(chat_id, old_msg_id, text, keyboard):
    """Универсальная функция для перехода между меню"""
    try:
        # Сначала удаляем старое сообщение
        try:
            await bot.delete_message(chat_id, old_msg_id)
        except:
            pass

        # Затем отправляем новое
        await bot.send_message(chat_id, text, reply_markup=keyboard)

    except Exception as e:
        print(f"Ошибка в navigate_to_menu: {e}")
        # Если не получилось, просто отправляем новое сообщение
        await bot.send_message(chat_id, text, reply_markup=keyboard)


async def send_preference_item(chat_id, old_msg_id=None):
    session = user_sessions.get(chat_id)
    if not session or "user_likes" not in session:
        await bot.send_message(chat_id, "Ошибка сессии.")
        return

    # Инициализируем список показанных рекомендаций если его нет
    if "shown_recommendations" not in session:
        session["shown_recommendations"] = []

    # Случайно выбираем лайкнутый item
    liked_item = random.choice(session["user_likes"])

    # Получаем рекомендации
    recommendations = get_recommendations(liked_item["type"], liked_item["tmdb_id"])

    if not recommendations:
        # Если нет рекомендаций, пробуем другой лайкнутый item
        await send_preference_item(chat_id, old_msg_id)
        return

    # Фильтруем уже показанные рекомендации
    available_recommendations = [r for r in recommendations if r["id"] not in session["shown_recommendations"]]

    # Если все рекомендации уже показаны, очищаем список
    if not available_recommendations:
        session["shown_recommendations"] = []
        available_recommendations = recommendations

    # Случайно выбираем одну рекомендацию из доступных
    chosen_item = random.choice(available_recommendations)

    # Добавляем в список показанных
    session["shown_recommendations"].append(chosen_item["id"])

    # Проверяем, не забанен ли контент
    if await is_banned(chosen_item["id"], liked_item["type"]):
        print(
            f"DEBUG: Пропускаем забаненный контент в рекомендациях - ID: {chosen_item['id']}, Type: {liked_item['type']}")
        await send_preference_item(chat_id, old_msg_id)
        return

    # Получаем детали
    details = get_item_details(liked_item["type"], chosen_item["id"])
    if not details:
        await send_preference_item(chat_id, old_msg_id)
        return

    # Применяем фильтры пользователя
    filters = await get_user_filters(chat_id)
    filters = filters or {"exclude_anime": False, "exclude_cartoons": False, "exclude_watched": False}

    if filters["exclude_anime"] and is_anime_by_details(liked_item["type"], details, chosen_item):
        await send_preference_item(chat_id, old_msg_id)
        return

    if filters["exclude_cartoons"] and is_cartoons_by_details(liked_item["type"], details, chosen_item):
        await send_preference_item(chat_id, old_msg_id)
        return

    if filters["exclude_watched"]:
        user_rating = await get_user_rating(chat_id, chosen_item["id"], liked_item["type"])
        if user_rating and user_rating["watched"]:
            await send_preference_item(chat_id, old_msg_id)
            return

    # Формируем карточку
    title = details.get("title") or details.get("name") or "Без названия"
    year = (details.get("release_date") or details.get("first_air_date") or "")[:4]
    rating = details.get("vote_average") or "—"
    overview = details.get("overview") or "Описание отсутствует."
    if len(overview) > 2000:
        overview = overview[:2000] + "..."
    poster = f"https://image.tmdb.org/t/p/w500{details.get('poster_path')}" if details.get("poster_path") else None

    avg_ratings = await get_ratings(chosen_item["id"], liked_item["type"])
    user_rating = await get_user_rating(chat_id, chosen_item["id"], liked_item["type"])
    watched_text = "✅ Вы смотрели" if user_rating and user_rating["watched"] else ""

    caption = (
        f"{title} ({year})\n"
        f"Рейтинг: {rating} (TMDB)\n"
        f"👍 Лайки: {avg_ratings['likes']} | 👎 Дизлайки: {avg_ratings['dislikes']} | 👀 Просмотров: {avg_ratings['watches']}\n"
        f"{watched_text}\n\n{overview}"
    )

    if len(caption) > 4096:
        max_overview_len = 4096 - len(
            f"{title} ({year})\nРейтинг: {rating} (TMDB)\n👍 Лайки: {avg_ratings['likes']} | 👎 Дизлайки: {avg_ratings['dislikes']} | 👀 Просмотров: {avg_ratings['watches']}\n{watched_text}\n\n"
        )
        overview = overview[:max_overview_len] + "..."
        caption = (
            f"{title} ({year})\n"
            f"Рейтинг: {rating} (TMDB)\n"
            f"👍 Лайки: {avg_ratings['likes']} | 👎 Дизлайки: {avg_ratings['dislikes']} | 👀 Просмотров: {avg_ratings['watches']}\n"
            f"{watched_text}\n\n{overview}"
        )

    # Клавиатура для рекомендаций
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ В коллекцию", callback_data=f"add_{chosen_item['id']}_{liked_item['type']}")],
        [InlineKeyboardButton(text="➡️ Следующая рекомендация", callback_data="next_preference")],
        [InlineKeyboardButton(text="🔍 Меню поиска", callback_data="search_menu")]
    ])

    if old_msg_id:
        try:
            await bot.delete_message(chat_id, old_msg_id)
        except Exception:
            pass

    try:
        if poster:
            # Дополнительная проверка URL
            if poster.startswith('http') and len(poster) > 10:
                await bot.send_photo(chat_id, photo=poster, caption=caption, reply_markup=keyboard)
            else:
                # Если poster невалидный, отправляем текстовое сообщение
                await bot.send_message(chat_id, text=caption, reply_markup=keyboard)
        else:
            await bot.send_message(chat_id, text=caption, reply_markup=keyboard)
    except Exception as e:
        # Если ошибка при отправке фото, отправляем текстовое сообщение
        print(f"Ошибка при отправке фото: {e}")
        await bot.send_message(chat_id, text=caption, reply_markup=keyboard)


async def send_friend_request(from_tg_id: int, to_tg_id: int):
    """Отправляет заявку в друзья"""
    async with db.acquire() as conn:
        from_user = await conn.fetchrow("SELECT user_id FROM users WHERE tg_id=$1", from_tg_id)
        to_user = await conn.fetchrow("SELECT user_id FROM users WHERE tg_id=$1", to_tg_id)

        if not from_user or not to_user or from_user["user_id"] == to_user["user_id"]:
            return False

        # Проверяем, нет ли уже заявки
        existing = await conn.fetchrow("""
            SELECT 1 FROM friend_requests 
            WHERE from_user_id = $1 AND to_user_id = $2 AND status = 'pending'
        """, from_user["user_id"], to_user["user_id"])

        if existing:
            return "already_sent"

        await conn.execute("""
            INSERT INTO friend_requests (from_user_id, to_user_id, status)
            VALUES ($1, $2, 'pending')
            ON CONFLICT (from_user_id, to_user_id) DO UPDATE 
            SET status = 'pending', created_at = NOW()
        """, from_user["user_id"], to_user["user_id"])

        return True


async def accept_friend_request(request_id: int):
    """Принимает заявку в друзья"""
    async with db.acquire() as conn:
        # Получаем информацию о заявке
        request = await conn.fetchrow("""
            SELECT fr.*, u1.tg_id as from_tg_id, u2.tg_id as to_tg_id
            FROM friend_requests fr
            JOIN users u1 ON fr.from_user_id = u1.user_id
            JOIN users u2 ON fr.to_user_id = u2.user_id
            WHERE fr.request_id = $1
        """, request_id)

        if not request:
            return False

        # Обновляем статус заявки
        await conn.execute("""
            UPDATE friend_requests SET status = 'accepted' WHERE request_id = $1
        """, request_id)

        # Добавляем взаимную дружбу
        await conn.execute("""
            INSERT INTO user_friends (user_id, friend_user_id)
            VALUES ($1, $2), ($2, $1)
            ON CONFLICT (user_id, friend_user_id) DO NOTHING
        """, request["from_user_id"], request["to_user_id"])

        return request


async def get_pending_friend_requests(tg_id: int):
    """Получает входящие заявки в друзья"""
    async with db.acquire() as conn:
        user = await conn.fetchrow("SELECT user_id FROM users WHERE tg_id=$1", tg_id)
        if not user:
            return []

        rows = await conn.fetch("""
            SELECT 
                fr.request_id,
                fr.created_at,
                u.tg_id,
                u.username
            FROM friend_requests fr
            JOIN users u ON fr.from_user_id = u.user_id
            WHERE fr.to_user_id = $1 AND fr.status = 'pending'
            ORDER BY fr.created_at DESC
        """, user["user_id"])

        return rows


async def send_friend_recommendation_card(chat_id, old_msg_id=None):
    session = user_sessions.get(chat_id)
    if not session or "friends_recommendations" not in session:
        await bot.send_message(chat_id, "❌ Рекомендации не найдены")
        return

    recommendations = session["friends_recommendations"]
    index = session.get("friends_rec_index", 0)

    if index >= len(recommendations):
        session["friends_rec_index"] = 0
        index = 0

    rec = recommendations[index]

    title = rec.get('title', 'Без названия')
    tmdb_id = rec.get('tmdb_id')
    type_ = rec.get('type', 'movie')

    # Получаем реальные username друзей из БД
    friend_usernames = []
    try:
        async with db.acquire() as conn:
            # Получаем username друзей которые лайкнули этот контент
            query = """
            SELECT u.username 
            FROM ratings r
            JOIN users u ON r.user_id = u.user_id
            WHERE r.tmdb_id = $1 AND r.type = $2 AND r.liked = true
            AND u.user_id IN (
                SELECT friend_user_id FROM user_friends WHERE user_id = (
                    SELECT user_id FROM users WHERE tg_id = $3
                )
                UNION
                SELECT user_id FROM user_friends WHERE friend_user_id = (
                    SELECT user_id FROM users WHERE tg_id = $3
                )
            )
            AND u.username IS NOT NULL AND u.username != ''
            """
            user_result = await conn.fetchval("SELECT user_id FROM users WHERE tg_id = $1", chat_id)
            if user_result:
                friends_data = await conn.fetch(query, tmdb_id, type_, chat_id)
                friend_usernames = [friend['username'] for friend in friends_data if friend['username']]
    except Exception as e:
        print(f"Error getting friend usernames: {e}")

    # Формируем упоминания
    if friend_usernames:
        # Берем до 3 друзей для упоминания
        mentioned_friends = []
        for username in friend_usernames[:3]:
            if username and username.strip():
                clean_username = username.lstrip('@').strip()
                mentioned_friends.append(f"@{clean_username}")

        friends_mention = ", ".join(mentioned_friends)

        # Если друзей больше, добавляем счетчик
        if len(friend_usernames) > 3:
            friends_mention += f" и ещё {len(friend_usernames) - 3}"
    else:
        friends_mention = "друзья"

    friend_likes = len(friend_usernames)

    # Получаем детали
    details = get_item_details(type_, tmdb_id)

    if details:
        year = (details.get('release_date') or details.get('first_air_date') or '')[:4]
        rating = details.get('vote_average', '—')
        poster = f"https://image.tmdb.org/t/p/w500{details.get('poster_path')}" if details.get('poster_path') else None
    else:
        year = "—"
        rating = "—"
        poster = None

    caption = (
        f"👥 Рекомендация от друзей\n\n"
        f"🎬 {title} ({year})\n"
        f"⭐ Рейтинг: {rating} (TMDB)\n"
        f"👍 Лайков от друзей: {friend_likes}\n"
        f"👤 Понравился: {friends_mention}\n\n"
        f"💡 Ваши друзья посмотрели и оценили этот фильм!"
    )

    is_in_collection = await is_in_user_collection(chat_id, tmdb_id, type_)

    keyboard = []
    if is_in_collection:
        keyboard.append([InlineKeyboardButton(text="✅ В коллекции", callback_data="already_in_collection")])
    else:
        keyboard.append(
            [InlineKeyboardButton(text="➕ В коллекцию", callback_data=f"add_{tmdb_id}_{type_}")])

    keyboard.append([InlineKeyboardButton(text="➡️ Следующая рекомендация", callback_data="next_friend_rec")])
    keyboard.append([InlineKeyboardButton(text="👥 К друзьям", callback_data="friends_menu")])
    keyboard.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="back_to_main")])

    if old_msg_id:
        try:
            await bot.delete_message(chat_id, old_msg_id)
        except:
            pass

    if poster:
        await bot.send_photo(
            chat_id,
            photo=poster,
            caption=caption,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard),
            parse_mode="HTML"
        )
    else:
        await bot.send_message(
            chat_id,
            text=caption,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard),
            parse_mode="HTML"
        )

    session["friends_rec_index"] = index + 1

async def send_person_results_page(chat_id: int, results: list, search_query: str, page: int,
                                   results_per_page: int = 10):
    total_results = len(results)
    start_idx = page * results_per_page
    end_idx = start_idx + results_per_page
    page_results = results[start_idx:end_idx]

    text = f"🎭 Найдено {total_results} человек по запросу: '{search_query}'\nСтраница {page + 1}/{(total_results + results_per_page - 1) // results_per_page}\n\nВыберите актера:"

    keyboard = []

    for item in page_results:
        name = item.get("name", "Без имени")
        known_for = item.get("known_for_department", "Актер")

        # Получаем известные работы для отображения

        btn_text = f"🎭 {name}"

        # Обрезаем текст если слишком длинный
        if len(btn_text) > 50:
            btn_text = btn_text[:47] + "..."

        keyboard.append([
            InlineKeyboardButton(
                text=btn_text,
                callback_data=f"select_person_{item['id']}"
            )
        ])

    # Пагинация
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"person_page_{page - 1}"))
    if end_idx < total_results:
        nav_buttons.append(InlineKeyboardButton(text="Вперед ➡️", callback_data=f"person_page_{page + 1}"))

    if nav_buttons:
        keyboard.append(nav_buttons)

    keyboard.append([InlineKeyboardButton(text="🔍 Новый поиск", callback_data="search_by_person")])
    keyboard.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="back_to_main")])

    await bot.send_message(chat_id, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard))


async def send_person_filmography_page(chat_id: int, filmography: list, person_name: str, page: int,
                                       results_per_page: int = 10):
    """Отображает фильмографию актера с пагинацией (сгруппированные роли)"""
    total_results = len(filmography)
    start_idx = page * results_per_page
    end_idx = start_idx + results_per_page
    page_results = filmography[start_idx:end_idx]
    total_pages = (total_results + results_per_page - 1) // results_per_page

    text = f"🎭 Фильмография: {person_name}\n"
    text += f"📁 Найдено {total_results} работ\n"
    text += f"📄 Страница {page + 1}/{total_pages}\n\n"

    keyboard = []

    for i, item in enumerate(page_results, start=start_idx + 1):
        media_type = item.get("media_type")
        title = item.get("title") or "Без названия"
        year = (item.get("release_date") or "")[:4]
        roles = item.get("person_role", [])  # Теперь это список ролей

        # Формируем текст кнопки
        type_icon = "🎬" if media_type == "movie" else "📺"

        btn_text = f"{type_icon} {title}"
        if year:
            btn_text += f" ({year})"

        # Добавляем информацию о ролях


        # Обрезаем если слишком длинный
        if len(btn_text) > 50:
            btn_text = btn_text[:47] + "..."

        keyboard.append([
            InlineKeyboardButton(
                text=btn_text,
                callback_data=f"select_{item['id']}_{media_type}"
            )
        ])

    # Пагинация
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"filmography_page_{page - 1}"))

    if end_idx < total_results:
        nav_buttons.append(InlineKeyboardButton(text="Вперед ➡️", callback_data=f"filmography_page_{page + 1}"))

    if nav_buttons:
        keyboard.append(nav_buttons)

    keyboard.append([InlineKeyboardButton(text="⬅️ К списку актеров", callback_data="back_to_person_list")])
    keyboard.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="back_to_main")])

    return text, InlineKeyboardMarkup(inline_keyboard=keyboard)


# Обновленная функция send_card
# Обновленная функция send_card
async def send_card(chat_id, old_msg_id=None):
    session = user_sessions.get(chat_id)
    if not session or "results" not in session:
        await bot.send_message(chat_id, "❌ Сессия истекла. Начните поиск заново.")
        return

    print(
        f"DEBUG: send_card - session type: {session.get('type')}, results count: {len(session['results'])}, index: {session['index']}")

    # Инициализируем множество показанных ID если его нет
    if "shown_ids" not in session:
        session["shown_ids"] = set()

    # Если достигли конца списка или нужно загрузить новые результаты
    if session["index"] >= len(session["results"]) or len(session["results"]) == 0:
        print(f"DEBUG: Загружаем новые результаты, показано уже: {len(session['shown_ids'])}")

        # Получаем активные фильтры
        current_filters = await get_current_filters(chat_id)

        # Загружаем новые результаты
        new_results = await discover_tmdb(
            session["type"],
            session.get("genre_id"),
            filters=current_filters
        )

        if not new_results:
            if old_msg_id:
                try:
                    await bot.delete_message(chat_id, old_msg_id)
                except Exception:
                    pass

            await bot.send_message(
                chat_id,
                "❌ Не удалось загрузить новые результаты. Попробуйте изменить фильтры.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🎲 Новый поиск", callback_data="search_menu")],
                    [InlineKeyboardButton(text="🏠 Главное меню", callback_data="back_to_main")]
                ])
            )
            return

        # Фильтруем уже показанные элементы
        filtered_results = []
        for item in new_results:
            if item["id"] not in session["shown_ids"]:
                filtered_results.append(item)

        if not filtered_results:
            # Если все новые результаты уже были показаны
            if old_msg_id:
                try:
                    await bot.delete_message(chat_id, old_msg_id)
                except Exception:
                    pass

            await bot.send_message(
                chat_id,
                "🎬 Показаны все доступные результаты по вашим фильтрам!\n\nПопробуйте изменить фильтры или начать новый поиск.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="⚡ Изменить фильтры", callback_data="search_filters")],
                    [InlineKeyboardButton(text="🎲 Новый поиск", callback_data="search_menu")],
                    [InlineKeyboardButton(text="🏠 Главное меню", callback_data="back_to_main")]
                ])
            )
            return

        # Обновляем сессию
        session["results"] = filtered_results
        session["index"] = 0
        print(f"DEBUG: Загружено {len(filtered_results)} новых результатов (после фильтрации)")

    item = session["results"][session["index"]]
    print(f"DEBUG: Processing item {session['index']}: {item.get('title') or item.get('name')}")

    # Пропускаем забаненные фильмы и применяем фильтры
    max_attempts = len(session["results"])
    attempts = 0

    while attempts < max_attempts and session["index"] < len(session["results"]):
        item = session["results"][session["index"]]

        # Проверяем бан
        if await is_banned(item["id"], session["type"]):
            print(f"DEBUG: Пропускаем забаненный контент - ID: {item['id']}")
            session["index"] += 1
            attempts += 1
            continue

        # Получаем детали
        details = get_item_details(session["type"], item["id"])
        if not details:
            session["index"] += 1
            attempts += 1
            continue

        # Применяем фильтры пользователя
        filters = await get_user_filters(chat_id)
        filters = filters or {"exclude_anime": False, "exclude_cartoons": False, "exclude_watched": False}

        if filters["exclude_anime"] and is_anime_by_details(session["type"], details, item):
            session["index"] += 1
            attempts += 1
            continue

        if filters["exclude_cartoons"] and is_cartoons_by_details(session["type"], details, item):
            session["index"] += 1
            attempts += 1
            continue

        if filters["exclude_watched"]:
            user_rating = await get_user_rating(chat_id, item["id"], session["type"])
            if user_rating and user_rating["watched"]:
                session["index"] += 1
                attempts += 1
                continue

        # Если все проверки пройдены, показываем карточку
        title = details.get("title") or details.get("name") or "Без названия"
        year = (details.get("release_date") or details.get("first_air_date") or "")[:4]
        rating = details.get("vote_average") or "—"
        overview = details.get("overview") or "Описание отсутствует."
        if len(overview) > 2000:
            overview = overview[:2000] + "..."
        poster = f"https://image.tmdb.org/t/p/w500{details.get('poster_path')}" if details.get("poster_path") else None
        avg_ratings = await get_ratings(item["id"], session["type"])
        user_rating = await get_user_rating(chat_id, item["id"], session["type"])
        watched_text = "✅ Вы смотрели" if user_rating and user_rating["watched"] else ""

        # Общая информация о прогрессе
        total_shown = len(session["shown_ids"]) + 1
        caption = (
            f"{title} ({year})\n"
            f"Рейтинг: {rating} (TMDB)\n"
            f"👍 Лайки: {avg_ratings['likes']} | 👎 Дизлайки: {avg_ratings['dislikes']} | 👀 Просмотров: {avg_ratings['watches']}\n"
            f"{watched_text}\n\n{overview}"
        )

        if len(caption) > 4096:
            max_overview_len = 4096 - len(
                f"{title} ({year})\nРейтинг: {rating} (TMDB)\n👍 Лайки: {avg_ratings['likes']} | 👎 Дизлайки: {avg_ratings['dislikes']} | 👀 Просмотров: {avg_ratings['watches']}\n{watched_text}\n\n"
            )
            overview = overview[:max_overview_len] + "..."
            caption = (
                f"{title} ({year})\n"
                f"Рейтинг: {rating} (TMDB)\n"
                f"👍 Лайки: {avg_ratings['likes']} | 👎 Дизлайки: {avg_ratings['dislikes']} | 👀 Просмотров: {avg_ratings['watches']}\n"
                f"{watched_text}\n\n{overview}"
            )

        # Добавляем ID в показанные
        session["shown_ids"].add(item["id"])

        is_genre_search = session.get("mode") == "genre"
        is_trending = session.get("mode") == "trending"

        if old_msg_id:
            try:
                await bot.delete_message(chat_id, old_msg_id)
            except Exception:
                pass

        if poster:
            await bot.send_photo(chat_id, photo=poster, caption=caption,
                                 reply_markup=await kb_card(chat_id, item["id"], session["type"], is_genre_search,
                                                            is_trending))
        else:
            await bot.send_message(chat_id, text=caption,
                                   reply_markup=await kb_card(chat_id, item["id"], session["type"], is_genre_search,
                                                              is_trending))

        # Увеличиваем индекс для следующего вызова
        session["index"] += 1
        return

    # Если не нашли подходящий контент в текущей выборке
    if old_msg_id:
        try:
            await bot.delete_message(chat_id, old_msg_id)
        except Exception:
            pass

    # Пробуем загрузить новые результаты
    session["index"] = len(session["results"])  # Принудительно загрузим новые результаты
    await send_card(chat_id)


async def get_user_rating(tg_id: int, tmdb_id: int, type_: str):
    async with db.acquire() as conn:
        user = await conn.fetchrow("SELECT user_id FROM users WHERE tg_id=$1", tg_id)
        if not user:
            return None
        row = await conn.fetchrow("""
            SELECT liked, disliked, watched, is_hidden
            FROM ratings
            WHERE user_id = $1
            AND tmdb_id = $2
            AND type = $3
        """, user["user_id"], tmdb_id, type_)
        if row:
            return {
                "liked": row["liked"],
                "disliked": row["disliked"],
                "watched": row["watched"],
                "is_hidden": row["is_hidden"]
            }
        return None


async def get_ratings_stats(sort_by: str = "updated", page: int = 0, limit: int = 15):
    """Получает статистику рейтингов с пагинацией и сортировкой"""
    async with db.acquire() as conn:
        # Определяем сортировку
        if sort_by == "likes":
            order_by = "likes DESC, watches DESC"
        elif sort_by == "dislikes":
            order_by = "dislikes DESC, watches DESC"
        elif sort_by == "watches":
            order_by = "watches DESC, likes DESC"
        else:  # updated (по умолчанию)
            order_by = "MAX(r.rating_id) DESC"

        # Получаем общее количество
        total_count = await conn.fetchval("""
            SELECT COUNT(DISTINCT CONCAT(r.tmdb_id, '-', r.type))
            FROM ratings r
            WHERE r.liked = TRUE OR r.disliked = TRUE OR r.watched = TRUE
        """)

        # Получаем данные с пагинацией
        offset = page * limit

        query = f"""
            SELECT 
                r.tmdb_id,
                r.type,
                r.title,
                COUNT(CASE WHEN r.liked = TRUE THEN 1 END) as likes,
                COUNT(CASE WHEN r.disliked = TRUE THEN 1 END) as dislikes,
                COUNT(CASE WHEN r.watched = TRUE THEN 1 END) as watches,
                MAX(r.rating_id) as last_updated
            FROM ratings r
            WHERE r.liked = TRUE OR r.disliked = TRUE OR r.watched = TRUE
            GROUP BY r.tmdb_id, r.type, r.title
            ORDER BY {order_by}
            LIMIT $1 OFFSET $2
        """

        rows = await conn.fetch(query, limit, offset)

        return {
            "items": rows,
            "total_count": total_count,
            "page": page,
            "total_pages": (total_count + limit - 1) // limit
        }

async def handle_rating(callback, action, tmdb_id=None, type_=None):
    chat_id = callback.message.chat.id

    # Если tmdb_id и type_ не переданы, извлекаем их из callback.data
    if tmdb_id is None or type_ is None:
        parts = callback.data.split("_")
        print(f"DEBUG: callback.data = {callback.data}, parts = {parts}")  # Отладочная информация

        # Для действия "watch" формат: mark_watched_{tmdb_id}_{type_}
        if action == "watch":
            if len(parts) < 3:
                await callback.answer("Ошибка: некорректные данные для отметки просмотра.")
                return
            try:
                tmdb_id = int(parts[2])
                type_ = parts[3] if len(parts) > 3 else "movie"  # default to movie if not specified
            except (ValueError, IndexError) as e:
                await callback.answer(f"Ошибка данных: {e}")
                return
        # Для действий like/dislike/reset формат: {action}_{tmdb_id}_{type_}
        else:
            if len(parts) < 3:
                await callback.answer("Ошибка: некорректные данные.")
                return
            try:
                tmdb_id = int(parts[1])
                type_ = parts[2]
            except (ValueError, IndexError) as e:
                await callback.answer(f"Ошибка данных: {e}")
                return

    print(f"DEBUG: handle_rating called with action={action}, tmdb_id={tmdb_id}, type_={type_}")

    user_rating = await get_user_rating(chat_id, tmdb_id, type_)
    print(f"DEBUG: user_rating before change = {user_rating}")

    liked = user_rating["liked"] if user_rating else False
    disliked = user_rating["disliked"] if user_rating else False
    watched = user_rating["watched"] if user_rating else False

    # Обновляем значения
    if action == "like":
        liked, disliked = (not liked, False) if liked else (True, False)
        await callback.answer("👍 Лайк добавлен!" if liked else "👍 Лайк убран!")
    elif action == "dislike":
        liked, disliked = (False, not disliked) if disliked else (False, True)
        await callback.answer("👎 Дизлайк добавлен!" if disliked else "👎 Дизлайк убран!")
    elif action == "reset":
        liked = disliked = False
        await callback.answer("🔄 Оценка сброшена!")
    elif action == "watch":
        watched = not watched
        await callback.answer("✅ Отмечено как просмотренное!" if watched else "👀 Снята отметка просмотренного!")

    print(f"DEBUG: after change - liked={liked}, disliked={disliked}, watched={watched}")

    details = get_item_details(type_, tmdb_id)
    title = details.get("title") or details.get("name") or "Без названия"

    success = await add_rating(chat_id, tmdb_id, type_, liked=liked, disliked=disliked, watched=watched, title=title)
    print(f"DEBUG: add_rating success = {success}")

    # Обновляем карточку
    avg_ratings = await get_ratings(tmdb_id, type_)
    watched_text = "✅ Вы смотрели" if watched else ""
    year = (details.get("release_date") or details.get("first_air_date") or "")[:4]
    rating_tmdb = details.get("vote_average") or "—"
    overview = details.get("overview") or "Описание отсутствует."
    if len(overview) > 2000:
        overview = overview[:2000] + "..."
    poster = f"https://image.tmdb.org/t/p/w500{details.get('poster_path')}" if details.get("poster_path") else None
    caption = (
        f"{title} ({year})\n"
        f"Рейтинг: {rating_tmdb} (TMDB)\n"
        f"👍 Лайки: {avg_ratings['likes']} | 👎 Дизлайки: {avg_ratings['dislikes']} | 👀 Просмотров: {avg_ratings['watches']}\n"
        f"{watched_text}\n\n{overview}"
    )

    try:
        if poster and callback.message.photo:
            await bot.edit_message_media(
                chat_id=chat_id,
                message_id=callback.message.message_id,
                media=types.InputMediaPhoto(media=poster, caption=caption),
                reply_markup=kb_collection_item(tmdb_id, type_, watched, liked, disliked)
            )
        else:
            await bot.edit_message_caption(
                chat_id=chat_id,
                message_id=callback.message.message_id,
                caption=caption,
                reply_markup=kb_collection_item(tmdb_id, type_, watched, liked, disliked)
            )
    except Exception as e:
        print(f"Ошибка при обновлении карточки: {e}")
        # Если не удалось отредактировать, просто отправляем уведомление
        await callback.answer("✅ Обновлено!")


# -------------------- RUN --------------------
async def main():
    await init_db()
    await set_bot_commands()
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())