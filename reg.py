import asyncio
import csv
import os
import re
from io import StringIO
from pathlib import Path

import aiosqlite
from aiogram import Bot, Dispatcher
from aiogram.exceptions import TelegramRetryAfter, TelegramForbiddenError, TelegramBadRequest
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import Message, BufferedInputFile
from dotenv import load_dotenv

# --- ENV ---
load_dotenv(Path(__file__).with_name(".env"))

BOT_TOKEN = os.getenv("BOT_TOKEN")
GROUP_CHAT_ID = os.getenv("GROUP_CHAT_ID")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))

DB_PATH = "registrations.db"

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN не задано. Перевір .env поруч із reg.py")
if not GROUP_CHAT_ID:
    raise RuntimeError("GROUP_CHAT_ID не задано. Перевір .env поруч із reg.py")

GROUP_CHAT_ID = int(GROUP_CHAT_ID)


class Reg(StatesGroup):
    first_name = State()
    last_name_or_nick = State()
    age = State()
    games = State()


def clean(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s+", " ", s)
    return s


def valid_first_name(s: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-zА-Яа-яЁёІіЇїЄєҐґ'’\- ]{2,50}", s))


def normalize_games_answer(s: str) -> str | None:
    t = clean(s).lower().replace("ё", "е")
    if t in {"так", "да", "yes", "y"}:
        return "так"
    if t in {"ні", "ни", "нет", "no", "n"}:
        return "ні"
    if t in {"не знаю", "незнаю", "не знаю.", "не знаю!", "не знаю?"}:
        return "не знаю"
    return None


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS registrations (
                tg_user_id INTEGER PRIMARY KEY,
                tg_username TEXT,
                first_name TEXT NOT NULL,
                last_name_or_nick TEXT NOT NULL,
                age INTEGER NOT NULL,
                games_answer TEXT,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # миграция для старой БД
        try:
            await db.execute("ALTER TABLE registrations ADD COLUMN games_answer TEXT")
        except Exception:
            pass

        await db.commit()


async def upsert_registration(
    tg_user_id: int,
    tg_username: str | None,
    first_name: str,
    last_name_or_nick: str,
    age: int,
    games_answer: str,
):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO registrations (tg_user_id, tg_username, first_name, last_name_or_nick, age, games_answer)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(tg_user_id) DO UPDATE SET
                tg_username=excluded.tg_username,
                first_name=excluded.first_name,
                last_name_or_nick=excluded.last_name_or_nick,
                age=excluded.age,
                games_answer=excluded.games_answer,
                updated_at=CURRENT_TIMESTAMP
        """, (tg_user_id, tg_username, first_name, last_name_or_nick, age, games_answer))
        await db.commit()


async def fetch_all():
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
            SELECT tg_user_id, tg_username, first_name, last_name_or_nick, age, games_answer, updated_at
            FROM registrations
            ORDER BY updated_at DESC
        """)
        return await cur.fetchall()


async def notify_group(bot: Bot, chat_id: int, text: str) -> bool:
    """
    Надёжная отправка в группу:
    - без Markdown (ничего не ломается на спецсимволах)
    - retry при TelegramRetryAfter
    - логирование ошибок
    Возвращает True/False (успешно/нет)
    """
    try:
        await bot.send_message(chat_id, text)
        return True
    except TelegramRetryAfter as e:
        wait_s = int(e.retry_after) + 1
        print(f"[GROUP] Rate limit. Sleep {wait_s}s then retry...")
        await asyncio.sleep(wait_s)
        try:
            await bot.send_message(chat_id, text)
            return True
        except Exception as e2:
            print(f"[GROUP] Retry failed: {e2}")
            return False
    except TelegramForbiddenError as e:
        print(f"[GROUP] Forbidden (нет прав/бот удалён/ограничен): {e}")
        return False
    except TelegramBadRequest as e:
        print(f"[GROUP] BadRequest: {e}\nTEXT={text}")
        return False
    except Exception as e:
        print(f"[GROUP] Unknown error: {e}")
        return False


async def notify_admin_fallback(bot: Bot, text: str):
    """Если задан ADMIN_ID — отправим тебе в личку как страховку."""
    if ADMIN_ID:
        try:
            await bot.send_message(ADMIN_ID, "⚠️ Не вдалося надіслати в групу. Ось реєстрація:\n\n" + text)
        except Exception as e:
            print(f"[ADMIN FALLBACK] Failed: {e}")


async def main():
    await init_db()

    bot = Bot(BOT_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())

    # убрать webhook-конфликт (если раньше был конструктор/вебхук)
    await bot.delete_webhook(drop_pending_updates=True)

    @dp.message(CommandStart())
    async def start(message: Message, state: FSMContext):
        await state.clear()
        await message.answer(
            "Реєстрація на захід 📝\n\n"
            "Вкажи *ім'я* учасника:",
            parse_mode="Markdown"
        )
        await state.set_state(Reg.first_name)

    @dp.message(Command("cancel"))
    async def cancel(message: Message, state: FSMContext):
        await state.clear()
        await message.answer("Скасовано. Щоб почати знову — /start")

    @dp.message(Command("export"))
    async def export_cmd(message: Message):
        if ADMIN_ID and message.from_user.id != ADMIN_ID:
            await message.answer("Ця команда доступна лише адміну.")
            return

        rows = await fetch_all()
        out = StringIO()
        writer = csv.writer(out)
        writer.writerow([
            "tg_user_id", "tg_username",
            "first_name", "last_name_or_nick",
            "age", "games_answer",
            "updated_at"
        ])
        writer.writerows(rows)

        data = out.getvalue().encode("utf-8")
        file = BufferedInputFile(data, filename="registrations.csv")
        await message.answer_document(file, caption=f"Всього реєстрацій: {len(rows)}")

    @dp.message(Reg.first_name)
    async def step_first_name(message: Message, state: FSMContext):
        name = clean(message.text)
        if not valid_first_name(name):
            await message.answer("Ім'я має бути літерами (можна з дефісом/апострофом). Спробуй ще раз.")
            return
        await state.update_data(first_name=name)
        await message.answer(
            "Тепер напиши *прізвище* або *нікнейм* учасника\n"
            "*(це потрібно для того, щоб підтвердити свою реєстрацію на вході)*:",
            parse_mode="Markdown"
        )
        await state.set_state(Reg.last_name_or_nick)

    @dp.message(Reg.last_name_or_nick)
    async def step_last_or_nick(message: Message, state: FSMContext):
        val = clean(message.text)
        if len(val) < 2 or len(val) > 50:
            await message.answer("Занадто коротко/довго. Напиши прізвище або нікнейм ще раз.")
            return
        await state.update_data(last_name_or_nick=val)
        await message.answer("Вкажи *вік* учасника (числом):", parse_mode="Markdown")
        await state.set_state(Reg.age)

    @dp.message(Reg.age)
    async def step_age(message: Message, state: FSMContext):
        txt = clean(message.text)
        if not txt.isdigit():
            await message.answer("Вік треба вказати числом. Наприклад: 18")
            return
        age = int(txt)
        if age < 5 or age > 120:
            await message.answer("Перевір вік — введи число від 5 до 120.")
            return

        await state.update_data(age=age)
        await message.answer(
            "Чи грав(-ла) учасник в одну або кілька з цих ігор: "
            "Діксіт, Коднеймс (Кодові імена), Каркасон або Кольт Експрес?\n\n"
            "Відповідь: *так / ні / не знаю*",
            parse_mode="Markdown"
        )
        await state.set_state(Reg.games)

    @dp.message(Reg.games)
    async def step_games(message: Message, state: FSMContext):
        ans = normalize_games_answer(message.text)
        if ans is None:
            await message.answer("Будь ласка, відповідай: *так* / *ні* / *не знаю*.", parse_mode="Markdown")
            return

        data = await state.get_data()
        first_name = data["first_name"]
        last_or_nick = data["last_name_or_nick"]
        age = data["age"]

        await upsert_registration(
            tg_user_id=message.from_user.id,
            tg_username=message.from_user.username,
            first_name=first_name,
            last_name_or_nick=last_or_nick,
            age=age,
            games_answer=ans
        )

        await message.answer(
            "✅ Реєстрацію збережено!\n"
            f"Ім'я: {first_name}\n"
            f"Прізвище/нік: {last_or_nick}\n"
            f"Вік: {age}\n"
            f"Досвід з іграми: {ans}\n\n"
            "Якщо треба змінити — натисни /start ще раз."
        )

        username = f"@{message.from_user.username}" if message.from_user.username else "—"

        group_text = (
            "📝 Нова реєстрація\n"
            f"• Ім'я: {first_name}\n"
            f"• Прізвище/нік: {last_or_nick}\n"
            f"• Вік: {age}\n"
            f"• Грав(-ла) в ці ігри?: {ans}\n"
            f"• TG: {username}\n"
            f"• ID: {message.from_user.id}"
        )

        ok = await notify_group(bot, GROUP_CHAT_ID, group_text)
        if not ok:
            await notify_admin_fallback(bot, group_text)

        await state.clear()

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
