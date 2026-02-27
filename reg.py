import asyncio
import csv
import os
import re
from io import StringIO
from pathlib import Path

import aiosqlite
from aiogram import Bot, Dispatcher
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
    raise RuntimeError("BOT_TOKEN не задано. Перевір .env поруч із bot.py")
if not GROUP_CHAT_ID:
    raise RuntimeError("GROUP_CHAT_ID не задано. Перевір .env поруч із bot.py")

GROUP_CHAT_ID = int(GROUP_CHAT_ID)


# --- FSM states ---
class Reg(StatesGroup):
    first_name = State()
    last_name_or_nick = State()
    age = State()
    games = State()


# --- helpers ---
def clean(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s+", " ", s)
    return s


def valid_first_name(s: str) -> bool:
    # букви (лат/кирилл/укр), пробіл, дефіс, апостроф
    return bool(re.fullmatch(r"[A-Za-zА-Яа-яЁёІіЇїЄєҐґ'’\- ]{2,50}", s))


def normalize_games_answer(s: str) -> str | None:
    """
    Повертає одне з: "так", "ні", "не знаю" або None, якщо невалідно.
    Дозволяємо також рос/укр варіанти для зручності.
    """
    t = clean(s).lower()
    t = t.replace("ё", "е")

    if t in {"так", "да", "yes", "y"}:
        return "так"
    if t in {"ні", "ни", "нет", "no", "n"}:
        return "ні"
    if t in {"не знаю", "незнаю", "не знаю.", "не знаю!", "не знаю?"}:
        return "не знаю"
    # інколи люди пишуть "не впевнений/не впевнена" — якщо хочеш, можна додати
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
        # Міграція для старої БД (якщо таблиця вже була без games_answer)
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


async def main():
    await init_db()

    bot = Bot(BOT_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())

    # прибираємо конфлікт, якщо раніше був webhook (Manybot тощо)
    await bot.delete_webhook(drop_pending_updates=True)

    # --- commands ---
    @dp.message(CommandStart())
    async def start(message: Message, state: FSMContext):
        await state.clear()
        await message.answer(
            "Вкажи *ім'я* учасника:",
            parse_mode="Markdown"
        )
        await state.set_state(Reg.first_name)

    @dp.message(Command("cancel"))
    async def cancel(message: Message, state: FSMContext):
        await state.clear()
        await message.answer("Скасовано. Щоб почати знову — /start")

    @dp.message(Command("myid"))
    async def myid(message: Message):
        await message.answer(f"your_user_id: {message.from_user.id}")

    @dp.message(Command("chatid"))
    async def chatid(message: Message):
        await message.answer(f"chat_id: {message.chat.id}")

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

    # --- registration flow ---
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

        # повідомлення користувачу
        await message.answer(
            "✅ Реєстрацію збережено!\n"
            f"Ім'я: {first_name}\n"
            f"Прізвище/нік: {last_or_nick}\n"
            f"Вік: {age}\n"
            f"Досвід з іграми: {ans}\n\n"
            "Якщо треба змінити — натисни /start ще раз."
        )

        # повідомлення в групу
        username = f"@{message.from_user.username}" if message.from_user.username else "—"
        await bot.send_message(
            GROUP_CHAT_ID,
            "📝 *Нова реєстрація*\n"
            f"• Ім'я: *{first_name}*\n"
            f"• Прізвище/нік: *{last_or_nick}*\n"
            f"• Вік: *{age}*\n"
            f"• Грав(-ла) в ці ігри?: *{ans}*\n"
            f"• TG: {username}\n"
            f"• ID: `{message.from_user.id}`",
            parse_mode="Markdown"
        )

        await state.clear()

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())