import asyncio
import logging
import sqlite3
import random
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton

# ================= КОНФИГУРАЦИЯ =================
API_TOKEN = '8216927939:AAHqPq7GYVl8O_ADU5XRdqMWppB6B2mtdVU'
ADMIN_ID = 5641246005
DB_NAME = 'esha_bot.db'

# Настройки редкости и автоматические цены
RARITY_EMOJI = {
    "Common": "⚪",
    "Rare": "🔵", 
    "Epic": "🟣", 
    "Legendary": "🟡"
}

RARITY_PRICES = {
    "Common": 500,
    "Rare": 850,
    "Epic": 1500,
    "Legendary": 3000
}

# ================= БАЗА ДАННЫХ =================
class Database:
    def __init__(self, db_file):
        self.conn = sqlite3.connect(db_file)
        self.cursor = self.conn.cursor()
        self.create_tables()

    def _ensure_column_exists(self, table_name, column_name, column_type):
        self.cursor.execute(f"PRAGMA table_info({table_name})")
        existing_columns = [col[1] for col in self.cursor.fetchall()]
        
        if column_name not in existing_columns:
            self.cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")
            logging.info(f"База обновлена: в таблицу {table_name} добавлена колонка {column_name}")

    def create_tables(self):
        # Базовые таблицы
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            balance INTEGER DEFAULT 0,
            last_esha TIMESTAMP,
            last_bonus TIMESTAMP,
            custom_cooldown INTEGER DEFAULT 0
        )''')
        
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS cards (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE,
            rarity TEXT,
            media_type TEXT,
            media_content TEXT,
            price INTEGER,
            weight INTEGER
        )''')
        
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS inventory (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            card_id INTEGER,
            FOREIGN KEY(card_id) REFERENCES cards(id)
        )''')
        
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS market (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            inventory_id INTEGER UNIQUE,
            price INTEGER,
            seller_id INTEGER,
            FOREIGN KEY(inventory_id) REFERENCES inventory(id)
        )''')
        
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS stats (
            user_id INTEGER PRIMARY KEY,
            total_cards INTEGER DEFAULT 0
        )''')

        # Таблица для глобальных настроек бота
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )''')

        # ТАБЛИЦА АУКЦИОНОВ
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS auctions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            inventory_id INTEGER UNIQUE,
            seller_id INTEGER,
            start_price INTEGER,
            current_price INTEGER,
            highest_bidder_id INTEGER,
            comment TEXT,
            FOREIGN KEY(inventory_id) REFERENCES inventory(id)
        )''')
        
        self.conn.commit()

        # Умное обновление структуры (миграции)
        self._ensure_column_exists('users', 'nickname', 'TEXT')
        self.conn.commit()

    # --- Настройки бота ---
    def get_setting(self, key, default=None):
        res = self.cursor.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
        return res[0] if res else default

    def set_setting(self, key, value):
        self.cursor.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, str(value)))
        self.conn.commit()

    # --- Игроки ---
    def add_user(self, user_id):
        self.cursor.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
        self.cursor.execute("INSERT OR IGNORE INTO stats (user_id) VALUES (?)", (user_id,))
        self.conn.commit()

    def set_nickname(self, user_id, nickname):
        existing = self.cursor.execute("SELECT user_id FROM users WHERE nickname = ?", (nickname,)).fetchone()
        if existing and existing[0] != user_id:
            return False 
            
        self.cursor.execute("UPDATE users SET nickname = ? WHERE user_id = ?", (nickname, user_id))
        self.conn.commit()
        return True

    def get_nickname(self, user_id):
        res = self.cursor.execute("SELECT nickname FROM users WHERE user_id = ?", (user_id,)).fetchone()
        return res[0] if res else None

    # --- Управление картами (Глобальное) ---
    def add_card(self, name, rarity, m_type, m_content, price, weight):
        try:
            self.cursor.execute("INSERT INTO cards (name, rarity, media_type, media_content, price, weight) VALUES (?, ?, ?, ?, ?, ?)",
                                (name, rarity, m_type, m_content, price, weight))
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.IntegrityError:
            return None

    def delete_card(self, card_id):
        self.cursor.execute("DELETE FROM market WHERE inventory_id IN (SELECT id FROM inventory WHERE card_id = ?)", (card_id,))
        self.cursor.execute("DELETE FROM auctions WHERE inventory_id IN (SELECT id FROM inventory WHERE card_id = ?)", (card_id,))
        self.cursor.execute("DELETE FROM inventory WHERE card_id = ?", (card_id,))
        self.cursor.execute("DELETE FROM cards WHERE id = ?", (card_id,))
        if self.cursor.rowcount > 0:
            self.conn.commit()
            return True
        return False

    def get_all_cards(self):
        return self.cursor.execute("SELECT * FROM cards").fetchall()

    def give_card(self, user_id, card_id):
        self.cursor.execute("INSERT INTO inventory (user_id, card_id) VALUES (?, ?)", (user_id, card_id))
        self.cursor.execute("UPDATE stats SET total_cards = total_cards + 1 WHERE user_id = ?", (user_id,))
        self.conn.commit()

    # --- Новый Инвентарь ---
    def get_card_counts(self, user_id):
        """Возвращает словарь {Редкость: количество_карт}"""
        query = '''
        SELECT c.rarity, COUNT(i.id) 
        FROM inventory i JOIN cards c ON i.card_id = c.id
        WHERE i.user_id = ? 
        AND i.id NOT IN (SELECT inventory_id FROM market)
        AND i.id NOT IN (SELECT inventory_id FROM auctions)
        GROUP BY c.rarity
        '''
        return dict(self.cursor.execute(query, (user_id,)).fetchall())

    def get_user_inventory_by_rarity(self, user_id, rarity):
        query = '''
        SELECT c.name, COUNT(i.id), c.id, c.price 
        FROM inventory i JOIN cards c ON i.card_id = c.id 
        WHERE i.user_id = ? AND c.rarity = ? 
        AND i.id NOT IN (SELECT inventory_id FROM market)
        AND i.id NOT IN (SELECT inventory_id FROM auctions)
        GROUP BY c.name
        '''
        return self.cursor.execute(query, (user_id, rarity)).fetchall()

    def get_card_details(self, user_id, card_id):
        query = '''
        SELECT c.name, c.rarity, COUNT(i.id), c.price 
        FROM inventory i JOIN cards c ON i.card_id = c.id 
        WHERE i.user_id = ? AND c.id = ? 
        AND i.id NOT IN (SELECT inventory_id FROM market)
        AND i.id NOT IN (SELECT inventory_id FROM auctions)
        '''
        return self.cursor.execute(query, (user_id, card_id)).fetchone()

    # --- Экономика (Скупщик и Маркет) ---
    def sell_fast(self, user_id, card_id):
        card = self.cursor.execute("SELECT price FROM cards WHERE id = ?", (card_id,)).fetchone()
        if not card: return False
        
        inv_item = self.cursor.execute('''
            SELECT id FROM inventory 
            WHERE user_id = ? AND card_id = ? 
            AND id NOT IN (SELECT inventory_id FROM market) 
            AND id NOT IN (SELECT inventory_id FROM auctions) LIMIT 1
        ''', (user_id, card_id)).fetchone()
        
        if not inv_item: return False
        
        sell_price = int(card[0] * 0.5)
        self.cursor.execute("DELETE FROM inventory WHERE id = ?", (inv_item[0],))
        self.cursor.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (sell_price, user_id))
        self.conn.commit()
        return sell_price

    def sell_all_fast(self, user_id, card_id):
        card = self.cursor.execute("SELECT price FROM cards WHERE id = ?", (card_id,)).fetchone()
        if not card: return False
        
        items = self.cursor.execute('''
            SELECT id FROM inventory WHERE user_id = ? AND card_id = ?
            AND id NOT IN (SELECT inventory_id FROM market) 
            AND id NOT IN (SELECT inventory_id FROM auctions)
        ''', (user_id, card_id)).fetchall()
        
        if not items: return False
        
        count = len(items)
        sell_price_per_item = int(card[0] * 0.5)
        total_profit = count * sell_price_per_item
        
        item_ids = [str(i[0]) for i in items]
        self.cursor.execute(f"DELETE FROM inventory WHERE id IN ({','.join(item_ids)})")
        self.cursor.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (total_profit, user_id))
        self.conn.commit()
        return total_profit, count

    def list_on_market(self, user_id, card_id, price):
        if price % 10 != 0: return "invalid_price"
        inv_item = self.cursor.execute('''
            SELECT id FROM inventory 
            WHERE user_id = ? AND card_id = ? 
            AND id NOT IN (SELECT inventory_id FROM market) 
            AND id NOT IN (SELECT inventory_id FROM auctions) LIMIT 1
        ''', (user_id, card_id)).fetchone()

        if not inv_item: return "no_item"

        self.cursor.execute("INSERT INTO market (inventory_id, price, seller_id) VALUES (?, ?, ?)", 
                            (inv_item[0], price, user_id))
        self.conn.commit()
        return "success"

    def get_market_listings(self):
        query = '''
        SELECT m.id, c.name, c.rarity, m.price, m.seller_id 
        FROM market m
        JOIN inventory i ON m.inventory_id = i.id
        JOIN cards c ON i.card_id = c.id
        '''
        return self.cursor.execute(query).fetchall()

    def buy_item(self, buyer_id, market_id):
        try:
            lot = self.cursor.execute('''
                SELECT m.inventory_id, m.price, m.seller_id, c.name 
                FROM market m
                JOIN inventory i ON m.inventory_id = i.id
                JOIN cards c ON i.card_id = c.id
                WHERE m.id = ?
            ''', (market_id,)).fetchone()
            
            if not lot: return "not_found"
            inv_id, price, seller_id, card_name = lot

            buyer_bal = self.cursor.execute("SELECT balance FROM users WHERE user_id = ?", (buyer_id,)).fetchone()[0]
            if buyer_bal < price: return "no_money"
            if buyer_id == seller_id: return "self_buy"

            seller_income = int(price * 0.9)
            
            self.cursor.execute("UPDATE users SET balance = balance - ? WHERE user_id = ?", (price, buyer_id))
            self.cursor.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (seller_income, seller_id))
            self.cursor.execute("UPDATE inventory SET user_id = ? WHERE id = ?", (buyer_id, inv_id))
            self.cursor.execute("DELETE FROM market WHERE id = ?", (market_id,))
            self.conn.commit()
            
            return {"status": "success", "seller_id": seller_id, "card_name": card_name, "price": price, "income": seller_income}
        except Exception as e:
            self.conn.rollback()
            return "error"

    # --- АУКЦИОНЫ ---
    def start_auction(self, user_id, card_id, start_price, comment):
        inv_item = self.cursor.execute('''
            SELECT id FROM inventory WHERE user_id = ? AND card_id = ? 
            AND id NOT IN (SELECT inventory_id FROM market) 
            AND id NOT IN (SELECT inventory_id FROM auctions) LIMIT 1
        ''', (user_id, card_id)).fetchone()
        
        if not inv_item: return False

        self.cursor.execute('''INSERT INTO auctions (inventory_id, seller_id, start_price, current_price, comment)
                               VALUES (?, ?, ?, ?, ?)''', (inv_item[0], user_id, start_price, start_price, comment))
        self.conn.commit()
        return True

    def get_active_auctions(self):
        query = '''
            SELECT a.id, c.name, c.rarity, a.current_price, u.nickname, a.comment, a.seller_id 
            FROM auctions a 
            JOIN inventory i ON a.inventory_id = i.id 
            JOIN cards c ON i.card_id = c.id 
            LEFT JOIN users u ON a.highest_bidder_id = u.user_id
        '''
        return self.cursor.execute(query).fetchall()

    def get_my_auctions(self, user_id):
        query = '''
            SELECT a.id, c.name, a.current_price, a.highest_bidder_id 
            FROM auctions a 
            JOIN inventory i ON a.inventory_id = i.id 
            JOIN cards c ON i.card_id = c.id 
            WHERE a.seller_id = ?
        '''
        return self.cursor.execute(query, (user_id,)).fetchall()

    def place_bid(self, user_id, auction_id, bid_amount):
        try:
            auction = self.cursor.execute("SELECT current_price, highest_bidder_id, seller_id FROM auctions WHERE id = ?", (auction_id,)).fetchone()
            if not auction: return "not_found"
            current_price, prev_bidder, seller_id = auction
            
            if user_id == seller_id: return "self_bid"
            if bid_amount <= current_price: return "low_bid"
            
            buyer_bal = self.cursor.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,)).fetchone()[0]
            if buyer_bal < bid_amount: return "no_money"

            # Атомарная транзакция: списываем у нового, возвращаем старому
            self.cursor.execute("UPDATE users SET balance = balance - ? WHERE user_id = ?", (bid_amount, user_id))
            if prev_bidder:
                self.cursor.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (current_price, prev_bidder))
                
            self.cursor.execute("UPDATE auctions SET current_price = ?, highest_bidder_id = ? WHERE id = ?", (bid_amount, user_id, auction_id))
            self.conn.commit()
            return "success"
        except Exception as e:
            self.conn.rollback()
            return "error"

    def close_auction(self, user_id, auction_id):
        try:
            auction = self.cursor.execute("SELECT inventory_id, current_price, highest_bidder_id, seller_id FROM auctions WHERE id = ?", (auction_id,)).fetchone()
            if not auction: return "not_found"
            inv_id, price, bidder_id, seller_id = auction
            
            if user_id != seller_id: return "not_yours"
            
            if bidder_id:
                # Если были ставки: деньги продавцу, карту победителю
                self.cursor.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (price, seller_id))
                self.cursor.execute("UPDATE inventory SET user_id = ? WHERE id = ?", (bidder_id, inv_id))
            
            self.cursor.execute("DELETE FROM auctions WHERE id = ?", (auction_id,))
            self.conn.commit()
            return {"status": "success", "bidder": bidder_id, "price": price}
        except:
            self.conn.rollback()
            return "error"

db = Database(DB_NAME)

# ================= ИНИЦИАЛИЗАЦИЯ БОТА =================
bot = Bot(token=API_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# ================= FSM СОСТОЯНИЯ =================
class AdminAddCard(StatesGroup):
    name = State()
    rarity = State()
    media = State()
    weight = State()

class AdminDeleteCard(StatesGroup):
    card_id = State()

class UserMarketAction(StatesGroup):
    waiting_price = State()
    card_id = State()

class UserRegister(StatesGroup):
    nickname = State()

class AuctionSetup(StatesGroup): 
    card_id = State()
    start_price = State()
    comment = State()

class AuctionBid(StatesGroup):
    auction_id = State()
    bid_amount = State()

# ================= КЛАВИАТУРЫ =================
def main_kb():
    kb = [
        [KeyboardButton(text="/esha 🎲"), KeyboardButton(text="/profile 👤")],
        [KeyboardButton(text="/market 🏪"), KeyboardButton(text="/auction ⚖️")],
        [KeyboardButton(text="/bonus 💰"), KeyboardButton(text="/top 🏆")]
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)

async def check_registration(message: types.Message) -> bool:
    if not db.get_nickname(message.from_user.id):
        await message.answer("Сначала тебе нужно придумать никнейм!\nНажми /start, чтобы начать регистрацию.")
        return False
    return True

def get_admin_keyboard():
    no_cd_status = db.get_setting("no_cooldown", "0")
    btn_cd_text = "⚡ Без Задержки: ВКЛ" if no_cd_status == "1" else "⚡ Без Задержки: ВЫКЛ"
    
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Добавить новую карту", callback_data="admin_add_card")],
        [InlineKeyboardButton(text="🗑 Удалить карту", callback_data="admin_delete_card")],
        [InlineKeyboardButton(text="🧹 Очистить маркетплейс", callback_data="admin_clear_market")],
        [InlineKeyboardButton(text=btn_cd_text, callback_data="admin_toggle_cooldown")]
    ])

# ================= АДМИН ПАНЕЛЬ =================
@dp.message(Command("admin"), F.from_user.id == ADMIN_ID)
async def cmd_admin(message: types.Message):
    text = (
        "🛠 <b>Админ-панель</b>\n\n"
        "Выберите действие с помощью кнопок ниже или используйте команду:\n"
        "👉 <code>/give_money [ID] [СУММА]</code> — выдать монеты игроку"
    )
    await message.answer(text, reply_markup=get_admin_keyboard(), parse_mode="HTML")

@dp.callback_query(F.data == "admin_toggle_cooldown", F.from_user.id == ADMIN_ID)
async def admin_toggle_cooldown_cb(callback: types.CallbackQuery):
    current = db.get_setting("no_cooldown", "0")
    new_val = "1" if current == "0" else "0"
    db.set_setting("no_cooldown", new_val)
    
    state_str = "ВКЛЮЧЕН ✅" if new_val == "1" else "ВЫКЛЮЧЕН ❌"
    await callback.message.answer(f"Режим 'Без задержки' для всех игроков <b>{state_str}</b>!", parse_mode="HTML")
    await callback.message.edit_reply_markup(reply_markup=get_admin_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "admin_clear_market", F.from_user.id == ADMIN_ID)
async def admin_clear_market_cb(callback: types.CallbackQuery):
    db.cursor.execute("DELETE FROM market")
    db.conn.commit()
    await callback.message.answer("Маркетплейс очищен от всех лотов 🧹")
    await callback.answer()

# --- Логика УДАЛЕНИЯ карты ---
@dp.callback_query(F.data == "admin_delete_card", F.from_user.id == ADMIN_ID)
async def admin_start_delete_cb(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer("Введите ID карты для удаления (она будет стерта из базы, инвентарей и маркета):")
    await state.set_state(AdminDeleteCard.card_id)
    await callback.answer()

@dp.message(AdminDeleteCard.card_id, F.from_user.id == ADMIN_ID)
async def admin_process_delete(message: types.Message, state: FSMContext):
    if not message.text.isdigit():
        return await message.answer("ID должен быть числом. Попробуйте еще раз:")
    
    card_id = int(message.text)
    success = db.delete_card(card_id)
    
    if success:
        await message.answer(f"Карта с ID <b>{card_id}</b> была успешно и полностью удалена из игры! 🗑✅", parse_mode="HTML")
    else:
        await message.answer(f"Ошибка: Карта с ID {card_id} не найдена в базе. ❌")
    await state.clear()

# --- Логика ДОБАВЛЕНИЯ карты ---
@dp.callback_query(F.data == "admin_add_card", F.from_user.id == ADMIN_ID)
async def admin_start_add_cb(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer("Введите название новой карты:")
    await state.set_state(AdminAddCard.name)
    await callback.answer()

@dp.message(AdminAddCard.name, F.from_user.id == ADMIN_ID)
async def admin_set_name(message: types.Message, state: FSMContext):
    await state.update_data(name=message.text)
    await message.answer("Выберите редкость (Common, Rare, Epic, Legendary):\n*Цена выставится автоматически.*")
    await state.set_state(AdminAddCard.rarity)

@dp.message(AdminAddCard.rarity, F.from_user.id == ADMIN_ID)
async def admin_set_rarity(message: types.Message, state: FSMContext):
    if message.text not in RARITY_EMOJI.keys():
        return await message.answer("Неверная редкость. Введите: Common, Rare, Epic или Legendary.")
    await state.update_data(rarity=message.text)
    await message.answer("Отправьте фото карты или эмодзи:")
    await state.set_state(AdminAddCard.media)

@dp.message(AdminAddCard.media, F.from_user.id == ADMIN_ID)
async def admin_set_media(message: types.Message, state: FSMContext):
    if message.photo:
        m_type = "photo"
        m_content = message.photo[-1].file_id
    else:
        m_type = "text"
        m_content = message.text
    
    await state.update_data(m_type=m_type, m_content=m_content)
    await message.answer("Введите вес вероятности (число, чем больше - тем чаще падает):")
    await state.set_state(AdminAddCard.weight)

@dp.message(AdminAddCard.weight, F.from_user.id == ADMIN_ID)
async def admin_finish(message: types.Message, state: FSMContext):
    if not message.text.isdigit():
        return await message.answer("Нужно число.")
    
    data = await state.get_data()
    price = RARITY_PRICES[data['rarity']]
    
    new_card_id = db.add_card(data['name'], data['rarity'], data['m_type'], data['m_content'], price, int(message.text))
    
    if new_card_id:
        await message.answer(f"Карта <b>{data['name']}</b> успешно добавлена!\nУстановлена базовая цена: {price} 💰\n<b>ID карты: <code>{new_card_id}</code></b> ✅", parse_mode="HTML")
    else:
        await message.answer("Ошибка: Карта с таким именем уже есть.")
    await state.clear()

@dp.message(Command("give_money"), F.from_user.id == ADMIN_ID)
async def give_money(message: types.Message, command: CommandObject):
    if not command.args: return await message.answer("Использование: /give_money ID СУММА")
    try:
        uid, amount = command.args.split()
        db.cursor.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (int(amount), int(uid)))
        db.conn.commit()
        await message.answer(f"Выдано {amount} монет пользователю {uid} ✅")
    except:
        await message.answer("Ошибка формата. Пример: /give_money 123456789 1000")

# ================= РЕГИСТРАЦИЯ И СТАРТ =================
@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    db.add_user(message.from_user.id)
    nickname = db.get_nickname(message.from_user.id)
    
    if not nickname:
        await message.answer(
            "Привет! Я кот Еша 🐱.\nЧтобы начать играть, придумай себе уникальный игровой никнейм (до 13 символов):", 
            reply_markup=types.ReplyKeyboardRemove()
        )
        await state.set_state(UserRegister.nickname)
    else:
        await message.answer(
            f"С возвращением, {nickname}! 👋\nСобирай карты, торгуй и стань богатейшим котиком!",
            reply_markup=main_kb()
        )

@dp.message(UserRegister.nickname)
async def process_nickname(message: types.Message, state: FSMContext):
    nick = message.text.strip()
    
    if len(nick) > 13:
        return await message.answer("Слишком длинный ник! Максимум 13 символов. Попробуй еще раз:")
    if len(nick) < 2:
        return await message.answer("Слишком короткий ник! Минимум 2 символа. Попробуй еще раз:")
    
    success = db.set_nickname(message.from_user.id, nick)
    
    if success:
        await state.clear()
        await message.answer(f"Отлично, <b>{nick}</b>! Добро пожаловать в игру 🎉", reply_markup=main_kb(), parse_mode="HTML")
    else:
        await message.answer("Этот ник уже занят кем-то другим 😿. Придумай другой:")

# ================= ИГРОВЫЕ МЕХАНИКИ =================
@dp.message(Command("esha"))
async def cmd_esha(message: types.Message):
    if not await check_registration(message): return
    
    user_id = message.from_user.id
    user = db.cursor.execute("SELECT last_esha, custom_cooldown FROM users WHERE user_id = ?", (user_id,)).fetchone()
    last_time_str = user[0]
    
    no_cooldown = db.get_setting("no_cooldown", "0") == "1"
    
    if last_time_str and not no_cooldown:
        last_time = datetime.fromisoformat(last_time_str)
        wait_time = user[1] if user[1] > 0 else 3600 
        if datetime.now() - last_time < timedelta(seconds=wait_time):
            left = int(wait_time - (datetime.now() - last_time).total_seconds())
            return await message.answer(f"Еша спит 😴. Приходи через {left // 60} мин.")

    cards = db.get_all_cards()
    if not cards:
        return await message.answer("Админ еще не добавил карты в базу! 😿")
    
    weights = [c[6] for c in cards]
    chosen_card = random.choices(cards, weights=weights, k=1)[0]
    
    db.give_card(user_id, chosen_card[0])
    db.cursor.execute("UPDATE users SET last_esha = ? WHERE user_id = ?", (datetime.now().isoformat(), user_id))
    db.conn.commit()
    
    caption = f"✨ Ты получил: <b>{chosen_card[1]}</b>\nРедкость: {RARITY_EMOJI[chosen_card[2]]} {chosen_card[2]}\nОценка: {chosen_card[5]} 💰"
    
    if chosen_card[3] == "photo":
        await message.answer_photo(photo=chosen_card[4], caption=caption, parse_mode="HTML")
    else:
        await message.answer(f"{chosen_card[4]}\n\n{caption}", parse_mode="HTML")

@dp.message(Command("bonus"))
async def cmd_bonus(message: types.Message):
    if not await check_registration(message): return
    
    user_id = message.from_user.id
    user = db.cursor.execute("SELECT last_bonus FROM users WHERE user_id = ?", (user_id,)).fetchone()
    
    if user and user[0]:
        last_time = datetime.fromisoformat(user[0])
        if datetime.now() - last_time < timedelta(hours=12):
            return await message.answer("Бонус доступен раз в 12 часов! ⏳")
            
    amount = 500
    db.cursor.execute("UPDATE users SET balance = balance + ?, last_bonus = ? WHERE user_id = ?", (amount, datetime.now().isoformat(), user_id))
    db.conn.commit()
    await message.answer(f"Еша дал тебе {amount} монет! 💰")

# ================= ИНВЕНТАРЬ ПО РЕДКОСТЯМ =================
@dp.message(Command("profile"))
async def cmd_profile(message: types.Message):
    if not await check_registration(message): return
    uid = message.from_user.id
    nickname = db.get_nickname(uid)
    bal = db.cursor.execute("SELECT balance FROM users WHERE user_id = ?", (uid,)).fetchone()[0]
    counts = db.get_card_counts(uid)
    
    text = f"👤 <b>Профиль: {nickname}</b>\n💰 Баланс: {bal}\n\n📦 <b>Инвентарь:</b>\nВыберите раздел для просмотра своих карточек:"
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"⚪ Common ({counts.get('Common', 0)})", callback_data="inv_Common"),
         InlineKeyboardButton(text=f"🔵 Rare ({counts.get('Rare', 0)})", callback_data="inv_Rare")],
        [InlineKeyboardButton(text=f"🟣 Epic ({counts.get('Epic', 0)})", callback_data="inv_Epic"),
         InlineKeyboardButton(text=f"🟡 Legendary ({counts.get('Legendary', 0)})", callback_data="inv_Legendary")]
    ])
    await message.answer(text, reply_markup=kb, parse_mode="HTML")

@dp.callback_query(F.data.startswith("inv_"))
async def inventory_page(callback: types.CallbackQuery):
    if callback.data == "inv_back":
        await callback.message.delete()
        return await cmd_profile(callback.message)

    rarity = callback.data.split("_")[1]
    uid = callback.from_user.id
    items = db.get_user_inventory_by_rarity(uid, rarity)
    
    text = f"📦 Твои карточки редкости <b>{rarity} {RARITY_EMOJI[rarity]}</b>:\n\n"
    if not items:
        text += "<i>У тебя пока нет карточек этой редкости...</i>"
    else:
        for item in items:
            # item[0]=name, item[1]=count, item[2]=card_id, item[3]=price
            text += f"▪️ <b>{item[0]}</b> (x{item[1]}) 👉 Управление: /card_{item[2]}\n"
    
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад в меню", callback_data="inv_back")]])
    await callback.message.edit_text(text, reply_markup=kb, parse_mode="HTML")

# ================= УПРАВЛЕНИЕ КОНКРЕТНОЙ КАРТОЙ =================
@dp.message(F.text.startswith("/card_"))
async def card_manage_menu(message: types.Message):
    if not await check_registration(message): return
    try:
        card_id = int(message.text.split("_")[1])
        details = db.get_card_details(message.from_user.id, card_id)
        if not details or details[2] == 0:
            return await message.answer("У вас нет этой карты (возможно она выставлена на аукцион или маркет).")
        
        name, rarity, count, price = details
        text = (
            f"🃏 <b>{name}</b> {RARITY_EMOJI[rarity]}\n"
            f"В наличии: {count} шт.\n"
            f"Базовая цена: {price} 💰\n\n"
            f"<i>Выберите действие с этой карточкой:</i>"
        )
        
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"Сдать 1 шт за {int(price*0.5)}💰", callback_data=f"act_sell1_{card_id}")],
            [InlineKeyboardButton(text=f"Сдать ВСЕ ({count}шт) за {int(price*0.5*count)}💰", callback_data=f"act_sellall_{card_id}")],
            [InlineKeyboardButton(text="⚖️ Начать аукцион", callback_data=f"act_auction_{card_id}")],
            [InlineKeyboardButton(text="🏪 На маркетплейс", callback_data=f"act_market_{card_id}")]
        ])
        await message.answer(text, reply_markup=kb, parse_mode="HTML")
    except Exception as e:
        await message.answer("Ошибка вызова меню карты.")

@dp.callback_query(F.data.startswith("act_sell1_"))
async def act_sell1(c: types.CallbackQuery):
    cid = int(c.data.split("_")[2])
    profit = db.sell_fast(c.from_user.id, cid)
    if profit: 
        await c.message.edit_text(f"✅ Карточка успешно продана скупщику за {profit} монет.")
    else: 
        await c.answer("Ошибка: карта не найдена в инвентаре.", show_alert=True)

@dp.callback_query(F.data.startswith("act_sellall_"))
async def act_sellall(c: types.CallbackQuery):
    cid = int(c.data.split("_")[2])
    res = db.sell_all_fast(c.from_user.id, cid)
    if res: 
        await c.message.edit_text(f"✅ Успешно продано {res[1]} шт. скупщику за общую сумму {res[0]} монет!")
    else: 
        await c.answer("Ошибка: карты не найдены в инвентаре.", show_alert=True)

# --- Маркет из меню карты ---
@dp.callback_query(F.data.startswith("act_market_"))
async def act_market_start(c: types.CallbackQuery, state: FSMContext):
    cid = int(c.data.split("_")[2])
    await state.update_data(card_id=cid)
    await c.message.answer("Введи цену для продажи на маркетплейсе (должна быть кратна 10):")
    await state.set_state(UserMarketAction.waiting_price)
    await c.answer()

@dp.message(UserMarketAction.waiting_price)
async def act_market_finish(m: types.Message, state: FSMContext):
    if not m.text.isdigit(): return await m.answer("Пожалуйста, введите число!")
    data = await state.get_data()
    price = int(m.text)
    
    res = db.list_on_market(m.from_user.id, data['card_id'], price)
    if res == "success": 
        await m.answer(f"✅ Карточка успешно выставлена на маркет за {price} монет!")
    elif res == "invalid_price":
        await m.answer("Ошибка: Цена должна быть кратна 10!")
    else: 
        await m.answer("Ошибка: карточка не найдена в инвентаре.")
    await state.clear()

# ================= АУКЦИОНЫ =================
@dp.callback_query(F.data.startswith("act_auction_"))
async def setup_auction(c: types.CallbackQuery, state: FSMContext):
    cid = int(c.data.split("_")[2])
    await state.update_data(card_id=cid)
    await c.message.answer("Введите начальную цену для аукциона (число):")
    await state.set_state(AuctionSetup.start_price)
    await c.answer()

@dp.message(AuctionSetup.start_price)
async def setup_auction_price(m: types.Message, state: FSMContext):
    if not m.text.isdigit(): return await m.answer("Пожалуйста, введите число!")
    await state.update_data(start_price=int(m.text))
    await m.answer("Введите короткий комментарий к лоту (до 100 символов, например 'Срочно нужны деньги!'):")
    await state.set_state(AuctionSetup.comment)

@dp.message(AuctionSetup.comment)
async def setup_auction_finish(m: types.Message, state: FSMContext):
    comment = m.text[:100]
    data = await state.get_data()
    if db.start_auction(m.from_user.id, data['card_id'], data['start_price'], comment):
        await m.answer("⚖️ Аукцион успешно запущен! Посмотреть его можно в меню /auction")
    else:
        await m.answer("Ошибка запуска аукциона: карта не найдена.")
    await state.clear()

@dp.message(Command("auction"))
async def cmd_auction(message: types.Message):
    if not await check_registration(message): return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔍 Активные аукционы", callback_data="auc_active")],
        [InlineKeyboardButton(text="📦 Мои аукционы", callback_data="auc_my")]
    ])
    await message.answer("⚖️ <b>Аукционный дом</b>\n\nЗдесь вы можете делать ставки на редкие карты других игроков.", reply_markup=kb, parse_mode="HTML")

@dp.callback_query(F.data == "auc_active")
async def show_active_auctions(c: types.CallbackQuery):
    aucs = db.get_active_auctions()
    if not aucs: return await c.message.edit_text("Сейчас нет активных аукционов 🍃")
    
    text = "🔍 <b>Активные торги:</b>\n\n"
    for a in aucs:
        # a = (id, name, rarity, cur_price, high_bidder_nick, comment, seller_id)
        bidder = a[4] if a[4] else "Нет ставок"
        text += f"Лот <code>{a[0]}</code> | {RARITY_EMOJI[a[2]]} <b>{a[1]}</b>\n"
        text += f"💬: <i>{a[5]}</i>\n"
        text += f"💰 Текущая ставка: <b>{a[3]}</b> (Лидер: {bidder})\n"
        text += f"Перебить ставку: /bid_{a[0]}\n\n"
        
    await c.message.edit_text(text, parse_mode="HTML")

@dp.callback_query(F.data == "auc_my")
async def show_my_auctions(c: types.CallbackQuery):
    aucs = db.get_my_auctions(c.from_user.id)
    if not aucs: return await c.message.edit_text("У вас нет активных аукционов.")
    
    text = "📦 <b>Ваши лоты на аукционе:</b>\n\n"
    for a in aucs:
        # a = (id, name, cur_price, bidder_id)
        status = "Есть ставка!" if a[3] else "Ждет ставок"
        text += f"Лот <code>{a[0]}</code> | <b>{a[1]}</b> | Ставка: {a[2]} ({status})\n"
        text += f"👉 Закрыть и забрать прибыль: /close_auc_{a[0]}\n\n"
        
    await c.message.edit_text(text, parse_mode="HTML")

@dp.message(F.text.startswith("/bid_"))
async def start_bid(message: types.Message, state: FSMContext):
    if not await check_registration(message): return
    try:
        auc_id = int(message.text.split("_")[1])
        await state.update_data(auction_id=auc_id)
        await message.answer(f"Вы ставите на лот <code>{auc_id}</code>.\nВведите сумму вашей ставки (должна быть больше текущей):", parse_mode="HTML")
        await state.set_state(AuctionBid.bid_amount)
    except:
        await message.answer("Ошибка в команде.")

@dp.message(AuctionBid.bid_amount)
async def process_bid(message: types.Message, state: FSMContext):
    if not message.text.isdigit(): return await message.answer("Пожалуйста, введите число!")
    data = await state.get_data()
    bid_amount = int(message.text)
    
    res = db.place_bid(message.from_user.id, data['auction_id'], bid_amount)
    
    if res == "success": 
        await message.answer("✅ Ваша ставка успешно принята! Вы лидер аукциона.")
    elif res == "low_bid": 
        await message.answer("❌ Ваша ставка должна быть больше текущей цены!")
    elif res == "no_money": 
        await message.answer("❌ У вас недостаточно монет для такой ставки!")
    elif res == "self_bid": 
        await message.answer("❌ Вы не можете делать ставки на свой собственный лот!")
    else: 
        await message.answer("❌ Аукцион не найден или уже закрыт продавцом.")
    await state.clear()

@dp.message(F.text.startswith("/close_auc_"))
async def close_my_auction(message: types.Message):
    if not await check_registration(message): return
    try:
        auc_id = int(message.text.split("_")[2])
        res = db.close_auction(message.from_user.id, auc_id)
        
        if isinstance(res, dict):
            if res['bidder']:
                await message.answer(f"✅ Аукцион закрыт! Карточка отдана победителю, а на ваш баланс зачислено {res['price']} монет.")
            else:
                await message.answer("✅ Аукцион отменен, так как никто не сделал ставок. Карточка благополучно вернулась в ваш инвентарь.")
        else:
            await message.answer("Ошибка: это не ваш аукцион или он уже был закрыт ранее.")
    except:
        await message.answer("Ошибка обработки команды.")

# ================= МАРКЕТ И РЕЙТИНГ =================
@dp.message(Command("market"))
async def cmd_market(message: types.Message):
    if not await check_registration(message): return
    lots = db.get_market_listings()
    if not lots: return await message.answer("Рынок пуст 🍃")
    
    text = "🏪 <b>Рынок карт (по фиксированной цене)</b>\n\n"
    for lot in lots:
        text += f"🆔 <code>{lot[0]}</code> | {RARITY_EMOJI[lot[2]]} {lot[1]} — {lot[3]}💰\nДля покупки нажми: /buy_{lot[0]}\n\n"
    
    await message.answer(text, parse_mode="HTML")

@dp.message(F.text.startswith("/buy_"))
async def cmd_buy_click(message: types.Message):
    if not await check_registration(message): return
    try:
        market_id = int(message.text.split("_")[1])
        res = db.buy_item(message.from_user.id, market_id)
        
        if isinstance(res, dict) and res.get("status") == "success":
            await message.answer(f"Поздравляю с покупкой карты <b>{res['card_name']}</b>! Карточка добавлена в инвентарь 🎉", parse_mode="HTML")
            
            buyer_nickname = db.get_nickname(message.from_user.id)
            try:
                seller_text = (
                    f"🔔 <b>Отличные новости!</b>\n"
                    f"Игрок <b>{buyer_nickname}</b> только что купил вашу карту <b>{res['card_name']}</b> за {res['price']} монет.\n"
                    f"С учетом комиссии рынка (10%), на ваш баланс зачислено: <b>{res['income']}</b> 💰."
                )
                await bot.send_message(res['seller_id'], seller_text, parse_mode="HTML")
            except Exception as e:
                logging.warning(f"Не удалось отправить уведомление продавцу {res['seller_id']}: {e}")
                
        elif res == "no_money":
            await message.answer("Недостаточно монет для покупки 💸")
        elif res == "self_buy":
            await message.answer("Нельзя купить карту у самого себя 🤨")
        else:
            await message.answer("Лот не найден или уже был продан кому-то другому.")
    except Exception as e:
         await message.answer("Ошибка при покупке.")

@dp.message(Command("top"))
async def cmd_top(message: types.Message):
    if not await check_registration(message): return
    
    rich = db.cursor.execute("SELECT nickname, balance FROM users WHERE nickname IS NOT NULL ORDER BY balance DESC LIMIT 5").fetchall()
    collectors = db.cursor.execute('''
        SELECT u.nickname, s.total_cards 
        FROM stats s 
        JOIN users u ON s.user_id = u.user_id 
        WHERE u.nickname IS NOT NULL 
        ORDER BY s.total_cards DESC 
        LIMIT 5
    ''').fetchall()
    
    text = "🏆 <b>Топ Магнатов:</b>\n"
    if not rich:
         text += "Пока никого нет...\n"
    for idx, u in enumerate(rich, 1):
        text += f"{idx}. <b>{u[0]}</b> — {u[1]} 💰\n"
        
    text += "\n🃏 <b>Топ Коллекционеров:</b>\n"
    if not collectors:
        text += "Пока никого нет...\n"
    for idx, u in enumerate(collectors, 1):
        text += f"{idx}. <b>{u[0]}</b> — {u[1]} карт\n"
        
    await message.answer(text, parse_mode="HTML")

# ================= ЗАПУСК =================
async def main():
    logging.basicConfig(level=logging.INFO)
    await dp.start_polling(bot)

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Бот выключен")
