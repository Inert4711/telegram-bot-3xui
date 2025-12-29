import os
import json
import time
import logging
from datetime import datetime, timedelta, time as dt_time
import asyncio

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    BotCommand,
)
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    CallbackQueryHandler,
    filters,
)

from ui_api import UIAPI

# -------------------------
# ЛОГИ
# -------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="bot.log",
    encoding="utf-8"
)
logger = logging.getLogger(__name__)

# -------------------------
# КОНФИГ
# -------------------------
def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, default))
    except Exception:
        return default

INBOUND_ID = _env_int("INBOUND_ID", 2)
SUPPORT_USERNAME = os.getenv("SUPPORT_USERNAME", "Maks640")
MAIN_ADMIN_ID = _env_int("MAIN_ADMIN_ID", 919845440)

# В проде — хранить в ENV
BOT_TOKEN = os.getenv("BOT_TOKEN", "") # ваш токен ТГ-бота
UI_BASE_URL = os.getenv("UI_BASE_URL", "") # ваш адрес сервера
UI_USERNAME = os.getenv("UI_USERNAME", "") # ваш логин для входа в панель 3x-ui
UI_PASSWORD = os.getenv("UI_PASSWORD", "") # ваш пароль для входа в панель 3x-ui

UI = UIAPI(
    base_url=UI_BASE_URL,
    username=UI_USERNAME,
    password=UI_PASSWORD,
)

# -------------------------
# ТАРИФЫ (минимум полей)
# -------------------------
TARIFFS = {
    "limited_1": {"months": 1, "traffic_limit": 30, "price": 70, "flow": "xtls-rprx-vision"},
    "limited_3": {"months": 3, "traffic_limit": 60, "price": 200, "flow": "xtls-rprx-vision"},
    "limited_6": {"months": 6, "traffic_limit": 90, "price": 450, "flow": "xtls-rprx-vision"},
    "unlimited_1": {"months": 1, "traffic_limit": 0, "price": 90, "flow": "xtls-rprx-vision"},
    "unlimited_3": {"months": 3, "traffic_limit": 0, "price": 250, "flow": "xtls-rprx-vision"},
    "unlimited_6": {"months": 6, "traffic_limit": 0, "price": 500, "flow": "xtls-rprx-vision"},
}

# Пакеты пополнения трафика (ГБ → цена)
ADDONS = {
    "gb10": {"gb": 10, "price": 40},
    "gb20": {"gb": 20, "price": 50},
    "gb30": {"gb": 30, "price": 60},
}

def _user_topup_keyboard(user_id: int, email: str) -> InlineKeyboardMarkup:
    rows = []
    for code, a in ADDONS.items():
        title = f"+{a['gb']} ГБ — {a['price']}₽"
        rows.append([InlineKeyboardButton(title, callback_data=f"topup_pick|{code}|{user_id}|{email}")])
    return InlineKeyboardMarkup(rows)

# -------------------------
# ХРАНИЛКИ
# -------------------------
def _load_json(path, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(default, f, ensure_ascii=False, indent=2)
        logger.warning(f"Created {path} due to: {e}")
        return default

def _save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def _apply_traffic_topup(email: str, add_gb: int):
    """
    Пополнение трафика для клиента:
    1) Если в UIAPI есть явный метод add_traffic/increase_client_traffic_gb — используем его.
    2) Иначе читаем текущий лимит и увеличиваем через update/edit_client(total_gb=new_total).
    """
    # 1) Явные методы
    if hasattr(UI, "add_traffic"):
        return UI.add_traffic(INBOUND_ID, email, add_gb)
    if hasattr(UI, "increase_client_traffic_gb"):
        return UI.increase_client_traffic_gb(INBOUND_ID, email, add_gb)

    # 2) Повышение общего лимита
    clients = UI.get_clients_list(INBOUND_ID)
    client = next((c for c in clients if c.get("email") == email), None)
    if not client:
        raise RuntimeError("Клиент не найден в панели")

    # Попробуем вытащить текущий лимит
    curr = client.get("total_gb")
    if curr is None:
        curr = client.get("totalGB")
    if curr is None:
        curr = client.get("total")

    if curr is None:
        raise RuntimeError("Не удалось определить текущий лимит трафика клиента")

    # Если значение похоже на байты — конвертируем в ГБ
    if isinstance(curr, int) and curr > 10**6:
        curr_gb = int(curr / (1024**3))
    else:
        curr_gb = int(curr)

    # 0 трактуем как безлимит — пополнять нечего
    if curr_gb == 0:
        raise RuntimeError("У клиента безлимитный тариф — пополнение трафика не требуется")

    new_total_gb = curr_gb + int(add_gb)

    if hasattr(UI, "update_client"):
        return UI.update_client(INBOUND_ID, email=email, total_gb=new_total_gb)
    if hasattr(UI, "edit_client"):
        return UI.edit_client(INBOUND_ID, email=email, total_gb=new_total_gb)

    raise RuntimeError("В UIAPI нет метода обновления трафика (update_client/edit_client). Добавьте его или реализуйте add_traffic.")

# Список админов — храним как массив
ALLOWED_ADMINS = _load_json("admins.json", [MAIN_ADMIN_ID])

# Пользователь → email
user_emails = _load_json("user_emails.json", {})

# Подписки (минимум для логики)
paid_users = _load_json("paid_users.json", {})

# Запросы на оплату
payment_requests = _load_json("payment_requests.json", {})

# Предпочитаемые логины перед покупкой
preferred_logins = _load_json("preferred_logins.json", {})

# -------------------------
# REMINDERS CONFIG
# -------------------------
REMINDERS_FILE = "reminders.json"
THRESHOLDS_DAYS = [15, 7, 3, 1]
reminders = _load_json(REMINDERS_FILE, {})

# -------------------------
# УТИЛИТЫ
# -------------------------
def calculate_expiry_time(months):
    """Возвращает expiryTime в миллисекундах (0 — бессрочно)."""
    if months == 0 or months == "permanent":
        return 0
    now = datetime.now()
    expiry_date = now + timedelta(days=30 * int(months))
    return int(expiry_date.timestamp() * 1000)

def _expiry_text_from_ms(expiry_ms: int) -> str:
    return "без ограничения срока" if expiry_ms == 0 else f"до {datetime.fromtimestamp(expiry_ms / 1000).strftime('%Y-%m-%d %H:%M')}"

def _admin_tariff_keyboard(email: str) -> InlineKeyboardMarkup:
    rows = []
    for code, t in TARIFFS.items():
        traffic = "безлимит" if t["traffic_limit"] == 0 else f'{t["traffic_limit"]} ГБ'
        title = f"{t['months']} мес, {traffic}, {t['price']}₽"
        rows.append([InlineKeyboardButton(title, callback_data=f"admin_plan|{code}|{email}")])
    return InlineKeyboardMarkup(rows)

# -------------------------
# КОМАНДЫ
# -------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id in ALLOWED_ADMINS:
        response = (
            "Привет! Я бот для управления VPN-ключами.\n"
            "Команды:\n"
            "/add_key login — Создать новый VPN-ключ\n"
            "/renew — Продлить подписку\n"
            "/add_traffic - Добавить трафик (только для лимитных тарифов)\n"
            "/get_id — Получить ваш Telegram ID\n"
            "/support — Связаться с техподдержкой\n"
            "/del_key <login> — Удалить клиента (админ)\n"
        )
    else:
        response = (
            "Привет! Я бот для управления VPN-ключами.\n"
            "Команды:\n"
            "/add_key login — Создать новый VPN-ключ (после оплаты)\n"
            "/renew — Продлить подписку\n"
            "/add_traffic - Добавить трафик (только для лимитных тарифов)\n"
            "/get_id — Получить ваш Telegram ID\n"
            "/support — Связаться с техподдержкой\n"
            "/my_stats - Показать текущую статистику подписки"
        )
    await update.message.reply_text(response)

async def get_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"Ваш Telegram ID: {update.effective_user.id}")

async def support(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [[InlineKeyboardButton("Связаться с техподдержкой", url=f"https://t.me/{SUPPORT_USERNAME}")]]
    await update.message.reply_text("Для связи с техподдержкой нажмите кнопку ниже:", reply_markup=InlineKeyboardMarkup(keyboard))

async def add_traffic_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Пользователь выбирает пакет пополнения трафика.
    Если не указал логин, пробуем взять из user_emails / preferred_logins.
    """
    uid = update.effective_user.id
    uid_str = str(uid)

    # Берём email из аргумента или из хранилок
    email = None
    if context.args and context.args[0].strip():
        email = context.args[0].strip()
    else:
        email = user_emails.get(uid_str) or preferred_logins.get(uid_str)

    if not email:
        await update.message.reply_text("Укажите логин: /add_traffic <логин>\nНапример: /add_traffic user123")
        return

    # 2. Пытаемся получить дату окончания из панели
    expiry_text = "неизвестно"
    try:
        clients = UI.get_clients_list(INBOUND_ID)
        client = next((c for c in clients if c.get("email") == email), None)
        if client:
            expiry_ms = client.get("expiryTime") or client.get("expiry_time") or 0
            expiry_text = _expiry_text_from_ms(expiry_ms)
    except Exception as e:
        logger.warning(f"Не удалось получить дату окончания для {email}: {e}")

    # Показываем плашки пополнений
    await update.message.reply_text(
        f"⚠️ Обратите внимание: добавляемый объём данных учитывается ТОЛЬКО до конца текущей подписки!\n"
        f"❗Окончание действия текущей подписки: {expiry_text}\n\n"
        f"👇Выберите объём пополнения для {email}:",
        reply_markup=_user_topup_keyboard(uid, email)
    )

def _user_tariff_keyboard(user_id: int, email: str) -> InlineKeyboardMarkup:
    rows = []
    for code, t in TARIFFS.items():
        traffic = "безлимит" if t["traffic_limit"] == 0 else f'{t["traffic_limit"]} ГБ'
        title = f"{t['months']} мес, {traffic}, {t['price']}₽"
        # callback: user_plan|<code>|<tgid>|<email>
        rows.append([InlineKeyboardButton(title, callback_data=f"user_plan|{code}|{user_id}|{email}")])
    return InlineKeyboardMarkup(rows)

async def add_key(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    - Для админа: сразу показывает тарифы для указанного логина (email).
    - Для пользователя: сохраняет желаемый логин и предлагает оплату (далее — ручное подтверждение админом).
    """
    user_id = update.effective_user.id
    user_id_str = str(user_id)

    if user_id_str in user_emails:
        existing_email = user_emails[user_id_str]
        await update.message.reply_text(
            f"⛔ У вас уже есть активный ключ: {existing_email}\n"
            "Один аккаунт может иметь только один ключ."
        )
        return

    if not context.args or not context.args[0].strip():
        await update.message.reply_text("Введите /add_key <логин>\nНапример: /add_key user123")
        return

    email = context.args[0].strip()

    if user_id in ALLOWED_ADMINS:
        # Проверим, есть ли уже клиент — если есть, отдадим ссылку из панели
        try:
            existing = UI.get_clients_list(INBOUND_ID)
            if any(c.get("email") == email for c in existing):
                link = UI.get_client_vless_link(INBOUND_ID, email)
                await update.message.reply_text(f"Клиент {email} уже существует, вот его ссылка:\n{link}")
                return
        except Exception:
            pass

        # Показать тарифы
        await update.message.reply_text(f"Выберите тариф для {email}:", reply_markup=_admin_tariff_keyboard(email))
        return

    # Пользователь: сохраняем желаемый логин и шлём кнопки-оплаты (упрощённо — текст и ожидание ручного подтверждения)
    preferred_logins[user_id_str] = email
    _save_json("preferred_logins.json", preferred_logins)
    await update.message.reply_text(
        f"Выберите тариф для {email}:",
        reply_markup=_user_tariff_keyboard(user_id, email)
    )

async def user_plan_pick_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        _, tariff_code, tgid, email = query.data.split("|", 3)
        t = TARIFFS[tariff_code]

        # Сообщаем пользователю реквизиты
        await query.message.reply_text(
            f"Оплатите {t['price']}₽ по реквизитам:\n"
            f"СБП/Карта: 1234 5678 9012 3456\n"
            f"После оплаты ожидайте подтверждения."
        )

        # Шлём админу уведомление на проверку
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Оплата поступила", callback_data=f"approve|{tariff_code}|{tgid}|{email}")],
            [InlineKeyboardButton("❌ Отказать", callback_data=f"reject|{tgid}")]
        ])
        for admin_id in ALLOWED_ADMINS:
            try:
                await context.bot.send_message(
                    chat_id=admin_id,
                    text=f"Запрос на ключ:\nПользователь {tgid}\nEmail: {email}\nТариф: {tariff_code}",
                    reply_markup=kb
                )
            except Exception:
                pass

    except Exception as e:
        await query.message.reply_text(f"Ошибка: {e}")

async def approve_payment_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if update.effective_user.id not in ALLOWED_ADMINS:
        return
    _, tariff_code, tgid, email = query.data.split("|", 3)
    t = TARIFFS[tariff_code]
    expiry_time = calculate_expiry_time(t["months"])

    link = UI.add_client(
        inbound_id=INBOUND_ID,
        email=email,
        limit_ip=2,
        total_gb=t["traffic_limit"],
        expiry_time_ms=expiry_time,
        flow=t.get("flow"),
        wait_seconds=15,
    )

    # Сохраняем связи и тариф
    user_emails[str(tgid)] = email
    paid_users[str(tgid)] = {
        "tariff": tariff_code,
        "traffic_limit": t["traffic_limit"],
    }
    _save_json("user_emails.json", user_emails)
    _save_json("paid_users.json", paid_users)

    await context.bot.send_message(chat_id=int(tgid), text=f"✅ Оплата подтверждена. Вот ваш ключ:\n{link}")
    await query.message.reply_text("Ключ выдан.")

async def reject_payment_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    _, tgid = query.data.split("|", 1)
    await context.bot.send_message(chat_id=int(tgid), text="❌ Оплата не подтверждена. Проверьте перевод или свяжитесь с поддержкой.")
    await query.message.reply_text("Отказ отправлен пользователю.")

async def renew(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Выбор тарифа для продления"""
    uid = update.effective_user.id
    uid_str = str(uid)
    email = user_emails.get(uid_str) or preferred_logins.get(uid_str)

    if not email:
        await update.message.reply_text("Укажите логин: /renew_key <логин>")
        return
    await update.message.reply_text(
        f"Выберите тариф для продления {email}:",
        reply_markup=_user_tariff_keyboard(uid, email)
    )

async def send_link_later(context: ContextTypes.DEFAULT_TYPE):
    """Отложенная отправка ссылки пользователю"""
    data = context.job.data
    tgid = data["tgid"]
    email = data["email"]
    try:
        link = UI.get_client_vless_link(INBOUND_ID, email)
        if link and "vless://" in link:
            await context.bot.send_message(chat_id=int(tgid), text=f"✅ Ваш ключ готов:\n{link}")
        else:
            await context.bot.send_message(chat_id=int(tgid), text="❌ Ключ создан, но ссылка пока недоступна. Попробуйте позже.")
    except Exception as e:
        await context.bot.send_message(chat_id=int(tgid), text=f"Ошибка при получении ссылки: {e}")

async def activate_key_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    _, email = query.data.split("|", 1)

    # Сообщаем сразу
    await query.message.reply_text("🔄 Ключ будет отправлен в течение минуты...")

    # Запускаем задачу через 60 секунд
    context.job_queue.run_once(
        send_activated_key,
        when=60,
        data={"chat_id": query.message.chat_id, "email": email}
    )

async def send_activated_key(context: ContextTypes.DEFAULT_TYPE):
    data = context.job.data
    chat_id = data["chat_id"]
    email = data["email"]

    try:
        link = UI.get_client_vless_link(INBOUND_ID, email)
        if link and "vless://" in link:
            await context.bot.send_message(chat_id=chat_id, text=f"✅ Ваш ключ готов:\n{link}")
        else:
            await context.bot.send_message(chat_id=chat_id, text="❌ Ключ пока не готов. Попробуйте позже.")
    except Exception as e:
        await context.bot.send_message(chat_id=chat_id, text=f"Ошибка при получении ключа: {e}")

async def retry_activate_key(context: ContextTypes.DEFAULT_TYPE):
    data = context.job.data
    chat_id = data["chat_id"]
    email = data["email"]
    attempt = data["attempt"]

    try:
        link = UI.get_client_vless_link(INBOUND_ID, email)
        if link and "vless://" in link:
            await context.bot.send_message(chat_id=chat_id, text=f"✅ Ваш ключ готов:\n{link}")
        else:
            if attempt < 2:
                await context.bot.send_message(chat_id=chat_id, text="Ключ пока не готов, пробую ещё раз через 30 секунд...")
                context.job_queue.run_once(
                    retry_activate_key,
                    when=30,
                    data={"chat_id": chat_id, "email": email, "attempt": attempt + 1}
                )
            else:
                await context.bot.send_message(chat_id=chat_id, text="Ключ всё ещё не готов. Попробуйте позже.")
    except Exception as e:
        await context.bot.send_message(chat_id=chat_id, text=f"Ошибка при получении ключа: {e}")

async def approve_payment_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if update.effective_user.id not in ALLOWED_ADMINS:
        return

    _, tariff_code, tgid, email = query.data.split("|", 3)
    t = TARIFFS.get(tariff_code)
    if not t:
        await query.message.reply_text(f"Неверный тарифный код: {tariff_code}")
        return

    # Получаем список клиентов
    clients = UI.get_clients_list(INBOUND_ID)
    client = next((c for c in clients if c.get("email") == email), None)

    # === 1. Если клиента нет — создаём и предлагаем активировать ===
    if not client:
        expiry_time = calculate_expiry_time(t["months"])
        try:
            UI.add_client(
                inbound_id=INBOUND_ID,
                email=email,
                limit_ip=2,
                total_gb=t["traffic_limit"],
                expiry_time_ms=expiry_time,
                flow=t.get("flow"),
                wait_seconds=15,
            )
        except Exception as e:
            await query.message.reply_text(f"❌ Ошибка при создании клиента: {e}")
            return

        user_emails[str(tgid)] = email
        paid_users[str(tgid)] = {
            "tariff": tariff_code,
            "traffic_limit": t["traffic_limit"],
        }
        _save_json("user_emails.json", user_emails)
        _save_json("paid_users.json", paid_users)

        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔑 Активировать ключ", callback_data=f"activate_key|{email}")]
        ])
        await context.bot.send_message(
            chat_id=int(tgid),
            text="✅ Оплата подтверждена. Нажмите кнопку ниже, чтобы активировать ключ.",
            reply_markup=kb
        )
        await query.message.reply_text("Клиент создан. Пользователь получил кнопку для активации ключа.")
        return

    # === 2. Если клиент есть — продлеваем ===
    now_ms = int(time.time() * 1000)
    current_expiry = client.get("expiryTime") or client.get("expiry_time") or 0
    if current_expiry > now_ms:
        base_dt = datetime.fromtimestamp(current_expiry / 1000)
    else:
        base_dt = datetime.now()
    new_expiry = int((base_dt + timedelta(days=30 * t["months"])).timestamp() * 1000)

    raw_limit = client.get("totalGB") or client.get("total_gb") or client.get("total") or 0
    if isinstance(raw_limit, int) and raw_limit > 10**6:
        raw_limit = raw_limit // (1024**3)
    if t["traffic_limit"] == 0:
        new_limit = 0
    else:
        new_limit = raw_limit + t["traffic_limit"]

    try:
        UI.update_client(
            inbound_id=INBOUND_ID,
            email=email,
            total_gb=new_limit,
            expiry_time_ms=new_expiry
        )
    except Exception as e:
        await query.message.reply_text(f"❌ Ошибка обновления клиента: {e}")
        return

    try:
        link = UI.get_client_vless_link(INBOUND_ID, email)
        if not link or "vless://" not in link:
            link = "❌ Не удалось получить ссылку после продления."
    except Exception:
        link = "❌ Не удалось получить ссылку после продления."

    user_emails[str(tgid)] = email
    paid_users[str(tgid)] = {
        "tariff": tariff_code,
        "traffic_limit": t["traffic_limit"],
    }
    _save_json("user_emails.json", user_emails)
    _save_json("paid_users.json", paid_users)

    await context.bot.send_message(
        chat_id=int(tgid),
        text=(
            f"✅ Подписка продлена на {t['months']} мес.\n"
            f"Новый лимит: {'безлимит' if new_limit == 0 else f'{new_limit} ГБ'}\n"
            f"Новая дата окончания: {datetime.fromtimestamp(new_expiry/1000).strftime('%Y-%m-%d %H:%M')}\n\n"
            f"Ваш ключ:\n{link}"
        )
    )
    await query.message.reply_text("Продление выполнено.")

async def user_topup_pick_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    topup_pick|<addon_code>|<tgid>|<email>
    Проверяем, что тариф лимитный. Отправляем реквизиты пользователю и уведомление админам.
    """
    query = update.callback_query
    await query.answer()
    try:
        _, addon_code, tgid, email = query.data.split("|", 3)
        addon = ADDONS.get(addon_code)
        if not addon:
            await query.message.reply_text("Некорректный пакет пополнения.")
            return

        # Проверим, что тариф лимитный
        limited = False
        info = paid_users.get(tgid)
        if info and info.get("traffic_limit", 0) != 0:
            limited = True
        else:
            try:
                existing = UI.get_clients_list(INBOUND_ID)
                c = next((c for c in existing if c.get("email") == email), None)
                if c:
                    total = c.get("total_gb") or c.get("totalGB") or c.get("total") or 0
                    # 0 считаем безлимитом
                    if isinstance(total, int) and total != 0:
                        limited = True
            except Exception:
                pass

        if not limited:
            await query.message.reply_text("У вас безлимитный тариф. Пополнение трафика не требуется.")
            return

        # Реквизиты пользователю
        await query.message.reply_text(
            f"Пополнение: +{addon['gb']} ГБ за {addon['price']}₽.\n"
            f"СБП/Карта: 1234 5678 9012 3456\n"
            f"После оплаты ожидайте подтверждения."
        )

        # Уведомление админам
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Оплата поступила, пополнить", callback_data=f"approve_topup|{addon_code}|{tgid}|{email}")],
            [InlineKeyboardButton("❌ Отказать", callback_data=f"reject_topup|{tgid}")]
        ])
        for admin_id in ALLOWED_ADMINS:
            try:
                await context.bot.send_message(
                    chat_id=admin_id,
                    text=f"Запрос на пополнение:\nПользователь {tgid}\nEmail: {email}\nПакет: +{addon['gb']} ГБ за {addon['price']}₽",
                    reply_markup=kb
                )
            except Exception:
                pass

    except Exception as e:
        logger.exception("user_topup_pick_callback")
        await query.message.reply_text(f"Ошибка: {e}")

async def approve_topup_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    approve_topup|<addon_code>|<tgid>|<email>
    Админ подтверждает оплату — пополняем трафик в панели.
    """
    query = update.callback_query
    await query.answer()

    if update.effective_user.id not in ALLOWED_ADMINS:
        await query.message.reply_text("Недоступно.")
        return

    try:
        _, addon_code, tgid, email = query.data.split("|", 3)
        addon = ADDONS.get(addon_code)
        if not addon:
            await query.message.reply_text("Некорректный пакет пополнения.")
            return

        _apply_traffic_topup(email=email, add_gb=addon["gb"])

        # Оповещения
        await context.bot.send_message(chat_id=int(tgid), text=f"✅ Пополнение зачислено: +{addon['gb']} ГБ.")
        await query.message.reply_text("Пополнение выполнено.")

    except Exception as e:
        logger.exception("approve_topup_callback")
        await query.message.reply_text(f"❌ Ошибка пополнения: {e}")

async def reject_topup_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    reject_topup|<tgid>
    Отказ в пополнении.
    """
    query = update.callback_query
    await query.answer()
    try:
        _, tgid = query.data.split("|", 1)
        await context.bot.send_message(
            chat_id=int(tgid),
            text="❌ Пополнение не подтверждено. Если вы оплатили, свяжитесь с поддержкой: /support."
        )
        await query.message.reply_text("Отказ по пополнению отправлен.")
    except Exception as e:
        logger.exception("reject_topup_callback")
        await query.message.reply_text(f"Ошибка: {e}")

async def del_key(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Удаление клиента (админ). Здесь — минимальная заглушка: просто снимаем локальную привязку.
    В реальном кейсе добавьте вызов метода панели удаления по UUID.
    """
    if update.effective_user.id not in ALLOWED_ADMINS:
        await update.message.reply_text("⛔ У вас нет прав на выполнение этой команды.")
        return

    if not context.args:
        await update.message.reply_text("Использование: /del_key <login>")
        return

    login = context.args[0].strip()
    # Синхронизация с локальными хранилищами
    # (Для полного удаления из панели нужно реализовать del_client по UUID)
    # Удаляем локальную привязку
    for uid, em in list(user_emails.items()):
        if em == login:
            user_emails.pop(uid, None)
            paid_users.pop(uid, None)
    _save_json("user_emails.json", user_emails)
    _save_json("paid_users.json", paid_users)
    await update.message.reply_text(f"Локальная привязка для {login} удалена. Удаление в панели реализуйте отдельно при необходимости.")

# -------------------------
# СТАТУС ПОДПИСКИ
# -------------------------
async def my_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /my_stats — показать логин, тариф, дату окончания и расход трафика
    """
    uid = update.effective_user.id
    uid_str = str(uid)

    # 1) Найти email клиента
    email = user_emails.get(uid_str)
    if not email:
        await update.message.reply_text("У вас нет активной подписки.")
        return

    # 2) Определить текущий тариф из локального paid_users
    pu = paid_users.get(uid_str, {})
    tariff_code = pu.get("tariff")
    t = TARIFFS.get(tariff_code, {})
    if tariff_code and t:
        traffic_limit = t["traffic_limit"]
        tariff_text = (
            f"{t['months']} мес, "
            f"{'безлимит' if traffic_limit == 0 else f'{traffic_limit} ГБ'}"
        )
    else:
        tariff_text = "неизвестен"

    # 3) Подтянуть из панели дату окончания и лимит/использование
    try:
        clients = UI.get_clients_list(INBOUND_ID)
        client = next(c for c in clients if c.get("email") == email)
    except StopIteration:
        await update.message.reply_text("Клиент не найден в панели.")
        return
    except Exception as e:
        await update.message.reply_text(f"Ошибка при обращении к панели: {e}")
        return

    # 3a) дата окончания
    expiry_ms = client.get("expiryTime") or client.get("expiry_time") or 0
    expiry_text = _expiry_text_from_ms(expiry_ms)

    # 3b) лимит
    raw_limit = client.get("totalGB") or client.get("total_gb") or pu.get("traffic_limit", 0)
    # если байты → ГБ
    if isinstance(raw_limit, int) and raw_limit > 10**6:
        limit_gb = raw_limit // (1024**3)
    else:
        limit_gb = int(raw_limit)

    # 3c) использование: складываем up+down (если есть) / конвертим в ГБ
    up = client.get("up") or client.get("uplink") or 0
    down = client.get("down") or client.get("downlink") or 0
    used_gb = (up + down) / (1024**3)

    if limit_gb == 0:
        usage_text = f"{used_gb:.2f} ГБ из безлимита"
    else:
        usage_text = f"{used_gb:.2f} ГБ из {limit_gb} ГБ"

    # 4) Собираем сообщение
    text = (
        f"🔹 Логин: {email}\n"
        f"🔹 Тариф: {tariff_text}\n"
        f"🔹 Действителен: {expiry_text}\n"
        f"🔹 Трафик: {usage_text}"
    )
    await update.message.reply_text(text)

# -------------------------
# ADMIN CALLBACKS
# -------------------------
async def admin_plan_pick_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    admin_plan|<tariff_code>|<email>
    Админ выбирает тариф и создаёт клиента. Бот возвращает ссылку ровно как в панели.
    """
    query = update.callback_query
    await query.answer()

    if update.effective_user.id not in ALLOWED_ADMINS:
        await query.message.reply_text("Недоступно.")
        return

    try:
        data = query.data or ""
        if not data.startswith("admin_plan|"):
            await query.message.reply_text("Некорректный запрос.")
            return
        _, tariff_code, email = data.split("|", 2)
        email = email.strip()

        if tariff_code not in TARIFFS:
            await query.message.reply_text("Неверный тариф.")
            return

        t = TARIFFS[tariff_code]
        expiry_time = calculate_expiry_time(t["months"])
        traffic_limit = t["traffic_limit"]
        flow = t.get("flow") or "xtls-rprx-vision"

        # Если уже существует — отдать ссылку
        existing_clients = UI.get_clients_list(INBOUND_ID)
        if any(c.get("email") == email for c in existing_clients):
            try:
                link = UI.get_client_vless_link(INBOUND_ID, email)
                await query.message.reply_text(f"Клиент {email} уже существует, вот его ссылка:\n{link}")
            except Exception:
                await query.message.reply_text(f"Клиент {email} уже существует в панели.")
            return

        # Создание клиента (ссылка строго панельная)
        try:
            link = UI.add_client(
                inbound_id=INBOUND_ID,
                email=email,
                limit_ip=2,
                total_gb=traffic_limit,
                expiry_time_ms=expiry_time,
                flow=flow,
                wait_seconds=15,
            )
        except Exception as e:
            logger.exception("admin_plan_pick_callback: создание клиента не удалось")
            await query.message.reply_text(f"❌ Ошибка при создании клиента: {e}")
            return

        # Привязки (минимально)
        # Если email вида user_<tgid>_*, создадим маппинг
        tg_id_from_email = None
        parts = email.split("_")
        if len(parts) >= 2 and parts[1].isdigit():
            tg_id_from_email = parts[1]

        if tg_id_from_email:
            user_emails[tg_id_from_email] = email
            paid_users[tg_id_from_email] = {
                "tariff": tariff_code,
                "traffic_limit": traffic_limit,
            }
            _save_json("user_emails.json", user_emails)
            _save_json("paid_users.json", paid_users)

        expiry_text = _expiry_text_from_ms(expiry_time)
        traffic_text = "без ограничения" if traffic_limit == 0 else f"{traffic_limit} ГБ"
        await query.message.reply_text(
            f"Ключ создан (админ):\n{link}\nСрок действия: {expiry_text}\nЛимит трафика: {traffic_text}"
        )

    except Exception as e:
        logger.exception("admin_plan_pick_callback")
        await query.message.reply_text(f"Ошибка: {e}")

# -------------------------
# ПРОВЕРКА И ОТПРАВКА НАПОМИНАНИЙ
# -------------------------

async def reminder_job(context: ContextTypes.DEFAULT_TYPE):
    now = datetime.now()
    # инвертируем user_emails: email -> chat_id
    email_to_chat = {em: int(uid) for uid, em in user_emails.items()}

    try:
        clients = UI.get_clients_list(INBOUND_ID)
    except Exception as e:
        logger.error(f"Не удалось получить список клиентов для напоминаний: {e}")
        return

    for c in clients:
        email = c.get("email")
        expiry_ms = c.get("expiryTime") or c.get("expiry_time") or 0
        if not email or not expiry_ms:
            continue

        expiry_dt = datetime.fromtimestamp(expiry_ms / 1000)
        days_left = (expiry_dt.date() - now.date()).days

        if days_left in THRESHOLDS_DAYS:
            sent_list = reminders.get(email, [])
            if days_left in sent_list:
                continue

            chat_id = email_to_chat.get(email)
            if not chat_id:
                logger.debug(f"Нет chat_id для {email}, пропускаем напоминание")
                continue

            text = (
                f"⚠️ Ваша подписка скоро истечёт!\n\n"
                f"Email: {email}\n"
                f"Осталось дней: {days_left}\n"
                f"Дата окончания: {expiry_dt.strftime('%Y-%m-%d %H:%M')}\n\n"
                "Чтобы продлить, воспользуйтесь командой /renew или свяжитесь с поддержкой."
            )
            try:
                await context.bot.send_message(chat_id=chat_id, text=text)
                # отмечаем, что напоминание для этого порога отправлено
                sent_list.append(days_left)
                reminders[email] = sent_list
                _save_json(REMINDERS_FILE, reminders)
                logger.info(f"Отправлено напоминание за {days_left} дней для {email}")
            except Exception as e:
                logger.error(f"Ошибка при отправке напоминания {email}: {e}")

# -------------------------
# КОМАНДЫ МЕНЮ
# -------------------------
async def set_commands_job(context: ContextTypes.DEFAULT_TYPE):
    await context.application.bot.set_my_commands([
        BotCommand("start", "Запустить бота"),
        BotCommand("add_key", "Создать новый VPN-ключ"),
        BotCommand("renew", "Продлить подписку"),
        BotCommand("add_traffic", "Пополнить трафик"),
        BotCommand("get_id", "Получить ваш Telegram ID"),
        BotCommand("support", "Связаться с техподдержкой"),
        BotCommand("my_stats", "Показать текущую статистику подписки"),
        BotCommand("del_key", "Удалить клиента (админ)"),
    ])

# -------------------------
# MAIN
# -------------------------
def main():
    if not BOT_TOKEN:
        raise RuntimeError("Установите переменную окружения BOT_TOKEN с токеном бота.")

    application = Application.builder().token(BOT_TOKEN).build()

    # Команды
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("get_id", get_id))
    application.add_handler(CommandHandler("support", support))
    application.add_handler(CommandHandler("add_key", add_key))
    application.add_handler(CommandHandler("renew", renew))
    application.add_handler(CommandHandler("add_traffic", add_traffic_cmd))
    application.add_handler(CommandHandler("my_stats", my_stats))
    application.add_handler(CommandHandler("del_key", del_key, filters=filters.User(ALLOWED_ADMINS)))

    # Админский колбэк выбора тарифа
    application.add_handler(CallbackQueryHandler(admin_plan_pick_callback, pattern=r"^admin_plan\|"))
    application.add_handler(CallbackQueryHandler(user_plan_pick_callback, pattern=r"^user_plan\|"))
    application.add_handler(CallbackQueryHandler(approve_payment_callback, pattern=r"^approve\|"))
    application.add_handler(CallbackQueryHandler(reject_payment_callback, pattern=r"^reject\|"))
    application.add_handler(CallbackQueryHandler(user_topup_pick_callback, pattern=r"^topup_pick\|"))
    application.add_handler(CallbackQueryHandler(approve_topup_callback, pattern=r"^approve_topup\|"))
    application.add_handler(CallbackQueryHandler(reject_topup_callback, pattern=r"^reject_topup\|"))
    application.add_handler(CallbackQueryHandler(activate_key_callback, pattern=r"^activate_key\|"))

    # Команды меню
    application.job_queue.run_once(set_commands_job, when=0)
    application.job_queue.run_daily(reminder_job, time=dt_time(hour=0, minute=0, second=0))

    application.run_polling()


if __name__ == "__main__":
    main()

