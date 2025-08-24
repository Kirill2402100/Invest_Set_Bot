# marketing_bot.py — STRIGI_KAPUSTU_BOT
# Telegram marketing / accounting bot for BMR-DCA strategy

import os, re, json, logging, math, time
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple

import gspread
from gspread.utils import rowcol_to_a1
from telegram import (
    Update, constants, BotCommand,
    BotCommandScopeAllPrivateChats, BotCommandScopeChat,
)
from telegram.ext import (
    ApplicationBuilder, Application, CommandHandler, ContextTypes,
)

# ─────────────────────────────────────────────────────────────────────────────
# ENV
# ─────────────────────────────────────────────────────────────────────────────
BOT_NAME   = "STRIGI_KAPUSTU_BOT"
BOT_TOKEN  = os.getenv("MARKETING_BOT_TOKEN")
SHEET_ID   = os.getenv("SHEET_ID")
CREDS_JSON = os.getenv("GOOGLE_CREDENTIALS")
SYSTEM_BANK_USDT = float(os.getenv("SYSTEM_BANK_USDT", "1000"))
PROFIT_USER_SHARE = 0.30  # 30%

def _parse_admin_ids(raw: str) -> set[int]:
    if not raw: return set()
    try:
        maybe = json.loads(raw)
        if isinstance(maybe, (list, tuple, set)): return {int(x) for x in maybe}
        if isinstance(maybe, (int, str)) and str(maybe).lstrip("-").isdigit(): return {int(maybe)}
    except Exception:
        pass
    out = set()
    for t in re.split(r"[\s,;]+", raw.strip()):
        t = t.strip().strip("[](){}\"'")
        if t and t.lstrip("-").isdigit(): out.add(int(t))
    return out

ADMIN_IDS = _parse_admin_ids(os.getenv("ADMIN_IDS", ""))

if not BOT_TOKEN or not SHEET_ID or not CREDS_JSON or not ADMIN_IDS:
    raise RuntimeError("MARKETING_BOT_TOKEN / SHEET_ID / GOOGLE_CREDENTIALS / ADMIN_IDS обязательны")

def now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

# ─────────────────────────────────────────────────────────────────────────────
# LOG
# ─────────────────────────────────────────────────────────────────────────────
log = logging.getLogger("marketing")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ─────────────────────────────────────────────────────────────────────────────
# Google Sheets
# ─────────────────────────────────────────────────────────────────────────────
gc = gspread.service_account_from_dict(json.loads(CREDS_JSON))
sh = gc.open_by_key(SHEET_ID)

LOG_SHEET    = "BMR_DCA_Log"       # пишет торговый бот
USERS_SHEET  = "Marketing_Users"
STATE_SHEET  = "Marketing_State"
LEDGER_SHEET = "Marketing_Ledger"

# — форматы
USERS_HEADERS = [
    "Chat_ID", "Name", "Deposit_USDT", "Active", "Pending_Deposit",
    "Bonus_Accrued", "Bonus_Paid", "Bonus_To_Deposit",
    "Wallet", "Network", "Last_Update"
]
STATE_HEADERS = ["Last_Row", "Start_UTC", "Profit_Total_USDT", "Last_Update"]
LEDGER_HEADERS = [
    "Timestamp_UTC", "Chat_ID", "Name", "Type", "Amount_USDT", "Note"
]

def ws(title: str):
    return sh.worksheet(title)

def fmt_usd(x: float) -> str:
    return f"{x:,.2f}".replace(",", " ")

def to_float(x) -> float:
    try:
        s = str(x).replace(" ", "").replace(",", ".")
        return float(s)
    except Exception:
        return 0.0

def parse_money(s: str) -> float:
    return to_float(re.sub(r"[^\d.,\-]", "", s))

# ─────────────────────────────────────────────────────────────────────────────
# Safe headers (fix for A1:R1 bug)
# ─────────────────────────────────────────────────────────────────────────────
def ensure_headers(ws_title: str, required: List[str], min_rows: int = 200):
    names = {w.title for w in sh.worksheets()}
    if ws_title not in names:
        w = sh.add_worksheet(ws_title, rows=max(min_rows, 10), cols=max(10, len(required)))
        w.update("A1", [required])  # anchor write, Google stretches to width
        return

    w = ws(ws_title)
    vals = w.get_all_values()
    existing = vals[0] if vals else []

    if not existing:
        # нет заголовков — пишем все сразу
        if w.col_count < len(required):
            w.add_cols(len(required) - w.col_count)
        w.update("A1", [required])
        return

    missing = [h for h in required if h not in existing]
    if not missing:
        return

    new_headers = existing + missing
    need_cols = len(new_headers)

    # расширяем лист по колонкам
    try:
        if w.col_count < need_cols:
            w.add_cols(need_cols - w.col_count)
    except Exception:
        w.resize(rows=max(len(vals), min_rows), cols=need_cols)

    # перезаписываем заголовки "с A1" (без правой границы)
    w.update("A1", [new_headers])

    # добиваем пустые ячейки под новые колонки для остальных строк
    if len(vals) > 1 and len(missing) > 0:
        start = rowcol_to_a1(2, len(existing) + 1)
        end   = rowcol_to_a1(len(vals), len(new_headers))
        blanks = [[""] * len(missing) for _ in range(len(vals) - 1)]
        w.update(f"{start}:{end}", blanks)

def ensure_sheets():
    # проверяем, что торговый лог существует
    names = {w.title for w in sh.worksheets()}
    if LOG_SHEET not in names:
        raise RuntimeError(f"Не найден лист {LOG_SHEET} (его пишет торговый бот).")

    ensure_headers(USERS_SHEET, USERS_HEADERS)
    ensure_headers(STATE_SHEET, STATE_HEADERS, min_rows=5)
    ensure_headers(LEDGER_SHEET, LEDGER_HEADERS)

    # инициализация STATE, если пусто
    st = ws(STATE_SHEET)
    vals = st.get_all_values()
    if len(vals) < 2:
        st.update("A2", [["0", now_utc_str(), "0", now_utc_str()]])
    else:
        if not (st.acell("B2").value or "").strip():
            st.update("B2", now_utc_str())
        st.update("D2", now_utc_str())

ensure_sheets()

# ─────────────────────────────────────────────────────────────────────────────
# Sheet helpers
# ─────────────────────────────────────────────────────────────────────────────
def _sheet_dicts(worksheet) -> List[Dict[str, Any]]:
    vals = worksheet.get_all_values()
    if not vals or len(vals) < 2: return []
    headers, out = vals[0], []
    for row in vals[1:]:
        out.append({headers[i]: (row[i] if i < len(row) else "") for i in range(len(headers))})
    return out

def _users_as_dict() -> Tuple[List[str], List[Dict[str, Any]]]:
    w = ws(USERS_SHEET)
    vals = w.get_all_values()
    headers = vals[0] if vals else USERS_HEADERS
    rows = []
    for r in vals[1:]:
        rows.append({headers[i]: (r[i] if i < len(r) else "") for i in range(len(headers))})
    return headers, rows

def _find_row_by_chat_id(headers: List[str], rows: List[Dict[str, Any]], chat_id: int) -> Optional[int]:
    # возвращает номер строки (1-based) на листе (учитывая заголовок)
    for idx, r in enumerate(rows, start=2):
        if str(r.get("Chat_ID", "")).strip() == str(chat_id):
            return idx
    return None

def upsert_user(chat_id: int, **fields):
    headers, rows = _users_as_dict()
    w = ws(USERS_SHEET)
    row_idx = _find_row_by_chat_id(headers, rows, chat_id)

    # гарантируем все заголовки
    ensure_headers(USERS_SHEET, USERS_HEADERS)

    # соберём текущую строку -> словарь
    cur = {h: "" for h in headers}
    if row_idx:
        vals = w.row_values(row_idx)
        for i, h in enumerate(headers):
            cur[h] = vals[i] if i < len(vals) else ""
    else:
        cur.update({"Chat_ID": str(chat_id), "Active": "TRUE", "Deposit_USDT": "0", "Pending_Deposit": "0",
                    "Bonus_Accrued": "0", "Bonus_Paid": "0", "Bonus_To_Deposit": "0", "Last_Update": now_utc_str()})

    # применяем апдейты
    for k, v in fields.items():
        if k in cur:
            cur[k] = str(v)

    cur["Last_Update"] = now_utc_str()

    # выравниваем по ширине
    out_row = [cur.get(h, "") for h in headers]

    if row_idx:
        # пишем всю строку целиком — меньше риска несоответствия диапазонов
        right = rowcol_to_a1(row_idx, len(headers))
        left  = rowcol_to_a1(row_idx, 1)
        w.update(f"{left}:{right}", [out_row])
    else:
        w.append_row(out_row, value_input_option="RAW")

def get_users(active_only: bool = False) -> List[Dict[str, Any]]:
    headers, rows = _users_as_dict()
    res = []
    for r in rows:
        try:
            item = {
                "chat_id": int(str(r.get("Chat_ID", "")).strip() or "0"),
                "name": (r.get("Name") or "").strip(),
                "deposit": to_float(r.get("Deposit_USDT")),
                "pending": to_float(r.get("Pending_Deposit")),
                "active": str(r.get("Active", "TRUE")).strip().upper() not in ("FALSE", "0", ""),
                "bonus_accrued": to_float(r.get("Bonus_Accrued")),
                "bonus_paid": to_float(r.get("Bonus_Paid")),
                "bonus_to_deposit": to_float(r.get("Bonus_To_Deposit")),
                "wallet": (r.get("Wallet") or "").strip(),
                "network": (r.get("Network") or "").strip(),
            }
            if (not active_only) or item["active"]:
                res.append(item)
        except Exception as e:
            log.warning(f"Skip user row: {r} ({e})")
    return res

def get_state() -> Tuple[int, str, float]:
    w = ws(STATE_SHEET)
    last_row  = w.acell("A2").value or "0"
    start_utc = w.acell("B2").value or now_utc_str()
    profit    = w.acell("C2").value or "0"
    try:
        return int(last_row), start_utc, to_float(profit)
    except Exception:
        return 0, start_utc, to_float(profit)

def set_state(last_row: Optional[int] = None, profit_total: Optional[float] = None):
    w = ws(STATE_SHEET)
    if last_row is not None:
        w.update("A2", str(last_row))
    if profit_total is not None:
        w.update("C2", str(profit_total))
    w.update("D2", now_utc_str())

def ledger_add(chat_id: int, name: str, typ: str, amount: float, note: str = ""):
    w = ws(LEDGER_SHEET)
    w.append_row([now_utc_str(), str(chat_id), name, typ, f"{amount:.2f}", note], value_input_option="RAW")

# ─────────────────────────────────────────────────────────────────────────────
# Presentation helpers
# ─────────────────────────────────────────────────────────────────────────────
START_TEXT = (
    "👋 <b>Привет!</b> Я <b>STRIGI_KAPUSTU_BOT</b>.\n\n"
    "<b>Чтобы начать:</b>\n"
    "1) Укажите имя: <code>/myname Имя Фамилия</code>\n"
    "2) Переведите USDT на адрес:\n"
    "   <code>TVSRhKYHAUKx8RnXzW3KXNeUk5aAQs7hJ4</code>\n"
    "   (сеть <b>TRON / TRC-20</b>).\n"
    "3) Сообщите сумму: <code>/add_deposit 500</code> (укажите ваш перевод).\n"
    "4) Дождитесь подтверждения — депозит активируется со следующей сделкой.\n"
    "5) Проверяйте состояние: <code>/balance</code>\n\n"
    "<b>Дополнительно:</b>\n"
    "• Пополнить из премии: <code>/add_from_bonus 100</code>\n"
    "• Вывод премии: <code>/withdraw_bonus 100</code> (или <code>all</code>)\n"
    "• Вывод всего депозита: <code>/withdraw_all</code>\n"
    "• Кошелёк для выплат: <code>/setwallet адрес TRC20</code>  |  Просмотр: <code>/wallet</code>"
)

ABOUT_TEXT = (
    "🤖 <b>О боте</b>\n\n"
    "Это инвестиционный бот, который ведёт алгоритмическую торговлю EUR↔USD через стейблкоины "
    "(<b>EURC/USDT</b>) на бирже. Алгоритм автоматически управляет входами, доборами и выходами, "
    "присылает уведомления и ведёт учёт сделок.\n\n"
    "📈 <b>Модель дохода</b>\n"
    "В отчётах отражается прибыль только закрытых сделок — это ваша «премия». "
    "Её можно вывести (<code>/withdraw_bonus</code>) или реинвестировать (<code>/add_from_bonus</code>).\n\n"
    "⚠️ <b>Дисклеймер о рисках</b>\n"
    "Торговля на рынке (в т.ч. с плечом) связана с высокой волатильностью и может привести к частичной "
    "или полной потере средств. Прошлые результаты не гарантируют будущую доходность. "
    "Используя бота, вы подтверждаете, что понимаете и принимаете эти риски."
)

def tier_emoji(profit_pct: float) -> str:
    if profit_pct >= 90: return "🚀"
    if profit_pct >= 80: return "🛩️"
    if profit_pct >= 70: return "🏎️"
    if profit_pct >= 50: return "🏍️"
    return "✅"

def base_from_pair(pair: str) -> str:
    base = (pair or "").split("/")[0].split(":")[0].upper()
    return base[:-1] if base.endswith("C") and len(base) > 3 else base

# ─────────────────────────────────────────────────────────────────────────────
# Menus
# ─────────────────────────────────────────────────────────────────────────────
USER_CMDS = [
    BotCommand("start", "Как начать"),
    BotCommand("about", "О боте"),
    BotCommand("myname", "Задать имя"),
    BotCommand("wallet", "Мой кошелёк для выплат"),
    BotCommand("setwallet", "Изменить кошелёк"),
    BotCommand("add_deposit", "Заявка на пополнение"),
    BotCommand("add_from_bonus", "Пополнить из премии"),
    BotCommand("withdraw_bonus", "Вывести премию"),
    BotCommand("withdraw_all", "Вывести депозит"),
    BotCommand("balance", "Баланс"),
]
ADMIN_CMDS = [
    BotCommand("help", "Команды админа"),
    BotCommand("list", "Список пользователей"),
    BotCommand("adduser", "Добавить пользователя"),
    BotCommand("setdep", "Изменить депозит (pending)"),
    BotCommand("setname", "Переименовать пользователя"),
    BotCommand("remove", "Отключить пользователя"),
]

async def set_default_menu(app: Application):
    await app.bot.set_my_commands([BotCommand("start", "Как начать")], scope=BotCommandScopeAllPrivateChats())

async def set_user_menu(app: Application, chat_id: int):
    await app.bot.set_my_commands(USER_CMDS, scope=BotCommandScopeChat(chat_id))

async def set_admin_menus(app: Application):
    for aid in ADMIN_IDS:
        try:
            await app.bot.set_my_commands(ADMIN_CMDS, scope=BotCommandScopeChat(aid))
        except Exception as e:
            log.warning(f"set_admin_menu failed for {aid}: {e}")

# ─────────────────────────────────────────────────────────────────────────────
# Telegram handlers: users
# ─────────────────────────────────────────────────────────────────────────────
def _is_admin(update: Update) -> bool:
    uid = update.effective_user.id if update.effective_user else None
    cid = update.effective_chat.id if update.effective_chat else None
    return (uid in ADMIN_IDS) or (cid in ADMIN_IDS)

async def start_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    # Выставляем меню пользователю
    try:
        await set_user_menu(ctx.application, update.effective_chat.id)
    except Exception:
        pass
    await update.message.reply_text(START_TEXT, parse_mode=constants.ParseMode.HTML, disable_web_page_preview=True)

async def about_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(ABOUT_TEXT, parse_mode=constants.ParseMode.HTML, disable_web_page_preview=True)

async def myname_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    args = update.message.text.split(maxsplit=1)
    if len(args) < 2 or not args[1].strip():
        return await update.message.reply_text("Использование: <code>/myname Имя Фамилия</code>", parse_mode=constants.ParseMode.HTML)
    name = args[1].strip()
    cid = update.effective_chat.id
    upsert_user(cid, Chat_ID=str(cid), Name=name, Active="TRUE")
    await set_user_menu(ctx.application, cid)
    await update.message.reply_text(f"✅ Имя сохранено: <b>{name}</b>", parse_mode=constants.ParseMode.HTML)

async def wallet_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_chat.id
    u = next((x for x in get_users() if x["chat_id"] == uid), None)
    if not u:
        return await update.message.reply_text("Вы ещё не подключены. Начните с /start")
    w = u.get("wallet") or "—"
    n = u.get("network") or "—"
    await update.message.reply_text(
        f"👛 <b>Кошелёк для выплат</b>\nАдрес: <code>{w}</code>\nСеть: <b>{n}</b>",
        parse_mode=constants.ParseMode.HTML
    )

async def setwallet_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    # /setwallet WALLET TRC20   | допускаем пробелы в адресе без <>.
    parts = update.message.text.split()
    if len(parts) < 3:
        return await update.message.reply_text(
            "Использование: <code>/setwallet ТВойАдрес TRC20</code>",
            parse_mode=constants.ParseMode.HTML
        )
    _, wallet, network = parts[0], parts[1].strip(), parts[2].strip().upper()
    cid = update.effective_chat.id
    u = next((x for x in get_users() if x["chat_id"] == cid), None)
    name = (u and u["name"]) or str(cid)
    upsert_user(cid, Chat_ID=str(cid), Name=name, Wallet=wallet, Network=network, Active="TRUE")
    await update.message.reply_text("✅ Кошелёк обновлён.", parse_mode=constants.ParseMode.HTML)

async def add_deposit_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    # пользовательская заявка на пополнение
    parts = update.message.text.split(maxsplit=1)
    if len(parts) < 2:
        return await update.message.reply_text("Использование: <code>/add_deposit 500</code>", parse_mode=constants.ParseMode.HTML)
    amount = parse_money(parts[1])
    if amount <= 0:
        return await update.message.reply_text("Сумма должна быть больше нуля.")
    cid = update.effective_chat.id
    u = next((x for x in get_users() if x["chat_id"] == cid), None)
    name = (u and u["name"]) or str(cid)
    # Запись заявки в Ledger
    ledger_add(cid, name, "REQUEST_ADD_DEPOSIT", amount, "User requested deposit increase")
    await update.message.reply_text("📝 Заявка на пополнение отправлена админу. После подтверждения депозит активируется со следующей сделкой.",
                                    parse_mode=constants.ParseMode.HTML)
    # Админу — уведомление
    txt = f"🆕 <b>Заявка на пополнение</b>\nID: <code>{cid}</code>\nИмя: <b>{name}</b>\nСумма: <b>${fmt_usd(amount)}</b>\nСтатус: <b>новый</b>"
    for aid in ADMIN_IDS:
        try:
            await ctx.application.bot.send_message(chat_id=aid, text=txt, parse_mode=constants.ParseMode.HTML)
        except Exception:
            pass

async def add_from_bonus_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    parts = update.message.text.split(maxsplit=1)
    if len(parts) < 2:
        return await update.message.reply_text("Использование: <code>/add_from_bonus 100</code>", parse_mode=constants.ParseMode.HTML)
    amount = parse_money(parts[1])
    if amount <= 0:
        return await update.message.reply_text("Сумма должна быть больше нуля.")
    cid = update.effective_chat.id
    u = next((x for x in get_users() if x["chat_id"] == cid), None)
    if not u:
        return await update.message.reply_text("Вы ещё не подключены. Начните с /start")
    if amount > max(0.0, u["bonus_accrued"] - u["bonus_paid"] - u["bonus_to_deposit"]):
        return await update.message.reply_text("Недостаточно доступной премии.")
    name = u["name"] or str(cid)
    ledger_add(cid, name, "REQUEST_ADD_FROM_BONUS", amount, "User requested convert bonus → deposit")
    await update.message.reply_text("📝 Заявка на пополнение из премии отправлена админу. Будет применено со следующей сделкой.",
                                    parse_mode=constants.ParseMode.HTML)
    txt = f"🆕 <b>Заявка из премии</b>\nID: <code>{cid}</code>\nИмя: <b>{name}</b>\nСумма: <b>${fmt_usd(amount)}</b>\nСтатус: <b>новый</b>"
    for aid in ADMIN_IDS:
        try:
            await ctx.application.bot.send_message(chat_id=aid, text=txt, parse_mode=constants.ParseMode.HTML)
        except Exception:
            pass

async def withdraw_bonus_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    parts = update.message.text.split(maxsplit=1)
    cid = update.effective_chat.id
    u = next((x for x in get_users() if x["chat_id"] == cid), None)
    if not u:
        return await update.message.reply_text("Вы ещё не подключены. Начните с /start")

    avail = max(0.0, u["bonus_accrued"] - u["bonus_paid"] - u["bonus_to_deposit"])
    if len(parts) < 2:
        return await update.message.reply_text(
            f"Доступно к выводу: <b>${fmt_usd(avail)}</b>\nИспользование: <code>/withdraw_bonus 100</code> или <code>/withdraw_bonus all</code>",
            parse_mode=constants.ParseMode.HTML
        )
    amt = avail if parts[1].strip().lower() == "all" else parse_money(parts[1])
    if amt <= 0 or amt > avail:
        return await update.message.reply_text("Некорректная сумма.")

    name = u["name"] or str(cid)
    ledger_add(cid, name, "REQUEST_WITHDRAW_BONUS", amt, "User requested bonus withdrawal")
    await update.message.reply_text("📝 Заявка на вывод премии отправлена админу.", parse_mode=constants.ParseMode.HTML)

    txt = f"🆕 <b>Заявка на вывод премии</b>\nID: <code>{cid}</code>\nИмя: <b>{name}</b>\nСумма: <b>${fmt_usd(amt)}</b>\nСтатус: <b>новый</b>"
    for aid in ADMIN_IDS:
        try:
            await ctx.application.bot.send_message(chat_id=aid, text=txt, parse_mode=constants.ParseMode.HTML)
        except Exception:
            pass

async def withdraw_all_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cid = update.effective_chat.id
    u = next((x for x in get_users() if x["chat_id"] == cid), None)
    if not u:
        return await update.message.reply_text("Вы ещё не подключены. Начните с /start")

    name = u["name"] or str(cid)
    # Считаем премию к выплате
    bonus_avail = max(0.0, u["bonus_accrued"] - u["bonus_paid"] - u["bonus_to_deposit"])
    total = u["deposit"] + bonus_avail
    ledger_add(cid, name, "REQUEST_WITHDRAW_ALL", total, "User requested full withdrawal (deposit + bonus)")
    await update.message.reply_text("📝 Заявка на вывод депозита отправлена админу.", parse_mode=constants.ParseMode.HTML)

    txt = (f"🆕 <b>Заявка на полный вывод</b>\n"
           f"ID: <code>{cid}</code>\nИмя: <b>{name}</b>\n"
           f"Депозит: <b>${fmt_usd(u['deposit'])}</b>  |  Премия: <b>${fmt_usd(bonus_avail)}</b>\n"
           f"Итого: <b>${fmt_usd(total)}</b>\nСтатус: <b>новый</b>")
    for aid in ADMIN_IDS:
        try:
            await ctx.application.bot.send_message(chat_id=aid, text=txt, parse_mode=constants.ParseMode.HTML)
        except Exception:
            pass

async def balance_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cid = update.effective_chat.id
    u = next((x for x in get_users() if x["chat_id"] == cid and x["active"]), None)
    if not u:
        return await update.message.reply_text("Вы ещё не подключены. Начните с /start")

    _, start_utc, profit_total = get_state()

    # для справки выводим «моя доля» от накопленной Profit_Total (30%)
    total_dep = sum(x["deposit"] for x in get_users(active_only=True)) or 1.0
    my_share = u["deposit"] / total_dep
    my_bonus_model = profit_total * my_share

    bonus_avail = max(0.0, u["bonus_accrued"] - u["bonus_paid"] - u["bonus_to_deposit"])
    text = (
        f"🧰 <b>Баланс</b>\n\n"
        f"Имя: <b>{u['name'] or cid}</b>\n"
        f"Депозит: <b>${fmt_usd(u['deposit'])}</b>\n"
        f"Премия (накоплено): <b>${fmt_usd(u['bonus_accrued'])}</b>\n"
        f"Доступно к выводу: <b>${fmt_usd(bonus_avail)}</b>\n"
        f"Реф. доля от общей модели (индикативно): <b>${fmt_usd(my_bonus_model)}</b>\n"
        f"Начало учета: <code>{start_utc}</code>\n"
    )
    await update.message.reply_text(text, parse_mode=constants.ParseMode.HTML)

# ─────────────────────────────────────────────────────────────────────────────
# Telegram handlers: admin
# ─────────────────────────────────────────────────────────────────────────────
async def help_admin(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not _is_admin(update): return
    text = (
        "Админ-команды:\n"
        "/list — список пользователей\n"
        "/adduser <chat_id> <Имя> <депозит>\n"
        "/setdep <chat_id> <депозит>   (pending — применится со след. сделки)\n"
        "/setname <chat_id> <Имя>\n"
        "/remove <chat_id>\n"
    )
    await update.message.reply_text(text)

async def list_admin(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not _is_admin(update): return
    users = get_users()
    if not users:
        return await update.message.reply_text("Список пуст.")
    lines = []
    for u in users:
        status = "✅ активный" if u["active"] else "⛔️ отключён"
        lines.append(f"{status}  {u['name'] or u['chat_id']}  |  id={u['chat_id']}  |  dep=${fmt_usd(u['deposit'])}  |  pend=${fmt_usd(u['pending'])}")
    await update.message.reply_text("\n".join(lines))

async def adduser_admin(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not _is_admin(update): return
    try:
        args = ctx.args
        if len(args) < 3: raise ValueError
        chat_id = int(args[0])
        dep = parse_money(args[-1])
        name = " ".join(args[1:-1]).strip() or str(chat_id)
    except Exception:
        return await update.message.reply_text("Использование: /adduser <chat_id> <Имя> <депозит>")
    upsert_user(chat_id, Chat_ID=str(chat_id), Name=name, Deposit_USDT=f"{dep}", Active="TRUE", Pending_Deposit="0")
    await set_user_menu(ctx.application, chat_id)
    await update.message.reply_text(f"OK. {name} (id={chat_id}) добавлен, депозит ${fmt_usd(dep)}.")
    try:
        await ctx.application.bot.send_message(chat_id=chat_id,
            text=f"👋 Добро пожаловать, <b>{name}</b>! Ваш депозит: <b>${fmt_usd(dep)}</b>.",
            parse_mode=constants.ParseMode.HTML)
    except Exception:
        pass

async def setdep_admin(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not _is_admin(update): return
    m = re.match(r"^/setdep\s+(-?\d+)\s+([0-9][\d\s.,]*)\s*$", update.message.text.strip(), re.I)
    if not m:
        return await update.message.reply_text("Использование: /setdep <chat_id> <депозит>")
    chat_id = int(m.group(1))
    dep = parse_money(m.group(2))
    upsert_user(chat_id, Pending_Deposit=f"{dep}")
    await update.message.reply_text(f"Pending-депозит для id={chat_id} установлен: ${fmt_usd(dep)}.\nБудет применён со следующей сделкой.")

async def setname_admin(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not _is_admin(update): return
    if len(ctx.args) < 2:
        return await update.message.reply_text("Использование: /setname <chat_id> <Имя>")
    try:
        chat_id = int(ctx.args[0])
    except Exception:
        return await update.message.reply_text("Неверный chat_id")
    name = " ".join(ctx.args[1:]).strip()
    if not name:
        return await update.message.reply_text("Пустое имя.")
    upsert_user(chat_id, Name=name)
    await update.message.reply_text("OK. Имя обновлено.")

async def remove_admin(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not _is_admin(update): return
    if not ctx.args:
        return await update.message.reply_text("Использование: /remove <chat_id>")
    try:
        chat_id = int(ctx.args[0])
    except Exception:
        return await update.message.reply_text("Неверный chat_id")
    upsert_user(chat_id, Active="FALSE")
    await update.message.reply_text("OK. Пользователь деактивирован.")
    try:
        await ctx.application.bot.set_my_commands([BotCommand("start", "Как начать")], scope=BotCommandScopeChat(chat_id))
    except Exception:
        pass

# ─────────────────────────────────────────────────────────────────────────────
# Poller: читаем LOG_SHEET и шлём сообщения
# ─────────────────────────────────────────────────────────────────────────────
open_positions: Dict[str, Dict[str, Any]] = {}  # sid -> {cum_margin, users}

async def _send_all(app: Application, text_by_user: Dict[int, str]):
    for chat_id, text in text_by_user.items():
        if not text.strip(): continue
        try:
            await app.bot.send_message(chat_id=chat_id, text=text, parse_mode=constants.ParseMode.HTML, disable_web_page_preview=True)
        except Exception as e:
            log.warning(f"send to {chat_id} failed: {e}")

def _records() -> List[Dict[str, Any]]:
    return _sheet_dicts(ws(LOG_SHEET))

def annual_forecast(profit_total: float, start_utc: str, deposit: float) -> Tuple[float, float]:
    try:
        start_dt = datetime.strptime(start_utc, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except Exception:
        return 0.0, 0.0
    days = max((datetime.now(timezone.utc) - start_dt).total_seconds() / 86400.0, 1.0)
    if deposit <= 0: return 0.0, 0.0
    annual_pct = (profit_total / deposit) * (365.0 / days) * 100.0
    return annual_pct, deposit * annual_pct / 100.0

async def poll_and_broadcast(app: Application):
    try:
        last_row, start_utc, profit_total = get_state()
        recs = _records()
        total_rows = len(recs) + 1  # с учётом заголовка

        if last_row == 0:
            # первая инициализация — пропустить историю
            set_state(last_row=total_rows, profit_total=0.0)
            return
        if total_rows <= last_row:
            return

        new = recs[(last_row - 1):]
        users_all = get_users(active_only=True)

        per_user_msgs: Dict[int, List[str]] = {}
        def push(uid: int, text: str):
            per_user_msgs.setdefault(uid, []).append(text)

        for r in new:
            ev  = (r.get("Event") or "").strip()
            sid = (r.get("Signal_ID") or "").strip()
            cm  = to_float(r.get("Cum_Margin_USDT"))
            pnl = to_float(r.get("PNL_Realized_USDT"))

            if ev in ("OPEN", "ADD", "RETEST_ADD"):
                if ev == "OPEN":
                    # применяем pending депозит ко всем активным пользователям
                    for u in users_all:
                        if u["pending"] > 0:
                            upsert_user(u["chat_id"], Deposit_USDT=f"{u['pending']}", Pending_Deposit="0")
                            u["deposit"], u["pending"] = u["pending"], 0
                    recipients = [u["chat_id"] for u in users_all]
                    open_positions[sid] = {"cum_margin": cm, "users": recipients}
                else:
                    snap = open_positions.setdefault(sid, {"cum_margin": 0.0, "users": [u["chat_id"] for u in users_all]})
                    snap["cum_margin"] = cm
                    recipients = snap["users"]

                used_pct = 100.0 * (cm / max(SYSTEM_BANK_USDT, 1e-9))
                if ev == "OPEN":
                    msg = f"📊 Сделка открыта. Задействовано {used_pct:.1f}% банка (<b>${fmt_usd(cm)}</b>)."
                else:
                    msg = f"➕ Добор по {base_from_pair(r.get('Pair', ''))}. Объём в сделке: {used_pct:.1f}% банка (<b>${fmt_usd(cm)}</b>)."
                for uid in recipients:
                    push(uid, msg)

            if ev in ("TP_HIT", "SL_HIT", "MANUAL_CLOSE"):
                snap = open_positions.get(sid, {})
                recipients = snap.get("users", [])
                cm2 = snap.get("cum_margin", cm)

                if not recipients:
                    # fallback: если не нашли — рассылаем всем активным
                    recipients = [u["chat_id"] for u in users_all]

                used_pct = 100.0 * (cm2 / max(SYSTEM_BANK_USDT, 1e-9))
                profit_pct = (pnl / max(cm2, 1e-9)) * 100.0 if cm2 > 0 else 0.0
                icon = tier_emoji(profit_pct) if pnl >= 0 else "🛑"

                # 30% модель
                pool_user_share = pnl * PROFIT_USER_SHARE
                profit_total += pool_user_share

                # распределение бонуса по пользователям, участвовавшим в сделке
                users_map = {u["chat_id"]: u for u in users_all if u["chat_id"] in recipients}
                total_dep = sum(u["deposit"] for u in users_map.values()) or 1.0

                for uid, u in users_map.items():
                    weight = u["deposit"] / total_dep
                    add_bonus = pool_user_share * weight
                    new_bonus = u["bonus_accrued"] + add_bonus
                    upsert_user(uid, Bonus_Accrued=f"{new_bonus}")
                    ann_pct, ann_usd = annual_forecast(new_bonus, start_utc, u["deposit"])
                    txt = (f"{icon} Сделка закрыта. Использовалось {used_pct:.1f}% банка (<b>${fmt_usd(cm2)}</b>). "
                           f"P&L: <b>${fmt_usd(pnl)}</b> ({profit_pct:+.2f}%).\n"
                           f"Ваша премия (30% модели, суммарно): <b>${fmt_usd(new_bonus)}</b> "
                           f"| индикатив годовых: ~{ann_pct:.1f}% (≈${fmt_usd(ann_usd)}/год).")
                    push(uid, txt)

                if sid in open_positions:
                    del open_positions[sid]

        # рассылка
        ready = {uid: "\n\n".join(msgs) for uid, msgs in per_user_msgs.items() if msgs}
        if ready:
            await _send_all(app, ready)

        set_state(last_row=total_rows, profit_total=profit_total)

    except Exception as e:
        log.exception("poll_and_broadcast error")

async def poll_job(context: ContextTypes.DEFAULT_TYPE):
    await poll_and_broadcast(context.application)

# ─────────────────────────────────────────────────────────────────────────────
# App init
# ─────────────────────────────────────────────────────────────────────────────
async def post_init(app: Application):
    await set_default_menu(app)
    await set_admin_menus(app)
    # постараемся восстановить меню у текущих активных пользователей
    try:
        for u in get_users(active_only=True):
            try:
                await set_user_menu(app, u["chat_id"])
            except Exception:
                pass
    except Exception:
        pass

def main():
    app = ApplicationBuilder().token(BOT_TOKEN).post_init(post_init).build()

    # user
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("about", about_cmd))
    app.add_handler(CommandHandler("myname", myname_cmd))
    app.add_handler(CommandHandler("wallet", wallet_cmd))
    app.add_handler(CommandHandler("setwallet", setwallet_cmd))
    app.add_handler(CommandHandler("add_deposit", add_deposit_cmd))
    app.add_handler(CommandHandler("add_from_bonus", add_from_bonus_cmd))
    app.add_handler(CommandHandler("withdraw_bonus", withdraw_bonus_cmd))
    app.add_handler(CommandHandler("withdraw_all", withdraw_all_cmd))
    app.add_handler(CommandHandler("balance", balance_cmd))

    # admin
    app.add_handler(CommandHandler("help", help_admin))
    app.add_handler(CommandHandler("list", list_admin))
    app.add_handler(CommandHandler("adduser", adduser_admin))
    app.add_handler(CommandHandler("setdep", setdep_admin))
    app.add_handler(CommandHandler("setname", setname_admin))
    app.add_handler(CommandHandler("remove", remove_admin))

    app.job_queue.run_repeating(poll_job, interval=10, first=5)
    log.info(f"{BOT_NAME} starting…")
    app.run_polling()

if __name__ == "__main__":
    main()
