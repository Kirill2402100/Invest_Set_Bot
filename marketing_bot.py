# marketing_bot.py — STRIGI_KAPUSTU_BOT (полная версия)

import os, logging, re, json
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple

import gspread
from telegram import (
    Update, constants, BotCommand,
    BotCommandScopeChat, BotCommandScopeAllPrivateChats
)
from telegram.ext import (
    ApplicationBuilder, Application, CommandHandler,
    ContextTypes
)

# ------------------- ENV -------------------
BOT_NAME = "STRIGI_KAPUSTU_BOT"
BOT_TOKEN = os.getenv("MARKETING_BOT_TOKEN")
SHEET_ID = os.getenv("SHEET_ID")

def parse_admin_ids(raw: str) -> set[int]:
    if not raw: return set()
    try:
        maybe = json.loads(raw)
        if isinstance(maybe, (list, tuple, set)): return {int(x) for x in maybe}
        if isinstance(maybe, (int, str)) and str(maybe).lstrip("-").isdigit(): return {int(maybe)}
    except Exception:
        pass
    out = set()
    for t in re.split(r'[\s,;]+', raw.strip()):
        t = t.strip().strip('[](){}"\'')
        if t and (t.lstrip("-").isdigit()):
            out.add(int(t))
    return out

ADMIN_IDS = parse_admin_ids(os.getenv("ADMIN_IDS", ""))
SYSTEM_BANK_USDT = float(os.getenv("SYSTEM_BANK_USDT", "1000"))

if not BOT_TOKEN or not SHEET_ID or not ADMIN_IDS:
    raise RuntimeError("MARKETING_BOT_TOKEN / SHEET_ID / ADMIN_IDS обязательны")

def now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

# ------------------- LOG -------------------
log = logging.getLogger("marketing")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log.info(f"ADMIN_IDS parsed={sorted(ADMIN_IDS)}")

# ------------------- Меню команд -------------------
USER_COMMANDS = [
    BotCommand("start", "Как подключиться"),
    BotCommand("about", "О боте"),
    BotCommand("myname", "Указать имя"),
    BotCommand("balance", "Баланс"),
    BotCommand("add_deposit", "Добавить депозит"),
    BotCommand("add_from_bonus", "Пополнить из премии"),
    BotCommand("withdraw_bonus", "Вывести премию"),
    BotCommand("withdraw_all", "Вывести весь депозит"),
    BotCommand("mywallet", "Мой кошелёк"),
    BotCommand("setwallet", "Задать кошелёк"),
]
ADMIN_COMMANDS = [
    BotCommand("help", "Команды админа"),
    BotCommand("list", "Список пользователей"),
    BotCommand("adduser", "Добавить пользователя"),
    BotCommand("setdep", "Изменить депозит (со след. сделки)"),
    BotCommand("setname", "Переименовать пользователя"),
    BotCommand("remove", "Отключить пользователя"),
    BotCommand("approve_wallet", "Подтвердить кошелёк"),
    BotCommand("reject_wallet", "Отклонить кошелёк"),
    BotCommand("apply_from_bonus", "В депозит из премии"),
    BotCommand("pay_bonus", "Выплатить премию"),
    BotCommand("pay_all", "Вывести всё и отключить"),
]

async def set_menu_default(app: Application):
    await app.bot.set_my_commands(
        [BotCommand("start", "Как подключиться"), BotCommand("about", "О боте")],
        scope=BotCommandScopeAllPrivateChats()
    )

async def set_menu_user(app: Application, chat_id: int):
    await app.bot.set_my_commands(USER_COMMANDS, scope=BotCommandScopeChat(chat_id))

async def set_menu_admins(app: Application):
    for aid in ADMIN_IDS:
        try:
            await app.bot.set_my_commands(ADMIN_COMMANDS, scope=BotCommandScopeChat(aid))
        except Exception as e:
            log.error(f"Failed to set menu for admin {aid}: {e}")

# ------------------- Sheets -------------------
CREDS_JSON = os.getenv("GOOGLE_CREDENTIALS")
if not CREDS_JSON:
    raise RuntimeError("GOOGLE_CREDENTIALS env var not set")

gc = gspread.service_account_from_dict(json.loads(CREDS_JSON))
sh = gc.open_by_key(SHEET_ID)

LOG_SHEET   = "BMR_DCA_Log"
USERS_SHEET = "Marketing_Users"
STATE_SHEET = "Marketing_State"
LEDGER_SHEET= "Marketing_Ledger"

# Требуемые хедеры таблиц
USERS_HEADERS = [
    "Chat_ID","Name","Deposit_USDT","Active","Pending_Deposit",
    "Bonus_Accrued","Bonus_Paid","Bonus_To_Deposit",
    "Wallet_Address","Wallet_Network",
    "Wallet_Pending_Address","Wallet_Pending_Network",
    "Wallet_Updated_UTC","Last_Update"
]
STATE_HEADERS = ["Last_Row","Start_UTC","Profit30_Total_USDT"]
LEDGER_HEADERS = [
    "Timestamp_UTC","Type","Chat_ID","Name","Amount_USDT","Note","Admin",
    "Signal_ID","Tx_Direction","Old_Address","Old_Network","New_Address","New_Network","Status"
]

# ------------- helpers -------------
def to_float(x) -> float:
    try:
        return float(str(x).replace(",", ".").strip())
    except (ValueError, TypeError):
        return 0.0

def parse_money(s: str) -> float:
    s = (s or "").strip()
    if s.lower() == "all":
        return float("nan")  # спец-значение: "all"
    return float(re.sub(r"[^\d.,\-]", "", s).replace(",", "."))

def fmt_usd(x) -> str:
    try:
        v = float(x)
    except:
        v = 0.0
    return f"{v:,.2f}".replace(",", " ")

def is_admin(update: Update) -> bool:
    uid = update.effective_user.id if update.effective_user else None
    cid = update.effective_chat.id if update.effective_chat else None
    return (uid in ADMIN_IDS) or (cid in ADMIN_IDS)

def base_from_pair(pair: str) -> str:
    base = (pair or "").split("/")[0].split(":")[0].upper()
    return base[:-1] if base.endswith("C") and len(base) > 3 else base

def sheet_dicts(worksheet) -> List[Dict[str, Any]]:
    vals = worksheet.get_all_values()
    if not vals or len(vals) < 2:
        return []
    headers, out = vals[0], []
    for row in vals[1:]:
        out.append({headers[i]: (row[i] if i < len(row) else "") for i in range(len(headers))})
    return out

def ws(title): return sh.worksheet(title)

def ensure_headers(ws_title: str, required: List[str]):
    names = {w.title for w in sh.worksheets()}
    if ws_title not in names:
        ws_new = sh.add_worksheet(ws_title, rows=200, cols=max(10, len(required)))
        ws_new.update(f"A1:{chr(64+len(required))}1", [required])
        return
    w = ws(ws_title)
    vals = w.get_all_values()
    existing = vals[0] if vals else []
    missing = [h for h in required if h not in existing]
    if not existing:
        w.update(f"A1:{chr(64+len(required))}1", [required])
        return
    if missing:
        # добавим недостающие столбцы в конец
        new_headers = existing + missing
        w.resize(rows=max(len(vals), 2), cols=len(new_headers))
        w.update(f"A1:{chr(64+len(new_headers))}1", [new_headers])
        # добьём пустые значения по новым колонкам
        if len(vals) > 1:
            blanks = [[""] * len(missing) for _ in range(len(vals)-1)]
            w.update(f"{chr(64+len(existing)+1)}2:{chr(64+len(new_headers))}{len(vals)}", blanks)

def ensure_sheets():
    ensure_headers(USERS_SHEET, USERS_HEADERS)
    ensure_headers(STATE_SHEET, STATE_HEADERS)
    ensure_headers(LEDGER_SHEET, LEDGER_HEADERS)
    names = {w.title for w in sh.worksheets()}
    if LOG_SHEET not in names:
        raise RuntimeError(f"Не найден лист {LOG_SHEET} (его пишет основной бот)")
    # init state defaults
    wst = ws(STATE_SHEET)
    vals = wst.get_all_values()
    if len(vals) < 2:
        wst.update("A2:C2", [["0", now_utc_str(), "0"]])
    else:
        if not (wst.acell("B2").value or "").strip():
            wst.update_acell("B2", now_utc_str())
        if not (wst.acell("C2").value or "").strip():
            wst.update_acell("C2", "0")

ensure_sheets()

# ------------ CRUD users/state/ledger ------------
def get_state() -> Tuple[int, str, float]:
    w = ws(STATE_SHEET)
    a, b, c = w.acell("A2").value, w.acell("B2").value, w.acell("C2").value
    last_row = int(a) if (a or "").strip().isdigit() else 0
    start_utc = b or now_utc_str()
    profit30_total = to_float(c)
    return last_row, start_utc, profit30_total

def set_state(last_row: Optional[int] = None, profit30_total: Optional[float] = None, start_utc: Optional[str] = None):
    w = ws(STATE_SHEET)
    if last_row is not None: w.update_acell("A2", str(last_row))
    if start_utc is not None: w.update_acell("B2", start_utc)
    if profit30_total is not None: w.update_acell("C2", str(profit30_total))

def get_users() -> List[Dict[str, Any]]:
    vals = ws(USERS_SHEET).get_all_records()
    res = []
    for r in vals:
        try:
            res.append({
                "chat_id": int(r.get("Chat_ID")),
                "name": r.get("Name") or "",
                "deposit": to_float(r.get("Deposit_USDT")),
                "active": str(r.get("Active", "TRUE")).strip().upper() not in ("FALSE", "0", ""),
                "pending": to_float(r.get("Pending_Deposit")),
                "bonus_acc": to_float(r.get("Bonus_Accrued")),
                "bonus_paid": to_float(r.get("Bonus_Paid")),
                "bonus_to_dep": to_float(r.get("Bonus_To_Deposit")),
                "w_addr": (r.get("Wallet_Address") or "").strip(),
                "w_net": (r.get("Wallet_Network") or "").strip().upper(),
                "w_p_addr": (r.get("Wallet_Pending_Address") or "").strip(),
                "w_p_net": (r.get("Wallet_Pending_Network") or "").strip().upper(),
                "updated": (r.get("Last_Update") or "").strip(),
            })
        except Exception as e:
            log.warning(f"Skipping invalid user row: {r} err={e}")
    return res

def find_user_row_idx(chat_id: int) -> Optional[int]:
    w = ws(USERS_SHEET)
    try:
        cell = w.find(str(chat_id), in_column=1)
        if cell: return cell.row
    except Exception:
        pass
    # fallback
    try:
        col = w.col_values(1)
        for i, v in enumerate(col, start=1):
            if str(v).strip() == str(chat_id):
                return i
    except Exception:
        pass
    return None

def upsert_user_row(
    chat_id: int,
    name: Optional[str] = None,
    deposit: Optional[float] = None,
    active: Optional[bool] = None,
    pending: Optional[float] = None,
    bonus_acc: Optional[float] = None,
    bonus_paid: Optional[float] = None,
    bonus_to_dep: Optional[float] = None,
    w_addr: Optional[str] = None,
    w_net: Optional[str] = None,
    w_p_addr: Optional[str] = None,
    w_p_net: Optional[str] = None,
):
    w = ws(USERS_SHEET)
    headers = w.row_values(1)
    idx = {h: i for i, h in enumerate(headers)}
    row_idx = find_user_row_idx(chat_id)
    def get(row_vals, h, default=""):
        return row_vals[idx[h]] if (h in idx and idx[h] < len(row_vals)) else default
    now = now_utc_str()
    if row_idx:
        cur = w.row_values(row_idx)
        values = {h: get(cur, h, "") for h in headers}
        # apply changes
        if name is not None: values["Name"] = name
        if deposit is not None: values["Deposit_USDT"] = str(deposit)
        if active is not None: values["Active"] = "TRUE" if active else "FALSE"
        if pending is not None: values["Pending_Deposit"] = str(pending)
        if bonus_acc is not None: values["Bonus_Accrued"] = str(bonus_acc)
        if bonus_paid is not None: values["Bonus_Paid"] = str(bonus_paid)
        if bonus_to_dep is not None: values["Bonus_To_Deposit"] = str(bonus_to_dep)
        if w_addr is not None: values["Wallet_Address"] = w_addr
        if w_net is not None: values["Wallet_Network"] = w_net
        if w_p_addr is not None: values["Wallet_Pending_Address"] = w_p_addr
        if w_p_net is not None: values["Wallet_Pending_Network"] = w_p_net
        values["Last_Update"] = now
        row = [values.get(h, "") for h in headers]
        w.update(f"A{row_idx}:{chr(64+len(headers))}{row_idx}", [row])
    else:
        row = {
            "Chat_ID": str(chat_id),
            "Name": name or "",
            "Deposit_USDT": str(deposit or 0),
            "Active": "FALSE" if (active is False) else "TRUE",
            "Pending_Deposit": str(pending or 0),
            "Bonus_Accrued": str(bonus_acc or 0),
            "Bonus_Paid": str(bonus_paid or 0),
            "Bonus_To_Deposit": str(bonus_to_dep or 0),
            "Wallet_Address": w_addr or "",
            "Wallet_Network": (w_net or "").upper(),
            "Wallet_Pending_Address": w_p_addr or "",
            "Wallet_Pending_Network": (w_p_net or "").upper(),
            "Wallet_Updated_UTC": "",
            "Last_Update": now
        }
        w.append_row([row.get(h, "") for h in headers], value_input_option="RAW")

def append_ledger(**kwargs):
    w = ws(LEDGER_SHEET)
    headers = w.row_values(1)
    row = [str(kwargs.get(h, "")) for h in headers]
    # если каких-то полей нет — заполним динамически по совпадению ключей
    for k, v in kwargs.items():
        if k not in headers:
            headers.append(k)
            # расширим лист + заголовок
            w.resize(cols=len(headers))
            w.update(f"A1:{chr(64+len(headers))}1", [headers])
            row.append(str(v))
    w.append_row(row, value_input_option="RAW")

# ------------------- расчёт годовых -------------------
def annual_forecast(user_bonus_total: float, start_utc: str, user_deposit: float) -> Tuple[float, float]:
    try:
        start_dt = datetime.strptime(start_utc, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except Exception:
        return 0.0, 0.0
    days = max((datetime.now(timezone.utc) - start_dt).total_seconds() / 86400.0, 1)
    if user_deposit <= 0:
        return 0.0, 0.0
    annual_pct = (user_bonus_total / user_deposit) * (365.0 / days) * 100.0
    return annual_pct, user_deposit * annual_pct / 100.0

# ------------------- Telegram: тексты -------------------
START_TEXT = (
    "👋 Привет! Я <b>STRIGI_KAPUSTU_BOT</b>.\n\n"
    "Чтобы начать:\n"
    "1) Отправьте команду: <code>/myname Имя Фамилия</code>\n"
    "2) Переведите USDT на адрес:\n"
    "   <code>TVSRhKYHAUKx8RnXzW3KXNeUk5aAQs7hJ4</code>\n"
    "   (сеть <b>TRON / TRC-20</b>)\n"
    "3) Отправьте команду: <code>/add_deposit 500</code> (укажите сумму перевода)\n"
    "4) Дождитесь подтверждения — депозит активируется со следующей сделкой\n"
    "5) Проверяйте состояние: <b>/balance</b>\n\n"
    "💼 Для выводов заранее сохраните кошелёк: <code>/setwallet &lt;адрес&gt; TRC20</code>\n\n"
    "Дополнительно:\n"
    "• Пополнить из премии: <code>/add_from_bonus 100</code>\n"
    "• Вывод премии: <code>/withdraw_bonus 100</code> (или <code>all</code>)\n"
    "• Вывод всего депозита: <b>/withdraw_all</b>\n"
)

ABOUT_TEXT = (
    "🤖 <b>О боте</b>\n\n"
    "Это инвестиционный бот, который ведёт алгоритмическую торговлю Евро ↔ Доллар через стейблкоины (EURC/USDT) на бирже. "
    "Алгоритм автоматически управляет входами, доборами и выходами, присылает уведомления и ведёт учёт сделок.\n\n"
    "📈 <b>Модель дохода</b>\n"
    "В отчётах вам отображается прибыль только закрытых сделок — это ваша «премия». "
    "Её можно вывести (<code>/withdraw_bonus</code>) или реинвестировать (<code>/add_from_bonus</code>).\n\n"
    "⚠️ <b>Дисклеймер о рисках</b>\n"
    "Торговля на рынке (в т.ч. с плечом) связана с высокой волатильностью и может привести к частичной или полной потере средств. "
    "Прошлые результаты не гарантируют будущую доходность. Используя бота, вы подтверждаете, что понимаете и принимаете эти риски."
)

# ------------------- Telegram handlers: Users -------------------
async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(START_TEXT, parse_mode=constants.ParseMode.HTML, disable_web_page_preview=True)

async def about(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(ABOUT_TEXT, parse_mode=constants.ParseMode.HTML, disable_web_page_preview=True)

async def myname(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    name = (update.message.text or "").replace("/myname", "", 1).strip()
    if not name:
        return await update.message.reply_text("Укажите имя: <code>/myname Имя Фамилия</code>", parse_mode=constants.ParseMode.HTML)
    users = get_users()
    u = next((x for x in users if x["chat_id"] == chat_id), None)
    if not u:
        upsert_user_row(chat_id, name=name, active=False)  # новый пользователь
        status = "новый"
    else:
        upsert_user_row(chat_id, name=name)
        status = "активный" if u["active"] else "новый"
    await update.message.reply_text(f"✅ Имя сохранено: <b>{name}</b>", parse_mode=constants.ParseMode.HTML)
    # уведомим админов
    for aid in ADMIN_IDS:
        try:
            await ctx.application.bot.send_message(
                chat_id=aid,
                text=f"👤 NEW/UPDATE NAME\nПользователь: <b>{name}</b> (id <code>{chat_id}</code>, {status})",
                parse_mode=constants.ParseMode.HTML
            )
        except Exception as e:
            log.warning(f"notify admin name failed: {e}")

async def balance(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    users = get_users()
    u = next((x for x in users if x["chat_id"] == chat_id), None)
    if not u or not u["active"]:
        return await update.message.reply_text("Вы ещё не подключены. Отправьте /start и передайте ваш chat_id админу.")
    # доступная премия = начислено - выплачено - переведено в депозит
    bonus_avail = max(0.0, u["bonus_acc"] - u["bonus_paid"] - u["bonus_to_dep"])
    # кошелёк
    wallet_line = "—"
    if u["w_addr"]:
        wallet_line = f"{u['w_addr']} / {u['w_net'] or 'TRC20'}"
    elif u["w_p_addr"]:
        wallet_line = f"(в ожидании) {u['w_p_addr']} / {u['w_p_net'] or 'TRC20'}"
    # итог
    txt = (
        f"🧰 <b>Баланс</b>\n\n"
        f"Депозит: <b>${fmt_usd(u['deposit'])}</b>\n"
        f"Премия (начислено): <b>${fmt_usd(u['bonus_acc'])}</b>\n"
        f"— выплачено: <b>${fmt_usd(u['bonus_paid'])}</b>\n"
        f"— переведено в депозит: <b>${fmt_usd(u['bonus_to_dep'])}</b>\n"
        f"Доступно к выводу: <b>${fmt_usd(bonus_avail)}</b>\n\n"
        f"Кошелёк для выводов: <b>{wallet_line}</b>"
    )
    await update.message.reply_text(txt, parse_mode=constants.ParseMode.HTML)

async def add_deposit(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    args = (ctx.args or [])
    if not args:
        return await update.message.reply_text("Использование: <code>/add_deposit 500</code>", parse_mode=constants.ParseMode.HTML)
    try:
        add = parse_money(args[0])
        if add != add or add <= 0:  # NaN или <=0
            raise ValueError
    except Exception:
        return await update.message.reply_text("Сумма некорректна. Пример: <code>/add_deposit 500</code>", parse_mode=constants.ParseMode.HTML)
    users = get_users()
    u = next((x for x in users if x["chat_id"] == chat_id), None)
    if not u:
        # создадим карточку пользователя (не активен)
        upsert_user_row(chat_id, name=str(chat_id), active=False, pending=add)
        current_dep = 0.0
        status = "новый"
        name = str(chat_id)
    else:
        current_dep = u["deposit"]
        name = u["name"] or str(chat_id)
        # Pending трактуем как целевой депозит (текущий + добавка)
        upsert_user_row(chat_id, pending=current_dep + add)
        status = "активный" if u["active"] else "новый"
    append_ledger(
        **{
            "Timestamp_UTC": now_utc_str(), "Type": "DEPOSIT_ADD_REQUEST", "Chat_ID": chat_id,
            "Name": name, "Amount_USDT": add, "Note": "Заявка на добавление депозита", "Status": "PENDING"
        }
    )
    await update.message.reply_text("📨 Заявка на пополнение депозита отправлена админу. Депозит активируется со следующей сделкой.")
    # уведомление админам + подсказка команды
    for aid in ADMIN_IDS:
        try:
            cmd = f"/setdep {chat_id} {current_dep + add:.2f}"
            await ctx.application.bot.send_message(
                chat_id=aid,
                text=(f"💵 DEPOSIT_ADD_REQUEST\n"
                      f"Пользователь: <b>{name}</b> (id <code>{chat_id}</code>, {status})\n"
                      f"Текущий депозит: ${fmt_usd(current_dep)}\n"
                      f"Запрошено добавить: ${fmt_usd(add)}\n"
                      f"👉 Применить со след. сделки: <code>{cmd}</code>"),
                parse_mode=constants.ParseMode.HTML
            )
        except Exception as e:
            log.warning(f"notify admin add_deposit failed: {e}")

async def add_from_bonus(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    args = (ctx.args or [])
    if not args:
        return await update.message.reply_text("Использование: <code>/add_from_bonus 100</code>", parse_mode=constants.ParseMode.HTML)
    users = get_users()
    u = next((x for x in users if x["chat_id"] == chat_id), None)
    if not u:
        return await update.message.reply_text("Сначала укажите имя /myname и добавьте депозит /add_deposit.")
    try:
        req = parse_money(args[0])
        bonus_avail = max(0.0, u["bonus_acc"] - u["bonus_paid"] - u["bonus_to_dep"])
        amount = bonus_avail if (req != req) else req  # NaN => all
        if amount <= 0 or amount > bonus_avail + 1e-9:
            raise ValueError
    except Exception:
        return await update.message.reply_text(f"Недостаточно средств. Доступно из премии: ${fmt_usd(max(0.0, u['bonus_acc']-u['bonus_paid']-u['bonus_to_dep']))}")
    target_dep = u["deposit"] + amount
    upsert_user_row(chat_id, pending=target_dep)
    append_ledger(
        **{
            "Timestamp_UTC": now_utc_str(), "Type": "BONUS_TO_DEPOSIT_REQUEST", "Chat_ID": chat_id,
            "Name": u["name"] or str(chat_id), "Amount_USDT": amount, "Note": "Премия в депозит", "Status": "PENDING"
        }
    )
    await update.message.reply_text("📨 Заявка на пополнение из премии отправлена админу. Изменение вступит со следующей сделкой.")
    for aid in ADMIN_IDS:
        try:
            cmd = f"/apply_from_bonus {chat_id} {amount:.2f}"
            cmd2 = f"/setdep {chat_id} {target_dep:.2f}"
            await ctx.application.bot.send_message(
                chat_id=aid,
                text=(f"💼 BONUS_TO_DEPOSIT_REQUEST\n"
                      f"Пользователь: <b>{u['name'] or chat_id}</b> (id <code>{chat_id}</code>, {'активный' if u['active'] else 'новый'})\n"
                      f"Доступно из премии: ${fmt_usd(max(0.0,u['bonus_acc']-u['bonus_paid']-u['bonus_to_dep']))}\n"
                      f"Запрошено перевести: ${fmt_usd(amount)}\n"
                      f"👉 Списать из премии: <code>{cmd}</code>\n"
                      f"👉 Обновить депозит со след. сделки: <code>{cmd2}</code>"),
                parse_mode=constants.ParseMode.HTML
            )
        except Exception as e:
            log.warning(f"notify admin add_from_bonus failed: {e}")

async def withdraw_bonus(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    args = (ctx.args or [])
    if not args:
        return await update.message.reply_text("Использование: <code>/withdraw_bonus 100</code> или <code>/withdraw_bonus all</code>", parse_mode=constants.ParseMode.HTML)
    users = get_users()
    u = next((x for x in users if x["chat_id"] == chat_id), None)
    if not u:
        return await update.message.reply_text("Сначала укажите имя /myname и добавьте депозит /add_deposit.")
    # проверим кошелёк
    if not u["w_addr"]:
        return await update.message.reply_text("⚠️ Кошелёк для выводов не указан. Установите: <code>/setwallet &lt;адрес&gt; TRC20</code>", parse_mode=constants.ParseMode.HTML)
    try:
        req = parse_money(args[0])
        bonus_avail = max(0.0, u["bonus_acc"] - u["bonus_paid"] - u["bonus_to_dep"])
        amount = bonus_avail if (req != req) else req  # NaN => all
        if amount <= 0 or amount > bonus_avail + 1e-9:
            raise ValueError
    except Exception:
        return await update.message.reply_text(f"Недостаточно средств. Доступно к выводу: ${fmt_usd(max(0.0,u['bonus_acc']-u['bonus_paid']-u['bonus_to_dep']))}")
    append_ledger(
        **{
            "Timestamp_UTC": now_utc_str(), "Type": "WITHDRAW_BONUS_REQUEST", "Chat_ID": chat_id,
            "Name": u["name"] or str(chat_id), "Amount_USDT": amount,
            "Note": f"Вывод премии на {u['w_addr']} / {u['w_net'] or 'TRC20'}", "Status": "PENDING"
        }
    )
    await update.message.reply_text("📨 Заявка на вывод премии отправлена админу. Ожидайте подтверждения.")
    for aid in ADMIN_IDS:
        try:
            cmd = f"/pay_bonus {chat_id} {amount:.2f}"
            await ctx.application.bot.send_message(
                chat_id=aid,
                text=(f"💸 WITHDRAW_BONUS_REQUEST\n"
                      f"Пользователь: <b>{u['name'] or chat_id}</b> (id <code>{chat_id}</code>, {'активный' if u['active'] else 'новый'})\n"
                      f"Сумма: ${fmt_usd(amount)}\n"
                      f"Кошелёк: {u['w_addr']} / {u['w_net'] or 'TRC20'}\n"
                      f"👉 Выплатить: <code>{cmd}</code>"),
                parse_mode=constants.ParseMode.HTML
            )
        except Exception as e:
            log.warning(f"notify admin withdraw_bonus failed: {e}")

async def withdraw_all(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    users = get_users()
    u = next((x for x in users if x["chat_id"] == chat_id), None)
    if not u:
        return await update.message.reply_text("Вы ещё не подключены. Отправьте /start и передайте ваш chat_id админу.")
    if not u["w_addr"]:
        return await update.message.reply_text("⚠️ Кошелёк для выводов не указан. Установите: <code>/setwallet &lt;адрес&gt; TRC20</code>", parse_mode=constants.ParseMode.HTML)
    bonus_avail = max(0.0, u["bonus_acc"] - u["bonus_paid"] - u["bonus_to_dep"])
    total = u["deposit"] + bonus_avail
    append_ledger(
        **{
            "Timestamp_UTC": now_utc_str(), "Type": "WITHDRAW_ALL_REQUEST", "Chat_ID": chat_id,
            "Name": u["name"] or str(chat_id), "Amount_USDT": total,
            "Note": f"Вывод депозита+премии на {u['w_addr']} / {u['w_net'] or 'TRC20'}", "Status": "PENDING"
        }
    )
    await update.message.reply_text("📨 Заявка на вывод депозита и премии отправлена админу. После обработки вы будете отключены.")
    for aid in ADMIN_IDS:
        try:
            cmd = f"/pay_all {chat_id}"
            await ctx.application.bot.send_message(
                chat_id=aid,
                text=(f"🏁 WITHDRAW_ALL_REQUEST\n"
                      f"Пользователь: <b>{u['name'] or chat_id}</b> (id <code>{chat_id}</code>, {'активный' if u['active'] else 'новый'})\n"
                      f"К выплате: депозит ${fmt_usd(u['deposit'])} + премия ${fmt_usd(bonus_avail)} = <b>${fmt_usd(total)}</b>\n"
                      f"Кошелёк: {u['w_addr']} / {u['w_net'] or 'TRC20'}\n"
                      f"👉 Выплатить и отключить: <code>{cmd}</code>"),
                parse_mode=constants.ParseMode.HTML
            )
        except Exception as e:
            log.warning(f"notify admin withdraw_all failed: {e}")

# ------------------- кошельки (user) -------------------
def guess_net(addr: str) -> str:
    a = (addr or "").strip()
    if a.startswith("0x") and len(a) == 42: return "ERC20"
    if a.startswith("T") and 30 <= len(a) <= 36: return "TRC20"
    if a.startswith(("EQ","UQ")): return "TON"
    return "TRC20"

async def mywallet(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    u = next((x for x in get_users() if x["chat_id"] == chat_id), None)
    if not u:
        return await update.message.reply_text("Кошелёк не задан. Установите: <code>/setwallet &lt;адрес&gt; [сеть]</code>", parse_mode=constants.ParseMode.HTML)
    if u["w_addr"]:
        txt = (f"💼 Текущий кошелёк для выводов:\n"
               f"<code>{u['w_addr']}</code> / <b>{u['w_net'] or 'TRC20'}</b>\n"
               f"Сменить: <code>/setwallet &lt;адрес&gt; [сеть]</code> или очистить <code>/clearwallet</code>.")
    else:
        pend = f"(ожидание) {u['w_p_addr']} / {u['w_p_net']}" if u["w_p_addr"] else "—"
        txt = (f"⚠️ Кошелёк не указан.\n"
               f"Установите: <code>/setwallet &lt;адрес&gt; [сеть]</code> (по умолчанию TRC20)\n"
               f"Текущая заявка: {pend}")
    await update.message.reply_text(txt, parse_mode=constants.ParseMode.HTML)

async def setwallet(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    args = ctx.args or []
    if not args:
        return await update.message.reply_text("Использование: <code>/setwallet TVS… TRC20</code>", parse_mode=constants.ParseMode.HTML)
    addr = args[0].strip()
    net  = (args[1].strip().upper() if len(args) >= 2 else guess_net(addr))
    u = next((x for x in get_users() if x["chat_id"] == chat_id), None)
    if not u:
        upsert_user_row(chat_id, name=str(chat_id), active=False, w_p_addr=addr, w_p_net=net)
        name = str(chat_id); status = "новый"
    else:
        upsert_user_row(chat_id, w_p_addr=addr, w_p_net=net)
        name = u["name"] or str(chat_id); status = "активный" if u["active"] else "новый"
    append_ledger(**{
        "Timestamp_UTC": now_utc_str(), "Type": "WALLET_SET_REQUEST",
        "Chat_ID": chat_id, "Name": name, "Old_Address": u["w_addr"] if u else "",
        "Old_Network": u["w_net"] if u else "", "New_Address": addr, "New_Network": net, "Status": "PENDING"
    })
    await update.message.reply_text("📨 Заявка на установку кошелька отправлена админу.")
    for aid in ADMIN_IDS:
        try:
            await ctx.application.bot.send_message(
                chat_id=aid,
                text=(f"📨 WALLET_SET_REQUEST\n"
                      f"Пользователь: <b>{name}</b> (id <code>{chat_id}</code>, {status})\n"
                      f"Старый: {u['w_addr'] if u else ''} / {u['w_net'] if u else ''}\n"
                      f"Новый: {addr} / {net}\n"
                      f"👉 Подтвердить: <code>/approve_wallet {chat_id}</code>\n"
                      f"👉 Отклонить: <code>/reject_wallet {chat_id} причина</code>"),
                parse_mode=constants.ParseMode.HTML
            )
        except Exception as e:
            log.warning(f"notify admin wallet failed: {e}")

async def clearwallet(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    u = next((x for x in get_users() if x["chat_id"] == chat_id), None)
    if not u:
        return await update.message.reply_text("Вы ещё не подключены. Отправьте /start.")
    upsert_user_row(chat_id, w_p_addr="", w_p_net="")
    await update.message.reply_text("Заявка на очистку кошелька отправлена админу. (Отклоните/подтвердите через /reject_wallet или /approve_wallet)")

# ------------------- Telegram handlers: Admin -------------------
async def help_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update): return
    text = (
        "Админ-команды:\n"
        "/adduser <chat_id> <Имя> <депозит>\n"
        "/setdep <chat_id> <депозит> (со след. сделки)\n"
        "/setname <chat_id> <Имя>\n"
        "/remove <chat_id>\n"
        "/list\n"
        "/approve_wallet <chat_id>\n"
        "/reject_wallet <chat_id> [причина]\n"
        "/apply_from_bonus <chat_id> <сумма|all>\n"
        "/pay_bonus <chat_id> <сумма|all>\n"
        "/pay_all <chat_id>"
    )
    await update.message.reply_text(text)

async def adduser(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update): return
    try:
        args = ctx.args
        if len(args) < 3: raise ValueError
        chat_id, dep = int(args[0]), parse_money(args[-1])
        name = " ".join(args[1:-1]).strip() or str(chat_id)
    except (ValueError, IndexError):
        return await update.message.reply_text("Использование: /adduser <chat_id> <Имя> <депозит>")
    upsert_user_row(chat_id, name=name, deposit=dep, active=True)
    await update.message.reply_text(f"OK. Пользователь {name} ({chat_id}) добавлен с депозитом ${fmt_usd(dep)}.")
    try:
        await set_menu_user(ctx.application, chat_id)
        await ctx.application.bot.send_message(
            chat_id=chat_id,
            text=f"👋 Добро пожаловать, <b>{name}</b>! Ваш депозит активирован: ${fmt_usd(dep)}.",
            parse_mode=constants.ParseMode.HTML
        )
    except Exception as e:
        log.warning(f"greet adduser failed: {e}")

def _parse_setdep_text(text: str):
    m = re.match(r"^/setdep\s+(-?\d+)\s+([0-9][\d\s.,]*)\s*$", (text or "").strip(), re.I)
    if not m: return None
    return int(m.group(1)), parse_money(m.group(2))

async def setdep(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update): return
    parsed = _parse_setdep_text(update.message.text)
    if not parsed:
        return await update.message.reply_text("Использование: /setdep <chat_id> <депозит>")
    chat_id, dep = parsed
    upsert_user_row(chat_id, pending=dep)
    await update.message.reply_text(f"OK. Pending-депозит ${fmt_usd(dep)} применится со следующей сделкой.")
    try:
        await ctx.application.bot.send_message(
            chat_id=chat_id,
            text=f"ℹ️ Ваш депозит будет установлен на ${fmt_usd(dep)} со следующей сделкой.",
            parse_mode=constants.ParseMode.HTML
        )
    except Exception: pass

async def setname_admin(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update): return
    try:
        chat_id = int(ctx.args[0]); name = " ".join(ctx.args[1:]).strip()
        if not name: raise ValueError
    except (IndexError, ValueError):
        return await update.message.reply_text("Использование: /setname <chat_id> <Новое Имя>")
    upsert_user_row(chat_id, name=name)
    await update.message.reply_text("OK. Имя обновлено.")

async def remove(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update): return
    try:
        chat_id = int(ctx.args[0])
    except (IndexError, ValueError):
        return await update.message.reply_text("Использование: /remove <chat_id>")
    upsert_user_row(chat_id, active=False)
    await update.message.reply_text("OK. Пользователь деактивирован.")
    try:
        await ctx.application.bot.set_my_commands([BotCommand("start", "Как подключиться"), BotCommand("about","О боте")], scope=BotCommandScopeChat(chat_id))
    except Exception:
        pass

async def list_users(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update): return
    users = get_users()
    if not users:
        return await update.message.reply_text("Список пуст.")
    lines = []
    for u in users:
        status = "✅ активный" if u["active"] else "🆕 новый/неактивный"
        lines.append(f"{status} — {u['name'] or u['chat_id']} | dep={fmt_usd(u['deposit'])} | pend={fmt_usd(u['pending'])} | id={u['chat_id']}")
    await update.message.reply_text("\n".join(lines))

async def approve_wallet(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update): return
    try:
        chat_id = int(ctx.args[0])
    except Exception:
        return await update.message.reply_text("Использование: /approve_wallet <chat_id>")
    users = get_users()
    u = next((x for x in users if x["chat_id"] == chat_id), None)
    if not u or not u["w_p_addr"]:
        return await update.message.reply_text("Нет ожидающей заявки на кошелёк.")
    # переносим pending -> активный
    upsert_user_row(chat_id, w_addr=u["w_p_addr"], w_net=u["w_p_net"], w_p_addr="", w_p_net="")
    append_ledger(**{
        "Timestamp_UTC": now_utc_str(), "Type": "WALLET_SET_APPROVED",
        "Chat_ID": chat_id, "Name": u["name"] or chat_id, "Old_Address": u["w_addr"], "Old_Network": u["w_net"],
        "New_Address": u["w_p_addr"], "New_Network": u["w_p_net"], "Admin": update.effective_user.id, "Status": "OK"
    })
    await update.message.reply_text("OK. Кошелёк утверждён.")
    try:
        await ctx.application.bot.send_message(chat_id=chat_id, text=f"✅ Кошелёк утверждён: <code>{u['w_p_addr']}</code> / <b>{u['w_p_net']}</b>", parse_mode=constants.ParseMode.HTML)
    except Exception: pass

async def reject_wallet(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update): return
    try:
        chat_id = int(ctx.args[0]); reason = " ".join(ctx.args[1:]).strip() or "—"
    except Exception:
        return await update.message.reply_text("Использование: /reject_wallet <chat_id> [причина]")
    users = get_users()
    u = next((x for x in users if x["chat_id"] == chat_id), None)
    upsert_user_row(chat_id, w_p_addr="", w_p_net="")
    append_ledger(**{
        "Timestamp_UTC": now_utc_str(), "Type": "WALLET_SET_REJECTED",
        "Chat_ID": chat_id, "Name": (u and (u["name"] or chat_id)) or chat_id,
        "New_Address": (u and u["w_p_addr"]) or "", "New_Network": (u and u["w_p_net"]) or "",
        "Admin": update.effective_user.id, "Status": "REJECT", "Note": reason
    })
    await update.message.reply_text("OK. Заявка отклонена.")
    try:
        await ctx.application.bot.send_message(chat_id=chat_id, text=f"❌ Заявка на кошелёк отклонена. Причина: {reason}")
    except Exception: pass

async def apply_from_bonus(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update): return
    try:
        chat_id = int(ctx.args[0]); req = parse_money(ctx.args[1])
    except Exception:
        return await update.message.reply_text("Использование: /apply_from_bonus <chat_id> <сумма|all>")
    u = next((x for x in get_users() if x["chat_id"] == chat_id), None)
    if not u:
        return await update.message.reply_text("Пользователь не найден.")
    avail = max(0.0, u["bonus_acc"] - u["bonus_paid"] - u["bonus_to_dep"])
    amount = avail if (req != req) else req
    if amount <= 0 or amount > avail + 1e-9:
        return await update.message.reply_text(f"Недостаточно средств. Доступно: ${fmt_usd(avail)}")
    # учтём перевод в депозит (со след. сделки)
    target_dep = u["deposit"] + amount
    upsert_user_row(chat_id, pending=target_dep, bonus_to_dep=u["bonus_to_dep"] + amount)
    append_ledger(**{
        "Timestamp_UTC": now_utc_str(), "Type": "BONUS_TO_DEPOSIT_APPLIED", "Chat_ID": chat_id,
        "Name": u["name"] or chat_id, "Amount_USDT": amount, "Admin": update.effective_user.id, "Status": "OK"
    })
    await update.message.reply_text(f"OK. Из премии переведено ${fmt_usd(amount)}. Pending депозит: ${fmt_usd(target_dep)}")

async def pay_bonus(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update): return
    try:
        chat_id = int(ctx.args[0]); req = parse_money(ctx.args[1])
    except Exception:
        return await update.message.reply_text("Использование: /pay_bonus <chat_id> <сумма|all>")
    u = next((x for x in get_users() if x["chat_id"] == chat_id), None)
    if not u:
        return await update.message.reply_text("Пользователь не найден.")
    avail = max(0.0, u["bonus_acc"] - u["bonus_paid"] - u["bonus_to_dep"])
    amount = avail if (req != req) else req
    if amount <= 0 or amount > avail + 1e-9:
        return await update.message.reply_text(f"Недостаточно средств. Доступно: ${fmt_usd(avail)}")
    upsert_user_row(chat_id, bonus_paid=u["bonus_paid"] + amount)
    append_ledger(**{
        "Timestamp_UTC": now_utc_str(), "Type": "BONUS_PAID",
        "Chat_ID": chat_id, "Name": u["name"] or chat_id, "Amount_USDT": amount,
        "Admin": update.effective_user.id, "Tx_Direction": "OUT", "Status": "OK",
        "Note": f"to {u['w_addr']} / {u['w_net'] or 'TRC20'}"
    })
    await update.message.reply_text(f"OK. Выплачено ${fmt_usd(amount)} премии пользователю {u['name'] or chat_id}.")
    try:
        await ctx.application.bot.send_message(chat_id=chat_id, text=f"💸 Перевод отправлен: ${fmt_usd(amount)} (премия).")
    except Exception: pass

async def pay_all(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update): return
    try:
        chat_id = int(ctx.args[0])
    except Exception:
        return await update.message.reply_text("Использование: /pay_all <chat_id>")
    u = next((x for x in get_users() if x["chat_id"] == chat_id), None)
    if not u:
        return await update.message.reply_text("Пользователь не найден.")
    bonus_avail = max(0.0, u["bonus_acc"] - u["bonus_paid"] - u["bonus_to_dep"])
    amount = u["deposit"] + bonus_avail
    # списываем всё: депозит -> 0, бонус_paid += bonus_avail, active=False
    upsert_user_row(chat_id, deposit=0.0, active=False, bonus_paid=u["bonus_paid"] + bonus_avail)
    append_ledger(**{
        "Timestamp_UTC": now_utc_str(), "Type": "ALL_WITHDRAWN",
        "Chat_ID": chat_id, "Name": u["name"] or chat_id, "Amount_USDT": amount,
        "Admin": update.effective_user.id, "Tx_Direction": "OUT", "Status": "OK",
        "Note": f"deposit+bonus to {u['w_addr']} / {u['w_net'] or 'TRC20'}"
    })
    await update.message.reply_text(f"OK. Выплачено ${fmt_usd(amount)} и пользователь отключён.")
    try:
        await ctx.application.bot.send_message(chat_id=chat_id, text=f"🏁 Перевод отправлен: ${fmt_usd(amount)} (депозит + премия). Вы отключены.")
        # Сбросим меню на дефолт
        await ctx.application.bot.set_my_commands([BotCommand("start","Как подключиться"), BotCommand("about","О боте")], scope=BotCommandScopeChat(chat_id))
    except Exception: pass

# ------------------- Trading log polling (30% модель) -------------------
open_positions: Dict[str, Dict[str, Any]] = {}  # sid -> {cum_margin, snapshot: [(chat_id, deposit)], users:[ids]}

async def send_all(app: Application, text_by_user: Dict[int, str]):
    for chat_id, text in text_by_user.items():
        if text.strip():
            try:
                await app.bot.send_message(chat_id=chat_id, text=text, parse_mode=constants.ParseMode.HTML, disable_web_page_preview=True)
            except Exception as e:
                log.warning(f"send to {chat_id} failed: {e}")

async def poll_and_broadcast(app: Application):
    try:
        last_row, start_utc, profit30_total = get_state()
        recs = sheet_dicts(ws(LOG_SHEET))
        total_rows = len(recs) + 1
        if last_row == 0:
            # первый запуск — пропускаем историю
            set_state(last_row=total_rows, profit30_total=0.0, start_utc=start_utc or now_utc_str())
            return
        if total_rows <= last_row:
            return
        new_records = recs[(last_row - 1):]
        users_all = get_users()
        per_user_msgs: Dict[int, List[str]] = {}
        def push(uid: int, text: str):
            per_user_msgs.setdefault(uid, []).append(text)

        for rec in new_records:
            ev = (rec.get("Event") or "").strip()
            sid = (rec.get("Signal_ID") or "").strip()
            cum_margin = to_float(rec.get("Cum_Margin_USDT"))
            pnl_usd = to_float(rec.get("PNL_Realized_USDT"))
            pair = rec.get("Pair", "")

            # Применяем pending депозиты при OPEN (для активных)
            if ev == "OPEN":
                users_all = get_users()  # свежий снимок
                for u in users_all:
                    if u["active"] and u["pending"] > 0:
                        upsert_user_row(u["chat_id"], deposit=u["pending"], pending=0.0)
                        u["deposit"], u["pending"] = u["pending"], 0.0
                # snapshot активных пользователей (с их депозитами на момент открытия)
                recipients = [u for u in users_all if u["active"] and u["deposit"] > 0]
                open_positions[sid] = {
                    "cum_margin": cum_margin,
                    "snapshot": [(u["chat_id"], u["deposit"]) for u in recipients],
                    "users": [u["chat_id"] for u in recipients]
                }
                used_pct = 100.0 * (cum_margin / max(SYSTEM_BANK_USDT, 1e-9))
                msg = (
                    f"📊 Сделка открыта по <b>{base_from_pair(pair)}</b>. "
                    f"Задействовано {used_pct:.1f}% банка (≈ ${fmt_usd(cum_margin)})."
                )
                for u in recipients:
                    push(u["chat_id"], msg)

            elif ev in ("ADD","RETEST_ADD"):
                snap = open_positions.setdefault(sid, {"cum_margin": 0.0, "snapshot": [], "users": []})
                snap["cum_margin"] = cum_margin
                if not snap.get("users"):
                    # fallback — если вдруг потеряли snapshot
                    recipients = [u for u in users_all if u["active"] and u["deposit"] > 0]
                    snap["users"] = [u["chat_id"] for u in recipients]
                    snap["snapshot"] = [(u["chat_id"], u["deposit"]) for u in recipients]
                used_pct = 100.0 * (cum_margin / max(SYSTEM_BANK_USDT, 1e-9))
                msg = f"🪙💵 Добор {base_from_pair(pair)}. Объём в сделке: {used_pct:.1f}% банка (≈ ${fmt_usd(cum_margin)})."
                for uid in snap["users"]:
                    push(uid, msg)

            elif ev in ("TP_HIT","SL_HIT","MANUAL_CLOSE"):
                snap = open_positions.get(sid, {})
                cm = snap.get("cum_margin", cum_margin)
                recipients_ids = snap.get("users", [])
                snapshot = snap.get("snapshot", [])
                if not recipients_ids:
                    # если нет — считаем всех активных на сейчас, без распределения по истории (редкий случай)
                    users_all = get_users()
                    recipients = [u for u in users_all if u["active"] and u["deposit"] > 0]
                    recipients_ids = [u["chat_id"] for u in recipients]
                    snapshot = [(u["chat_id"], u["deposit"]) for u in recipients]
                # 30%-модель
                pool30 = pnl_usd * 0.30
                profit30_total += pool30  # в State хранится сумма к выплате (30% от PnL)
                # распределение по депо на момент OPEN
                total_dep_snap = sum(dep for _, dep in snapshot) or 1.0
                used_pct = 100.0 * (cm / max(SYSTEM_BANK_USDT, 1e-9))
                # относительная доходность на сделке (в 30%-м выражении)
                # исходный profit_pct по марже сделки: pnl_usd / cm * 100
                profit_pct_raw = (pnl_usd / cm * 100.0) if cm > 0 else 0.0
                profit_pct_30 = profit_pct_raw * 0.30

                # Разошлём и начислим
                for (uid, dep_snap) in snapshot:
                    u = next((x for x in get_users() if x["chat_id"] == uid), None)
                    if not u:  # пользователь уже удалён
                        continue
                    my_bonus = pool30 * (dep_snap / total_dep_snap)
                    # начислим премию пользователю
                    upsert_user_row(uid, bonus_acc=u["bonus_acc"] + my_bonus)
                    # текст
                    ann_pct, ann_usd = annual_forecast(
                        user_bonus_total=(u["bonus_acc"] + my_bonus),  # после начисления
                        start_utc=start_utc,
                        user_deposit=u["deposit"]  # текущий депозит (Ок для оценки)
                    )
                    icon = "🚀" if my_bonus >= 0 else "🛑"
                    txt = (
                        f"{icon} Сделка закрыта по <b>{base_from_pair(pair)}</b>.\n"
                        f"Использовалось {used_pct:.1f}% банка (≈ ${fmt_usd(cm)}).\n"
                        f"P&L (30% пул): <b>${fmt_usd(pool30)}</b> ({profit_pct_30:+.2f}%)\n"
                        f"Ваша премия за сделку: <b>${fmt_usd(my_bonus)}</b>\n\n"
                        f"Оценка годовых для вашего депозита (${fmt_usd(u['deposit'])}): "
                        f"~{ann_pct:.1f}% (≈ ${fmt_usd(ann_usd)}/год)."
                    )
                    push(uid, txt)
                # очистим
                if sid in open_positions:
                    del open_positions[sid]

        # отправим накопленные сообщения
        if per_user_msgs:
            await send_all(app, {uid: "\n\n".join(msgs) for uid, msgs in per_user_msgs.items()})
        set_state(last_row=total_rows, profit30_total=profit30_total)
    except Exception as e:
        log.exception("poll_and_broadcast error")

async def poll_job(context: ContextTypes.DEFAULT_TYPE):
    await poll_and_broadcast(context.application)

# ------------------- post_init & main -------------------
async def post_init(app: Application):
    await set_menu_default(app)
    await set_menu_admins(app)
    # восстановим меню юзерам
    try:
        for u in get_users():
            if u.get("active"):
                try:
                    await set_menu_user(app, int(u["chat_id"]))
                except Exception as e:
                    log.warning(f"set_menu_user failed for {u}: {e}")
    except Exception as e:
        log.warning(f"post_init restore menus failed: {e}")

def main():
    app = ApplicationBuilder().token(BOT_TOKEN).post_init(post_init).build()
    # user
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("about", about))
    app.add_handler(CommandHandler("myname", myname))
    app.add_handler(CommandHandler("balance", balance))
    app.add_handler(CommandHandler("add_deposit", add_deposit))
    app.add_handler(CommandHandler("add_from_bonus", add_from_bonus))
    app.add_handler(CommandHandler("withdraw_bonus", withdraw_bonus))
    app.add_handler(CommandHandler("withdraw_all", withdraw_all))
    app.add_handler(CommandHandler("mywallet", mywallet))
    app.add_handler(CommandHandler("setwallet", setwallet))
    app.add_handler(CommandHandler("clearwallet", clearwallet))
    # admin
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("adduser", adduser))
    app.add_handler(CommandHandler("setdep", setdep))
    app.add_handler(CommandHandler("setname", setname_admin))
    app.add_handler(CommandHandler("remove", remove))
    app.add_handler(CommandHandler("list", list_users))
    app.add_handler(CommandHandler("approve_wallet", approve_wallet))
    app.add_handler(CommandHandler("reject_wallet", reject_wallet))
    app.add_handler(CommandHandler("apply_from_bonus", apply_from_bonus))
    app.add_handler(CommandHandler("pay_bonus", pay_bonus))
    app.add_handler(CommandHandler("pay_all", pay_all))

    # poller
    app.job_queue.run_repeating(poll_job, interval=10, first=5)
    log.info(f"{BOT_NAME} starting…")
    app.run_polling()

if __name__ == "__main__":
    main()
