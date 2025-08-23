# marketing_bot.py — STRIGI_KAPUSTU_BOT (upgraded)
import os, logging, re, json
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

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
ADMIN_IDS_RAW = os.getenv("ADMIN_IDS", "")

SYSTEM_BANK_USDT = float(os.getenv("SYSTEM_BANK_USDT", "1000"))
USER_SHARE = float(os.getenv("USER_SHARE", "0.30"))  # 30% по умолчанию

SAFE_EMOJI = True  # используем «безопасные» эмодзи
EMJ = {
    "hi": "👋", "balance": "🧰", "open": "📊", "add": "➕", "coin": "💰",
    "ok": "✅", "bad": "🛑", "plane": "✈️", "rocket": "🚀", "car": "🏎️", "bike": "🏍️"
}

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
        if t and (t.lstrip("-").isdigit()): out.add(int(t))
    return out

ADMIN_IDS = parse_admin_ids(ADMIN_IDS_RAW)

if not BOT_TOKEN or not SHEET_ID or not ADMIN_IDS:
    raise RuntimeError("MARKETING_BOT_TOKEN / SHEET_ID / ADMIN_IDS обязательны")

def now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

# ------------------- LOG -------------------
log = logging.getLogger("marketing")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ------------------- Sheets -------------------
CREDS_JSON = os.getenv("GOOGLE_CREDENTIALS")
if not CREDS_JSON: raise RuntimeError("GOOGLE_CREDENTIALS env var not set")

gc = gspread.service_account_from_dict(json.loads(CREDS_JSON))
sh = gc.open_by_key(SHEET_ID)

LOG_SHEET   = "BMR_DCA_Log"
USERS_SHEET = "Marketing_Users"
STATE_SHEET = "Marketing_State"  # тут и «state»-ячейки, и журнал событий (append_row)

# «расширенные» колонки пользователей
U_HDR = [
    "Chat_ID", "Name", "Deposit_USDT", "Active", "Pending_Deposit",
    "Bonus_Accrued", "Bonus_Paid", "Bonus_To_Deposit", "Last_Update"
]

def ensure_sheets():
    names = {ws.title for ws in sh.worksheets()}
    if USERS_SHEET not in names:
        ws = sh.add_worksheet(USERS_SHEET, rows=200, cols=len(U_HDR))
        ws.update("1:1", [U_HDR])
    else:
        ws = sh.worksheet(USERS_SHEET)
        # апгрейд заголовка до расширенного
        head = ws.row_values(1)
        if head != U_HDR:
            # увеличим кол-во колонок, если надо
            if ws.col_count < len(U_HDR):
                ws.add_cols(len(U_HDR) - ws.col_count)
            ws.update("1:1", [U_HDR])

    if STATE_SHEET not in names:
        ws = sh.add_worksheet(STATE_SHEET, rows=1000, cols=20)
        # A2:C2 — «state», ниже — журнал
        ws.update("A1:C1", [["Last_Row", "Start_UTC", "Profit_Total_USDT"]])
        ws.update("A2:C2", [["0", now_utc_str(), "0"]])
        ws.update("A4:Q4", [[
            "TS_UTC","Event","Chat_ID","Name","Signal_ID","Kind","Amount",
            "Bank_Used_Pct","ROI_on_Margin_%","NetPnL_Total","User_Share","APR_%","Comment",
            "Deposit_After","BonusAccrued","BonusPaid","BonusToDep"
        ]])
    else:
        ws = sh.worksheet(STATE_SHEET)
        vals = ws.get_all_values()
        if len(vals) < 2:
            ws.update("A1:C1", [["Last_Row", "Start_UTC", "Profit_Total_USDT"]])
            ws.update("A2:C2", [["0", now_utc_str(), "0"]])
        elif not (ws.acell("B2").value or "").strip():
            ws.update_acell("B2", now_utc_str())

    if LOG_SHEET not in names:
        raise RuntimeError(f"Не найден лист {LOG_SHEET} (его пишет основной бот)")

ensure_sheets()
def ws(title): return sh.worksheet(title)

# ------------------- Model -------------------
def to_float(x) -> float:
    try: return float(str(x).replace(",", "."))
    except (ValueError, TypeError): return 0.0

def get_state():
    w = ws(STATE_SHEET)
    a2, b2, c2 = w.acell("A2").value, w.acell("B2").value, w.acell("C2").value
    last_row = int(a2) if (a2 or "").strip().isdigit() else 0
    start_utc = b2 or ""
    profit_total = to_float(c2)
    return last_row, start_utc, profit_total

def set_state(last_row: Optional[int] = None, profit_total: Optional[float] = None, start_utc: Optional[str] = None):
    w = ws(STATE_SHEET)
    if last_row is not None: w.update_acell("A2", str(last_row))
    if start_utc is not None: w.update_acell("B2", start_utc)
    if profit_total is not None: w.update_acell("C2", str(profit_total))

def get_users() -> List[Dict[str, Any]]:
    rows = ws(USERS_SHEET).get_all_records(head=1, expected_headers=U_HDR, default_blank="")
    res = []
    for r in rows:
        try:
            res.append({
                "chat_id": int(r.get("Chat_ID")),
                "name": r.get("Name") or "",
                "deposit": to_float(r.get("Deposit_USDT")),
                "active": str(r.get("Active", "TRUE")).strip().upper() not in ("FALSE","0",""),
                "pending": to_float(r.get("Pending_Deposit")),
                "bonus_acc": to_float(r.get("Bonus_Accrued")),
                "bonus_paid": to_float(r.get("Bonus_Paid")),
                "bonus_to_dep": to_float(r.get("Bonus_To_Deposit")),
            })
        except Exception as e:
            log.warning(f"skip row {r}: {e}")
    return res

def find_user(chat_id: int) -> Optional[Dict[str, Any]]:
    for u in get_users():
        if u["chat_id"] == chat_id: return u
    return None

def bonus_available(u: Dict[str, Any]) -> float:
    return max(0.0, u["bonus_acc"] - u["bonus_paid"] - u["bonus_to_dep"])

def upsert_user_row(chat_id: int, name: str = None, deposit: float = None,
                    active: bool = None, pending: float = None,
                    bonus_acc: float = None, bonus_paid: float = None, bonus_to_dep: float = None):
    w = ws(USERS_SHEET)
    row_idx = None
    try:
        cell = w.find(str(chat_id), in_column=1)
        if cell and getattr(cell, "row", None): row_idx = cell.row
    except Exception:
        pass
    if row_idx is None:
        # append
        w.append_row([
            str(chat_id), name or "", str(deposit or 0), "TRUE" if (active is None or active) else "FALSE",
            str(pending or 0), str(bonus_acc or 0), str(bonus_paid or 0), str(bonus_to_dep or 0), now_utc_str()
        ], value_input_option="RAW")
        return
    # update
    row = w.row_values(row_idx) + [""] * max(0, len(U_HDR) - len(w.row_values(row_idx)))
    def pick(i, new, cur=row):
        return cur[i] if new is None else (str(new) if i not in (3,) else ("TRUE" if new else "FALSE"))
    values = [
        str(chat_id),
        pick(1, name),
        pick(2, deposit if deposit is None else float(deposit)),
        ("TRUE" if active else "FALSE") if active is not None else row[3],
        pick(4, pending if pending is None else float(pending)),
        pick(5, bonus_acc if bonus_acc is None else float(bonus_acc)),
        pick(6, bonus_paid if bonus_paid is None else float(bonus_paid)),
        pick(7, bonus_to_dep if bonus_to_dep is None else float(bonus_to_dep)),
        now_utc_str()
    ]
    w.update(f"A{row_idx}:I{row_idx}", [values])

# ------------------- Helpers -------------------
def fmt_usd(x): 
    try: return f"{float(x):,.2f}".replace(",", " ")
    except Exception: return str(x)
def parse_money(s: str) -> float:
    return float(re.sub(r"[^\d.,\-]", "", s).replace(",", "."))

def sheet_dicts(worksheet) -> List[Dict[str, Any]]:
    vals = worksheet.get_all_values()
    if not vals or len(vals) < 2: return []
    headers, out = vals[0], []
    for row in vals[1:]:
        out.append({headers[i]: (row[i] if i < len(row) else "") for i in range(len(headers))})
    return out

def append_state_event(event: str, chat_id: int, name: str, signal_id: str,
                       kind: str, amount: Any, bank_used_pct: float = None,
                       roi_pct: float = None, net_pnl_total: float = None,
                       user_share: float = None, apr_pct: float = None, comment: str = "",
                       dep_after: float = None, b_acc: float = None, b_paid: float = None, b_to_dep: float = None):
    ws(STATE_SHEET).append_row([
        now_utc_str(), event, str(chat_id), name or "", signal_id or "", kind or "", "" if amount is None else amount,
        "" if bank_used_pct is None else round(bank_used_pct, 6),
        "" if roi_pct is None else round(roi_pct, 6),
        "" if net_pnl_total is None else round(net_pnl_total, 6),
        "" if user_share is None else round(user_share, 6),
        "" if apr_pct is None else round(apr_pct, 4),
        comment or "",
        "" if dep_after is None else round(dep_after, 6),
        "" if b_acc is None else round(b_acc, 6),
        "" if b_paid is None else round(b_paid, 6),
        "" if b_to_dep is None else round(b_to_dep, 6),
    ], value_input_option="USER_ENTERED")

# ------------------- Telegram Menus -------------------
USER_COMMANDS = [
    BotCommand("start", "Как подключиться"),
    BotCommand("balance", "Показать баланс"),
    BotCommand("add_deposit", "Добавить депозит"),
    BotCommand("add_deposit_bonus", "Добавить депозит с премии"),
    BotCommand("withdraw_bonus", "Вывести премию"),
    BotCommand("withdraw_all", "Вывести весь депозит"),
]
ADMIN_COMMANDS = [
    BotCommand("start", "Показать chat_id"),
    BotCommand("help", "Команды админа"),
    BotCommand("list", "Список пользователей"),
    BotCommand("adduser", "Добавить пользователя"),
    BotCommand("setdep", "Изменить депозит (со след. сделки)"),
    BotCommand("setname", "Переименовать пользователя"),
    BotCommand("remove", "Отключить пользователя"),
    BotCommand("admin_add_deposit", "Админ: пополнить депозит [bonus]"),
    BotCommand("admin_withdraw_bonus", "Админ: вывести премию"),
    BotCommand("admin_withdraw_all", "Админ: вывести всё"),
]

async def set_menu_default(app: Application):
    await app.bot.set_my_commands([BotCommand("start", "Как подключиться")], scope=BotCommandScopeAllPrivateChats())

async def set_menu_user(app: Application, chat_id: int):
    await app.bot.set_my_commands(USER_COMMANDS, scope=BotCommandScopeChat(chat_id))

async def set_menu_admins(app: Application):
    for aid in ADMIN_IDS:
        try: await app.bot.set_my_commands(ADMIN_COMMANDS, scope=BotCommandScopeChat(aid))
        except Exception as e: log.error(f"Failed to set menu for admin {aid}: {e}")

# ------------------- Telegram Handlers -------------------
def is_admin(update: Update) -> bool:
    uid = update.effective_user.id if update.effective_user else None
    cid = update.effective_chat.id if update.effective_chat else None
    return (uid in ADMIN_IDS) or (cid in ADMIN_IDS)

async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid, cid = update.effective_user.id, update.effective_chat.id
    txt = (f"{EMJ['hi']} Привет! Я <b>{BOT_NAME}</b>.\n"
           f"Твой <b>user_id</b>: <code>{uid}</code>\n"
           f"Твой <b>chat_id</b>: <code>{cid}</code>\n"
           f"Чтобы подключиться, передай этот chat_id админу.")
    await update.message.reply_text(txt, parse_mode=constants.ParseMode.HTML)

async def whoami(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid, cid = update.effective_user.id, update.effective_chat.id
    await update.message.reply_text(f"user_id={uid}\nchat_id={cid}\nadmin={is_admin(update)}")

async def help_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update): return
    text = (
        "Админ-команды:\n"
        "/adduser <chat_id> <Имя> <депозит>\n"
        "/setdep <chat_id> <депозит>  (pending, применится со следующей сделкой)\n"
        "/setname <chat_id> <Имя>\n"
        "/remove <chat_id>\n"
        "/list\n"
        "/admin_add_deposit <chat_id> <amount> [bonus]\n"
        "/admin_withdraw_bonus <chat_id> [amount]\n"
        "/admin_withdraw_all <chat_id>"
    )
    await update.message.reply_text(text)

async def adduser(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update): return
    try:
        args = ctx.args
        if len(args) < 3: raise ValueError
        chat_id = int(args[0]); dep = parse_money(args[-1])
        name = " ".join(args[1:-1]).strip() or str(chat_id)
    except (ValueError, IndexError):
        return await update.message.reply_text("Использование: /adduser <chat_id> <Имя> <депозит>")
    upsert_user_row(chat_id, name=name, deposit=dep, active=True)
    await update.message.reply_text(f"OK. Пользователь {name} ({chat_id}) добавлен с депозитом {fmt_usd(dep)} USDT.")
    await set_menu_user(ctx.application, chat_id)
    try:
        await ctx.application.bot.send_message(
            chat_id=chat_id,
            text=f"{EMJ['hi']} Добро пожаловать, <b>{name}</b>! Ваш депозит: ${fmt_usd(dep)}.",
            parse_mode=constants.ParseMode.HTML
        )
    except Exception as e:
        logging.warning(f"Не удалось отправить приветствие {chat_id}: {e}")

async def setdep(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update): return
    try:
        m = re.match(r"^/setdep\s+(-?\d+)\s+([0-9][\d\s.,]*)\s*$", update.message.text.strip(), re.I)
        if not m: raise ValueError
        chat_id, dep = int(m.group(1)), parse_money(m.group(2))
    except Exception:
        return await update.message.reply_text("Использование: /setdep <chat_id> <депозит>")
    u = find_user(chat_id)
    cur_pending = u["pending"] if u else 0.0
    upsert_user_row(chat_id, pending=cur_pending + dep)
    await update.message.reply_text(f"OK. Pending-депозит {fmt_usd(dep)} USDT будет добавлен со следующей сделки.")

async def setname(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update): return
    try:
        chat_id = int(ctx.args[0]); name = " ".join(ctx.args[1:])
        if not name: raise ValueError
    except (IndexError, ValueError): return await update.message.reply_text("Использование: /setname <chat_id> <Новое Имя>")
    upsert_user_row(chat_id, name=name)
    await update.message.reply_text("OK. Имя обновлено.")

async def remove(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update): return
    try: chat_id = int(ctx.args[0])
    except (IndexError, ValueError): return await update.message.reply_text("Использование: /remove <chat_id>")
    upsert_user_row(chat_id, active=False)
    await update.message.reply_text("OK. Пользователь деактивирован.")
    try:
        await ctx.application.bot.set_my_commands([BotCommand("start", "Как подключиться")], scope=BotCommandScopeChat(chat_id))
    except Exception as e:
        log.warning(f"set default menu for {chat_id} failed: {e}")

async def list_users(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update): return
    users = get_users()
    if not users: return await update.message.reply_text("Список пуст.")
    lines = [f"{'✅' if u['active'] else '⛔️'} {u['name'] or u['chat_id']} | dep={fmt_usd(u['deposit'])} | pending={fmt_usd(u['pending'])} | bonusAvail={fmt_usd(bonus_available(u))} | id={u['chat_id']}" for u in users]
    await update.message.reply_text("\n".join(lines))

async def balance(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cid = update.effective_chat.id
    u = find_user(cid)
    if not (u and u["active"]):
        return await update.message.reply_text("Вы ещё не подключены. Отправьте /start и передайте ваш chat_id админу.")
    b_avail = bonus_available(u)
    await update.message.reply_text(
        f"{EMJ['balance']} <b>Баланс</b>\n\n"
        f"Депозит: <b>${fmt_usd(u['deposit'])}</b>\n"
        f"Премия (к выплате): <b>${fmt_usd(b_avail)}</b>\n"
        f"Итого: <b>${fmt_usd(u['deposit'] + b_avail)}</b>",
        parse_mode=constants.ParseMode.HTML
    )

# ---- USER REQUESTS ----
async def add_deposit(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cid = update.effective_chat.id
    try:
        amt = parse_money(ctx.args[0]) if ctx.args else None
        if amt is None or amt <= 0: raise ValueError
    except Exception:
        return await update.message.reply_text("Использование: /add_deposit 100")
    u = find_user(cid) or {"name": str(cid)}
    append_state_event("REQUEST", cid, u["name"], "", "ADD_DEPOSIT", amt, comment="user")
    await update.message.reply_text("✅ Заявка на пополнение отправлена админу.")
    for aid in ADMIN_IDS:
        try:
            await ctx.application.bot.send_message(aid, f"📝 Заявка от {u['name']} ({cid}): ADD_DEPOSIT — {fmt_usd(amt)} USDT")
        except Exception: pass

async def add_deposit_bonus(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cid = update.effective_chat.id
    amt = None
    if ctx.args:
        try:
            v = parse_money(ctx.args[0])
            if v > 0: amt = v
        except Exception: pass
    u = find_user(cid) or {"name": str(cid)}
    append_state_event("REQUEST", cid, u["name"], "", "ADD_DEPOSIT_FROM_BONUS", amt, comment="user")
    await update.message.reply_text("✅ Заявка на пополнение депозита с премии отправлена админу.")
    for aid in ADMIN_IDS:
        try:
            await ctx.application.bot.send_message(aid, f"📝 Заявка от {u['name']} ({cid}): ADD_DEPOSIT_FROM_BONUS — {('всё доступное' if amt is None else fmt_usd(amt)+' USDT')}")
        except Exception: pass

async def withdraw_bonus(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cid = update.effective_chat.id
    amt = None
    if ctx.args:
        try:
            v = parse_money(ctx.args[0])
            if v > 0: amt = v
        except Exception: pass
    u = find_user(cid) or {"name": str(cid)}
    append_state_event("REQUEST", cid, u["name"], "", "WITHDRAW_BONUS", amt, comment="user")
    await update.message.reply_text("✅ Заявка на вывод премии отправлена админу.")
    for aid in ADMIN_IDS:
        try:
            await ctx.application.bot.send_message(aid, f"📝 Заявка от {u['name']} ({cid}): WITHDRAW_BONUS — {('всё доступное' if amt is None else fmt_usd(amt)+' USDT')}")
        except Exception: pass

async def withdraw_all(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cid = update.effective_chat.id
    u = find_user(cid) or {"name": str(cid)}
    append_state_event("REQUEST", cid, u["name"], "", "WITHDRAW_ALL", None, comment="user")
    await update.message.reply_text("✅ Заявка на вывод депозита отправлена админу.")
    for aid in ADMIN_IDS:
        try:
            await ctx.application.bot.send_message(aid, f"📝 Заявка от {u['name']} ({cid}): WITHDRAW_ALL")
        except Exception: pass

# ---- ADMIN ACTIONS ----
async def admin_add_deposit(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update): return
    try:
        chat_id = int(ctx.args[0]); amt = parse_money(ctx.args[1])
        source = "bonus" if (len(ctx.args) >= 3 and ctx.args[2].lower().startswith("b")) else "external"
    except Exception:
        return await update.message.reply_text("Использование: /admin_add_deposit <chat_id> <amount> [bonus]")
    u = find_user(chat_id)
    if not u: return await update.message.reply_text("user not found")
    if source == "bonus":
        avail = bonus_available(u)
        amt = min(amt, avail)
        upsert_user_row(chat_id, bonus_to_dep=u["bonus_to_dep"] + amt, pending=u["pending"] + amt)
        comment = "from bonus"
    else:
        upsert_user_row(chat_id, pending=u["pending"] + amt)
        comment = "external"
    u2 = find_user(chat_id)
    append_state_event("APPROVED", chat_id, u2["name"], "", "ADD_DEPOSIT" if source=="external" else "ADD_DEPOSIT_FROM_BONUS",
                       amt, comment=comment, dep_after=u2["deposit"], b_acc=u2["bonus_acc"], b_paid=u2["bonus_paid"], b_to_dep=u2["bonus_to_dep"])
    await update.message.reply_text("OK")
    try:
        await ctx.application.bot.send_message(chat_id, f"✅ Пополнение {fmt_usd(amt)} USDT одобрено. Будет добавлено со следующей сделки.")
    except Exception: pass

async def admin_withdraw_bonus(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update): return
    try:
        chat_id = int(ctx.args[0]); amt = parse_money(ctx.args[1]) if len(ctx.args)>=2 else None
    except Exception:
        return await update.message.reply_text("Использование: /admin_withdraw_bonus <chat_id> [amount]")
    u = find_user(chat_id)
    if not u: return await update.message.reply_text("user not found")
    avail = bonus_available(u)
    pay = avail if (amt is None or amt <= 0) else min(avail, amt)
    if pay <= 0: return await update.message.reply_text("nothing to withdraw")
    upsert_user_row(chat_id, bonus_paid=u["bonus_paid"] + pay)
    u2 = find_user(chat_id)
    append_state_event("WITHDRAW", chat_id, u2["name"], "", "WITHDRAW_BONUS", pay,
                       dep_after=u2["deposit"], b_acc=u2["bonus_acc"], b_paid=u2["bonus_paid"], b_to_dep=u2["bonus_to_dep"])
    await update.message.reply_text("OK")
    try:
        await ctx.application.bot.send_message(chat_id, f"✅ Перевод отправлен. Выплата премии: {fmt_usd(pay)} USDT.")
    except Exception: pass

async def admin_withdraw_all(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update): return
    try:
        chat_id = int(ctx.args[0])
    except Exception:
        return await update.message.reply_text("Использование: /admin_withdraw_all <chat_id>")
    u = find_user(chat_id)
    if not u: return await update.message.reply_text("user not found")
    pay = u["deposit"] + bonus_available(u)
    upsert_user_row(chat_id, deposit=0.0, pending=0.0, bonus_paid=u["bonus_paid"] + bonus_available(u), active=False)
    u2 = find_user(chat_id) or {"name": str(chat_id), "deposit": 0, "bonus_acc": 0, "bonus_paid": 0, "bonus_to_dep": 0}
    append_state_event("WITHDRAW", chat_id, u2["name"], "", "WITHDRAW_ALL", pay,
                       dep_after=0.0, b_acc=u2["bonus_acc"], b_paid=u2["bonus_paid"], b_to_dep=u2["bonus_to_dep"])
    await update.message.reply_text("OK")
    try:
        await ctx.application.bot.send_message(chat_id, f"✅ Перевод отправлен. Сумма: {fmt_usd(pay)} USDT (депозит + премия).")
    except Exception: pass
    try:
        await set_menu_default(ctx.application)
        await ctx.application.bot.set_my_commands([BotCommand("start", "Как подключиться")], scope=BotCommandScopeChat(chat_id))
    except Exception: pass

# ------------------- Poller & Main Logic -------------------
async def send_all(app: Application, text_by_user: Dict[int, str]):
    for chat_id, text in text_by_user.items():
        if text.strip():
            try:
                await app.bot.send_message(chat_id=chat_id, text=text, parse_mode=constants.ParseMode.HTML, disable_web_page_preview=True)
            except Exception as e:
                log.warning(f"send to {chat_id} failed: {e}")

async def poll_and_broadcast(app: Application):
    try:
        last_row, start_utc, profit_total = get_state()
        if not (start_utc or "").strip():
            start_utc = now_utc_str()
            set_state(start_utc=start_utc)
        records = sheet_dicts(ws(LOG_SHEET))
        total_rows_in_sheet = len(records) + 1
        if last_row == 0:
            set_state(last_row=total_rows_in_sheet, profit_total=0.0)
            return
        if total_rows_in_sheet <= last_row: return
        new_records = records[(last_row - 1):]

        users_all = get_users()
        users = [u for u in users_all if u["active"]]
        if not users:
            set_state(last_row=total_rows_in_sheet)
            return

        # пер-user сообщения
        per_user_msgs: Dict[int, List[str]] = {}
        def push(uid: int, text: str):
            if text: per_user_msgs.setdefault(uid, []).append(text)

        open_positions: Dict[str, Dict[str, Any]] = {}

        for rec in new_records:
            ev = rec.get("Event") or ""
            sid = rec.get("Signal_ID") or ""
            cum_margin = to_float(rec.get("Cum_Margin_USDT"))
            pnl_usd = to_float(rec.get("PNL_Realized_USDT"))
            time_min = to_float(rec.get("Time_In_Trade_min"))

            if ev in ("OPEN", "ADD", "RETEST_ADD"):
                if ev == "OPEN":
                    # применяем pending -> deposit (прибавляем)
                    for u in users:
                        if u["pending"] > 0:
                            upsert_user_row(u["chat_id"], deposit=u["deposit"] + u["pending"], pending=0.0)
                    users = [x for x in get_users() if x["active"]]  # перечитаем после апдейта
                    recipients = [u["chat_id"] for u in users]
                    open_positions[sid] = {"cum_margin": cum_margin, "users": recipients}
                else:
                    snap = open_positions.setdefault(sid, {"cum_margin": 0.0, "users": [u["chat_id"] for u in users]})
                    snap["cum_margin"] = cum_margin
                    recipients = snap["users"]
                if not recipients: continue
                used_pct = 100.0 * (cum_margin / max(SYSTEM_BANK_USDT, 1e-9))
                msg = f"{EMJ['open']} Сделка {'открыта' if ev=='OPEN' else 'усреднена'}. Задействовано {used_pct:.1f}% банка ({fmt_usd(cum_margin)})."
                for uid in recipients: push(uid, msg)

            if ev in ("TP_HIT", "SL_HIT", "MANUAL_CLOSE"):
                # вытаскиваем последний cum_margin из open_positions либо из истории
                cm = cum_margin
                if sid in open_positions:
                    cm = open_positions[sid].get("cum_margin", cm)
                    recipients = open_positions[sid].get("users", [u["chat_id"] for u in users])
                else:
                    recipients = [u["chat_id"] for u in users]

                used_frac = (cm / max(SYSTEM_BANK_USDT, 1e-9)) if SYSTEM_BANK_USDT > 0 else 0.0
                roi_on_margin = (pnl_usd / cm) if cm > 0 else 0.0  # доля
                used_pct = used_frac * 100.0
                profit_total += pnl_usd  # агрегатка по всем сделкам (можешь оставить для общей аналитики)

                # персональные начисления 30% и сообщения
                for u in users:
                    if u["chat_id"] not in recipients: continue
                    used_usd = u["deposit"] * used_frac
                    gross_user = used_usd * roi_on_margin
                    user_share = max(0.0, gross_user * USER_SHARE)
                    apr = 0.0
                    if u["deposit"] > 0 and time_min > 0:
                        apr = (user_share / u["deposit"]) * (525600.0 / time_min) * 100.0

                    # апдейт бонусов
                    upsert_user_row(u["chat_id"], bonus_acc=u["bonus_acc"] + user_share)
                    u2 = find_user(u["chat_id"])
                    append_state_event("TRADE_PNL", u["chat_id"], u2["name"], sid, "", "",
                                       bank_used_pct=used_pct, roi_pct=roi_on_margin*100.0,
                                       net_pnl_total=pnl_usd, user_share=user_share, apr_pct=apr,
                                       dep_after=u2["deposit"], b_acc=u2["bonus_acc"],
                                       b_paid=u2["bonus_paid"], b_to_dep=u2["bonus_to_dep"])

                    icon = EMJ['ok'] if pnl_usd >= 0 else EMJ['bad']
                    txt = (
                        f"{icon} Сделка закрыта. Использовалось {used_pct:.1f}% банка ({fmt_usd(cm)}).\n"
                        f"Ваш результат (30%): <b>{fmt_usd(user_share)} USDT</b>\n"
                        f"Оценка годовых по депозиту {fmt_usd(u2['deposit'])}: <b>{apr:.1f}%</b>"
                    )
                    push(u["chat_id"], txt)

                if sid in open_positions: del open_positions[sid]

        # рассылка
        final_messages = {uid: "\n\n".join(msgs) for uid, msgs in per_user_msgs.items() if msgs}
        if final_messages:
            await send_all(app, final_messages)

        set_state(last_row=total_rows_in_sheet, profit_total=profit_total)
    except Exception as e:
        log.exception("poll_and_broadcast error")

async def poll_job(context: ContextTypes.DEFAULT_TYPE):
    await poll_and_broadcast(context.application)

# ------------------- Main -------------------
async def post_init(app: Application):
    await set_menu_default(app)
    await set_menu_admins(app)
    try:
        users = [u for u in get_users() if u.get("active")]
        for u in users:
            try:
                await set_menu_user(app, int(u["chat_id"]))
            except Exception as e:
                log.warning(f"set_menu_user failed for {u}: {e}")
    except Exception as e:
        log.warning(f"post_init: could not restore user menus: {e}")

def main():
    app = ApplicationBuilder().token(BOT_TOKEN).post_init(post_init).build()
    # user
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("balance", balance))
    app.add_handler(CommandHandler("add_deposit", add_deposit))
    app.add_handler(CommandHandler("add_deposit_bonus", add_deposit_bonus))
    app.add_handler(CommandHandler("withdraw_bonus", withdraw_bonus))
    app.add_handler(CommandHandler("withdraw_all", withdraw_all))
    # admin
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("whoami", whoami))
    app.add_handler(CommandHandler("list", list_users))
    app.add_handler(CommandHandler("adduser", adduser))
    app.add_handler(CommandHandler("setdep", setdep))
    app.add_handler(CommandHandler("setname", setname))
    app.add_handler(CommandHandler("remove", remove))
    app.add_handler(CommandHandler("admin_add_deposit", admin_add_deposit))
    app.add_handler(CommandHandler("admin_withdraw_bonus", admin_withdraw_bonus))
    app.add_handler(CommandHandler("admin_withdraw_all", admin_withdraw_all))

    app.job_queue.run_repeating(poll_job, interval=10, first=5)
    log.info(f"{BOT_NAME} starting…")
    app.run_polling()

if __name__ == "__main__":
    main()
