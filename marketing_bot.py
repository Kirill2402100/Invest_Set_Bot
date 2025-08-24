# marketing_bot.py — STRIGI_KAPUSTU_BOT (обновлено)
import os, time, logging, math, re, json
from datetime import datetime, timezone, timedelta
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

def parse_admin_ids(raw: str) -> set[int]:
    if not raw: return set()
    try:
        maybe = json.loads(raw)
        if isinstance(maybe, (list, tuple, set)): return {int(x) for x in maybe}
        if isinstance(maybe, (int, str)) and str(maybe).lstrip("-").isdigit(): return {int(maybe)}
    except Exception: pass
    out = set()
    for t in re.split(r'[\s,;]+', raw.strip()):
        t = t.strip().strip('[](){}"\'')
        if t and (t.lstrip("-").isdigit()): out.add(int(t))
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

# --- Меню команд ---
USER_COMMANDS = [
    BotCommand("start", "Как подключиться"),
    BotCommand("about", "О боте"),
    BotCommand("myname", "Указать имя"),
    BotCommand("add_deposit", "Добавить депозит"),
    BotCommand("add_from_bonus", "Пополнить из премии"),
    BotCommand("withdraw_bonus", "Вывести премию"),
    BotCommand("withdraw_all", "Вывести весь депозит"),
    BotCommand("balance", "Показать баланс")
]
ADMIN_COMMANDS = [
    BotCommand("start", "Показать chat_id"), BotCommand("help", "Команды админа"),
    BotCommand("list", "Список пользователей"), BotCommand("adduser", "Добавить пользователя"),
    BotCommand("setdep", "Изменить депозит (со след. сделки)"),
    BotCommand("setname", "Переименовать пользователя"), BotCommand("remove", "Отключить пользователя"),
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
        try: await app.bot.set_my_commands(ADMIN_COMMANDS, scope=BotCommandScopeChat(aid))
        except Exception as e: log.error(f"Failed to set menu for admin {aid}: {e}")

# ------------------- Sheets -------------------
CREDS_JSON = os.getenv("GOOGLE_CREDENTIALS")
if not CREDS_JSON: raise RuntimeError("GOOGLE_CREDENTIALS env var not set")

gc = gspread.service_account_from_dict(json.loads(CREDS_JSON))
sh = gc.open_by_key(SHEET_ID)

LOG_SHEET   = "BMR_DCA_Log"
USERS_SHEET = "Marketing_Users"
STATE_SHEET = "Marketing_State"
LEDGER_SHEET= "Marketing_Ledger"

USERS_COLS = [
    "Chat_ID","Name","Deposit_USDT","Active","Pending_Deposit",
    "Bonus_Accrued","Bonus_Paid","Bonus_To_Deposit","Last_Update"
]

def to_float(x) -> float:
    try: return float(str(x).replace(",", "."))
    except (ValueError, TypeError): return 0.0

def ws(title): return sh.worksheet(title)

def ensure_header(worksheet, required_cols: List[str]):
    vals = worksheet.get_all_values()
    current = vals[0] if vals else []
    if current == required_cols:
        return
    # обновляем хедер (остальные строки остаются)
    end_col = chr(ord('A') + len(required_cols) - 1)
    worksheet.update(f"A1:{end_col}1", [required_cols])

def ensure_sheets():
    names = {ws.title for ws in sh.worksheets()}
    if USERS_SHEET not in names:
        ws_u = sh.add_worksheet(USERS_SHEET, rows=2000, cols=len(USERS_COLS))
        ensure_header(ws_u, USERS_COLS)
    else:
        ws_u = sh.worksheet(USERS_SHEET)
        ensure_header(ws_u, USERS_COLS)

    if STATE_SHEET not in names:
        ws_s = sh.add_worksheet(STATE_SHEET, rows=10, cols=3)
        ws_s.update("A1:C1", [["Last_Row", "Start_UTC", "Profit_Total_USDT"]])
        ws_s.update("A2:C2", [["0", now_utc_str(), "0"]])
    else:
        ws_s = sh.worksheet(STATE_SHEET)
        vals = ws_s.get_all_values()
        if len(vals) < 2:
            ws_s.update("A2:C2", [["0", now_utc_str(), "0"]])
        if not (ws_s.acell("B2").value or "").strip():
            ws_s.update_acell("B2", now_utc_str())

    if LEDGER_SHEET not in names:
        ws_l = sh.add_worksheet(LEDGER_SHEET, rows=2000, cols=8)
        ws_l.update("A1:H1", [[
            "Timestamp_UTC","Type","Chat_ID","Name","Amount_USDT","Note","Admin","Status"
        ]])

    if LOG_SHEET not in names:
        raise RuntimeError(f"Не найден лист {LOG_SHEET} (его пишет основной бот)")
ensure_sheets()

# --------- Helpers for Users sheet ----------
def sheet_dicts(worksheet) -> List[Dict[str, Any]]:
    vals = worksheet.get_all_values()
    if not vals or len(vals) < 2: return []
    headers, out = vals[0], []
    for row in vals[1:]:
        out.append({headers[i]: (row[i] if i < len(row) else "") for i in range(len(headers))})
    return out

def get_users() -> List[Dict[str, Any]]:
    vals = sheet_dicts(ws(USERS_SHEET))
    res = []
    for r in vals:
        try:
            res.append({
                "chat_id": int(r.get("Chat_ID")),
                "name": (r.get("Name") or "").strip(),
                "deposit": to_float(r.get("Deposit_USDT")),
                "active": str(r.get("Active", "TRUE")).strip().upper() not in ("FALSE", "0", ""),
                "pending": to_float(r.get("Pending_Deposit")),
                "bonus_acc": to_float(r.get("Bonus_Accrued")),
                "bonus_paid": to_float(r.get("Bonus_Paid")),
                "bonus_to_dep": to_float(r.get("Bonus_To_Deposit")),
                "last_upd": r.get("Last_Update") or ""
            })
        except Exception as e:
            log.warning(f"Skipping invalid user row {r}: {e}")
    return res

def find_user_row(chat_id: int) -> Optional[int]:
    w = ws(USERS_SHEET)
    try:
        cell = w.find(str(chat_id), in_column=1)
        if cell: return cell.row
    except Exception:
        pass
    # fallback scan
    try:
        col = w.col_values(1)
        for i, v in enumerate(col, start=1):
            if str(v).strip() == str(chat_id):
                return i
    except Exception:
        pass
    return None

def upsert_user(
    chat_id: int,
    name: Optional[str] = None,
    deposit: Optional[float] = None,
    active: Optional[bool] = None,
    pending: Optional[float] = None,
    bonus_acc: Optional[float] = None,
    bonus_paid: Optional[float] = None,
    bonus_to_dep: Optional[float] = None,
    touch_update: bool = True
):
    w = ws(USERS_SHEET)
    row = find_user_row(chat_id)
    now = now_utc_str() if touch_update else ""

    # read current
    cur = {}
    if row:
        cur_vals = w.row_values(row)
        for i, col in enumerate(USERS_COLS, start=1):
            cur[col] = cur_vals[i-1] if i-1 < len(cur_vals) else ""

    def pick(v, key, default=""):
        return cur.get(key, default) if v is None else v

    data = {
        "Chat_ID": str(chat_id),
        "Name": pick(name, "Name", ""),
        "Deposit_USDT": str(pick(deposit, "Deposit_USDT", "0")),
        "Active": ("TRUE" if (pick(active, "Active", "TRUE") in (True, "TRUE")) else "FALSE") if isinstance(pick(active, "Active", "TRUE"), (bool,)) else pick(active, "Active", "TRUE"),
        "Pending_Deposit": str(pick(pending, "Pending_Deposit", "0")),
        "Bonus_Accrued": str(pick(bonus_acc, "Bonus_Accrued", "0")),
        "Bonus_Paid": str(pick(bonus_paid, "Bonus_Paid", "0")),
        "Bonus_To_Deposit": str(pick(bonus_to_dep, "Bonus_To_Deposit", "0")),
        "Last_Update": now or cur.get("Last_Update", "")
    }

    # normalize bool
    if isinstance(active, bool):
        data["Active"] = "TRUE" if active else "FALSE"

    # write
    row_values = [data[c] for c in USERS_COLS]
    end_col = chr(ord('A') + len(USERS_COLS) - 1)
    if row:
        ws(USERS_SHEET).update(f"A{row}:{end_col}{row}", [row_values])
    else:
        ws(USERS_SHEET).append_row(row_values, value_input_option="RAW")

def adjust_user_bonus(chat_id: int, delta_acc=0.0, delta_paid=0.0, delta_to_dep=0.0):
    users = get_users()
    u = next((x for x in users if x["chat_id"] == chat_id), None)
    if not u:
        # создать черновик
        upsert_user(chat_id, deposit=0, active=False)
        u = {"bonus_acc":0.0,"bonus_paid":0.0,"bonus_to_dep":0.0}
    upsert_user(
        chat_id,
        bonus_acc = u["bonus_acc"] + delta_acc,
        bonus_paid= u["bonus_paid"] + delta_paid,
        bonus_to_dep = u["bonus_to_dep"] + delta_to_dep
    )

def bonus_available(u: Dict[str, Any]) -> float:
    return max(0.0, u["bonus_acc"] - u["bonus_paid"] - u["bonus_to_dep"])

def append_ledger(op_type: str, chat_id: int, name: str, amount: float, note: str = "", admin: str = "", status: str = ""):
    w = ws(LEDGER_SHEET)
    w.append_row([
        now_utc_str(), op_type, str(chat_id), name or "", f"{amount:.2f}", note or "", admin or "", status or ""
    ], value_input_option="RAW")

# ------------------- Misc helpers -------------------
def fmt_usd(x): 
    try:
        return f"{float(x):,.2f}".replace(",", " ")
    except Exception:
        return str(x)

def base_from_pair(pair: str) -> str:
    base = (pair or "").split("/")[0].split(":")[0].upper()
    return base[:-1] if base.endswith("C") and len(base) > 3 else base

def parse_money(s: str) -> float:
    return float(re.sub(r"[^\d.,\-]", "", s).replace(",", "."))

def parse_setdep_text(text: str):
    m = re.match(r"^/setdep\s+(-?\d+)\s+([0-9][\d\s.,]*)\s*(?:\((bonus)\))?\s*$", text.strip(), re.I)
    if not m: return None
    return int(m.group(1)), parse_money(m.group(2)), (m.group(3) == "bonus")

open_positions: Dict[str, Dict[str, Any]] = {}  # Signal_ID -> {cum_margin, recipients: [chat_ids]}

def annual_forecast_user(bonus_acc: float, start_utc: str, deposit: float) -> (float, float):
    try: start_dt = datetime.strptime(start_utc, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except (ValueError, TypeError): return 0.0, 0.0
    days_passed = (datetime.now(timezone.utc) - start_dt).total_seconds() / (24 * 3600)
    days = max(days_passed, 1.0)
    if deposit <= 0: return 0.0, 0.0
    annual_pct = (bonus_acc / deposit) * (365.0 / days) * 100.0
    return annual_pct, deposit * annual_pct / 100.0

# ------------------- Telegram: role helpers -------------------
def is_admin(update: Update) -> bool:
    uid = update.effective_user.id if update.effective_user else None
    cid = update.effective_chat.id if update.effective_chat else None
    return (uid in ADMIN_IDS) or (cid in ADMIN_IDS)

def user_status_label(u: Optional[Dict[str, Any]]) -> str:
    if not u: return "новый"
    return "активный" if u["active"] else "неактивный"

# ------------------- User commands -------------------
START_TEXT = (
    "👋 Привет! Я <b>STRIGI_KAPUSTU_BOT</b>.\n\n"
    "Чтобы начать:\n"
    "1) Отправьте команду <code>/myname Имя Фамилия</code>\n"
    "2) Переведите USDT на адрес:\n"
    "   <code>TVSRhKYHAUKx8RnXzW3KXNeUk5aAQs7hJ4</code>\n"
    "   (сеть TRON, TRC-20).\n"
    "3) Отправьте команду:\n"
    "   <code>/add_deposit 500</code>\n"
    "   (укажите сумму вашего перевода).\n"
    "4) Дождитесь подтверждения — депозит активируется со следующей сделкой.\n"
    "5) Проверяйте состояние в любой момент: <code>/balance</code>\n\n"
    "Дополнительно:\n"
    "• Пополнить из премии: <code>/add_from_bonus 100</code>\n"
    "• Вывод премии: <code>/withdraw_bonus 100</code> (или <code>all</code>)\n"
    "• Вывод всего депозита: <code>/withdraw_all</code>"
)

ABOUT_TEXT = (
    "🤖 <b>О боте</b>\n\n"
    "Это инвестиционный бот, который ведёт алгоритмическую торговлю Евро ↔ Доллар через стейблкоины (EURC/USDT) на бирже.\n"
    "Алгоритм автоматически управляет входами, доборами и выходами, присылает уведомления и ведёт учёт сделок.\n\n"
    "📈 <b>Модель дохода</b>\n"
    "В отчётах вам отображается прибыль только закрытых сделок — это ваша «премия».\n"
    "Её можно вывести (<code>/withdraw_bonus</code>) или реинвестировать (<code>/add_from_bonus</code>).\n\n"
    "⚠️ <b>Дисклеймер о рисках</b>\n"
    "Торговля на рынке (в т.ч. с плечом) связана с высокой волатильностью и может привести к частичной или полной потере средств. "
    "Прошлые результаты не гарантируют будущую доходность. Используя бота, вы подтверждаете, что понимаете и принимаете эти риски."
)

async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid, cid = update.effective_user.id, update.effective_chat.id
    await set_menu_user(ctx.application, cid)
    await update.message.reply_text(START_TEXT, parse_mode=constants.ParseMode.HTML, disable_web_page_preview=True)

async def about(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(ABOUT_TEXT, parse_mode=constants.ParseMode.HTML, disable_web_page_preview=True)

async def myname(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cid = update.effective_chat.id
    args = ctx.args
    name = " ".join(args).strip() if args else ""
    if not name:
        return await update.message.reply_text("Укажите имя: <code>/myname Имя Фамилия</code>", parse_mode=constants.ParseMode.HTML)
    # создать/обновить пользователя
    u_all = get_users()
    u = next((x for x in u_all if x["chat_id"] == cid), None)
    was_new = (u is None)
    upsert_user(cid, name=name, deposit=(u["deposit"] if u else 0.0), active=(u["active"] if u else False))
    await update.message.reply_text(f"✅ Имя сохранено: <b>{name}</b>", parse_mode=constants.ParseMode.HTML)
    # уведомление админу
    status = user_status_label(u)
    for aid in ADMIN_IDS:
        try:
            await ctx.application.bot.send_message(
                aid, f"👤 Пользователь обновил имя: <b>{name}</b> (chat_id <code>{cid}</code>), статус: <b>{status}</b>",
                parse_mode=constants.ParseMode.HTML
            )
        except Exception as e:
            log.warning(f"notify admin name failed: {e}")

async def add_deposit(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cid = update.effective_chat.id
    try:
        amt = parse_money(" ".join(ctx.args))
        if amt <= 0: raise ValueError
    except Exception:
        return await update.message.reply_text("Использование: <code>/add_deposit 500</code>", parse_mode=constants.ParseMode.HTML)
    users = get_users()
    u = next((x for x in users if x["chat_id"] == cid), None)
    if not u:
        upsert_user(cid, name="", deposit=0.0, active=False)
        u = next((x for x in get_users() if x["chat_id"] == cid), None)
    upsert_user(cid, pending=amt)  # применится на следующем OPEN
    append_ledger("DEPOSIT_REQUEST", cid, u["name"], amt, note="user requested external deposit")
    await update.message.reply_text(
        f"📨 Заявка на пополнение <b>{fmt_usd(amt)}</b> USDT отправлена админу.\n"
        f"После подтверждения депозит будет учтён со следующей сделкой. Проверьте: /balance",
        parse_mode=constants.ParseMode.HTML
    )
    # админу
    status = user_status_label(u)
    for aid in ADMIN_IDS:
        try:
            await ctx.application.bot.send_message(
                aid,
                f"🧾 Заявка на пополнение: +<b>{fmt_usd(amt)}</b> USDT от <b>{u['name'] or cid}</b> "
                f"(chat_id <code>{cid}</code>), статус: <b>{status}</b>\n"
                f"Подсказка: новый → <code>/adduser {cid} {u['name'] or cid} {amt}</code>, активный → <code>/setdep {cid} {amt}</code>",
                parse_mode=constants.ParseMode.HTML
            )
        except Exception as e:
            log.warning(f"notify admin add_deposit failed: {e}")

async def add_from_bonus(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cid = update.effective_chat.id
    try:
        amt = parse_money(" ".join(ctx.args))
        if amt <= 0: raise ValueError
    except Exception:
        return await update.message.reply_text("Использование: <code>/add_from_bonus 100</code>", parse_mode=constants.ParseMode.HTML)
    u = next((x for x in get_users() if x["chat_id"] == cid), None)
    if not u:
        return await update.message.reply_text("Сначала укажите имя (/myname) и пополните депозит (/add_deposit).")
    avail = bonus_available(u)
    append_ledger("BONUS_REINVEST_REQUEST", cid, u["name"], amt, note=f"available={avail:.2f}")
    await update.message.reply_text(
        f"📨 Заявка на пополнение из премии <b>{fmt_usd(amt)}</b> USDT отправлена админу.\n"
        f"После подтверждения сумма будет добавлена со следующей сделкой.",
        parse_mode=constants.ParseMode.HTML
    )
    status = user_status_label(u)
    for aid in ADMIN_IDS:
        try:
            await ctx.application.bot.send_message(
                aid,
                f"🧾 Заявка из премии: +<b>{fmt_usd(amt)}</b> USDT от <b>{u['name'] or cid}</b> "
                f"(chat_id <code>{cid}</code>), статус: <b>{status}</b>, доступно: <b>{fmt_usd(avail)}</b>\n"
                f"Подсказка: подтвердить как реинвест → <code>/setdep {cid} {amt} (bonus)</code>",
                parse_mode=constants.ParseMode.HTML
            )
        except Exception as e:
            log.warning(f"notify admin add_from_bonus failed: {e}")

async def withdraw_bonus(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cid = update.effective_chat.id
    if not ctx.args:
        return await update.message.reply_text("Использование: <code>/withdraw_bonus 100</code> или <code>/withdraw_bonus all</code>", parse_mode=constants.ParseMode.HTML)
    arg = ctx.args[0].strip().lower()
    u = next((x for x in get_users() if x["chat_id"] == cid), None)
    if not u:
        return await update.message.reply_text("Вы ещё не подключены. Отправьте /start и следуйте инструкции.")
    avail = bonus_available(u)
    if arg == "all":
        amt = avail
    else:
        try:
            amt = parse_money(arg)
        except Exception:
            return await update.message.reply_text("Неверная сумма. Пример: <code>/withdraw_bonus 150</code> или <code>all</code>", parse_mode=constants.ParseMode.HTML)
    if amt <= 0:
        return await update.message.reply_text("Доступной премии нет к выводу.")
    append_ledger("BONUS_WITHDRAW_REQUEST", cid, u["name"], amt, note=f"available={avail:.2f}")
    await update.message.reply_text(
        f"📨 Заявка на вывод премии (<b>{'all' if arg=='all' else fmt_usd(amt)}</b>) отправлена админу.",
        parse_mode=constants.ParseMode.HTML
    )
    status = user_status_label(u)
    for aid in ADMIN_IDS:
        try:
            await ctx.application.bot.send_message(
                aid,
                f"🧾 Заявка на вывод премии: <b>{'all' if arg=='all' else fmt_usd(amt)}</b> от <b>{u['name'] or cid}</b> "
                f"(chat_id <code>{cid}</code>), статус: <b>{status}</b>, доступно: <b>{fmt_usd(avail)}</b>",
                parse_mode=constants.ParseMode.HTML
            )
        except Exception as e:
            log.warning(f"notify admin withdraw_bonus failed: {e}")

async def withdraw_all(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cid = update.effective_chat.id
    u = next((x for x in get_users() if x["chat_id"] == cid), None)
    if not u:
        return await update.message.reply_text("Вы ещё не подключены. Отправьте /start и следуйте инструкции.")
    avail = bonus_available(u)
    total_payout = u["deposit"] + avail
    append_ledger("WITHDRAW_ALL_REQUEST", cid, u["name"], total_payout, note=f"deposit={u['deposit']:.2f}, bonus_avail={avail:.2f}")
    await update.message.reply_text("📨 Заявка на вывод всего депозита и премии отправлена админу. После перевода учётная запись будет отключена.", parse_mode=constants.ParseMode.HTML)
    status = user_status_label(u)
    for aid in ADMIN_IDS:
        try:
            await ctx.application.bot.send_message(
                aid,
                f"🧾 Заявка на вывод ВСЕГО от <b>{u['name'] or cid}</b> (chat_id <code>{cid}</code>), статус: <b>{status}</b>.\n"
                f"К выплате ориентировочно: депозит <b>{fmt_usd(u['deposit'])}</b> + премия <b>{fmt_usd(avail)}</b> = <b>{fmt_usd(total_payout)}</b>.",
                parse_mode=constants.ParseMode.HTML
            )
        except Exception as e:
            log.warning(f"notify admin withdraw_all failed: {e}")

async def balance(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cid = update.effective_chat.id
    u = next((x for x in get_users() if x["chat_id"] == cid and x["active"]), None)
    if not u:
        return await update.message.reply_text("Вы ещё не подключены. Отправьте /start и передайте ваш chat_id админу.")
    _, start_utc, _ = get_state()
    avail = bonus_available(u)
    ann_pct, ann_usd = annual_forecast_user(u["bonus_acc"], start_utc, u["deposit"])
    await update.message.reply_text(
        f"🧰 <b>Баланс</b>\n\n"
        f"Депозит: <b>${fmt_usd(u['deposit'])}</b>\n"
        f"Премия (накоплено): <b>${fmt_usd(u['bonus_acc'])}</b>\n"
        f"— выплачено: <b>${fmt_usd(u['bonus_paid'])}</b>, реинвестировано: <b>${fmt_usd(u['bonus_to_dep'])}</b>\n"
        f"Доступно к выводу: <b>${fmt_usd(avail)}</b>\n\n"
        f"Оценка годовых к депозиту {fmt_usd(u['deposit'])}: ~{ann_pct:.1f}% (≈{fmt_usd(ann_usd)}/год).",
        parse_mode=constants.ParseMode.HTML
    )

# ------------------- Admin commands -------------------
async def help_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update): return
    text = (
        "Админ-команды:\n"
        "/adduser <chat_id> <Имя (можно с пробелами)> <депозит>\n"
        "/setdep <chat_id> <депозит> (со следующей сделкой) (доп. флаг: (bonus) — реинвест из премии)\n"
        "/setname <chat_id> <Имя>\n"
        "/remove <chat_id>\n"
        "/list"
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
        return await update.message.reply_text("Использование: /adduser <chat_id> <Имя (можно с пробелами)> <депозит>")
    upsert_user(chat_id, name=name, deposit=dep, active=True, pending=0)
    append_ledger("ADMIN_ADDUSER", chat_id, name, dep, admin=str(update.effective_user.id), status="applied")
    await update.message.reply_text(f"OK. Пользователь {name} ({chat_id}) добавлен с депозитом {fmt_usd(dep)} USDT.")
    try:
        await set_menu_user(ctx.application, chat_id)
        await ctx.application.bot.send_message(
            chat_id,
            text=f"👋 Добро пожаловать, <b>{name}</b>! Ваш депозит: ${fmt_usd(dep)}.\nДепозит будет учтён со следующей сделкой.",
            parse_mode=constants.ParseMode.HTML
        )
    except Exception as e:
        logging.warning(f"Не удалось отправить приветствие {chat_id}: {e}")

def get_state():
    w = ws(STATE_SHEET)
    val_last_row, val_start_utc, val_profit_total = w.acell("A2").value, w.acell("B2").value, w.acell("C2").value
    last_row = int(val_last_row) if (val_last_row or "").strip().isdigit() else 0
    start_utc = val_start_utc or ""
    profit_total = to_float(val_profit_total)
    return last_row, start_utc, profit_total

def set_state(last_row: Optional[int] = None, profit_total: Optional[float] = None, start_utc: Optional[str] = None):
    w = ws(STATE_SHEET)
    if last_row is not None: w.update_acell("A2", str(last_row))
    if start_utc is not None: w.update_acell("B2", start_utc)
    if profit_total is not None: w.update_acell("C2", f"{profit_total:.6f}")

async def setdep(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update): return
    parsed = parse_setdep_text(update.message.text or "")
    if not parsed:
        return await update.message.reply_text("Использование: /setdep <chat_id> <депозит> (опц. (bonus))")
    chat_id, dep, is_bonus = parsed
    u = next((x for x in get_users() if x["chat_id"] == chat_id), None)
    if not u:
        return await update.message.reply_text("Пользователь не найден.")
    # если это реинвест из премии — уменьшаем доступную премию и фиксируем как Bonus_To_Deposit
    if is_bonus:
        avail = bonus_available(u)
        if dep > avail:
            return await update.message.reply_text(f"Недостаточно премии. Доступно: {fmt_usd(avail)}")
        adjust_user_bonus(chat_id, delta_to_dep=dep)  # зарезервировали под реинвест
        append_ledger("ADMIN_BONUS_TO_DEP", chat_id, u["name"], dep, admin=str(update.effective_user.id), status="reserved_for_next_open")
    upsert_user(chat_id, pending=dep)
    await update.message.reply_text(f"OK. Pending-депозит {fmt_usd(dep)} USDT применится со следующей сделкой. {'(из премии)' if is_bonus else ''}")

async def setname(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update): return
    try:
        chat_id = int(ctx.args[0]); name = " ".join(ctx.args[1:])
        if not name: raise ValueError
    except (IndexError, ValueError): return await update.message.reply_text("Использование: /setname <chat_id> <Новое Имя>")
    upsert_user(chat_id, name=name)
    await update.message.reply_text("OK. Имя обновлено.")

async def remove(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update): return
    try: chat_id = int(ctx.args[0])
    except (IndexError, ValueError): return await update.message.reply_text("Использование: /remove <chat_id>")
    upsert_user(chat_id, active=False)
    append_ledger("ADMIN_REMOVE", chat_id, "", 0.0, admin=str(update.effective_user.id), status="deactivated")
    await update.message.reply_text("OK. Пользователь деактивирован.")
    try:
        await ctx.application.bot.set_my_commands([BotCommand("start", "Как подключиться"), BotCommand("about","О боте")], scope=BotCommandScopeChat(chat_id))
    except Exception as e:
        log.warning(f"set default menu for {chat_id} failed: {e}")

async def list_users(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update): return
    users = get_users()
    if not users: return await update.message.reply_text("Список пуст.")
    lines = [f"{'✅' if u['active'] else '⛔️'} {u['name'] or u['chat_id']} | dep={fmt_usd(u['deposit'])} | pending={fmt_usd(u['pending'])} | bonus_avail={fmt_usd(bonus_available(u))} | id={u['chat_id']}" for u in users]
    await update.message.reply_text("\n".join(lines))

# ------------------- Poller: сделки из лога -------------------
async def send_all(app: Application, text_by_user: Dict[int, str]):
    for chat_id, text in text_by_user.items():
        if text.strip():
            try: await app.bot.send_message(chat_id=chat_id, text=text, parse_mode=constants.ParseMode.HTML, disable_web_page_preview=True)
            except Exception as e: log.warning(f"send to {chat_id} failed: {e}")

async def poll_and_broadcast(app: Application):
    try:
        last_row, start_utc, profit_total = get_state()
        if not (start_utc or "").strip():
            start_utc = now_utc_str()
            set_state(start_utc=start_utc)
        # читаем сделки
        records = sheet_dicts(ws(LOG_SHEET))
        total_rows_in_sheet = len(records) + 1
        if last_row == 0:
            log.info(f"First run detected. Skipping {total_rows_in_sheet} historical records.")
            set_state(last_row=total_rows_in_sheet, profit_total=0.0)
            return
        if total_rows_in_sheet <= last_row: return
        new_records = records[(last_row - 1):]

        # активные пользователи на текущий момент (для OPEN будем брать их список)
        users_all = get_users()
        active_users_now = [u for u in users_all if u["active"]]

        per_user_msgs: Dict[int, List[str]] = {}
        def push(uid: int, text: str):
            if not text: return
            per_user_msgs.setdefault(uid, []).append(text)

        for rec in new_records:
            ev, sid = rec.get("Event") or "", rec.get("Signal_ID") or ""
            cum_margin = to_float(rec.get("Cum_Margin_USDT"))
            pnl_usd = to_float(rec.get("PNL_Realized_USDT"))
            pair = rec.get("Pair","")

            # OPEN / ADD / RETEST_ADD: фиксируем пользователей сделки и применяем pending/реинвесты
            if ev in ("OPEN", "ADD", "RETEST_ADD"):
                # при первом OPEN сделки — применяем pending и переносим реинвесты (Bonus_To_Deposit)
                if ev == "OPEN":
                    # применим pending для всех активных
                    updated_users = []
                    for u in get_users():
                        if not u["active"]: continue
                        pend = u["pending"]
                        if pend > 0:
                            # если эта pending помечена как реинвест (мы зарезервировали через Bonus_To_Deposit)
                            # её уже учли в Bonus_To_Deposit. Просто увеличим депозит и обнулим pending.
                            upsert_user(u["chat_id"], deposit=u["deposit"] + pend, pending=0.0)
                            append_ledger("PENDING_APPLIED", u["chat_id"], u["name"], pend, note="applied on OPEN")
                            updated_users.append(u["chat_id"])
                    # recipients = активные пользователи на момент открытия
                    recipients = [u["chat_id"] for u in get_users() if u["active"]]
                    open_positions[sid] = {"cum_margin": cum_margin, "recipients": recipients}

                else:
                    # обновим снимок
                    snap = open_positions.setdefault(sid, {"cum_margin": 0.0, "recipients": []})
                    snap["cum_margin"] = cum_margin

                # уведомления пользователям сделки
                snap = open_positions.get(sid, {})
                recipients = snap.get("recipients", [])
                if not recipients:
                    continue
                used_pct = 100.0 * (cum_margin / max(SYSTEM_BANK_USDT, 1e-9))
                if ev == "OPEN":
                    msg = f"📊 Сделка открыта. Задействовано {used_pct:.1f}% банка ({fmt_usd(cum_margin)})."
                else:
                    msg = f"🪙💵 Докупили {base_from_pair(pair)}. Объём в сделке: {used_pct:.1f}% банка ({fmt_usd(cum_margin)})."
                for uid in recipients:
                    push(uid, msg)

            # Закрытие сделки — распределяем премию 30%
            if ev in ("TP_HIT", "SL_HIT", "MANUAL_CLOSE"):
                snap = open_positions.get(sid, {})
                recipients = snap.get("recipients", [])
                cm = snap.get("cum_margin", cum_margin)

                if not recipients:
                    # fallback: если по какой-то причине нет снимка — считаем по всем активным сейчас
                    recipients = [u["chat_id"] for u in get_users() if u["active"]]

                if not recipients:
                    continue

                # пул премии 30% от net PnL сделки (может быть отрицательным)
                bonus_pool = pnl_usd * 0.30

                # депозиты только тех, кто был в получателях сделки
                users_map = {u["chat_id"]: u for u in get_users()}
                dep_sum = sum(users_map[uid]["deposit"] for uid in recipients if uid in users_map) or 1.0

                used_pct = 100.0 * (cm / max(SYSTEM_BANK_USDT, 1e-9))
                profit_pct_vs_cm = (pnl_usd / max(cm, 1e-9)) * 100.0 if cm > 0 else 0.0
                icon = "✅" if pnl_usd >= 0 else "🛑"

                # распределяем и копим Bonus_Accrued
                for uid in recipients:
                    u = users_map.get(uid)
                    if not u: continue
                    share = (u["deposit"] / dep_sum) if dep_sum > 0 else 0.0
                    my_bonus = bonus_pool * share
                    if abs(my_bonus) > 1e-9:
                        adjust_user_bonus(uid, delta_acc=my_bonus)
                    # сообщение пользователю
                    # годовые считаем от накопленной премии (Bonus_Accrued) к его депозиту
                    u_after = next((x for x in get_users() if x["chat_id"] == uid), u)
                    ann_pct, ann_usd = annual_forecast_user(u_after["bonus_acc"], start_utc, u_after["deposit"])
                    txt = (
                        f"{icon} Сделка закрыта. Использовалось {used_pct:.1f}% банка ({fmt_usd(cm)}). "
                        f"Net P&L сделки: <b>{fmt_usd(pnl_usd)}</b> ({profit_pct_vs_cm:+.2f}%).\n"
                        f"Ваша премия за эту сделку (30% от P&L по доле депозита): <b>{fmt_usd(my_bonus)}</b>.\n"
                        f"Оценка годовых по вашему депозиту {fmt_usd(u_after['deposit'])}: ~{ann_pct:.1f}% (≈{fmt_usd(ann_usd)}/год)."
                    )
                    push(uid, txt)

                # агрегатор в STATE — суммируем именно 30% (для общей статистики системы)
                profit_total += bonus_pool
                set_state(profit_total=profit_total)

                if sid in open_positions:
                    del open_positions[sid]

        # отправим накопившиеся сообщения
        final_messages = {uid: "\n\n".join(msgs) for uid, msgs in per_user_msgs.items() if msgs}
        if final_messages:
            await send_all(app, final_messages)

        set_state(last_row=total_rows_in_sheet)
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
    handlers = [
        # пользователи
        CommandHandler("start", start),
        CommandHandler("about", about),
        CommandHandler("myname", myname),
        CommandHandler("add_deposit", add_deposit),
        CommandHandler("add_from_bonus", add_from_bonus),
        CommandHandler("withdraw_bonus", withdraw_bonus),
        CommandHandler("withdraw_all", withdraw_all),
        CommandHandler("balance", balance),
        # админ
        CommandHandler("help", help_cmd),
        CommandHandler("adduser", adduser),
        CommandHandler("setdep", setdep),
        CommandHandler("setname", setname),
        CommandHandler("remove", remove),
        CommandHandler("list", list_users),
    ]
    for h in handlers: app.add_handler(h)
    app.job_queue.run_repeating(poll_job, interval=10, first=5)
    log.info(f"{BOT_NAME} starting…")
    app.run_polling()

if __name__ == "__main__":
    main()
