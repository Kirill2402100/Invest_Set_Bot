# marketing_bot.py — STRIGI_KAPUSTU_BOT
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

log.info(f"gspread version: {getattr(gspread, '__version__', 'unknown')}")
log.info(f"ADMIN_IDS raw={os.getenv('ADMIN_IDS')}")
log.info(f"ADMIN_IDS parsed={sorted(ADMIN_IDS)}")

# --- Меню команд ---
USER_COMMANDS = [BotCommand("start", "Как подключиться"), BotCommand("balance", "Показать баланс")]
ADMIN_COMMANDS = [
    BotCommand("start", "Показать chat_id"), BotCommand("help", "Команды админа"),
    BotCommand("list", "Список пользователей"), BotCommand("adduser", "Добавить пользователя"),
    BotCommand("setdep", "Изменить депозит (со след. сделки)"),
    BotCommand("setname", "Переименовать пользователя"), BotCommand("remove", "Отключить пользователя"),
]

async def set_menu_default(app: Application):
    await app.bot.set_my_commands([BotCommand("start", "Как подключиться")], scope=BotCommandScopeAllPrivateChats())

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

LOG_SHEET, USERS_SHEET, STATE_SHEET = "BMR_DCA_Log", "Marketing_Users", "Marketing_State"

def to_float(x) -> float:
    try: return float(str(x).replace(",", "."))
    except (ValueError, TypeError): return 0.0

def ensure_sheets():
    names = {ws.title for ws in sh.worksheets()}
    if USERS_SHEET not in names:
        ws = sh.add_worksheet(USERS_SHEET, rows=100, cols=10)
        ws.update("A1:E1", [["Chat_ID", "Name", "Deposit_USDT", "Active", "Pending_Deposit"]])
    if STATE_SHEET not in names:
        ws = sh.add_worksheet(STATE_SHEET, rows=10, cols=3)
        ws.update("A1:C1", [["Last_Row", "Start_UTC", "Profit_Total_USDT"]])
        ws.update("A2:C2", [["0", now_utc_str(), "0"]])
    else:
        ws = sh.worksheet(STATE_SHEET)
        vals = ws.get_all_values()
        if len(vals) < 2:
            ws.update("A2:C2", [["0", now_utc_str(), "0"]])
        else:
            cur = (ws.acell("B2").value or "").strip()
            if not cur:
                ws.update_acell("B2", now_utc_str())
    if LOG_SHEET not in names:
        raise RuntimeError(f"Не найден лист {LOG_SHEET} (его пишет основной бот)")

ensure_sheets()
def ws(title): return sh.worksheet(title)

# ------------------- Model -------------------
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
    if profit_total is not None: w.update_acell("C2", str(profit_total))

def get_users() -> List[Dict[str, Any]]:
    vals = ws(USERS_SHEET).get_all_records()
    res = []
    for r in vals:
        try:
            res.append({
                "chat_id": int(r.get("Chat_ID")), "name": r.get("Name") or "",
                "deposit": float(r.get("Deposit_USDT") or 0),
                "active": str(r.get("Active", "TRUE")).strip().upper() not in ("FALSE", "0", ""),
                "pending": float(r.get("Pending_Deposit") or 0.0),
            })
        except (ValueError, TypeError):
            log.warning(f"Skipping invalid user row: {r}")
            continue
    return res

def upsert_user_row(chat_id: int, name: str = None, deposit: float = None, active: bool = None, pending: float = None):
    w = ws(USERS_SHEET)
    row_idx = None
    try:
        cell = w.find(str(chat_id), in_column=1)
        if cell is not None and getattr(cell, "row", None):
            row_idx = cell.row
    except Exception as e:
        log.info(f"find() failed or not supported fully, fallback to scan: {e}")
    if row_idx is None:
        try:
            col = w.col_values(1)
            for i, v in enumerate(col, start=1):
                if str(v).strip() == str(chat_id):
                    row_idx = i
                    break
        except Exception as e:
            log.warning(f"col_values scan failed: {e}")
    def v(new_val, current): return current if new_val is None else new_val
    if row_idx:
        current = w.row_values(row_idx)
        while len(current) < 5: current.append("")
        new_name, new_deposit = v(name, current[1]), str(v(deposit, to_float(current[2])))
        new_active  = "TRUE" if active else "FALSE" if active is not None else current[3]
        new_pending = str(pending) if pending is not None else str(to_float(current[4]))
        w.update(f"A{row_idx}:E{row_idx}", [[str(chat_id), new_name, new_deposit, new_active, new_pending]])
    else:
        w.append_row([str(chat_id), name or "", str(deposit or 0), "TRUE" if (active is None or active) else "FALSE", str(pending or 0)], value_input_option="RAW")

# ------------------- Helpers & Parsers -------------------
def fmt_usd(x): return f"{x:,.2f}".replace(",", " ")
def tier_emoji(profit_pct: float) -> str:
    if profit_pct >= 90: return "🚀"
    if profit_pct >= 80: return "🛩️"
    if profit_pct >= 70: return "🏎️"
    if profit_pct >= 50: return "🏍️"
    return "✅"
def base_from_pair(pair: str) -> str:
    base = (pair or "").split("/")[0].split(":")[0].upper()
    return base[:-1] if base.endswith("C") and len(base) > 3 else base
def parse_money(s: str) -> float:
    return float(re.sub(r"[^\d.,\-]", "", s).replace(",", "."))
def parse_setdep_text(text: str):
    m = re.match(r"^/setdep\s+(-?\d+)\s+([0-9][\d\s.,]*)\s*$", text.strip(), re.I)
    if not m: return None
    return int(m.group(1)), parse_money(m.group(2))
open_positions: Dict[str, Dict[str, Any]] = {}
def annual_forecast(profit_total: float, start_utc: str, deposit: float) -> (float, float):
    try: start_dt = datetime.strptime(start_utc, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except (ValueError, TypeError): return 0.0, 0.0
    days_passed = (datetime.now(timezone.utc) - start_dt).total_seconds() / (24 * 3600)
    days = max(days_passed, 1)
    if deposit <= 0: return 0.0, 0.0
    annual_pct = (profit_total / deposit) * (365.0 / days) * 100.0
    return annual_pct, deposit * annual_pct / 100.0

# ------------------- Telegram Handlers -------------------
def is_admin(update: Update) -> bool:
    uid = update.effective_user.id if update.effective_user else None
    cid = update.effective_chat.id if update.effective_chat else None
    return (uid in ADMIN_IDS) or (cid in ADMIN_IDS)
async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid, cid = update.effective_user.id, update.effective_chat.id
    txt = (f"Привет! Я <b>{BOT_NAME}</b>.\n"
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
        "/adduser <chat_id> <Имя (можно с пробелами)> <депозит>\n"
        "/setdep <chat_id> <депозит> (со следующей сделкой)\n"
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
    upsert_user_row(chat_id, name=name, deposit=dep, active=True)
    await update.message.reply_text(f"OK. Пользователь {name} ({chat_id}) добавлен с депозитом {fmt_usd(dep)} USDT.")
    await set_menu_user(ctx.application, chat_id)
    try:
        await ctx.application.bot.send_message(
            chat_id=chat_id,
            text=f"👋 Добро пожаловать, <b>{name}</b>! Поздравляем, вы начали зарабатывать! Ваш депозит: ${fmt_usd(dep)}.",
            parse_mode=constants.ParseMode.HTML
        )
    except Exception as e:
        logging.warning(f"Не удалось отправить приветствие {chat_id}: {e}")
async def setdep(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update): return
    parsed = parse_setdep_text(update.message.text or "")
    if not parsed:
        return await update.message.reply_text("Использование: /setdep <chat_id> <депозит>")
    chat_id, dep = parsed
    upsert_user_row(chat_id, pending=dep)
    await update.message.reply_text(f"OK. Pending-депозит {fmt_usd(dep)} USDT применится со следующей сделкой.")
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
    lines = [f"{'✅' if u['active'] else '⛔️'} {u['name'] or u['chat_id']} | dep={fmt_usd(u['deposit'])} | pending={fmt_usd(u['pending'])} | id={u['chat_id']}" for u in users]
    await update.message.reply_text("\n".join(lines))
async def balance(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cid = update.effective_chat.id
    u = next((x for x in get_users() if x["chat_id"] == cid and x["active"]), None)
    if not u:
        return await update.message.reply_text("Вы ещё не подключены. Отправьте /start и передайте ваш chat_id админу.")
    _, start_utc, profit_total = get_state()
    total_dep = sum(x["deposit"] for x in get_users() if x["active"]) or 1.0
    my_profit = profit_total * (u["deposit"] / total_dep)
    await update.message.reply_text(
        f"🧰 <b>Баланс</b>\n\n"
        f"Депозит: <b>${fmt_usd(u['deposit'])}</b>\n"
        f"Прибыль (закрытые сделки): <b>${fmt_usd(my_profit)}</b>\n"
        f"Итого: <b>${fmt_usd(u['deposit'] + my_profit)}</b>",
        parse_mode=constants.ParseMode.HTML
    )

# ------------------- Poller & Main Logic -------------------
async def send_all(app: Application, text_by_user: Dict[int, str]):
    for chat_id, text in text_by_user.items():
        if text.strip():
            try: await app.bot.send_message(chat_id=chat_id, text=text, parse_mode=constants.ParseMode.HTML, disable_web_page_preview=True)
            except Exception as e: log.warning(f"send to {chat_id} failed: {e}")
def sheet_dicts(worksheet) -> List[Dict[str, Any]]:
    vals = worksheet.get_all_values()
    if not vals or len(vals) < 2: return []
    headers, out = vals[0], []
    for row in vals[1:]:
        out.append({headers[i]: (row[i] if i < len(row) else "") for i in range(len(headers))})
    return out
async def poll_and_broadcast(app: Application):
    try:
        last_row, start_utc, profit_total = get_state()
        if not (start_utc or "").strip():
            start_utc = now_utc_str()
            set_state(start_utc=start_utc)
        records = sheet_dicts(ws(LOG_SHEET))
        total_rows_in_sheet = len(records) + 1
        if last_row == 0:
            log.info(f"First run detected. Skipping {total_rows_in_sheet} historical records.")
            set_state(last_row=total_rows_in_sheet, profit_total=0.0)
            return
        if total_rows_in_sheet <= last_row: return
        new_records = records[(last_row - 1):]
        users = [u for u in get_users() if u["active"]]
        if not users:
            set_state(last_row=total_rows_in_sheet)
            return
        
        per_user_msgs: Dict[int, List[str]] = {}
        def push(uid: int, text: str):
            if not text: return
            per_user_msgs.setdefault(uid, []).append(text)
        
        for rec in new_records:
            ev, sid = rec.get("Event") or "", rec.get("Signal_ID") or ""
            cum_margin, pnl_usd = to_float(rec.get("Cum_Margin_USDT")), to_float(rec.get("PNL_Realized_USDT"))
            
            if ev in ("OPEN", "ADD", "RETEST_ADD"):
                if ev == "OPEN":
                    for u in users:
                        if u["pending"] > 0:
                            upsert_user_row(u["chat_id"], deposit=u["pending"], pending=0)
                            u["deposit"], u["pending"] = u["pending"], 0
                    recipients = [u["chat_id"] for u in users]
                    open_positions[sid] = {"cum_margin": cum_margin, "users": recipients}
                else:
                    snap = open_positions.setdefault(sid, {"cum_margin": 0.0, "users": []})
                    snap["cum_margin"] = cum_margin
                    recipients = snap["users"]
                if not recipients: continue
                used_pct = 100.0 * (cum_margin / max(SYSTEM_BANK_USDT, 1e-9))
                if ev == "OPEN":
                    msg = f"📊 Сделка открыта. Задействовано {used_pct:.1f}% банка ({fmt_usd(cum_margin)})."
                else:
                    msg = f"🪙💵 Докупили {base_from_pair(rec.get('Pair', ''))}. Объём в сделке: {used_pct:.1f}% банка ({fmt_usd(cum_margin)})."
                for uid in recipients: push(uid, msg)
            
            if ev in ("TP_HIT", "SL_HIT", "MANUAL_CLOSE"):
                snapshot = open_positions.get(sid, {})
                recipients, cm = snapshot.get("users", []), snapshot.get("cum_margin", cum_margin)
                if not recipients:
                    try:
                        for r in reversed(records[-300:]):
                            if r.get("Signal_ID") == sid and r.get("Event") == "OPEN":
                                recipients = [u["chat_id"] for u in users]
                                cm = to_float(r.get("Cum_Margin_USDT")) or cm
                                break
                    except Exception as e:
                        log.warning(f"fallback recipients failed for {sid}: {e}")
                if not recipients: continue
                
                profit_total += pnl_usd
                total_dep = sum(x["deposit"] for x in users if x["active"]) or 1.0
                
                used_pct = 100.0 * (cm / max(SYSTEM_BANK_USDT, 1e-9))
                profit_pct = (pnl_usd / max(cm, 1e-9)) * 100.0 if cm > 0 else 0.0
                icon = tier_emoji(profit_pct) if pnl_usd >= 0 else "🛑"

                for u in users:
                    if u["chat_id"] not in recipients: continue
                    my_profit_total = profit_total * (u["deposit"] / total_dep)
                    ann_pct, ann_usd = annual_forecast(my_profit_total, start_utc, u["deposit"])
                    txt = (f"{icon} Сделка закрыта. Использовалось {used_pct:.1f}% банка ({fmt_usd(cm)}). "
                           f"P&L: {fmt_usd(pnl_usd)} ({profit_pct:+.2f}%).\n"
                           f"Оценка годовых по депозиту {fmt_usd(u['deposit'])}: ~{ann_pct:.1f}% (≈{fmt_usd(ann_usd)}/год).")
                    push(u["chat_id"], txt)
                if sid in open_positions: del open_positions[sid]
        
        final_messages = {uid: "\n\n".join(msgs) for uid, msgs in per_user_msgs.items() if msgs}
        if final_messages:
            await send_all(app, final_messages)
        set_state(last_row=total_rows_in_sheet, profit_total=profit_total)
    except Exception as e: log.exception("poll_and_broadcast error")

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
        CommandHandler("start", start), CommandHandler("help", help_cmd),
        CommandHandler("whoami", whoami), CommandHandler("balance", balance),
        CommandHandler("adduser", adduser), CommandHandler("setdep", setdep),
        CommandHandler("setname", setname), CommandHandler("remove", remove),
        CommandHandler("list", list_users)
    ]
    for handler in handlers: app.add_handler(handler)
    app.job_queue.run_repeating(poll_job, interval=10, first=5)
    log.info(f"{BOT_NAME} starting…")
    app.run_polling()
if __name__ == "__main__":
    main()
