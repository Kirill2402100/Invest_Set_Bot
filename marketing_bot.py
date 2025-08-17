# marketing_bot.py — STRIGI_KAPUSTU_BOT
import os, time, logging, math
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional

import gspread
from google.oauth2.service_account import Credentials
from telegram import Update, constants, BotCommand
from telegram.ext import (
    ApplicationBuilder, Application, CommandHandler,
    ContextTypes
)

# ------------------- ENV -------------------
BOT_NAME = "STRIGI_KAPUSTU_BOT"
BOT_TOKEN = os.getenv("MARKETING_BOT_TOKEN")
SHEET_ID = os.getenv("SHEET_ID")
ADMIN_IDS = {int(x) for x in os.getenv("ADMIN_IDS", "").replace(",", " ").split() if x.isdigit()}
SYSTEM_BANK_USDT = float(os.getenv("SYSTEM_BANK_USDT", "1000"))  # % от банка считаем пока от этой величины

if not BOT_TOKEN or not SHEET_ID or not ADMIN_IDS:
    raise RuntimeError("MARKETING_BOT_TOKEN / SHEET_ID / ADMIN_IDS обязательны")

# ------------------- LOG -------------------
log = logging.getLogger("marketing")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ------------------- Sheets -------------------
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
CREDS_JSON = os.getenv("GOOGLE_CREDENTIALS")
if not CREDS_JSON:
    raise RuntimeError("GOOGLE_CREDENTIALS env var not set")

gc = gspread.service_account_from_dict(eval(CREDS_JSON))
sh = gc.open_by_key(SHEET_ID)

# Названия листов
LOG_SHEET = "BMR_DCA_Log"         # пишет основной бот
USERS_SHEET = "Marketing_Users"   # Chat_ID | Name | Deposit_USDT | Active | Pending_Deposit
STATE_SHEET = "Marketing_State"   # в A1: last_row, в A2: start_utc, в A3: profit_total_usdt

def ensure_sheets():
    names = {ws.title for ws in sh.worksheets()}
    if USERS_SHEET not in names:
        ws = sh.add_worksheet(USERS_SHEET, rows=100, cols=10)
        ws.update("A1:E1", [["Chat_ID", "Name", "Deposit_USDT", "Active", "Pending_Deposit"]])
    if STATE_SHEET not in names:
        ws = sh.add_worksheet(STATE_SHEET, rows=10, cols=3)
        ws.update("A1:C1", [["Last_Row", "Start_UTC", "Profit_Total_USDT"]])
        ws.update("A2", [["0"], [datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")], ["0"]])
    if LOG_SHEET not in names:
        raise RuntimeError(f"Не найден лист {LOG_SHEET} (его пишет основной бот)")

ensure_sheets()

def ws(title): return sh.worksheet(title)

# ------------------- Model -------------------
def get_state():
    s = ws(STATE_SHEET).get_all_values()
    # строки: заголовок, затем значения
    last_row = int(s[1][0]) if len(s) > 1 and s[1][0] else 0
    start_utc = s[2][0] if len(s) > 2 and s[2][0] else datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    profit_total = float(s[3][0]) if len(s) > 3 and s[3][0] else 0.0
    return last_row, start_utc, profit_total

def set_state(last_row=None, profit_total=None):
    w = ws(STATE_SHEET)
    if last_row is not None:
        w.update("A2", str(last_row))
    if profit_total is not None:
        w.update("C2", str(profit_total))

def get_users() -> List[Dict[str, Any]]:
    vals = ws(USERS_SHEET).get_all_records()
    res = []
    for r in vals:
        try:
            res.append({
                "chat_id": int(r.get("Chat_ID")),
                "name": r.get("Name") or "",
                "deposit": float(r.get("Deposit_USDT") or 0),
                "active": str(r.get("Active", "TRUE")).strip().upper() not in ("FALSE", "0", ""),
                "pending": float(r.get("Pending_Deposit") or 0.0),
            })
        except Exception:
            continue
    return res

def upsert_user_row(chat_id: int, name: str = None, deposit: float = None, active: bool = None, pending: float = None):
    w = ws(USERS_SHEET)
    values = w.get_all_values()
    header = values[0]
    rows = values[1:]
    col = {name: idx for idx, name in enumerate(header, start=1)}
    row_idx = None
    for i, r in enumerate(rows, start=2):
        if str(r[col["Chat_ID"]-1]).strip() == str(chat_id):
            row_idx = i
            break
    def v(key, cur):
        return cur if key is None else key
    if row_idx:
        row = w.row_values(row_idx)
        # расширим ряд, если короче
        while len(row) < len(header): row.append("")
        row[col["Chat_ID"]-1] = str(chat_id)
        row[col["Name"]-1] = v(name, row[col["Name"]-1])
        row[col["Deposit_USDT"]-1] = str(v(deposit, float(row[col["Deposit_USDT"]-1] or 0)))
        if active is not None:
            row[col["Active"]-1] = "TRUE" if active else "FALSE"
        if pending is not None:
            row[col["Pending_Deposit"]-1] = str(pending)
        w.update(f"A{row_idx}:E{row_idx}", [row[:5]])
    else:
        w.append_row([
            str(chat_id),
            name or "",
            str(deposit or 0),
            "TRUE" if (active is None or active) else "FALSE",
            str(pending or 0)
        ])

# ------------------- Helpers -------------------
def fmt_usd(x): return f"{x:,.2f}".replace(",", " ")
def fmt_pct(x): return f"{x:.2f}%"

def tier_emoji(profit_pct_of_margin: float) -> str:
    if profit_pct_of_margin >= 90: return "🚀"
    if profit_pct_of_margin >= 80: return "🛩️"
    if profit_pct_of_margin >= 70: return "🏎️"
    if profit_pct_of_margin >= 50: return "🏍️"
    return "✅"

# хранить состояние позиций, чтобы знать текущую задействованную маржу по signal_id
open_positions: Dict[str, Dict[str, Any]] = {}

def annual_forecast(profit_total_usdt: float, start_utc: str, deposit: float) -> (float, float):
    try:
        start_dt = datetime.strptime(start_utc, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except Exception:
        start_dt = datetime.now(timezone.utc) - timedelta(days=1)
    days = max((datetime.now(timezone.utc) - start_dt).days, 1)
    annual_pct = (profit_total_usdt / days * 365.0) / max(deposit, 1e-9) * 100.0
    annual_usd = deposit * annual_pct / 100.0
    return annual_pct, annual_usd

# ------------------- Telegram -------------------
def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cid = update.effective_chat.id
    txt = (f"Привет! Я <b>{BOT_NAME}</b>.\n"
           f"Твой <b>chat_id</b>: <code>{cid}</code>.\n"
           f"Подписку и депозит назначает админ.")
    await update.message.reply_text(txt, parse_mode=constants.ParseMode.HTML)

async def help_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return await update.message.reply_text("Команды управления доступны только админу.")
    await update.message.reply_text(
        "/adduser <chat_id> <Имя> <депозит>\n"
        "/setdep <chat_id> <депозит>   (вступит в силу со следующей сделкой)\n"
        "/setname <chat_id> <Имя>\n"
        "/remove <chat_id>\n"
        "/list",
        parse_mode=constants.ParseMode.HTML
    )

async def adduser(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    try:
        chat_id = int(ctx.args[0]); name = ctx.args[1]; dep = float(ctx.args[2])
    except Exception:
        return await update.message.reply_text("Использование: /adduser <chat_id> <Имя> <депозит>")
    upsert_user_row(chat_id, name=name, deposit=dep, active=True)
    await update.message.reply_text(f"OK. Пользователь {name} ({chat_id}) добавлен с депозитом {fmt_usd(dep)} USDT.")

async def setdep(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    try:
        chat_id = int(ctx.args[0]); dep = float(ctx.args[1])
    except Exception:
        return await update.message.reply_text("Использование: /setdep <chat_id> <депозит>")
    # откладываем применение до следующего OPEN
    upsert_user_row(chat_id, pending=dep)
    await update.message.reply_text(f"OK. Pending-депозит {fmt_usd(dep)} USDT применится со следующей сделкой.")

async def setname(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    try:
        chat_id = int(ctx.args[0]); name = " ".join(ctx.args[1:])
    except Exception:
        return await update.message.reply_text("Использование: /setname <chat_id> <Имя>")
    upsert_user_row(chat_id, name=name)
    await update.message.reply_text("OK. Имя обновлено.")

async def remove(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    try:
        chat_id = int(ctx.args[0])
    except Exception:
        return await update.message.reply_text("Использование: /remove <chat_id>")
    upsert_user_row(chat_id, active=False)
    await update.message.reply_text("OK. Пользователь деактивирован.")

async def list_users(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    users = get_users()
    if not users: return await update.message.reply_text("Список пуст.")
    lines = []
    for u in users:
        lines.append(f"{'✅' if u['active'] else '⛔️'} {u['name'] or u['chat_id']} | dep={fmt_usd(u['deposit'])} | pending={fmt_usd(u['pending'])} | id={u['chat_id']}")
    await update.message.reply_text("\n".join(lines))

# ------------------- Messaging -------------------
async def send_all(app: Application, text_by_user: Dict[int, str]):
    for chat_id, text in text_by_user.items():
        try:
            await app.bot.send_message(chat_id=chat_id, text=text, parse_mode=constants.ParseMode.HTML, disable_web_page_preview=True)
        except Exception as e:
            log.warning(f"send to {chat_id} failed: {e}")

# ------------------- Poller -------------------
def sheet_dicts(ws) -> List[Dict[str, Any]]:
    vals = ws.get_all_values()
    if not vals or len(vals) < 2: return []
    headers = vals[0]
    out = []
    for row in vals[1:]:
        d = {headers[i]: (row[i] if i < len(row) else "") for i in range(len(headers))}
        out.append(d)
    return out

def to_float(x) -> float:
    try: return float(str(x).replace(",", "."))
    except: return 0.0

async def poll_and_broadcast(app: Application):
    try:
        last_row, start_utc, profit_total = get_state()
        records = sheet_dicts(ws(LOG_SHEET))
        total_rows = len(records) + 1  # + header row
        if total_rows <= last_row:
            return  # нет новых событий

        new_records = records[(last_row-1):] if last_row > 1 else records
        users = [u for u in get_users() if u["active"]]
        if not users:
            set_state(last_row=total_rows)
            return

        # Персонализированные тексты (на CLOSE), а OPEN/ADD — одинаковые всем
        broadcast_general: List[str] = []
        personal_close_texts: Dict[int, str] = {}

        for rec in new_records:
            ev = rec.get("Event") or ""
            sid = rec.get("Signal_ID") or ""
            step = int(rec.get("Step_No") or 0)
            ts = rec.get("Timestamp_UTC") or ""
            cum_margin = to_float(rec.get("Cum_Margin_USDT"))
            step_margin = to_float(rec.get("Step_Margin_USDT"))
            entry_price = rec.get("Entry_Price") or ""
            side = rec.get("Side") or ""
            pnl_usd = to_float(rec.get("PNL_Realized_USDT"))
            tp_price = rec.get("TP_Price") or rec.get("TP_Pct") or ""
            # поддерживаем слежение за текущей маржой
            if ev in ("OPEN", "ADD", "RETEST_ADD"):
                # при OPEN применяем pending-депозиты
                if ev == "OPEN":
                    # применяем всем pending → deposit, затем очищаем pending
                    for u in users:
                        if u["pending"] > 0:
                            upsert_user_row(u["chat_id"], deposit=u["pending"], pending=0)
                            u["deposit"] = u["pending"]; u["pending"] = 0
                open_positions[sid] = {"cum_margin": cum_margin or (open_positions.get(sid, {}).get("cum_margin", 0.0))}
                used_pct = 100.0 * (open_positions[sid]["cum_margin"] / max(SYSTEM_BANK_USDT, 1e-9))
                if ev == "OPEN":
                    broadcast_general.append(
                        f"🎯 <b>Открыли сделку</b> ({side})\n"
                        f"Задействовано: <b>{fmt_pct(used_pct)}</b> от банка ({fmt_usd(open_positions[sid]['cum_margin'])} USDT)\n"
                        f"Цена входа: <code>{entry_price}</code>"
                    )
                else:
                    used_pct = 100.0 * (cum_margin / max(SYSTEM_BANK_USDT, 1e-9))
                    broadcast_general.append(
                        f"➕ <b>Усреднение #{max(step-1,0)}</b>\n"
                        f"Объём в сделке: <b>{fmt_pct(used_pct)}</b> от банка ({fmt_usd(cum_margin)} USDT)"
                    )

            if ev in ("TP_HIT", "SL_HIT", "MANUAL_CLOSE"):
                cm = open_positions.get(sid, {}).get("cum_margin", 0.0)
                used_pct = 100.0 * (cm / max(SYSTEM_BANK_USDT, 1e-9))
                # % прибыли от задействованной суммы:
                profit_pct_of_margin = (pnl_usd / max(cm, 1e-9)) * 100.0 if cm > 0 else 0.0
                icon = tier_emoji(profit_pct_of_margin) if pnl_usd >= 0 else "🛑"

                profit_total += pnl_usd  # копим общую прибыль для годовой

                # персонализируем финальную строку под депозит каждого
                for u in users:
                    ann_pct, ann_usd = annual_forecast(profit_total, start_utc, u["deposit"])
                    txt = (
                        f"{icon} <b>Сделка закрыта</b> ({'прибыль' if pnl_usd>=0 else 'убыток'})\n"
                        f"Задействовано: <b>{fmt_pct(used_pct)}</b> от банка ({fmt_usd(cm)} USDT)\n"
                        f"Результат: <b>{fmt_usd(pnl_usd)} USDT</b> ({fmt_pct(profit_pct_of_margin)} от задейств.)\n"
                        f"Прогноз на год для твоего депозита {fmt_usd(u['deposit'])} USDT:\n"
                        f"≈ <b>{fmt_pct(ann_pct)}</b> / <b>{fmt_usd(ann_usd)} USDT</b>"
                    )
                    personal_close_texts[u["chat_id"]] = txt

                # позицию можно забыть
                if sid in open_positions:
                    del open_positions[sid]

        # сначала общие тексты (OPEN/ADD), потом персональные CLOSE
        if broadcast_general:
            text = "\n\n".join(broadcast_general)
            for u in users:
                personal_close_texts.setdefault(u["chat_id"], "")
                personal_close_texts[u["chat_id"]] = (text + ("\n\n" + personal_close_texts[u["chat_id"]] if personal_close_texts[u["chat_id"]] else ""))

        if personal_close_texts:
            await send_all(app, personal_close_texts)

        # фиксируем новый last_row и общую прибыль
        set_state(last_row=total_rows, profit_total=profit_total)

    except Exception as e:
        log.exception("poll_and_broadcast error")

# ------------------- Main -------------------
async def post_init(app: Application):
    await app.bot.set_my_commands([
        BotCommand("start", "Показать chat_id и помощь"),
        BotCommand("help", "Команды админа"),
        BotCommand("list", "Список пользователей (админ)")
    ])

def main():
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("adduser", adduser))
    app.add_handler(CommandHandler("setdep", setdep))
    app.add_handler(CommandHandler("setname", setname))
    app.add_handler(CommandHandler("remove", remove))
    app.add_handler(CommandHandler("list", list_users))

    # Пуллер каждые 10 сек
    app.job_queue.run_repeating(lambda ctx: poll_and_broadcast(app), interval=10, first=5)

    log.info(f"{BOT_NAME} starting…")
    app.run_polling()

if __name__ == "__main__":
    main()
