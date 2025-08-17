from __future__ import annotations
import os, json, time, asyncio, logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

import gspread
from telegram import Update, constants, BotCommand
from telegram.ext import (
    Application, ApplicationBuilder, CommandHandler,
    ContextTypes, PicklePersistence
)

log = logging.getLogger("marketing_bot")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# =========================
# CONFIG
# =========================
ASSETS_DIR = os.getenv("ASSETS_DIR", "assets")
SHEET_ID   = os.getenv("SHEET_ID")
ADMIN_IDS  = {int(x.strip()) for x in os.getenv("ADMIN_IDS","").split(",") if x.strip().isdigit()}
BOT_TOKEN  = os.getenv("MARKETING_BOT_TOKEN")

# Банк по умолчанию для процента «задействовано». Админ может поменять командой /setbank
DEFAULT_BANK_USDT = float(os.getenv("BANK_USDT", "1000"))

# Сколько % от маржи — план на TP (для «рангов» картинок закрытия).
# При плече 50 и TP +1% по нотиону, комиссии ~0.1% * notional ≈ ~5% от маржи → ~45% «чистыми».
PLANNED_RETURN_ON_MARGIN_AT_TP = 45.0

# Интервал опроса Google Sheets
POLL_SEC = 5

STATE_FILE = "marketing_state.json"
WSHEET_TITLE = "BMR_DCA_Log"   # лист, куда пишет торговый бот

# =========================
# STATE (persist)
# =========================
"""
state = {
  "bank_usdt": 1000.0,
  "users": { "user_id": { "id": int, "name": str, "deposit": float,
                          "status": "active"|"pending",
                          "pending_deposit": float|None,
                          "joined_at": iso|None,
                          "chat_id": int|None } },
  "last_row_processed": 1,   # номер последней обработанной строки листа
  "pnl_totals": { "total_profit_usd": float, "first_trade_ts": float|None },
  "cohorts": { "signal_id": { "users": [{"id":..,"name":..,"deposit":..}], "bank_usdt": float, "opened_at": iso } }
}
"""
def load_state() -> Dict[str, Any]:
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {
        "bank_usdt": DEFAULT_BANK_USDT,
        "users": {},
        "last_row_processed": 1,
        "pnl_totals": { "total_profit_usd": 0.0, "first_trade_ts": None },
        "cohorts": {}
    }

def save_state(st: Dict[str, Any]) -> None:
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(st, f, ensure_ascii=False, indent=2)

# =========================
# Utils
# =========================
def is_admin(uid: int) -> bool:
    return uid in ADMIN_IDS

def fmt_money(x: Optional[float]) -> str:
    if x is None: return "—"
    return f"{x:.2f}"

def fmt_pct(x: Optional[float]) -> str:
    if x is None: return "—"
    return f"{x:.2f}%"

def choose_close_image(real_ret_margin_pct: float) -> str:
    # Ранги закрытия по доле от планового TP результата на маржу
    if real_ret_margin_pct <= 0:
        return "sl.png"
    ratio = real_ret_margin_pct / PLANNED_RETURN_ON_MARGIN_AT_TP  # 1.0 = 100% планового
    if 0.50 <= ratio < 0.60: return "tp_50.png"
    if 0.70 <= ratio < 0.80: return "tp_70.png"
    if 0.80 <= ratio < 0.90: return "tp_80.png"
    if 0.90 <= ratio < 1.00: return "tp_90.png"
    return "tp_100.png"

def image_for_event(event: str, step_no: Optional[int]=None, retest: bool=False, ret_margin_pct: Optional[float]=None) -> str:
    if event == "OPEN":      return "open.png"
    if event == "ADD":
        idx = max(1, min(4, (step_no or 1)))  # 1..4
        return f"dca{idx}.png"
    if event == "RETEST_ADD":
        return "reserve.png"
    if event in ("TP_HIT", "MANUAL_CLOSE", "SL_HIT"):
        return choose_close_image(ret_margin_pct or 0.0) if event != "SL_HIT" else "sl.png"
    return "open.png"

def annualized(st: Dict[str, Any]) -> (float, float):
    """Вернёт (A_percent, B_usd) = годовые на весь банк и в $."""
    totals = st["pnl_totals"]
    bank   = st["bank_usdt"]
    if not totals["first_trade_ts"] or bank <= 0:
        return (0.0, 0.0)
    days = max(1e-9, (time.time() - float(totals["first_trade_ts"])) / 86400.0)
    a_pct = (totals["total_profit_usd"] / days * 365.0) / bank * 100.0
    b_usd = bank * a_pct / 100.0
    return (a_pct, b_usd)

async def send_picture(app: Application, chat_id: int, image_name: str, caption: str):
    path = os.path.join(ASSETS_DIR, image_name)
    try:
        with open(path, "rb") as f:
            await app.bot.send_photo(chat_id=chat_id, photo=f, caption=caption, parse_mode=constants.ParseMode.HTML)
    except FileNotFoundError:
        # если нет картинки — просто текст
        await app.bot.send_message(chat_id=chat_id, text=caption, parse_mode=constants.ParseMode.HTML)

# =========================
# Google Sheets
# =========================
def open_sheet():
    creds = os.environ.get("GOOGLE_CREDENTIALS")
    if not creds:
        raise RuntimeError("GOOGLE_CREDENTIALS env var is not set")
    gc = gspread.service_account_from_dict(json.loads(creds))
    sh = gc.open_by_key(SHEET_ID)
    ws = sh.worksheet(WSHEET_TITLE)
    return ws

def read_new_rows(ws, start_row: int) -> (List[Dict[str, Any]], int):
    """
    Возвращает (список событий, последний_обработанный_ряд).
    Ожидаем, что первая строка — заголовки.
    """
    values = ws.get_all_values()
    if len(values) <= start_row:
        return [], start_row
    headers = values[0]
    rows = []
    for r in range(start_row, len(values)):
        row = values[r]
        if not any(row):  # пустая строка
            continue
        ev = {headers[i]: row[i] if i < len(row) else "" for i in range(len(headers))}
        rows.append(ev)
    return rows, len(values)

# =========================
# Event processing
# =========================
def parse_float(x: Any) -> Optional[float]:
    try:
        if x == "" or x is None: return None
        return float(x)
    except:
        return None

async def handle_event(app: Application, st: Dict[str,Any], ev: Dict[str,Any]):
    """
    Ожидаемые поля в шите (из вашего торгового бота):
      Event, Signal_ID, Step_No, Cum_Margin_USDT, PNL_Realized_USDT, Entry_Price, Avg_Price, TP_Price, SL_Price, ...
    """
    etype = ev.get("Event") or ev.get("event") or ""
    signal_id = ev.get("Signal_ID") or ev.get("signal_id") or "UNKNOWN"
    step_no = int(parse_float(ev.get("Step_No")) or 0)
    cum_margin = parse_float(ev.get("Cum_Margin_USDT")) or 0.0
    pnl_usd = parse_float(ev.get("PNL_Realized_USDT"))  # только на закрытии
    timestamp_iso = ev.get("Timestamp_UTC") or datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    bank = float(st["bank_usdt"])

    # ====== OPEN: применяем pending, фиксируем когорту ======
    if etype == "OPEN":
        # применить pending → active
        for u in st["users"].values():
            if u.get("status") == "pending":
                if u.get("pending_deposit") is not None:
                    u["deposit"] = float(u["pending_deposit"])
                    u["pending_deposit"] = None
                u["status"] = "active"
                if not u.get("joined_at"):
                    u["joined_at"] = timestamp_iso

        cohort_users = [
            {"id": u["id"], "name": u.get("name","—"), "deposit": float(u.get("deposit",0))}
            for u in st["users"].values() if u.get("status") == "active" and u.get("deposit",0) > 0 and u.get("chat_id")
        ]
        st["cohorts"][signal_id] = {
            "users": cohort_users,
            "bank_usdt": bank,
            "opened_at": timestamp_iso
        }
        save_state(st)

        x_pct = (cum_margin / bank * 100.0) if bank > 0 else 0.0
        caption = (f"📈 <b>Открыта новая сделка</b>\n"
                   f"Задействовано: <b>{fmt_pct(x_pct)}</b> банка")
        # рассылаем всем активным подписчикам из users (а не только когорте, это — публичное открытие)
        for u in st["users"].values():
            if u.get("chat_id"):
                await send_picture(app, u["chat_id"], image_for_event("OPEN"), caption)
        return

    # ====== ADD / RETEST_ADD: показываем текущую долю задействованного ======
    if etype in ("ADD", "RETEST_ADD"):
        x_pct = (cum_margin / bank * 100.0) if bank > 0 else 0.0
        title = "↗️ Докупка" if etype == "ADD" else "↩️ Ретест-докупка"
        img = image_for_event(etype, step_no=step_no, retest=(etype=="RETEST_ADD"))
        caption = (f"{title}\nВ позиции теперь: <b>{fmt_pct(x_pct)}</b> банка")
        # только текущая когорта этой сделки
        cohort = st["cohorts"].get(signal_id, {})
        for u in cohort.get("users", []):
            uid = u["id"]
            chat_id = st["users"].get(str(uid)) and st["users"][str(uid)].get("chat_id")
            if chat_id:
                await send_picture(app, chat_id, img, caption)
        return

    # ====== Закрытие: TP_HIT / SL_HIT / MANUAL_CLOSE ======
    if etype in ("TP_HIT", "SL_HIT", "MANUAL_CLOSE"):
        # обновим агрегаты для годовых
        if pnl_usd is not None:
            if not st["pnl_totals"]["first_trade_ts"]:
                st["pnl_totals"]["first_trade_ts"] = time.time()
            st["pnl_totals"]["total_profit_usd"] += pnl_usd
            save_state(st)

        # «доход от задействованной суммы»:
        y_pct = (pnl_usd / cum_margin * 100.0) if (pnl_usd is not None and cum_margin > 0) else 0.0
        a_pct, b_usd = annualized(st)

        img = image_for_event(etype, ret_margin_pct=y_pct)
        title = "✅ Закрытие по TP" if etype == "TP_HIT" else ("🧰 Ручное закрытие" if etype=="MANUAL_CLOSE" else "❌ Закрытие по SL")
        caption = (f"{title}\n"
                   f"Использовано: <b>{fmt_pct(cum_margin / bank * 100.0 if bank>0 else 0.0)}</b> банка\n"
                   f"Профит от задействованной суммы: <b>{fmt_pct(y_pct)}</b>\n"
                   f"Прогноз годовых на банк: <b>{fmt_pct(a_pct)}</b> (≈ <b>${fmt_money(b_usd)}</b>)")

        cohort = st["cohorts"].get(signal_id, {})
        for u in cohort.get("users", []):
            uid = u["id"]
            chat_id = st["users"].get(str(uid)) and st["users"][str(uid)].get("chat_id")
            if chat_id:
                await send_picture(app, chat_id, img, caption)
        return

# =========================
# Poller
# =========================
async def poll_loop(app: Application):
    st = app.bot_data["state"]
    try:
        ws = open_sheet()
    except Exception as e:
        log.error(f"Sheet open error: {e}")
        return

    while True:
        try:
            start = max(1, int(st.get("last_row_processed", 1)))
            rows, last = read_new_rows(ws, start)
            if rows:
                for ev in rows:
                    await handle_event(app, st, ev)
                st["last_row_processed"] = last
                save_state(st)
            await asyncio.sleep(POLL_SEC)
        except Exception:
            log.exception("Poll error")
            await asyncio.sleep(POLL_SEC)

# =========================
# Commands
# =========================
async def post_init(app: Application):
    app.bot_data["state"] = load_state()
    await app.bot.set_my_commands([
        BotCommand("start", "Подписаться на уведомления"),
        BotCommand("balance", "Показать мой баланс"),
        # Админские:
        BotCommand("adduser", "Админ: добавить/изменить пользователя"),
        BotCommand("setdeposit", "Админ: изменить депозит"),
        BotCommand("rename", "Админ: переименовать пользователя"),
        BotCommand("users", "Админ: список пользователей"),
        BotCommand("setbank", "Админ: банк (для процентов)"),
        BotCommand("broadcast", "Админ: отправить объявление")
    ])
    asyncio.create_task(poll_loop(app))

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    st = ctx.bot_data["state"]
    uid = update.effective_user.id
    chat_id = update.effective_chat.id
    u = st["users"].get(str(uid))
    if not u:
        # пользователь есть, но депозит/имя задаёт админ — пока pending без депозита
        st["users"][str(uid)] = {
            "id": uid, "name": update.effective_user.full_name or f"U{uid}",
            "deposit": 0.0, "status": "pending", "pending_deposit": None,
            "joined_at": None, "chat_id": chat_id
        }
    else:
        u["chat_id"] = chat_id
        if not u.get("name"):
            u["name"] = update.effective_user.full_name or f"U{uid}"
    save_state(st)
    await update.message.reply_text("Готово! Вы подписаны на уведомления. Ваш депозит и имя задаёт администратор.")

async def cmd_balance(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    st = ctx.bot_data["state"]
    uid = update.effective_user.id
    u = st["users"].get(str(uid))
    if not u:
        await update.message.reply_text("Вы ещё не зарегистрированы. Нажмите /start.")
        return
    bank = st["bank_usdt"]
    # Виртуальный профит пропорционален банку: profit_per_1usd = total_profit / bank
    total = st["pnl_totals"]["total_profit_usd"]
    profit_per_1usd = (total / bank) if bank > 0 else 0.0
    user_profit = profit_per_1usd * float(u.get("deposit", 0.0))
    await update.message.reply_text(
        f"👤 <b>{u.get('name','—')}</b>\n"
        f"Депозит: <b>${fmt_money(u.get('deposit',0))}</b>\n"
        f"Накоплено профита: <b>${fmt_money(user_profit)}</b>\n"
        f"Итог: <b>${fmt_money(float(u.get('deposit',0))+user_profit)}</b>",
        parse_mode=constants.ParseMode.HTML
    )

# ---- ADMIN ----
async def cmd_setbank(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    try:
        val = float(ctx.args[0])
        st = ctx.bot_data["state"]
        st["bank_usdt"] = val
        save_state(st)
        await update.message.reply_text(f"✅ Банк установлен: ${val:.2f}")
    except:
        await update.message.reply_text("Использование: /setbank 1000")

async def cmd_adduser(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    try:
        uid = int(ctx.args[0]); name = ctx.args[1]; deposit = float(ctx.args[2])
    except:
        await update.message.reply_text("Использование: /adduser <id> <имя> <депозит>")
        return
    st = ctx.bot_data["state"]
    u = st["users"].get(str(uid))
    if not u:
        st["users"][str(uid)] = {
            "id": uid, "name": name, "deposit": 0.0,
            "status": "pending", "pending_deposit": deposit,
            "joined_at": None, "chat_id": None
        }
    else:
        u["name"] = name or u.get("name")
        u["pending_deposit"] = deposit
        u["status"] = "pending"
    save_state(st)
    await update.message.reply_text(f"✅ Пользователь {name} ({uid}) добавлен/обновлён. Вступит в силу на следующем OPEN.")

async def cmd_setdeposit(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    try:
        uid = int(ctx.args[0]); deposit = float(ctx.args[1])
    except:
        await update.message.reply_text("Использование: /setdeposit <id> <депозит>")
        return
    st = ctx.bot_data["state"]
    u = st["users"].get(str(uid))
    if not u:
        await update.message.reply_text("Не найден пользователь. Сначала /adduser.")
        return
    u["pending_deposit"] = deposit
    u["status"] = "pending"
    save_state(st)
    await update.message.reply_text(f"✅ Новый депозит ${deposit:.2f} применится на следующем OPEN.")

async def cmd_rename(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    try:
        uid = int(ctx.args[0]); name = " ".join(ctx.args[1:])
    except:
        await update.message.reply_text("Использование: /rename <id> <имя>")
        return
    st = ctx.bot_data["state"]
    u = st["users"].get(str(uid))
    if not u:
        await update.message.reply_text("Не найден пользователь. Сначала /adduser.")
        return
    u["name"] = name
    save_state(st)
    await update.message.reply_text(f"✅ Имя обновлено: {name}")

async def cmd_users(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    st = ctx.bot_data["state"]
    act = [u for u in st["users"].values() if u.get("status")=="active"]
    pend= [u for u in st["users"].values() if u.get("status")=="pending"]
    def line(u):
        pd = u.get("pending_deposit")
        pd_txt = f" → pending {pd:.2f}" if pd is not None else ""
        return f"{u['id']} • {u.get('name','—')} • dep=${u.get('deposit',0):.2f}{pd_txt}"
    txt = "👥 <b>Пользователи</b>\n\n<b>Активные:</b>\n" + \
          ("\n".join(line(u) for u in act) if act else "—") + \
          "\n\n<b>Pending:</b>\n" + \
          ("\n".join(line(u) for u in pend) if pend else "—")
    await update.message.reply_text(txt, parse_mode=constants.ParseMode.HTML)

async def cmd_broadcast(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    st = ctx.bot_data["state"]
    msg = " ".join(ctx.args)
    if not msg:
        await update.message.reply_text("Использование: /broadcast <текст>")
        return
    for u in st["users"].values():
        if u.get("chat_id"):
            try:
                await ctx.bot.send_message(chat_id=u["chat_id"], text=msg)
            except Exception:
                pass
    await update.message.reply_text("✅ Отправлено.")

# =========================
# Main
# =========================
def main():
    if not BOT_TOKEN:
        raise RuntimeError("MARKETING_BOT_TOKEN env var is not set")
    persistence = PicklePersistence(filepath="marketing_persist")
    app = ApplicationBuilder().token(BOT_TOKEN).post_init(post_init).persistence(persistence).build()
    app.add_handler(CommandHandler("start",    cmd_start))
    app.add_handler(CommandHandler("balance",  cmd_balance))
    app.add_handler(CommandHandler("adduser",  cmd_adduser))
    app.add_handler(CommandHandler("setdeposit", cmd_setdeposit))
    app.add_handler(CommandHandler("rename",   cmd_rename))
    app.add_handler(CommandHandler("users",    cmd_users))
    app.add_handler(CommandHandler("setbank",  cmd_setbank))
    app.add_handler(CommandHandler("broadcast", cmd_broadcast))
    log.info("STRIGI_KAPUSTU_BOT starting…")
    app.run_polling()

if __name__ == "__main__":
    main()
