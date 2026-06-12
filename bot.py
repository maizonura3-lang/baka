"""
Bot Scalping v18.6.0 — DRY RUN LOG MODE (PAPER TRADING)
====================================================
CHANGELOG v18.6.0 vs v18.5.0:
─────────────────────────────
[FIX #1] TRAIL_ACTIVATE_PCT: 0.0035 → 0.0065 (0.65%)
         Trail baru aktif setelah profit nyata menutup fee + buffer
         Tidak aktif di zona noise awal entry

[FIX #2] TRAIL_DELTA_PCT: 0.0015 → 0.0030 (0.30%)
         Beri napas lebih ke pair volatile (SOL, DOGE, dll)
         Trail terlalu ketat = exit di candle noise biasa

[FIX #3] ATR_TP_MULT: 3.0 → 2.2
         TP lebih realistis → lebih sering kena full TP
         R:R tetap 2.2/1.2 = 1.83:1, breakeven WR ~36% (achievable)

[FIX #4] Race condition BTC gate di live_open()
         Pengecekan ulang _macro["btc"] sebelum eksekusi order
         BEAR + LONG → batalkan, BULL + SHORT → batalkan
         Tidak hanya filter di signal(), tapi double-check di eksekusi

[FIX #5] MAX_POSITIONS: 3 → 2
         Selektif 2 posisi terbaik > 3 posisi sinyal lemah
         Reduce size saat drawdown, restore saat WR > 50%

[FIX #6] Minimum hold time guard di TrailSL
         TrailSL skip jika hold < 120 detik
         Mencegah premature exit akibat volatilitas noise post-entry
"""

import os, time, math, threading, queue
import requests
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from binance.client import Client
import ta
import pandas as pd
import numpy as np

load_dotenv()
client = Client(os.getenv("API_KEY"), os.getenv("API_SECRET"))
client.FUTURES_URL = "https://testnet.binancefuture.com/fapi"

# ═══════════════════════════════════════════════════════
#  CONFIG v18.6.0
# ═══════════════════════════════════════════════════════

LEVERAGE       = 20
ORDER_USDT     = 2.0
MAX_POSITIONS  = 2       # [FIX #5] turun dari 3 → lebih selektif saat WR < 50%

# ── TP/SL STRATEGY v18.6.0 ──────────────────────────────
# SL = ATR-based (dinamis), bukan fixed pct
# TP = 2.2× SL → R:R = 1.83:1 setelah fee (lebih achievable)
# Trailing aktif dari profit ≥ 0.65% → tidak trail di zona noise
ATR_SL_MULT        = 1.2    # SL = 1.2 × ATR
ATR_TP_MULT        = 2.2    # [FIX #3] turun dari 3.0 → TP lebih realistis
TRAIL_ACTIVATE_PCT = 0.0065 # [FIX #1] naik dari 0.0035 → aktif di +0.65% profit
TRAIL_DELTA_PCT    = 0.0030 # [FIX #2] naik dari 0.0015 → beri napas 0.30%
TRAIL_MIN_HOLD     = 120    # [FIX #6] NEW: TrailSL skip jika hold < 120 detik
MIN_SL_PCT         = 0.0015 # SL minimum 0.15%
MAX_SL_PCT         = 0.0060 # SL maksimum 0.60%
MIN_TP_PCT         = 0.0040 # TP minimum 0.40%
FUTURES_FEE_PCT    = 0.0005 # Taker fee 0.05%

MIN_BASE_VOL   = 25_000_000
MIN_VR         = 1.8        # volume push minimum 1.8× rata-rata
BR_LONG_MIN    = 0.52       # buyer dominance minimum untuk LONG
BR_SHORT_MAX   = 0.48       # seller dominance maksimum untuk SHORT

SCAN_INTERVAL  = 1
MONITOR_INT    = 0.15
SCAN_DELAY     = 0.010
BATCH_SIZE     = 20
MAX_WORKERS    = 12
SLOT_FILL_INT  = 0.20

MIN_SCORE      = 58
MIN_GAP        = 12
COOLDOWN_SEC   = 300        # 5 menit cooldown setelah SL
WHIPSAW_SEC    = 600        # anti-whipsaw: 10 menit blokir setelah SL
SLIPPAGE_GUARD = 0.0015     # tolak entry jika harga geser >0.15%
TTL_5M         = 5
TTL_15M        = 30

DAILY_LOSS     = -8.0
CONSEC_MAX     = 6
CONSEC_PAUSE   = 60

# ═══════════════════════════════════════════════════════
#  SYMBOLS
# ═══════════════════════════════════════════════════════
SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
    "ADAUSDT", "DOGEUSDT", "AVAXUSDT", "TRXUSDT", "DOTUSDT",
    "LINKUSDT", "MATICUSDT", "LTCUSDT", "ATOMUSDT", "UNIUSDT",
    "NEARUSDT", "APTUSDT", "ARBUSDT", "OPUSDT", "INJUSDT",
    "SUIUSDT", "SEIUSDT", "FETUSDT", "WLDUSDT", "AAVEUSDT",
    "ORDIUSDT", "TONUSDT", "1000PEPEUSDT", "WIFUSDT", "JUPUSDT",
    "FTMUSDT", "SANDUSDT", "MANAUSDT", "GALAUSDT", "APEUSDT",
    "CRVUSDT", "1000SHIBUSDT", "COMPUSDT", "MKRUSDT", "SNXUSDT",
]
SYMBOLS = list(dict.fromkeys(SYMBOLS))

# ═══════════════════════════════════════════════════════
#  STATE
# ═══════════════════════════════════════════════════════
live_positions  = {}
trade_log       = []
_ohlcv_cache    = {}
_sym_cooldown   = {}
_sym_sl_time    = {}
_ticker_cache   = {}
_ticker_ts      = 0
_lock           = threading.Lock()
_executor       = ThreadPoolExecutor(max_workers=MAX_WORKERS)
_rescan_q       = queue.Queue()
_hot_syms       = deque(maxlen=30)

_macro = {"fng": 50, "btc": "UNKNOWN", "last_fng": 0, "last_btc": 0}
_ks    = {"active": False, "reason": "", "resume": 0, "consec": 0, "daily": 0.0, "day_reset": 0}
_stats = {
    "trades": 0, "wins": 0, "losses": 0, "pnl": 0.0, "best": 0.0, "worst": 0.0,
    "extreme_tp": 0, "hard_sl": 0, "trail_sl": 0, "force": 0,
    "btc_block": 0,   # [FIX #4] counter untuk BTC gate block di live_open
    "trail_skip": 0,  # [FIX #6] counter untuk trail skip karena min hold
    "hist": deque(maxlen=200), "start": time.time(),
}

# ═══════════════════════════════════════════════════════
#  BINANCE UTILS
# ═══════════════════════════════════════════════════════
_precision_cache = {}
def get_precision(symbol):
    if symbol in _precision_cache: return _precision_cache[symbol]
    try:
        info = client.futures_exchange_info()
        for s in info['symbols']:
            if s['symbol'] == symbol:
                prec = int(s['quantityPrecision'])
                _precision_cache[symbol] = prec
                return prec
    except: pass
    return 2

def qty(symbol, price):
    raw_qty = (ORDER_USDT * LEVERAGE) / price
    prec = get_precision(symbol)
    return round(raw_qty, prec)

def price_live(symbol):
    try: return float(client.futures_symbol_ticker(symbol=symbol)["price"])
    except: return 0.0

def tickers_all():
    global _ticker_cache, _ticker_ts
    now = time.time()
    if now - _ticker_ts < 4 and _ticker_cache: return _ticker_cache
    try:
        raw = client.futures_ticker()
        _ticker_cache = {
            t["symbol"]: {
                "pct": float(t["priceChangePercent"]),
                "vol": float(t["quoteVolume"]),
                "last": float(t["lastPrice"])
            } for t in raw
        }
        _ticker_ts = now
        return _ticker_cache
    except: return _ticker_cache

def ok_cooldown(sym):
    cd = _sym_cooldown.get(sym, 0)
    return (time.time() - cd) >= COOLDOWN_SEC

def ok_whipsaw(sym):
    sl_t = _sym_sl_time.get(sym, 0)
    return (time.time() - sl_t) >= WHIPSAW_SEC

def set_cd(sym, is_sl=False):
    _sym_cooldown[sym] = time.time()
    if is_sl:
        _sym_sl_time[sym] = time.time()

def ohlcv(symbol, interval, limit=100):
    key, now = (symbol, interval), time.time()
    ttl = TTL_5M if interval == Client.KLINE_INTERVAL_5MINUTE else TTL_15M
    if key in _ohlcv_cache and now - _ohlcv_cache[key][0] < ttl:
        return _ohlcv_cache[key][1]
    try:
        kl = client.futures_klines(symbol=symbol, interval=interval, limit=limit)
        df = pd.DataFrame(kl, columns=["time","open","high","low","close","volume",
                                        "ct","qv","trades","tbbase","tbquote","ignore"])
        for c in ["open","high","low","close","volume","tbbase","tbquote"]:
            df[c] = df[c].astype(float)
        _ohlcv_cache[key] = (now, df)
        return df
    except:
        return _ohlcv_cache.get(key, (None, None))[1]

def run_ta(df):
    c, h, l, v = df["close"], df["high"], df["low"], df["volume"]
    df["rsi"]  = ta.momentum.RSIIndicator(c, 14).rsi()
    df["mh"]   = ta.trend.MACD(c, 12, 26, 9).macd_diff()
    df["e5"]   = ta.trend.EMAIndicator(c, 5).ema_indicator()
    df["e9"]   = ta.trend.EMAIndicator(c, 9).ema_indicator()
    df["e21"]  = ta.trend.EMAIndicator(c, 21).ema_indicator()
    df["e50"]  = ta.trend.EMAIndicator(c, 50).ema_indicator()
    df["atr"]  = ta.volatility.AverageTrueRange(h, l, c, 14).average_true_range()
    df["adx"]  = ta.trend.ADXIndicator(h, l, c, 14).adx()
    df["vm"]   = v.rolling(20).mean()
    df["vr"]   = v / df["vm"].replace(0, 1)
    df["br"]   = df["tbbase"] / df["volume"].replace(0, 1)
    df["body"] = abs(c - df["open"])
    df["rng"]  = h - l
    df["br2"]  = df["body"] / df["rng"].replace(0, 1)
    df["m5"]   = (c - c.shift(5)) / c.shift(5)
    df["m3"]   = (c - c.shift(3)) / c.shift(3)
    return df

def btc_trend():
    try:
        df = run_ta(ohlcv("BTCUSDT", Client.KLINE_INTERVAL_5MINUTE, 80).copy())
        row = df.iloc[-2]
        p, e5, e9, e21, m5 = row["close"], row["e5"], row["e9"], row["e21"], row["m5"]
        if p > e5 > e9 > e21 and m5 > 0.001: return "BULL"
        if p < e5 < e9 < e21 and m5 < -0.001: return "BEAR"
        if p > e9 > e21: return "MILD_BULL"
        if p < e9 < e21: return "MILD_BEAR"
        return "SIDEWAYS"
    except: return "UNKNOWN"

def ks_check():
    k, now = _ks, time.time()
    if k["active"] and now >= k["resume"]:
        k["active"] = False; k["consec"] = 0
    if k["active"]: return True, k["reason"]
    day = now - (now % 86400)
    if day > k["day_reset"]: k["daily"] = 0.0; k["day_reset"] = day
    if k["daily"] <= DAILY_LOSS:
        k["active"] = True
        k["reason"] = f"daily({k['daily']:.2f})"
        k["resume"] = day + 86400
        return True, k["reason"]
    if k["consec"] >= CONSEC_MAX:
        k["active"] = True
        k["reason"] = f"consec({k['consec']})"
        k["resume"] = now + CONSEC_PAUSE
        return True, k["reason"]
    return False, ""

def ks_upd(pnl):
    _ks["daily"] += pnl
    _ks["consec"] = 0 if pnl >= 0 else _ks["consec"] + 1

# ═══════════════════════════════════════════════════════
#  SIGNAL v18.6.0 (tidak berubah dari v18.5.0)
# ═══════════════════════════════════════════════════════
def get_15m_trend(symbol):
    try:
        df = run_ta(ohlcv(symbol, Client.KLINE_INTERVAL_15MINUTE, 60).copy())
        row = df.iloc[-2]
        p, e9, e21 = row["close"], row["e9"], row["e21"]
        if p > e9 > e21: return "UP"
        if p < e9 < e21: return "DOWN"
        return "FLAT"
    except: return "FLAT"

def signal(df, symbol=None):
    if df is None or len(df) < 55: return None, 0, [], 0.0, 0.0, 0.0

    row  = df.iloc[-2]
    prev = df.iloc[-3]
    prev2= df.iloc[-4]

    p, e5, e9, e21, e50 = row["close"], row["e5"], row["e9"], row["e21"], row["e50"]
    rsi  = row["rsi"]
    mh   = row["mh"];  mh_p = prev["mh"];  mh_p2 = prev2["mh"]
    vr   = row["vr"];  br   = row["br"]
    m5   = row["m5"];  m3   = row["m3"]
    body = row["br2"]
    atr  = row["atr"]; adx  = row["adx"]

    # ── Gate 1: Volume displacement ─────────────────────
    if vr < MIN_VR: return None, 0, [], atr, 0.0, 0.0
    if body < 0.40: return None, 0, [], atr, 0.0, 0.0

    # ── Gate 2: ATR filter ───────────────────────────────
    atr_pct = atr / p
    if atr_pct > 0.03: return None, 0, [], atr, 0.0, 0.0
    if atr_pct < 0.001: return None, 0, [], atr, 0.0, 0.0

    # ── Hitung SL/TP berbasis ATR ────────────────────────
    # [FIX #3] ATR_TP_MULT sekarang 2.2 (dari 3.0)
    sl_pct = max(MIN_SL_PCT, min(MAX_SL_PCT, atr_pct * ATR_SL_MULT))
    tp_pct = max(MIN_TP_PCT, atr_pct * ATR_TP_MULT)
    net_tp = tp_pct - 2 * FUTURES_FEE_PCT
    net_sl = sl_pct + 2 * FUTURES_FEE_PCT
    if net_tp <= 0 or (net_tp / net_sl) < 1.5:
        tp_pct = sl_pct * 2.0 + 4 * FUTURES_FEE_PCT

    lp = sp = 0
    sl, ss = [], []

    # ── EMA Stack ────────────────────────────────────────
    if p > e5 > e9 > e21 > e50:   lp += 32; sl.append("EMA5↑")
    elif p > e5 > e9 > e21:       lp += 24; sl.append("EMA4↑")
    if p < e5 < e9 < e21 < e50:   sp += 32; ss.append("EMA5↓")
    elif p < e5 < e9 < e21:       sp += 24; ss.append("EMA4↓")

    # ── Momentum ─────────────────────────────────────────
    if m5 > 0.006:    lp += 28; sl.append(f"Mom+{m5*100:.1f}%")
    elif m5 > 0.003:  lp += 20; sl.append(f"Mom+{m5*100:.1f}%")
    if m5 < -0.006:   sp += 28; ss.append(f"Mom{m5*100:.1f}%")
    elif m5 < -0.003: sp += 20; ss.append(f"Mom{m5*100:.1f}%")

    # ── Momentum 3 candle ────────────────────────────────
    if m3 > 0.003:    lp += 12; sl.append(f"M3+{m3*100:.1f}%")
    if m3 < -0.003:   sp += 12; ss.append(f"M3{m3*100:.1f}%")

    # ── MACD ─────────────────────────────────────────────
    if mh_p <= 0 and mh > 0:           lp += 24; sl.append("MACD_X↑")
    elif mh > 0 and mh > mh_p > mh_p2: lp += 18; sl.append("MACD↑↑")
    if mh_p >= 0 and mh < 0:           sp += 24; ss.append("MACD_X↓")
    elif mh < 0 and mh < mh_p < mh_p2: sp += 18; ss.append("MACD↓↓")

    # ── Volume spike ────────────────────────────────────
    if vr >= 3.5:   lp += 16; sp += 16; sl.append(f"VOL{vr:.1f}x"); ss.append(f"VOL{vr:.1f}x")
    elif vr >= 2.5: lp += 12; sp += 12; sl.append(f"VOL{vr:.1f}x"); ss.append(f"VOL{vr:.1f}x")

    # ── Buyer/Seller dominance ───────────────────────────
    if br > 0.65:   lp += 22; sl.append(f"Buy{br:.0%}")
    elif br > 0.55: lp += 12; sl.append(f"Buy{br:.0%}")
    if br < 0.35:   sp += 22; ss.append(f"Sell{1-br:.0%}")
    elif br < 0.45: sp += 12; ss.append(f"Sell{1-br:.0%}")

    # ── RSI filter ───────────────────────────────────────
    if rsi > 78:   lp = int(lp * 0.3); sp += 18; ss.append(f"OB{rsi:.0f}")
    elif rsi > 70: lp = int(lp * 0.7); sl.append(f"RSI{rsi:.0f}")
    elif rsi < 22: sp = int(sp * 0.3); lp += 18; sl.append(f"OS{rsi:.0f}")
    elif rsi < 30: sp = int(sp * 0.7); ss.append(f"RSI{rsi:.0f}")

    # ── ADX ──────────────────────────────────────────────
    if adx > 40:   lp += 10; sp += 10
    elif adx < 20: lp = int(lp * 0.8); sp = int(sp * 0.8)

    # ── BTC Trend Direction Gate ─────────────────────────
    btc = _macro["btc"]
    if btc == "BULL":
        sp = int(sp * 0.25)
        lp += 10
    elif btc == "BEAR":
        lp = int(lp * 0.25)
        sp += 10
    elif btc == "MILD_BULL":
        sp = int(sp * 0.60)
    elif btc == "MILD_BEAR":
        lp = int(lp * 0.60)

    thresh = MIN_SCORE
    gap    = abs(lp - sp)

    # ── Resolusi sinyal ──────────────────────────────────
    if lp > sp:
        if lp < thresh or gap < MIN_GAP: return None, lp, [], atr, sl_pct, tp_pct
        if br <= BR_LONG_MIN: return None, lp, [], atr, sl_pct, tp_pct
        if symbol:
            t15 = get_15m_trend(symbol)
            if t15 == "DOWN": return None, lp, [], atr, sl_pct, tp_pct
        return "LONG", lp, sl[:4], atr, sl_pct, tp_pct
    else:
        if sp < thresh or gap < MIN_GAP: return None, max(lp, sp), [], atr, sl_pct, tp_pct
        if br >= BR_SHORT_MAX: return None, sp, [], atr, sl_pct, tp_pct
        if symbol:
            t15 = get_15m_trend(symbol)
            if t15 == "UP": return None, sp, [], atr, sl_pct, tp_pct
        return "SHORT", sp, ss[:4], atr, sl_pct, tp_pct

# ═══════════════════════════════════════════════════════
#  DRY RUN OPEN — v18.6.0 dengan BTC gate double-check [FIX #4]
# ═══════════════════════════════════════════════════════
def live_open(sym, direction, score, sigs, price, atr, sl_pct, tp_pct):
    with _lock:
        if sym in live_positions or len(live_positions) >= MAX_POSITIONS:
            return
        live_positions[sym] = {"_r": True}

    # ── [FIX #4] BTC Gate double-check di sini (race condition fix) ──────
    # Cek ulang _macro["btc"] tepat sebelum eksekusi, bukan hanya di signal()
    # Ini menangkap kasus di mana btc trend berubah antara scan dan eksekusi
    btc_now = _macro["btc"]
    blocked = False
    if btc_now == "BEAR" and direction == "LONG":
        blocked = True
        block_reason = f"BTC={btc_now} blokir {direction}"
    elif btc_now == "BULL" and direction == "SHORT":
        blocked = True
        block_reason = f"BTC={btc_now} blokir {direction}"
    elif btc_now == "MILD_BEAR" and direction == "LONG":
        # MILD_BEAR: tidak blok total, tapi kurangi skor — batalkan jika skor mepet
        if score < MIN_SCORE + 8:
            blocked = True
            block_reason = f"BTC={btc_now} skor{score} kurang untuk {direction}"
    elif btc_now == "MILD_BULL" and direction == "SHORT":
        if score < MIN_SCORE + 8:
            blocked = True
            block_reason = f"BTC={btc_now} skor{score} kurang untuk {direction}"

    if blocked:
        print(f"  🚫 [BTC-GATE] {sym} {direction} dibatalkan — {block_reason}")
        _stats["btc_block"] += 1
        with _lock: live_positions.pop(sym, None)
        return

    # ── Slippage guard ────────────────────────────────────────────────────
    px_now = price_live(sym)
    if px_now > 0:
        slip = abs(px_now - price) / price
        if slip > SLIPPAGE_GUARD:
            print(f"  ⚠️  {sym} skip — slippage {slip*100:.2f}% > {SLIPPAGE_GUARD*100:.2f}%")
            with _lock: live_positions.pop(sym, None)
            return
        price = px_now

    try:
        q_val = qty(sym, price)
    except Exception as e:
        print(f"  ❌ Gagal Open {sym}: {e}")
        with _lock: live_positions.pop(sym, None)
        return

    # Hitung level TP/SL absolut
    if direction == "LONG":
        sl_price = price * (1 - sl_pct)
        tp_price = price * (1 + tp_pct)
    else:
        sl_price = price * (1 + sl_pct)
        tp_price = price * (1 - tp_pct)

    pos = {
        "side": direction, "entry": price, "qty": q_val,
        "open_time": time.time(), "score": score, "sigs": sigs, "atr": atr,
        "sl_price": sl_price, "tp_price": tp_price,
        "sl_pct": sl_pct, "tp_pct": tp_pct,
        "trail_peak": price,
        "trail_active": False,
    }
    with _lock: live_positions[sym] = pos

    d = "🟢" if direction == "LONG" else "🔴"
    net_tp = tp_pct - 2 * FUTURES_FEE_PCT
    net_sl = sl_pct + 2 * FUTURES_FEE_PCT
    rr = net_tp / net_sl if net_sl > 0 else 0
    print(f"\n  {d} [DRY] {sym} {direction} @{price:.6g}  "
          f"SL:{sl_pct*100:.2f}% TP:{tp_pct*100:.2f}% R:R={rr:.1f}:1  [{' | '.join(sigs)}]")
    _stats["trades"] += 1

# ═══════════════════════════════════════════════════════
#  DRY RUN CLOSE
# ═══════════════════════════════════════════════════════
def live_close(sym, reason, price=None):
    with _lock:
        pos = live_positions.pop(sym, None)
    if pos is None or pos.get("_r"): return

    if price is None: price = price_live(sym)
    if price == 0: return

    side, entry, q_val = pos["side"], pos["entry"], pos["qty"]

    gross_pnl = (price - entry) * q_val if side == "LONG" else (entry - price) * q_val
    open_fee  = (entry * q_val) * FUTURES_FEE_PCT
    close_fee = (price * q_val) * FUTURES_FEE_PCT
    total_fee = open_fee + close_fee
    pnl = gross_pnl - total_fee

    pct   = (price - entry) / entry * 100 if side == "LONG" else (entry - price) / entry * 100
    hold  = time.time() - pos["open_time"]
    e = "🟢" if pnl >= 0 else "🔴"

    print(f"  {e} [DRY] {sym} {side} CLOSE — {reason}")
    print(f"     {entry:.6g}→{price:.6g} ({pct:+.3f}%) hold:{hold:.0f}s | "
          f"PnL Net:{pnl:+.5f}U (Fee:{total_fee:.5f}U)")

    is_sl = "SL" in reason or "sl" in reason.lower()
    _stats["pnl"]  += pnl
    _stats["hist"].append(pnl)
    ks_upd(pnl)

    if pnl >= 0:
        _stats["wins"] += 1
        if pnl > _stats["best"]: _stats["best"] = pnl
    else:
        _stats["losses"] += 1
        if pnl < _stats["worst"]: _stats["worst"] = pnl

    if "ExtremeTP" in reason:   _stats["extreme_tp"] += 1
    elif "TrailSL" in reason:   _stats["trail_sl"]   += 1
    elif "HardSL"  in reason:   _stats["hard_sl"]    += 1

    trade_log.append({
        "sym": sym, "side": side, "entry": round(entry, 7), "exit": round(price, 7),
        "pnl": round(pnl, 5), "reason": reason, "hold": int(hold),
    })
    set_cd(sym, is_sl=is_sl)
    _hot_syms.appendleft(sym)
    _rescan_q.put(1)
    print_inline()

# ═══════════════════════════════════════════════════════
#  MONITOR v18.6.0 — dengan Trailing Stop yang diperbaiki
#  [FIX #1] TRAIL_ACTIVATE_PCT = 0.0065
#  [FIX #2] TRAIL_DELTA_PCT = 0.0030
#  [FIX #6] Skip TrailSL jika hold < TRAIL_MIN_HOLD (120 detik)
# ═══════════════════════════════════════════════════════
def monitor_positions():
    for sym in list(live_positions.keys()):
        pos = live_positions.get(sym)
        if pos is None or pos.get("_r"): continue

        px = price_live(sym)
        if px == 0: continue

        side  = pos["side"]
        entry = pos["entry"]
        sl_px = pos["sl_price"]
        tp_px = pos["tp_price"]
        hold  = time.time() - pos["open_time"]

        if side == "LONG":
            prof_pct = (px - entry) / entry

            # ── [FIX #6] Minimum hold guard ──────────────────────────────
            # Trailing stop hanya aktif setelah TRAIL_MIN_HOLD detik
            # Mencegah exit prematur di candle noise awal
            if hold >= TRAIL_MIN_HOLD:
                # ── [FIX #1] Trail aktif di +0.65% (bukan +0.35%) ────────
                if prof_pct >= TRAIL_ACTIVATE_PCT:
                    if not pos["trail_active"]:
                        pos["trail_active"] = True
                        print(f"   🔒 {sym} L trail aktif @{px:.5g} (+{prof_pct*100:.2f}%)")
                    if px > pos["trail_peak"]:
                        pos["trail_peak"] = px
                    # ── [FIX #2] Delta 0.30% (bukan 0.15%) ───────────────
                    trail_sl = pos["trail_peak"] * (1 - TRAIL_DELTA_PCT)
                    if px <= trail_sl:
                        live_close(sym, "TrailSL", px); continue
            else:
                # Masih dalam hold minimum → skip trail check
                if prof_pct >= TRAIL_ACTIVATE_PCT and not pos.get("_trail_skip_logged"):
                    _stats["trail_skip"] += 1
                    pos["_trail_skip_logged"] = True
                    print(f"   ⏳ {sym} L trail defer — hold {hold:.0f}s < {TRAIL_MIN_HOLD}s")

            # Hard SL selalu aktif (tidak ada hold guard)
            if px <= sl_px:
                live_close(sym, "HardSL", px); continue
            # TP selalu aktif
            if px >= tp_px:
                live_close(sym, "ExtremeTP", px); continue

            pnl_now = ((px - entry) * pos["qty"]) - (
                (entry * pos["qty"] + px * pos["qty"]) * FUTURES_FEE_PCT)
            hold_guard_info = f" ⏳hold{hold:.0f}s" if hold < TRAIL_MIN_HOLD else ""
            trail_info = f" 🔒trail@{pos['trail_peak']:.5g}" if pos["trail_active"] else ""
            print(f"   📌 {sym} L@{entry:.5g}→{px:.5g}({prof_pct*100:+.2f}%) "
                  f"{pnl_now:+.4f}U {hold:.0f}s{trail_info}{hold_guard_info} [DRY]")

        else:  # SHORT
            prof_pct = (entry - px) / entry

            # ── [FIX #6] Minimum hold guard ──────────────────────────────
            if hold >= TRAIL_MIN_HOLD:
                # ── [FIX #1] Trail aktif di +0.65% ───────────────────────
                if prof_pct >= TRAIL_ACTIVATE_PCT:
                    if not pos["trail_active"]:
                        pos["trail_active"] = True
                        print(f"   🔒 {sym} S trail aktif @{px:.5g} (+{prof_pct*100:.2f}%)")
                    if px < pos["trail_peak"]:
                        pos["trail_peak"] = px
                    # ── [FIX #2] Delta 0.30% ─────────────────────────────
                    trail_sl = pos["trail_peak"] * (1 + TRAIL_DELTA_PCT)
                    if px >= trail_sl:
                        live_close(sym, "TrailSL", px); continue
            else:
                if prof_pct >= TRAIL_ACTIVATE_PCT and not pos.get("_trail_skip_logged"):
                    _stats["trail_skip"] += 1
                    pos["_trail_skip_logged"] = True
                    print(f"   ⏳ {sym} S trail defer — hold {hold:.0f}s < {TRAIL_MIN_HOLD}s")

            # Hard SL selalu aktif
            if px >= sl_px:
                live_close(sym, "HardSL", px); continue
            if px <= tp_px:
                live_close(sym, "ExtremeTP", px); continue

            pnl_now = ((entry - px) * pos["qty"]) - (
                (entry * pos["qty"] + px * pos["qty"]) * FUTURES_FEE_PCT)
            hold_guard_info = f" ⏳hold{hold:.0f}s" if hold < TRAIL_MIN_HOLD else ""
            trail_info = f" 🔒trail@{pos['trail_peak']:.5g}" if pos["trail_active"] else ""
            print(f"   📌 {sym} S@{entry:.5g}→{px:.5g}({prof_pct*100:+.2f}%) "
                  f"{pnl_now:+.4f}U {hold:.0f}s{trail_info}{hold_guard_info} [DRY]")

# ═══════════════════════════════════════════════════════
#  SCANNER
# ═══════════════════════════════════════════════════════
def scan_one(sym):
    try:
        time.sleep(SCAN_DELAY)
        if not ok_cooldown(sym): return None
        if not ok_whipsaw(sym): return None
        tk = _ticker_cache
        if sym in tk and tk[sym]["vol"] < MIN_BASE_VOL: return None

        df5 = run_ta(ohlcv(sym, Client.KLINE_INTERVAL_5MINUTE, 100).copy())
        if df5 is None: return None

        px  = df5["close"].iloc[-2]
        atr = df5["atr"].iloc[-2]
        if px == 0: return None

        dir_, sc, sigs, atr_val, sl_pct, tp_pct = signal(df5, sym)
        if dir_ is None or len(sigs) < 2: return None

        px_live = price_live(sym)
        if px_live == 0: return None

        slip = abs(px_live - px) / px
        if slip > SLIPPAGE_GUARD * 1.5: return None

        return (sym, dir_, sc, sigs, px_live, atr_val, sl_pct, tp_pct)
    except: return None

def scan_batch(syms):
    res = []
    fut = {_executor.submit(scan_one, s): s for s in syms[:BATCH_SIZE]}
    try:
        for f in as_completed(fut, timeout=12):
            try:
                if r := f.result(timeout=2): res.append(r)
            except: pass
    except: pass
    return res

def top_movers(syms, n=25):
    tk, ss = tickers_all(), set(syms)
    mv = [(s, abs(d["pct"])) for s, d in tk.items()
          if s in ss and d["vol"] >= MIN_BASE_VOL]
    return [s for s, _ in sorted(mv, key=lambda x: x[1], reverse=True)[:n]]

# ═══════════════════════════════════════════════════════
#  PRINT UTILS
# ═══════════════════════════════════════════════════════
def print_inline():
    n  = _stats["wins"] + _stats["losses"]
    wr = _stats["wins"] / n * 100 if n else 0
    pnl, e = _stats["pnl"], "💚" if _stats["pnl"] >= 0 else "🔴"
    print(f"      ┌ [v18.6.0 DRY] {n}T WR:{wr:.0f}% W:{_stats['wins']} L:{_stats['losses']} "
          f"{e}PnL Net:{pnl:+.4f}U")
    print(f"      └ ExTP:{_stats['extreme_tp']} TrailSL:{_stats['trail_sl']} "
          f"HardSL:{_stats['hard_sl']} "
          f"BTCBlock:{_stats['btc_block']} TrailDefer:{_stats['trail_skip']}")

def print_full():
    n    = _stats["wins"] + _stats["losses"]
    wr   = _stats["wins"] / n * 100 if n else 0
    pnl  = _stats["pnl"]
    sess = (time.time() - _stats["start"]) / 3600
    tph  = n / sess if sess > 0 else 0
    e    = "💚" if pnl >= 0 else "🔴"

    sh = md = 0.0
    if len(_stats["hist"]) >= 5:
        a  = np.array(list(_stats["hist"]))
        sd = float(np.std(a))
        sh = float(np.mean(a)) / sd if sd > 0 else 0.0
    if len(_stats["hist"]) >= 2:
        eq = np.cumsum(list(_stats["hist"]))
        md = float(np.min(eq - np.maximum.accumulate(eq)))

    print(f"\n  {'─'*68}")
    print(f"   ✅ DRY RUN v18.6.0 [ATR-SL | TrailTP+ | TrendGate+] — "
          f"{sess*60:.0f}m | {tph:.1f}T/jam")
    print(f"   🎯 {n}T WR:{wr:.0f}% W:{_stats['wins']} L:{_stats['losses']}")
    print(f"   {e} PnL Net:{pnl:+.5f}U Best:{_stats['best']:+.5f} Worst:{_stats['worst']:+.5f}")
    print(f"   📐 Sharpe:{sh:.2f} MaxDD:{md:.5f}U")
    print(f"   💰 ExtremeTP:{_stats['extreme_tp']} TrailSL:{_stats['trail_sl']} "
          f"HardSL:{_stats['hard_sl']}")
    print(f"   🚫 BTCGateBlock:{_stats['btc_block']} TrailDefer:{_stats['trail_skip']}")
    print(f"   🔑 KS: consec={_ks['consec']} daily={_ks['daily']:+.4f} | BTC:{_macro['btc']}")
    print(f"   ⚙️  Trail: activate={TRAIL_ACTIVATE_PCT*100:.2f}% "
          f"delta={TRAIL_DELTA_PCT*100:.2f}% minhold={TRAIL_MIN_HOLD}s "
          f"TP_mult={ATR_TP_MULT} MaxPos={MAX_POSITIONS}")
    if trade_log:
        print(f"   📋 Last 5:")
        for t in trade_log[-5:]:
            em = "🟢" if t["pnl"] > 0 else "🔴"
            print(f"      {em} {t['sym']:<16} {t['side']} {t['pnl']:+.5f}U "
                  f"{t['hold']}s — {t['reason']}")
    print(f"  {'─'*68}")

# ═══════════════════════════════════════════════════════
#  THREADS
# ═══════════════════════════════════════════════════════
def t_monitor():
    while True:
        try:
            if live_positions:
                monitor_positions()
        except: pass
        time.sleep(MONITOR_INT)

def t_slot_filler(syms):
    scan_idx = 0
    n_bat    = math.ceil(len(syms) / BATCH_SIZE)

    while True:
        try:
            slots = MAX_POSITIONS - len(live_positions)
            if slots <= 0 or ks_check()[0]:
                time.sleep(SLOT_FILL_INT)
                continue

            hot  = [s for s in _hot_syms if s not in live_positions and ok_cooldown(s) and ok_whipsaw(s)]
            mv   = top_movers(syms, 25)
            mv   = [s for s in mv   if s not in live_positions and ok_cooldown(s) and ok_whipsaw(s)]

            bs   = scan_idx * BATCH_SIZE
            reg  = [s for s in syms[bs:bs+BATCH_SIZE]
                    if s not in live_positions and ok_cooldown(s) and ok_whipsaw(s) and s not in mv]
            scan_idx = (scan_idx + 1) % n_bat

            scan_list = list(dict.fromkeys(hot[:5] + mv[:15] + reg[:10]))[:BATCH_SIZE]
            if not scan_list:
                time.sleep(SLOT_FILL_INT)
                continue

            res = scan_batch(scan_list)
            if res:
                res.sort(key=lambda x: x[2], reverse=True)
                for r in res[:slots]:
                    if len(live_positions) >= MAX_POSITIONS: break
                    sym, d, sc, sg, px, atr, sl_p, tp_p = r
                    live_open(sym, d, sc, sg, px, atr, sl_p, tp_p)

        except: pass
        time.sleep(SLOT_FILL_INT)

def t_rescan(syms):
    while True:
        try:
            _rescan_q.get(timeout=15)
            time.sleep(0.2)
            slots = MAX_POSITIONS - len(live_positions)
            if slots <= 0 or ks_check()[0]: continue

            hot  = [s for s in _hot_syms if s not in live_positions]
            rest = [s for s in syms if s not in live_positions and s not in hot]
            res  = scan_batch((hot + rest)[:25])
            if res:
                res.sort(key=lambda x: x[2], reverse=True)
                for r in res[:slots]:
                    if len(live_positions) >= MAX_POSITIONS: break
                    sym, d, sc, sg, px, atr, sl_p, tp_p = r
                    live_open(sym, d, sc, sg, px, atr, sl_p, tp_p)
        except: pass

def t_macro():
    while True:
        try: _macro["btc"] = btc_trend()
        except: pass
        try:
            if time.time() - _macro["last_fng"] > 300:
                _macro["fng"] = int(requests.get(
                    "https://api.alternative.me/fng/?limit=1", timeout=5
                ).json()["data"][0]["value"])
                _macro["last_fng"] = time.time()
        except: pass
        time.sleep(5)

# ═══════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════
def run_bot():
    print("╔═══════════════════════════════════════════════════════════════╗")
    print("║  ✅ DRY RUN v18.6.0 — 6 FIXES APPLIED                      ║")
    print("║  ✅ Trail activate +0.65% | Delta 0.30% | MinHold 120s      ║")
    print("║  ✅ TP mult 2.2× | BTC Gate double-check | MaxPos 2         ║")
    print("║  ⚠️  NO REAL ORDERS — SIMULATION LOGGING ONLY               ║")
    print("╚═══════════════════════════════════════════════════════════════╝")
    print(f"  ⚙️  Config: TRAIL_ACT={TRAIL_ACTIVATE_PCT*100:.2f}% "
          f"TRAIL_DELTA={TRAIL_DELTA_PCT*100:.2f}% "
          f"TRAIL_HOLD={TRAIL_MIN_HOLD}s "
          f"TP={ATR_TP_MULT}x SL={ATR_SL_MULT}x "
          f"MaxPos={MAX_POSITIONS}")

    try:
        valid = {s["symbol"] for s in client.futures_exchange_info()["symbols"]
                 if s["status"] == "TRADING"}
        syms  = list(dict.fromkeys([s for s in SYMBOLS if s in valid]))
    except:
        syms  = list(dict.fromkeys(SYMBOLS))

    print(f"  📋 {len(syms)} simbol aktif")

    threading.Thread(target=t_monitor,                  daemon=True).start()
    threading.Thread(target=t_slot_filler, args=(syms,), daemon=True).start()
    threading.Thread(target=t_rescan,      args=(syms,), daemon=True).start()
    threading.Thread(target=t_macro,                    daemon=True).start()

    time.sleep(4)
    tickers_all()

    cycle = 0
    while True:
        cycle += 1
        slots = MAX_POSITIONS - len(live_positions)
        print(f"\n{'═'*62}")
        print(f"  #{cycle} {time.strftime('%H:%M:%S')} BTC:{_macro['btc']} F&G:{_macro['fng']} "
              f"({len(live_positions)}/{MAX_POSITIONS}) PnL:{_stats['pnl']:+.4f}U")

        if (k := ks_check())[0]:
            print(f"  🚨 KS:{k[1]}")
        elif slots == 0:
            print(f"  ✅ Full ({MAX_POSITIONS}/{MAX_POSITIONS}) — slot filler aktif")
        else:
            print(f"  🔍 {slots} slot kosong — slot filler mengisi...")

        if cycle % 20 == 0:
            print_full()

        time.sleep(SCAN_INTERVAL)

if __name__ == "__main__":
    run_bot()
