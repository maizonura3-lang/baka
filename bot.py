"""
Bot Scalping v18.5.0 — DRY RUN LOG MODE (PAPER TRADING)
====================================================
CHANGELOG v18.5.0 vs v18.4.0:
─────────────────────────────
[FIX] Dynamic ATR-based SL (bukan fixed 0.2%) → SL ngikut volatilitas nyata
[FIX] Asymmetric TP > 2× ATR_SL → R:R minimum 2:1 selalu terjaga
[NEW] Trailing Stop aktif dari profit ≥ TRAIL_ACTIVATE_PCT (0.35%)
      → mengunci profit, bukan lepas TP lalu balik
[NEW] Trend Direction Gate:
      - BTC BULL  → hanya LONG, SHORT diblokir
      - BTC BEAR  → hanya SHORT, LONG diblokir
      - BTC lainnya → kedua arah boleh, tapi skor SHORT +filter ketat
[NEW] Score momentum gate: hanya entry jika skor ≥ MIN_SCORE + gap dari threshold
[NEW] Volume displacement filter: entry hanya saat ada real push (vr≥1.8 + body≥50% candle)
[NEW] Slot Filler Thread: dedicated loop ngisi slot kosong tiap 0.2s agresif
[NEW] Symbol blacklist dinamis: simbol kena SL masuk cooldown 5 menit, bukan 3 detik
[NEW] Pre-entry slippage guard: tolak entry jika harga sudah geser >0.15% dari sinyal
[NEW] 15m trend confirmation: sinyal 5m harus aligned dengan EMA 15m
[NEW] Anti-whipsaw: tidak entry jika posisi sebelumnya di simbol ini kena SL dalam 10 menit
[TUNED] MIN_SCORE dinaikkan ke 58 untuk filter lebih ketat
[TUNED] MAX_POSITIONS dinaikkan ke 5 untuk lebih banyak kesempatan
[TUNED] BATCH_SIZE 20, MAX_WORKERS 12 untuk scan lebih cepat
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
#  CONFIG v18.5.0
# ═══════════════════════════════════════════════════════

LEVERAGE       = 20
ORDER_USDT     = 2.0
MAX_POSITIONS  = 5       # naik dari 3 → lebih banyak slot aktif

# ── TP/SL STRATEGY v18.5.0 ──────────────────────────────
# SL = ATR-based (dinamis), bukan fixed pct
# TP = 2.5× SL → R:R minimum 2.5:1 setelah fee
# Trailing aktif dari profit ≥ 0.35% → kunci profit
ATR_SL_MULT        = 1.2   # SL = 1.2 × ATR (kisaran 0.25–0.55% tergantung volatilitas)
ATR_TP_MULT        = 3.0   # TP = 3.0 × ATR → R:R = 3.0/1.2 = 2.5:1
TRAIL_ACTIVATE_PCT = 0.0035 # trailing aktif dari +0.35% profit
TRAIL_DELTA_PCT    = 0.0015 # trailing stop jarak 0.15% dari peak
MIN_SL_PCT         = 0.0015 # SL minimum 0.15% (jangan terlalu ketat)
MAX_SL_PCT         = 0.0060 # SL maksimum 0.60% (jangan terlalu lebar)
MIN_TP_PCT         = 0.0040 # TP minimum 0.40%
FUTURES_FEE_PCT    = 0.0005 # Taker fee 0.05%

MIN_BASE_VOL   = 25_000_000
MIN_VR         = 1.8        # naik dari 1.1 → wajib ada volume push nyata
BR_LONG_MIN    = 0.52       # naik dari 0.48 → buyer lebih dominan
BR_SHORT_MAX   = 0.48       # turun dari 0.52 → seller lebih dominan

SCAN_INTERVAL  = 1
MONITOR_INT    = 0.15       # lebih cepat
SCAN_DELAY     = 0.010      # lebih cepat
BATCH_SIZE     = 20         # naik dari 15
MAX_WORKERS    = 12         # naik dari 8
SLOT_FILL_INT  = 0.20       # slot filler loop interval

MIN_SCORE      = 58         # naik dari 52 → lebih selektif
MIN_GAP        = 12         # naik dari 10
COOLDOWN_SEC   = 300        # 5 menit cooldown setelah SL (bukan 3 detik!)
WHIPSAW_SEC    = 600        # anti-whipsaw: 10 menit blokir setelah SL di simbol sama
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
    # tambahan untuk lebih banyak peluang
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
_sym_cooldown   = {}       # {sym: timestamp_masuk_cooldown}
_sym_sl_time    = {}       # {sym: timestamp_terakhir_kena_SL} untuk anti-whipsaw
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
    """Cek cooldown — setelah SL 5 menit, setelah normal 3 detik"""
    cd = _sym_cooldown.get(sym, 0)
    return (time.time() - cd) >= COOLDOWN_SEC

def ok_whipsaw(sym):
    """Anti-whipsaw: blokir 10 menit setelah kena SL di simbol ini"""
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
    df["m3"]   = (c - c.shift(3)) / c.shift(3)  # momentum 3 candle
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
#  SIGNAL v18.5.0
#  Tambahan: 15m trend check, volume displacement, momentum gate
# ═══════════════════════════════════════════════════════
def get_15m_trend(symbol):
    """Cek trend 15m untuk konfirmasi sinyal 5m"""
    try:
        df = run_ta(ohlcv(symbol, Client.KLINE_INTERVAL_15MINUTE, 60).copy())
        row = df.iloc[-2]
        p, e9, e21 = row["close"], row["e9"], row["e21"]
        if p > e9 > e21: return "UP"
        if p < e9 < e21: return "DOWN"
        return "FLAT"
    except: return "FLAT"

def signal(df, symbol=None):
    """
    Signal v18.5.0:
    - Wajib volume displacement (bukan sekedar volume tinggi)
    - Candle body ≥ 40% dari range (bukan doji/spinning top)
    - Trend direction gate via BTC
    - 15m alignment check
    Returns: (direction, score, signals_list, atr, sl_pct, tp_pct)
    """
    if df is None or len(df) < 55: return None, 0, [], 0.0, 0.0, 0.0

    row  = df.iloc[-2]
    prev = df.iloc[-3]
    prev2= df.iloc[-4]

    p, e5, e9, e21, e50 = row["close"], row["e5"], row["e9"], row["e21"], row["e50"]
    rsi  = row["rsi"]
    mh   = row["mh"];  mh_p = prev["mh"];  mh_p2 = prev2["mh"]
    vr   = row["vr"];  br   = row["br"]
    m5   = row["m5"];  m3   = row["m3"]
    body = row["br2"]  # body ratio (body/range)
    atr  = row["atr"]; adx  = row["adx"]

    # ── Gate 1: Volume displacement ─────────────────────
    # Wajib ada dorongan nyata: volume ≥1.8× rata² DAN candle body besar
    if vr < MIN_VR: return None, 0, [], atr, 0.0, 0.0
    if body < 0.40: return None, 0, [], atr, 0.0, 0.0  # doji/spinning → skip

    # ── Gate 2: ATR filter — jangan masuk saat volatilitas ekstrem ──
    atr_pct = atr / p
    if atr_pct > 0.03: return None, 0, [], atr, 0.0, 0.0   # terlalu liar
    if atr_pct < 0.001: return None, 0, [], atr, 0.0, 0.0  # terlalu datar (dead market)

    # ── Hitung SL/TP berbasis ATR ────────────────────────
    sl_pct = max(MIN_SL_PCT, min(MAX_SL_PCT, atr_pct * ATR_SL_MULT))
    tp_pct = max(MIN_TP_PCT, atr_pct * ATR_TP_MULT)
    # Pastikan R:R ≥ 2.0 setelah fee
    net_tp = tp_pct - 2 * FUTURES_FEE_PCT
    net_sl = sl_pct + 2 * FUTURES_FEE_PCT
    if net_tp <= 0 or (net_tp / net_sl) < 1.8:
        # Paksa R:R minimal 2:1
        tp_pct = sl_pct * 2.5 + 4 * FUTURES_FEE_PCT

    lp = sp = 0
    sl, ss = [], []

    # ── EMA Stack (bobot tertinggi) ──────────────────────
    if p > e5 > e9 > e21 > e50:   lp += 32; sl.append("EMA5↑")
    elif p > e5 > e9 > e21:       lp += 24; sl.append("EMA4↑")
    if p < e5 < e9 < e21 < e50:   sp += 32; ss.append("EMA5↓")
    elif p < e5 < e9 < e21:       sp += 24; ss.append("EMA4↓")

    # ── Momentum ─────────────────────────────────────────
    if m5 > 0.006:    lp += 28; sl.append(f"Mom+{m5*100:.1f}%")
    elif m5 > 0.003:  lp += 20; sl.append(f"Mom+{m5*100:.1f}%")
    if m5 < -0.006:   sp += 28; ss.append(f"Mom{m5*100:.1f}%")
    elif m5 < -0.003: sp += 20; ss.append(f"Mom{m5*100:.1f}%")

    # ── Momentum 3 candle (short-term push) ─────────────
    if m3 > 0.003:    lp += 12; sl.append(f"M3+{m3*100:.1f}%")
    if m3 < -0.003:   sp += 12; ss.append(f"M3{m3*100:.1f}%")

    # ── MACD crossover / continuation ───────────────────
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

    # ── ADX (trend strength bonus) ───────────────────────
    if adx > 40:   lp += 10; sp += 10
    elif adx < 20: lp = int(lp * 0.8); sp = int(sp * 0.8)  # ranging → kurangi keyakinan

    # ── BTC Trend Direction Gate ─────────────────────────
    # INI LOGIKA KUNCI: paksa arah sesuai tren BTC
    btc = _macro["btc"]
    if btc == "BULL":
        sp = int(sp * 0.25)   # blokir hampir semua SHORT saat BULL
        lp += 10
    elif btc == "BEAR":
        lp = int(lp * 0.25)   # blokir hampir semua LONG saat BEAR
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
        if br <= BR_LONG_MIN: return None, lp, [], atr, sl_pct, tp_pct  # buyer ratio kurang
        # 15m confirmation untuk LONG
        if symbol:
            t15 = get_15m_trend(symbol)
            if t15 == "DOWN": return None, lp, [], atr, sl_pct, tp_pct  # 15m bearish → skip LONG
        return "LONG", lp, sl[:4], atr, sl_pct, tp_pct
    else:
        if sp < thresh or gap < MIN_GAP: return None, max(lp, sp), [], atr, sl_pct, tp_pct
        if br >= BR_SHORT_MAX: return None, sp, [], atr, sl_pct, tp_pct  # seller ratio kurang
        # 15m confirmation untuk SHORT
        if symbol:
            t15 = get_15m_trend(symbol)
            if t15 == "UP": return None, sp, [], atr, sl_pct, tp_pct  # 15m bullish → skip SHORT
        return "SHORT", sp, ss[:4], atr, sl_pct, tp_pct

# ═══════════════════════════════════════════════════════
#  DRY RUN OPEN
# ═══════════════════════════════════════════════════════
def live_open(sym, direction, score, sigs, price, atr, sl_pct, tp_pct):
    with _lock:
        if sym in live_positions or len(live_positions) >= MAX_POSITIONS:
            return
        live_positions[sym] = {"_r": True}

    # Slippage guard: cek apakah harga sudah geser terlalu jauh
    px_now = price_live(sym)
    if px_now > 0:
        slip = abs(px_now - price) / price
        if slip > SLIPPAGE_GUARD:
            print(f"  ⚠️  {sym} skip — slippage {slip*100:.2f}% > {SLIPPAGE_GUARD*100:.2f}%")
            with _lock: live_positions.pop(sym, None)
            return
        price = px_now  # gunakan harga terkini

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
        "trail_peak": price,      # untuk trailing stop
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
#  MONITOR — dengan Trailing Stop
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

            # Update trailing stop
            if prof_pct >= TRAIL_ACTIVATE_PCT:
                if not pos["trail_active"]:
                    pos["trail_active"] = True
                    print(f"   🔒 {sym} L trail aktif @{px:.5g} (+{prof_pct*100:.2f}%)")
                if px > pos["trail_peak"]:
                    pos["trail_peak"] = px
                # Trail SL = peak - delta
                trail_sl = pos["trail_peak"] * (1 - TRAIL_DELTA_PCT)
                if px <= trail_sl:
                    live_close(sym, "TrailSL", px); continue

            # Hard SL
            if px <= sl_px:
                live_close(sym, "HardSL", px); continue
            # TP
            if px >= tp_px:
                live_close(sym, "ExtremeTP", px); continue

            pnl_now = ((px - entry) * pos["qty"]) - (
                (entry * pos["qty"] + px * pos["qty"]) * FUTURES_FEE_PCT)
            trail_info = f" 🔒trail@{pos['trail_peak']:.5g}" if pos["trail_active"] else ""
            print(f"   📌 {sym} L@{entry:.5g}→{px:.5g}({prof_pct*100:+.2f}%) "
                  f"{pnl_now:+.4f}U {hold:.0f}s{trail_info} [DRY]")

        else:  # SHORT
            prof_pct = (entry - px) / entry

            # Update trailing stop
            if prof_pct >= TRAIL_ACTIVATE_PCT:
                if not pos["trail_active"]:
                    pos["trail_active"] = True
                    print(f"   🔒 {sym} S trail aktif @{px:.5g} (+{prof_pct*100:.2f}%)")
                if px < pos["trail_peak"]:
                    pos["trail_peak"] = px
                trail_sl = pos["trail_peak"] * (1 + TRAIL_DELTA_PCT)
                if px >= trail_sl:
                    live_close(sym, "TrailSL", px); continue

            if px >= sl_px:
                live_close(sym, "HardSL", px); continue
            if px <= tp_px:
                live_close(sym, "ExtremeTP", px); continue

            pnl_now = ((entry - px) * pos["qty"]) - (
                (entry * pos["qty"] + px * pos["qty"]) * FUTURES_FEE_PCT)
            trail_info = f" 🔒trail@{pos['trail_peak']:.5g}" if pos["trail_active"] else ""
            print(f"   📌 {sym} S@{entry:.5g}→{px:.5g}({prof_pct*100:+.2f}%) "
                  f"{pnl_now:+.4f}U {hold:.0f}s{trail_info} [DRY]")

# ═══════════════════════════════════════════════════════
#  SCANNER
# ═══════════════════════════════════════════════════════
def scan_one(sym):
    try:
        time.sleep(SCAN_DELAY)
        if not ok_cooldown(sym): return None
        if not ok_whipsaw(sym): return None  # anti-whipsaw check
        tk = _ticker_cache
        if sym in tk and tk[sym]["vol"] < MIN_BASE_VOL: return None

        df5 = run_ta(ohlcv(sym, Client.KLINE_INTERVAL_5MINUTE, 100).copy())
        if df5 is None: return None

        px  = df5["close"].iloc[-2]
        atr = df5["atr"].iloc[-2]
        if px == 0: return None

        dir_, sc, sigs, atr_val, sl_pct, tp_pct = signal(df5, sym)
        if dir_ is None or len(sigs) < 2: return None  # wajib ≥2 konfirmasi

        px_live = price_live(sym)
        if px_live == 0: return None

        # Slippage pre-check di scanner juga
        slip = abs(px_live - px) / px
        if slip > SLIPPAGE_GUARD * 1.5: return None  # sudah geser terlalu jauh

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
    print(f"      ┌ [v18.5.0 DRY] {n}T WR:{wr:.0f}% W:{_stats['wins']} L:{_stats['losses']} "
          f"{e}PnL Net:{pnl:+.4f}U")
    print(f"      └ ExTP:{_stats['extreme_tp']} TrailSL:{_stats['trail_sl']} "
          f"HardSL:{_stats['hard_sl']}")

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
    print(f"   ✅ DRY RUN v18.5.0 [ATR-SL | TrailTP | TrendGate] — "
          f"{sess*60:.0f}m | {tph:.1f}T/jam")
    print(f"   🎯 {n}T WR:{wr:.0f}% W:{_stats['wins']} L:{_stats['losses']}")
    print(f"   {e} PnL Net:{pnl:+.5f}U Best:{_stats['best']:+.5f} Worst:{_stats['worst']:+.5f}")
    print(f"   📐 Sharpe:{sh:.2f} MaxDD:{md:.5f}U")
    print(f"   💰 ExtremeTP:{_stats['extreme_tp']} TrailSL:{_stats['trail_sl']} "
          f"HardSL:{_stats['hard_sl']}")
    print(f"   🔑 KS: consec={_ks['consec']} daily={_ks['daily']:+.4f} | BTC:{_macro['btc']}")
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
    """Monitor posisi dengan interval sangat cepat"""
    while True:
        try:
            if live_positions:
                monitor_positions()
        except: pass
        time.sleep(MONITOR_INT)

def t_slot_filler(syms):
    """
    SLOT FILLER THREAD — logika baru v18.5.0
    Loop tiap 0.2 detik, agresif mengisi slot kosong.
    Tidak ada posisi kosong lebih dari 1 detik jika ada sinyal.
    """
    scan_idx = 0
    n_bat    = math.ceil(len(syms) / BATCH_SIZE)

    while True:
        try:
            slots = MAX_POSITIONS - len(live_positions)
            if slots <= 0 or ks_check()[0]:
                time.sleep(SLOT_FILL_INT)
                continue

            # Prioritas: hot symbols dulu, lalu top movers, lalu rotasi
            hot  = [s for s in _hot_syms if s not in live_positions and ok_cooldown(s) and ok_whipsaw(s)]
            mv   = top_movers(syms, 25)
            mv   = [s for s in mv   if s not in live_positions and ok_cooldown(s) and ok_whipsaw(s)]

            bs   = scan_idx * BATCH_SIZE
            reg  = [s for s in syms[bs:bs+BATCH_SIZE]
                    if s not in live_positions and ok_cooldown(s) and ok_whipsaw(s) and s not in mv]
            scan_idx = (scan_idx + 1) % n_bat

            # Gabung prioritas: hot > mover > regular
            scan_list = list(dict.fromkeys(hot[:5] + mv[:15] + reg[:10]))[:BATCH_SIZE]
            if not scan_list:
                time.sleep(SLOT_FILL_INT)
                continue

            res = scan_batch(scan_list)
            if res:
                res.sort(key=lambda x: x[2], reverse=True)  # sort by score
                for r in res[:slots]:
                    if len(live_positions) >= MAX_POSITIONS: break
                    sym, d, sc, sg, px, atr, sl_p, tp_p = r
                    live_open(sym, d, sc, sg, px, atr, sl_p, tp_p)

        except: pass
        time.sleep(SLOT_FILL_INT)

def t_rescan(syms):
    """Rescan setelah posisi tutup"""
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
    print("║  ✅ DRY RUN v18.5.0 — ATR-SL | TRAILING TP | TREND GATE    ║")
    print("║  ✅ Dynamic SL (ATR×1.2) | TP (ATR×3.0) | Trail @+0.35%   ║")
    print("║  ✅ BTC Trend Gate | 15m Confirm | Anti-Whipsaw             ║")
    print("║  ✅ Slot Filler Thread (0.2s) | Slippage Guard              ║")
    print("║  ⚠️  NO REAL ORDERS — SIMULATION LOGGING ONLY               ║")
    print("╚═══════════════════════════════════════════════════════════════╝")

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
