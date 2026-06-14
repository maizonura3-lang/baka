"""
Bot Scalping Quant v20.1.0 — HIGH WIN RATE & FEE COVERAGE
==========================================================
MODIFIKASI WIN RATE:
- TP/SL lebih pendek (ATR multiplier rendah) -> win rate tinggi
- Minimal TP menutup fee round-trip (0.1%)
- Filter lebih longgar tapi tetap disiplin
- Break Even & Trailing di level rendah
- Profit Protection lebih responsif
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
client.FUTURES_URL = "https://fapi.binance.com/fapi"

# ═══════════════════════════════════════════════════════
#  CONFIG v20.1.0
# ═══════════════════════════════════════════════════════
LEVERAGE       = 20
ORDER_USDT     = 2.0
MAX_POSITIONS  = 3 
FUTURES_FEE_PCT = 0.0005  # 0.05% per side (0.10% round trip)

SCAN_INTERVAL  = 0.2     
MONITOR_INT    = 0.05    
SCAN_DELAY     = 0.002   
BATCH_SIZE     = 40      
MAX_WORKERS    = 20      
SLOT_FILL_INT  = 0.01    

MIN_SCORE      = 65       # Lebih rendah agar lebih banyak sinyal
MIN_GAP        = 5
SLIPPAGE_GUARD = 0.0015  
TTL_5M         = 2       

DAILY_LOSS     = -20.0
CONSEC_MAX     = 15

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
#  STATE & MEMORY
# ═══════════════════════════════════════════════════════
live_positions  = {}
trade_log       = []
_ohlcv_cache    = {}
_ticker_cache   = {}
_ticker_ts      = 0
_lock           = threading.Lock()
_executor       = ThreadPoolExecutor(max_workers=MAX_WORKERS)
_rescan_q       = queue.Queue()
_hot_syms       = deque(maxlen=30)

_past_trades    = deque(maxlen=50)
_cooldowns      = {}
_macro = {"fng": 50, "btc": "UNKNOWN", "last_fng": 0, "last_btc": 0}

_prot = {
    "consec_loss": 0, 
    "consec_win": 0, 
    "active_min_score": MIN_SCORE, 
    "pause_until": 0
}
_ks    = {"active": False, "reason": "", "resume": 0, "consec": 0, "daily": 0.0, "day_reset": 0}
_stats = {
    "trades": 0, "wins": 0, "losses": 0, "pnl": 0.0, "best": 0.0, "worst": 0.0,
    "extreme_tp": 0, "hard_sl": 0, "force": 0, "btc_block": 0,
    "hist": deque(maxlen=200), "start": time.time(),
}

# ═══════════════════════════════════════════════════════
#  BINANCE UTILS (tidak berubah)
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
    return round(raw_qty, get_precision(symbol))

def price_live(symbol):
    try: return float(client.futures_symbol_ticker(symbol=symbol)["price"])
    except: return 0.0

def tickers_all():
    global _ticker_cache, _ticker_ts
    now = time.time()
    if now - _ticker_ts < 2 and _ticker_cache: return _ticker_cache
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

def ohlcv(symbol, interval, limit=100):
    key, now = (symbol, interval), time.time()
    if key in _ohlcv_cache and now - _ohlcv_cache[key][0] < TTL_5M:
        return _ohlcv_cache[key][1]
    try:
        kl = client.futures_klines(symbol=symbol, interval=interval, limit=limit)
        df = pd.DataFrame(kl, columns=["time","open","high","low","close","volume",
                                        "ct","qv","trades","tbbase","tbquote","ignore"])
        for c in ["open","high","low","close","volume","tbbase","tbquote"]:
            df[c] = df[c].astype(float)
        _ohlcv_cache[key] = (now, df)
        return df
    except: return _ohlcv_cache.get(key, (None, None))[1]

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
    df["body"] = abs(c - df["open"])
    df["rng"]  = h - l
    df["br2"]  = df["body"] / df["rng"].replace(0, 1)
    df["m5"]   = (c - c.shift(5)) / c.shift(5)
    return df

def btc_trend():
    try:
        df = run_ta(ohlcv("BTCUSDT", Client.KLINE_INTERVAL_5MINUTE, 80).copy())
        row = df.iloc[-2]
        p, e5, e9, e21, m5 = row["close"], row["e5"], row["e9"], row["e21"], row["m5"]
        if p > e5 > e9 > e21 and m5 > 0.001: return "BULL"
        if p < e5 < e9 < e21 and m5 < -0.001: return "BEAR"
        return "SIDEWAYS"
    except: return "UNKNOWN"

def ks_check():
    k, now = _ks, time.time()
    if _prot["pause_until"] > now:
        return True, "PROT_PAUSE(20m)"  # pause 20 menit setelah 5 loss

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
    return False, ""

def ks_upd(pnl):
    _ks["daily"] += pnl

# ═══════════════════════════════════════════════════════
#  SIGNAL ENGINE dengan TP/SL untuk win rate tinggi
# ═══════════════════════════════════════════════════════
def signal(df, symbol=None):
    if df is None or len(df) < 55: return None, 0, [], 0.0, 0.0, 0.0

    row  = df.iloc[-2]
    prev = df.iloc[-3]

    p, e5, e9, e21, e50 = row["close"], row["e5"], row["e9"], row["e21"], row["e50"]
    rsi  = row["rsi"]; adx = row["adx"]; vr = row["vr"]
    mh   = row["mh"]; mh_p = prev["mh"]
    atr  = row["atr"]; body = row["br2"]
    
    atr_pct = atr / p if p > 0 else 0

    btc = _macro.get("btc", "UNKNOWN")
    
    l_valid, s_valid = True, True

    # ================= FILTER (DILONGGARKAN) =================
    if body < 0.10 or adx <= 18 or vr <= 1.1 or atr_pct < 0.0020:
        return None, 0, [], 0.0, 0.0, 0.0

    # LONG
    if not (p > e5 > e9 > e21 > e50): l_valid = False
    if mh <= mh_p: l_valid = False
    if not (45 <= rsi <= 72): l_valid = False

    # SHORT
    if not (p < e5 < e9 < e21 < e50): s_valid = False
    if mh >= mh_p: s_valid = False
    if not (30 <= rsi <= 55): s_valid = False

    # BTC FILTER
    if btc == "BEAR": l_valid = False
    if btc == "BULL": s_valid = False
    
    if not l_valid and not s_valid:
        return None, 0, [], 0.0, 0.0, 0.0

    # ================= SMART SCORE =================
    score = 0
    score += 30   # trend valid
    score += 15   # ADX > 18
    score += 15   # volume > 1.1
    score += 15   # MACD arah benar
    score += 10   # RSI ok
    if btc == "BULL" and l_valid: score += 15
    elif btc == "BEAR" and s_valid: score += 15
    elif btc == "SIDEWAYS": score -= 5   # tidak terlalu menghukum
    
    if score < _prot["active_min_score"]:
        return None, 0, [], 0.0, 0.0, 0.0

    # ================= TP/SL untuk WIN RATE TINGGI =================
    # Multiplier lebih kecil agar sinyal lebih sering TP
    # SL = ATR * 0.7 (flexible 0.6~0.8)
    sl_mult = 0.7
    # TP = ATR * 1.0 (bisa 0.9~1.2)
    tp_mult = 1.0
    
    sl_pct = max(0.0020, atr_pct * sl_mult)   # minimal 0.20%
    tp_pct = max(0.0035, atr_pct * tp_mult)   # minimal 0.35% agar nutup fee
    
    # Pastikan TP_net (setelah fee) > 0.05% dan minimal lebih besar dari SL+2*fee
    # agar satu kemenangan bisa menutup fee dan sedikit loss
    min_tp_needed = sl_pct + (2 * FUTURES_FEE_PCT) + 0.0005  # ekstra 0.05%
    if tp_pct < min_tp_needed:
        tp_pct = min_tp_needed

    # COOLDOWN symbol setelah loss
    if symbol in _cooldowns and time.time() < _cooldowns[symbol]:
        return None, 0, [], 0.0, 0.0, 0.0

    if l_valid:
        return "LONG", score, ["Valid_L", f"ADX:{adx:.1f}"], atr, sl_pct, tp_pct
    else:
        return "SHORT", score, ["Valid_S", f"ADX:{adx:.1f}"], atr, sl_pct, tp_pct

# ═══════════════════════════════════════════════════════
#  OPEN POSITION (tidak berubah)
# ═══════════════════════════════════════════════════════
def live_open(sym, direction, score, sigs, price, atr, sl_pct, tp_pct):
    with _lock:
        if sym in live_positions or len(live_positions) >= MAX_POSITIONS: return
        live_positions[sym] = {"_r": True}

    px_now = price_live(sym)
    if px_now > 0:
        slip = abs(px_now - price) / price
        if slip > SLIPPAGE_GUARD:
            with _lock: live_positions.pop(sym, None)
            return
        price = px_now

    try: q_val = qty(sym, price)
    except:
        with _lock: live_positions.pop(sym, None)
        return

    sl_price = price * (1 - sl_pct) if direction == "LONG" else price * (1 + sl_pct)
    tp_price = price * (1 + tp_pct) if direction == "LONG" else price * (1 - tp_pct)

    pos = {
        "side": direction, "entry": price, "qty": q_val,
        "open_time": time.time(), "score": score, "sigs": sigs, "atr": atr,
        "sl_price": sl_price, "tp_price": tp_price,
        "sl_pct": sl_pct, "tp_pct": tp_pct, "max_prof": 0.0, "stage": 0, "adx": float(sigs[1].split(":")[1])
    }
    with _lock: live_positions[sym] = pos

    d = "🟢" if direction == "LONG" else "🔴"
    rr_net = (tp_pct - FUTURES_FEE_PCT) / (sl_pct + FUTURES_FEE_PCT)
    print(f"\n  {d} [DRY] {sym} {direction} @{price:.6g} SL:{sl_pct*100:.2f}% TP:{tp_pct*100:.2f}% Net RR:1:{rr_net:.1f}")
    _stats["trades"] += 1

# ═══════════════════════════════════════════════════════
#  CLOSE POSITION & PROFIT PROTECTION (disesuaikan)
# ═══════════════════════════════════════════════════════
def live_close(sym, reason, price=None):
    with _lock: pos = live_positions.pop(sym, None)
    if pos is None or pos.get("_r"): return

    if price is None: price = price_live(sym)
    if price == 0: return

    side, entry, q_val = pos["side"], pos["entry"], pos["qty"]
    gross_pnl = (price - entry) * q_val if side == "LONG" else (entry - price) * q_val
    total_fee = ((entry * q_val) + (price * q_val)) * FUTURES_FEE_PCT
    pnl = gross_pnl - total_fee

    pct   = (price - entry) / entry * 100 if side == "LONG" else (entry - price) / entry * 100
    hold  = time.time() - pos["open_time"]
    e = "🟢" if pnl >= 0 else "🔴"

    print(f"  {e} [DRY] {sym} {side} CLOSE — {reason}")
    print(f"     {entry:.6g}→{price:.6g} ({pct:+.3f}%) hold:{hold:.0f}s | PnL Net:{pnl:+.5f}U (Fee:{total_fee:.5f}U)")

    _stats["pnl"]  += pnl
    ks_upd(pnl)

    _past_trades.append({
        "sym": sym, "score": pos["score"], "adx": pos["adx"], "pnl": pnl
    })

    # Profit Protection (disesuaikan)
    if pnl >= 0:
        _stats["wins"] += 1
        _prot["consec_win"] += 1
        _prot["consec_loss"] = 0
        if _prot["consec_win"] >= 3 and _prot["active_min_score"] > MIN_SCORE:
            _prot["active_min_score"] = MIN_SCORE
            print(f"  ✅ 3 Win beruntun → Score turun ke {MIN_SCORE}")
    else:
        _stats["losses"] += 1
        _prot["consec_loss"] += 1
        _prot["consec_win"] = 0
        
        # Cooldown 15 menit untuk pair yang kena SL
        if "SL" in reason: _cooldowns[sym] = time.time() + 900
        
        if _prot["consec_loss"] >= 5:
            _prot["pause_until"] = time.time() + 1200   # 20 menit (lebih cepat pulih)
            _prot["consec_loss"] = 0
            print("  🚨 5 Loss beruntun! Bot pause 20 menit.")
        elif _prot["consec_loss"] >= 3:
            _prot["active_min_score"] = 75   # naikkan threshold tapi tidak terlalu tinggi
            print("  ⚠️ 3 Loss beruntun → Min Score naik ke 75")

    trade_log.append({
        "sym": sym, "side": side, "entry": round(entry, 7), "exit": round(price, 7),
        "pnl": round(pnl, 5), "reason": reason, "hold": int(hold),
    })
    _hot_syms.appendleft(sym)
    _rescan_q.put(1)

# ═══════════════════════════════════════════════════════
#  MONITOR POSITIONS (BE & TRAIL disesuaikan untuk TP kecil)
# ═══════════════════════════════════════════════════════
def monitor_positions():
    for sym in list(live_positions.keys()):
        pos = live_positions.get(sym)
        if pos is None or pos.get("_r"): continue

        px = price_live(sym)
        if px == 0: continue

        side, entry = pos["side"], pos["entry"]
        sl_px, tp_px = pos["sl_price"], pos["tp_price"]
        
        prof_pct = (px - entry) / entry if side == "LONG" else (entry - px) / entry
        pos["max_prof"] = max(pos["max_prof"], prof_pct)

        stage = pos["stage"]
        
        # Break Even di +0.30% (lebih rendah)
        if pos["max_prof"] >= 0.0030 and stage < 1:
            pos["stage"] = 1
            # SL pindah ke entry + fee + 0.05% buffer
            if side == "LONG": pos["sl_price"] = entry * (1 + FUTURES_FEE_PCT + 0.0005)
            else: pos["sl_price"] = entry * (1 - (FUTURES_FEE_PCT + 0.0005))
            print(f"  🛡️ {sym} Break-Even active!")

        # Trail Stage 1 di +0.50% (trail 0.20%)
        if pos["max_prof"] >= 0.0050 and stage < 2: pos["stage"] = 2
            
        # Trail Stage 2 di +0.80% (trail 0.25%)
        if pos["max_prof"] >= 0.0080 and stage < 3: pos["stage"] = 3

        # Apply trailing
        if pos["stage"] == 2:
            if side == "LONG": pos["sl_price"] = max(pos["sl_price"], px * (1 - 0.0020))
            else: pos["sl_price"] = min(pos["sl_price"], px * (1 + 0.0020))
        elif pos["stage"] == 3:
            if side == "LONG": pos["sl_price"] = max(pos["sl_price"], px * (1 - 0.0025))
            else: pos["sl_price"] = min(pos["sl_price"], px * (1 + 0.0025))

        sl_px = pos["sl_price"]
        
        # Eksekusi exit
        if side == "LONG":
            if px <= sl_px:
                live_close(sym, "TrailSL" if stage > 0 else "HardSL", px); continue
            if px >= tp_px:
                live_close(sym, "TakeProfit", px); continue
        else:
            if px >= sl_px:
                live_close(sym, "TrailSL" if stage > 0 else "HardSL", px); continue
            if px <= tp_px:
                live_close(sym, "TakeProfit", px); continue

# ═══════════════════════════════════════════════════════
#  SCANNER & THREADS (sama seperti sebelumnya)
# ═══════════════════════════════════════════════════════
def scan_one(sym):
    try:
        time.sleep(SCAN_DELAY)
        df5 = run_ta(ohlcv(sym, Client.KLINE_INTERVAL_5MINUTE, 100).copy())
        if df5 is None: return None
        px  = df5["close"].iloc[-2]
        if px == 0: return None
        dir_, sc, sigs, atr_val, sl_pct, tp_pct = signal(df5, sym)
        if dir_ is None: return None
        px_live = price_live(sym)
        if px_live == 0: return None
        return (sym, dir_, sc, sigs, px_live, atr_val, sl_pct, tp_pct)
    except: return None

def scan_batch(syms):
    res = []
    fut = {_executor.submit(scan_one, s): s for s in syms[:BATCH_SIZE]}
    try:
        for f in as_completed(fut, timeout=5):
            if r := f.result(timeout=1): res.append(r)
    except: pass
    return res

def top_movers(syms, n=30):
    tk, ss = tickers_all(), set(syms)
    mv = [(s, abs(d["pct"])) for s, d in tk.items() if s in ss]
    return [s for s, _ in sorted(mv, key=lambda x: x[1], reverse=True)[:n]]

def print_full():
    n    = _stats["wins"] + _stats["losses"]
    wr   = _stats["wins"] / n * 100 if n else 0
    pnl  = _stats["pnl"]
    e    = "💚" if pnl >= 0 else "🔴"
    print(f"\n  {'─'*68}")
    print(f"    ✅ HFT QUANT v20.1.0 [HIGH WIN RATE | FEE COVERAGE]")
    print(f"    🎯 {n}T WR:{wr:.0f}% W:{_stats['wins']} L:{_stats['losses']}")
    print(f"    {e} PnL Net:{pnl:+.5f}U")
    if trade_log:
        print(f"    📋 Last 5:")
        for t in trade_log[-5:]:
            em = "🟢" if t["pnl"] > 0 else "🔴"
            print(f"       {em} {t['sym']:<16} {t['side']} {t['pnl']:+.5f}U {t['hold']}s — {t['reason']}")
    print(f"  {'─'*68}")

def t_monitor():
    while True:
        try:
            if live_positions: monitor_positions()
        except: pass
        time.sleep(MONITOR_INT)

def t_slot_filler(syms):
    scan_idx = 0
    n_bat    = math.ceil(len(syms) / BATCH_SIZE)
    while True:
        try:
            slots = MAX_POSITIONS - len(live_positions)
            if slots <= 0 or ks_check()[0]:
                time.sleep(SLOT_FILL_INT); continue
            
            hot  = [s for s in _hot_syms if s not in live_positions]
            mv   = [s for s in top_movers(syms, 30) if s not in live_positions]
            bs   = scan_idx * BATCH_SIZE
            reg  = [s for s in syms[bs:bs+BATCH_SIZE] if s not in live_positions and s not in mv]
            scan_idx = (scan_idx + 1) % n_bat

            scan_list = list(dict.fromkeys(hot[:5] + mv[:20] + reg[:15]))[:BATCH_SIZE]
            if not scan_list:
                time.sleep(SLOT_FILL_INT); continue

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
            _rescan_q.get(timeout=5)
            time.sleep(0.05)
            slots = MAX_POSITIONS - len(live_positions)
            if slots <= 0 or ks_check()[0]: continue

            hot  = [s for s in _hot_syms if s not in live_positions]
            rest = [s for s in syms if s not in live_positions and s not in hot]
            res  = scan_batch((hot + rest)[:30])
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
        time.sleep(10)

# ═══════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════
def run_bot():
    print("╔═══════════════════════════════════════════════════════════════╗")
    print("║  ✅ HFT QUANT v20.1.0 — HIGH WIN RATE & FEE COVERAGE          ║")
    print("║  🎯 Target win rate > 50% dengan TP/SL pendek                 ║")
    print("║  💰 Setiap profit bersih setelah fee > 0.05%                  ║")
    print("║  🛡️ Break-Even & Trailing Profit di level rendah              ║")
    print("╚═══════════════════════════════════════════════════════════════╝")

    syms = list(dict.fromkeys(SYMBOLS))
    print(f"  📋 {len(syms)} simbol terpantau")

    threading.Thread(target=t_monitor,         daemon=True).start()
    threading.Thread(target=t_slot_filler, args=(syms,), daemon=True).start()
    threading.Thread(target=t_rescan,      args=(syms,), daemon=True).start()
    threading.Thread(target=t_macro,                     daemon=True).start()

    time.sleep(2); tickers_all()

    cycle = 0
    while True:
        cycle += 1
        slots = MAX_POSITIONS - len(live_positions)
        print(f"\n{'═'*62}")
        print(f"  #{cycle} {time.strftime('%H:%M:%S')} BTC:{_macro['btc']} Score_Min:{_prot['active_min_score']} ({len(live_positions)}/{MAX_POSITIONS}) PnL:{_stats['pnl']:+.4f}U")

        ks_status, ks_reason = ks_check()
        if ks_status: print(f"  🚨 SYSTEM LOCK: {ks_reason}")
        elif slots == 0: print(f"  ✅ Slots full")
        else: print(f"  🔍 {slots} slot kosong — Sniper scanning...")

        if cycle % 30 == 0: print_full()
        time.sleep(SCAN_INTERVAL)

if __name__ == "__main__":
    run_bot()
