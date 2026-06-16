"""
Bot Scalping v21.1.0 — PULLBACK CONTINUATION + QUALITY SCORE + LOSS CLUSTER (INVERSE STRATEGY)
================================================================================
PERUBAHAN vs v21.0.0:
1. MIN_QUALITY_SCORE turun 50 → lebih banyak sinyal
2. Pullback range diperlebar 0.05% - 1.0%
3. PAUSE_CANDLES 3 menit (dari 5)
4. MAX_TP_PCT 1.5% (dari 2.5%) → profit lebih sering
5. confirm_entry lebih fleksibel (izinkan retest dalam 2 candle)
6. BTC opposing jadi PENALTY 20%, bukan total reject
7. Trailing stop aktif setelah profit >0.4%
8. Partial take profit 50% di 0.5R
9. INVERSE STRATEGY: Membalik arah hasil analisa (LONG jadi SHORT, SHORT jadi LONG) dan menukar SL/TP.

PERBAIKAN TERBARU:
- MIN_TP_NET_PCT = 0.001 (0.1% net setelah fee)
  → TP minimum dihitung dari harga entry + fee buka + fee tutup + 0.1%
  → calculate_adaptive_tp_sl() meng-enforce minimum ini
  → live_open() memvalidasi ulang TP inverse sebelum posisi dibuka
"""

import os
import time
import math
import threading
import queue
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Optional, Tuple, List, Dict, Any
from dotenv import load_dotenv
from binance.client import Client
import ta
import pandas as pd
import numpy as np

load_dotenv()
client = Client(os.getenv("API_KEY"), os.getenv("API_SECRET"))
client.FUTURES_URL = "https://testnet.binancefuture.com/fapi"

# ═══════════════════════════════════════════════════════════════════════════
#  CONFIG v21.1.0
# ═══════════════════════════════════════════════════════════════════════════

LEVERAGE = 20
ORDER_USDT = 2.0
MAX_POSITIONS = 3

FUTURES_FEE_PCT = 0.0005

SCAN_INTERVAL = 0.2
MONITOR_INT = 0.05
SCAN_DELAY = 0.002
BATCH_SIZE = 40
MAX_WORKERS = 20
SLOT_FILL_INT = 0.01

# ── QUALITY FILTERS ──────────────────────────────────────────────────────
MIN_QUALITY_SCORE = 50              # Turun dari 65
MAX_SIGNALS_PER_CYCLE = 3

# ── LOSS CLUSTER ─────────────────────────────────────────────────────────
MAX_CONSECUTIVE_LOSSES = 3
PAUSE_CANDLES = 3                   # 3 menit (dari 5)
CLUSTER_WINDOW_SECONDS = 1800

# ── MARKET QUALITY ───────────────────────────────────────────────────────
MAX_SPREAD_PCT = 0.0005
MIN_VOLUME_24H = 5_000_000

# ── TP/SL LIMITS ─────────────────────────────────────────────────────────
MIN_SL_PCT = 0.0025
MAX_SL_PCT = 0.008
MAX_TP_PCT = 0.015                  # 1.5% (dari 2.5%)

# ── MINIMUM TP BERSIH (NET SETELAH FEE) ──────────────────────────────────
# Fee total = fee_buka + fee_tutup = 2 × FUTURES_FEE_PCT = 0.1%
# Agar TP net ≥ 0.1%, maka TP gross ≥ 0.1% + 0.1% = 0.2%
# Rumus: MIN_TP_GROSS_PCT = MIN_TP_NET_PCT + (2 * FUTURES_FEE_PCT)
MIN_TP_NET_PCT    = 0.001           # 0.1% net yang diinginkan setelah fee
MIN_TP_GROSS_PCT  = MIN_TP_NET_PCT + (2 * FUTURES_FEE_PCT)   # = 0.002 = 0.2% gross

# ── ENTRY TIMING ─────────────────────────────────────────────────────────
MIN_PULLBACK_PCT = 0.0005           # 0.05%
MAX_PULLBACK_PCT = 0.010            # 1.0%

# ── TRAILING STOP ────────────────────────────────────────────────────────
TRAIL_ACTIVATE_PCT = 0.004          # Aktif setelah profit 0.4%
TRAIL_DISTANCE_PCT = 0.002          # Jarak trailing 0.2%

# ── PARTIAL TAKE PROFIT ───────────────────────────────────────────────────
PARTIAL_RR_RATIO = 0.5              # Partial di 0.5R
PARTIAL_CLOSE_PCT = 0.5             # Tutup 50% posisi

# ── DAILY LIMITS ─────────────────────────────────────────────────────────
DAILY_LOSS_LIMIT = -8.0
CONSEC_MAX = 15
CONSEC_PAUSE = 10

TTL_5M = 2
SLIPPAGE_GUARD = 0.0015

# ═══════════════════════════════════════════════════════════════════════════
#  SYMBOLS
# ═══════════════════════════════════════════════════════════════════════════
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

# ═══════════════════════════════════════════════════════════════════════════
#  STATE
# ═══════════════════════════════════════════════════════════════════════════
live_positions = {}
trade_log = []
_ohlcv_cache = {}
_ticker_cache = {}
_ticker_ts = 0
_lock = threading.Lock()
_executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
_rescan_q = queue.Queue()
_hot_syms = deque(maxlen=30)

_macro = {"fng": 50, "btc": "UNKNOWN", "last_fng": 0, "last_btc": 0}
_ks = {"active": False, "reason": "", "resume": 0, "consec": 0, "daily": 0.0, "day_reset": 0}
_stats = {
    "trades": 0, "wins": 0, "losses": 0, "pnl": 0.0, "best": 0.0, "worst": 0.0,
    "extreme_tp": 0, "hard_sl": 0, "force": 0, "btc_block": 0, "quality_filter": 0,
    "hist": deque(maxlen=200), "start": time.time(),
}

# ═══════════════════════════════════════════════════════════════════════════
#  LOSS CLUSTER DETECTOR
# ═══════════════════════════════════════════════════════════════════════════
class LossClusterDetector:
    def __init__(self, max_consecutive_losses=3, pause_candles=3, cluster_window=1800):
        self.consecutive_losses = 0
        self.max_loss = max_consecutive_losses
        self.pause_candles = pause_candles
        self.cluster_window = cluster_window
        self.pause_until = 0
        self.loss_timestamps = deque(maxlen=10)
        self.cluster_detected = False
    
    def record_loss(self, timestamp):
        self.consecutive_losses += 1
        self.loss_timestamps.append(timestamp)
        
        if len(self.loss_timestamps) >= 3:
            oldest = self.loss_timestamps[0]
            if timestamp - oldest < self.cluster_window:
                self.cluster_detected = True
                self.pause_until = timestamp + (self.pause_candles * 60)
                return True
        return False
    
    def record_win(self):
        self.consecutive_losses = 0
        if self.cluster_detected and time.time() > self.pause_until:
            self.cluster_detected = False
            self.loss_timestamps.clear()
    
    def can_trade(self, current_time) -> Tuple[bool, str]:
        if self.consecutive_losses >= self.max_loss:
            if current_time < self.pause_until:
                return False, f"loss_streak_{self.consecutive_losses}"
            else:
                self.consecutive_losses = 0
        
        if self.cluster_detected and current_time < self.pause_until:
            return False, "loss_cluster_pause"
        
        return True, ""
    
    def get_status(self) -> str:
        if self.cluster_detected:
            remaining = max(0, self.pause_until - time.time())
            return f"CLUSTER_PAUSE:{remaining:.0f}s"
        elif self.consecutive_losses > 0:
            return f"STREAK:{self.consecutive_losses}"
        return "ACTIVE"


# ═══════════════════════════════════════════════════════════════════════════
#  SIGNAL RANKER
# ═══════════════════════════════════════════════════════════════════════════
class SignalRanker:
    def __init__(self, max_signals_per_cycle=3, min_score_threshold=50):
        self.max_signals = max_signals_per_cycle
        self.min_score = min_score_threshold
        self.signal_history = deque(maxlen=50)
    
    def _get_sector(self, symbol: str) -> str:
        sector_map = {
            'layer1': ['BTC', 'ETH', 'BNB', 'SOL', 'ADA', 'AVAX', 'DOT', 'ATOM', 'TRX'],
            'dex': ['UNI', 'CAKE', 'SUSHI', 'CRV'],
            'defi': ['AAVE', 'COMP', 'MKR', 'SNX', 'LINK'],
            'meme': ['DOGE', 'SHIB', 'PEPE', 'WIF', '1000PEPE', '1000SHIB'],
            'gaming': ['SAND', 'MANA', 'GALA', 'APE', 'AXS'],
            'ai': ['FET', 'AGIX', 'OCEAN', 'WLD'],
            'l2': ['ARB', 'OP', 'MATIC']
        }
        for sector, tokens in sector_map.items():
            if any(t in symbol.upper() for t in tokens):
                return sector
        return 'other'
    
    def rank_and_filter(self, raw_signals: List, current_time: float, recent_loss_symbols: List = None) -> List:
        if not raw_signals:
            return []
        
        valid = [s for s in raw_signals if s[2] >= self.min_score]
        if not valid:
            return []
        
        valid_with_penalty = []
        for sig in valid:
            sym, direction, score, reasons, price, atr, sl, tp = sig
            sector = self._get_sector(sym)
            valid_with_penalty.append({
                'symbol': sym, 'direction': direction, 'score': score,
                'reasons': reasons, 'price': price, 'atr_pct': atr,
                'sl_pct': sl, 'tp_pct': tp, 'sector': sector
            })
        
        sector_counts = {}
        for v in valid_with_penalty:
            sector_counts[v['sector']] = sector_counts.get(v['sector'], 0) + 1
        
        for v in valid_with_penalty:
            if sector_counts[v['sector']] > 2:
                v['score'] *= 0.7
        
        if recent_loss_symbols:
            for v in valid_with_penalty:
                if v['symbol'] in recent_loss_symbols:
                    v['score'] *= 0.5
        
        valid_with_penalty.sort(key=lambda x: x['score'], reverse=True)
        top = valid_with_penalty[:self.max_signals]
        
        for v in top:
            self.signal_history.append({
                'symbol': v['symbol'],
                'score': v['score'],
                'timestamp': current_time
            })
        
        return [(v['symbol'], v['direction'], v['score'], v['reasons'],
                 v['price'], v['atr_pct'], v['sl_pct'], v['tp_pct']) for v in top]


# ═══════════════════════════════════════════════════════════════════════════
#  BINANCE UTILS
# ═══════════════════════════════════════════════════════════════════════════
_precision_cache = {}

def get_precision(symbol):
    if symbol in _precision_cache:
        return _precision_cache[symbol]
    try:
        info = client.futures_exchange_info()
        for s in info['symbols']:
            if s['symbol'] == symbol:
                prec = int(s['quantityPrecision'])
                _precision_cache[symbol] = prec
                return prec
    except:
        pass
    return 2

def qty(symbol, price):
    raw_qty = (ORDER_USDT * LEVERAGE) / price
    prec = get_precision(symbol)
    return round(raw_qty, prec)

def price_live(symbol):
    try:
        return float(client.futures_symbol_ticker(symbol=symbol)["price"])
    except:
        return 0.0

def tickers_all():
    global _ticker_cache, _ticker_ts
    now = time.time()
    if now - _ticker_ts < 2 and _ticker_cache:
        return _ticker_cache
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
    except:
        return _ticker_cache

def ohlcv(symbol, interval, limit=100):
    key, now = (symbol, interval), time.time()
    ttl = TTL_5M
    if key in _ohlcv_cache and now - _ohlcv_cache[key][0] < ttl:
        return _ohlcv_cache[key][1]
    try:
        kl = client.futures_klines(symbol=symbol, interval=interval, limit=limit)
        df = pd.DataFrame(kl, columns=["time", "open", "high", "low", "close", "volume",
                                       "ct", "qv", "trades", "tbbase", "tbquote", "ignore"])
        for c in ["open", "high", "low", "close", "volume", "tbbase", "tbquote"]:
            df[c] = df[c].astype(float)
        _ohlcv_cache[key] = (now, df)
        return df
    except:
        return _ohlcv_cache.get(key, (None, None))[1]

def run_ta(df):
    c, h, l, v = df["close"], df["high"], df["low"], df["volume"]
    df["rsi"] = ta.momentum.RSIIndicator(c, 14).rsi()
    df["mh"] = ta.trend.MACD(c, 12, 26, 9).macd_diff()
    df["e5"] = ta.trend.EMAIndicator(c, 5).ema_indicator()
    df["e9"] = ta.trend.EMAIndicator(c, 9).ema_indicator()
    df["e21"] = ta.trend.EMAIndicator(c, 21).ema_indicator()
    df["e50"] = ta.trend.EMAIndicator(c, 50).ema_indicator()
    df["atr"] = ta.volatility.AverageTrueRange(h, l, c, 14).average_true_range()
    df["adx"] = ta.trend.ADXIndicator(h, l, c, 14).adx()
    df["vm"] = v.rolling(20).mean()
    df["vr"] = v / df["vm"].replace(0, 1)
    df["br"] = df["tbbase"] / df["volume"].replace(0, 1)
    df["body"] = abs(c - df["open"])
    df["rng"] = h - l
    df["br2"] = df["body"] / df["rng"].replace(0, 1)
    df["m5"] = (c - c.shift(5)) / c.shift(5)
    df["m3"] = (c - c.shift(3)) / c.shift(3)
    return df

def btc_trend():
    try:
        df = run_ta(ohlcv("BTCUSDT", Client.KLINE_INTERVAL_5MINUTE, 80).copy())
        row = df.iloc[-2]
        p, e5, e9, e21, m5 = row["close"], row["e5"], row["e9"], row["e21"], row["m5"]
        if p > e5 > e9 > e21 and m5 > 0.001:
            return "BULL"
        if p < e5 < e9 < e21 and m5 < -0.001:
            return "BEAR"
        if p > e9 > e21:
            return "MILD_BULL"
        if p < e9 < e21:
            return "MILD_BEAR"
        return "SIDEWAYS"
    except:
        return "UNKNOWN"

def ks_check():
    k, now = _ks, time.time()
    if k["active"] and now >= k["resume"]:
        k["active"] = False
        k["consec"] = 0
    if k["active"]:
        return True, k["reason"]
    day = now - (now % 86400)
    if day > k["day_reset"]:
        k["daily"] = 0.0
        k["day_reset"] = day
    if k["daily"] <= DAILY_LOSS_LIMIT:
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


# ═══════════════════════════════════════════════════════════════════════════
#  REGIME DETECTION & ADAPTIVE TP/SL
# ═══════════════════════════════════════════════════════════════════════════
def get_regime(df):
    adx = df["adx"].iloc[-2]
    atr_pct = df["atr"].iloc[-2] / df["close"].iloc[-2]
    if adx > 25 and atr_pct > 0.002:
        return "TRENDING"
    elif adx < 20:
        return "RANGING"
    else:
        return "TRANSITION"

def calculate_adaptive_tp_sl(df, direction, btc_trend, regime, quality_score):
    atr_pct = df['atr'].iloc[-2] / df['close'].iloc[-2]
    adx = df['adx'].iloc[-2]

    if regime == "TRENDING" and adx > 30:
        sl_mult = 0.8
    elif regime == "TRENDING":
        sl_mult = 1.0
    elif regime == "RANGING":
        sl_mult = 1.5
    else:
        sl_mult = 1.2

    sl_pct = atr_pct * sl_mult
    sl_pct = max(sl_pct, MIN_SL_PCT)
    sl_pct = min(sl_pct, MAX_SL_PCT)

    if quality_score >= 80:
        rr = 2.5
    elif quality_score >= 65:
        rr = 2.0
    elif quality_score >= 50:
        rr = 1.8
    else:
        return None, None

    tp_pct = sl_pct * rr
    tp_pct = min(tp_pct, MAX_TP_PCT)

    if (direction == "LONG" and btc_trend in ["BULL", "MILD_BULL"]) or \
       (direction == "SHORT" and btc_trend in ["BEAR", "MILD_BEAR"]):
        tp_pct *= 1.15

    # ─── ENFORCE MINIMUM TP GROSS ────────────────────────────────────────
    # TP gross harus ≥ MIN_TP_GROSS_PCT agar setelah dikurangi fee,
    # profit bersih tetap ≥ MIN_TP_NET_PCT (0.1%).
    # Catatan: nilai ini berlaku untuk TP *normal* (sebelum inverse).
    # Pada live_open(), TP inverse (= SL lama) akan divalidasi ulang.
    if tp_pct < MIN_TP_GROSS_PCT:
        tp_pct = MIN_TP_GROSS_PCT

    return sl_pct, tp_pct


# ═══════════════════════════════════════════════════════════════════════════
#  QUALITY SCORE (0-100)
# ═══════════════════════════════════════════════════════════════════════════
def calculate_quality_score(df, direction, btc_trend):
    row = df.iloc[-2]
    
    score = 50
    bonus = []
    
    # === KONTRINDIKATOR (PENALTY) ===
    if direction == "LONG":
        if row['rsi'] > 75:
            score -= 25
            bonus.append("RSI_too_high")
        elif row['rsi'] > 70:
            score -= 10
            bonus.append("RSI_high")
        
        if row['m5'] > 0.008:
            score -= 20
            bonus.append("momentum_too_high")
        elif row['m5'] > 0.006:
            score -= 10
            bonus.append("momentum_high")
    else:
        if row['rsi'] < 25:
            score -= 25
            bonus.append("RSI_too_low")
        elif row['rsi'] < 30:
            score -= 10
            bonus.append("RSI_low")
        
        if row['m5'] < -0.008:
            score -= 20
            bonus.append("momentum_too_low")
        elif row['m5'] < -0.006:
            score -= 10
            bonus.append("momentum_low")
    
    if row['vr'] > 2.5:
        score -= 30
        bonus.append("volume_climax")
    elif row['vr'] > 2.0:
        score -= 15
        bonus.append("high_volume")
    
    if row['br2'] > 0.85:
        score -= 20
        bonus.append("marubozu_chase")
    elif row['br2'] > 0.75:
        score -= 10
        bonus.append("long_candle")
    
    if row['adx'] > 45:
        score -= 15
        bonus.append("adx_over_extended")
    elif row['adx'] > 40:
        score -= 8
        bonus.append("adx_high")
    
    # === KONFIRMATOR (BONUS) ===
    if direction == "LONG":
        high_3 = df['high'].iloc[-5:-2].max()
        pullback_pct = (high_3 - row['close']) / high_3 if high_3 > 0 else 0
        if MIN_PULLBACK_PCT < pullback_pct < MAX_PULLBACK_PCT:
            score += 25
            bonus.append(f"pullback_{pullback_pct:.3f}")
    else:
        low_3 = df['low'].iloc[-5:-2].min()
        pullback_pct = (row['close'] - low_3) / low_3 if low_3 > 0 else 0
        if MIN_PULLBACK_PCT < pullback_pct < MAX_PULLBACK_PCT:
            score += 25
            bonus.append(f"pullback_{pullback_pct:.3f}")
    
    if direction == "LONG" and row['close'] > row['e9'] > row['e21']:
        score += 15
        bonus.append("ema_aligned")
    elif direction == "SHORT" and row['close'] < row['e9'] < row['e21']:
        score += 15
        bonus.append("ema_aligned")
    
    # === BTC ALIGNMENT (Penalty, bukan reject) ===
    if (direction == "LONG" and btc_trend in ["BULL", "MILD_BULL"]) or \
       (direction == "SHORT" and btc_trend in ["BEAR", "MILD_BEAR"]):
        score += 20
        bonus.append("btc_aligned")
    elif btc_trend in ["SIDEWAYS", "UNKNOWN"]:
        pass
    else:
        score -= 20           # Penalty 20% (tidak reject)
        bonus.append("btc_opposing")
    
    if 1.2 < row['vr'] < 2.0:
        score += 15
        bonus.append("healthy_volume")
    
    if direction == "LONG" and 0.002 < row['m5'] < 0.006:
        score += 10
        bonus.append("momentum_moderate")
    elif direction == "SHORT" and -0.006 < row['m5'] < -0.002:
        score += 10
        bonus.append("momentum_moderate")
    
    score = max(0, min(100, score))
    return score, bonus


# ═══════════════════════════════════════════════════════════════════════════
#  ENTRY CONFIRMATION (FLEKSIBEL)
# ═══════════════════════════════════════════════════════════════════════════
def confirm_entry(df, direction):
    """
    Entry diizinkan jika:
    - Bukan candle breakout pada candle SEDANG
    - Atau sudah terjadi retest dalam 2 candle terakhir
    """
    if df is None or len(df) < 10:
        return False, "insufficient_data"
    
    current = df.iloc[-2]
    prev = df.iloc[-3]
    prev2 = df.iloc[-4]
    
    if direction == "LONG":
        resistance = max(prev2['high'], prev['high'])
        
        breakout_candle_prev = prev['close'] > resistance and prev['close'] > prev['open'] * 1.003
        is_retesting = current['low'] <= resistance + (resistance * 0.0015)
        
        if current['close'] > resistance and current['close'] > current['open'] * 1.003:
            return False, "currently_breaking_out"
        
        if breakout_candle_prev and is_retesting:
            return True, "retest_after_breakout"
        
        if is_retesting and current['close'] > current['open']:
            return True, "retest_without_breakout"
        
        return False, "waiting_retest"
    
    else:  # SHORT
        support = min(prev2['low'], prev['low'])
        breakout_candle_prev = prev['close'] < support and prev['close'] < prev['open'] * 0.997
        is_retesting = current['high'] >= support - (support * 0.0015)
        
        if current['close'] < support and current['close'] < current['open'] * 0.997:
            return False, "currently_breaking_out"
        
        if breakout_candle_prev and is_retesting:
            return True, "retest_after_breakout"
        
        if is_retesting and current['close'] < current['open']:
            return True, "retest_without_breakout"
        
        return False, "waiting_retest"


# ═══════════════════════════════════════════════════════════════════════════
#  MULTI-TIMEFRAME CONFIRMATION
# ═══════════════════════════════════════════════════════════════════════════
def multi_timeframe_confirmation(symbol, direction, btc_trend):
    try:
        df_15m = run_ta(ohlcv(symbol, Client.KLINE_INTERVAL_15MINUTE, 50).copy())
        df_5m = run_ta(ohlcv(symbol, Client.KLINE_INTERVAL_5MINUTE, 100).copy())
        df_1m = run_ta(ohlcv(symbol, Client.KLINE_INTERVAL_1MINUTE, 50).copy())
        
        if any(df is None for df in [df_15m, df_5m, df_1m]):
            return False, "data_missing"
        
        row_15m = df_15m.iloc[-2]
        row_5m = df_5m.iloc[-2]
        row_1m = df_1m.iloc[-2]
        
        conf_score = 0
        reasons = []
        
        if direction == "LONG":
            if row_15m['close'] > row_15m['e21']:
                conf_score += 25
                reasons.append("15m_trend_up")
            if row_15m['rsi'] > 70 and row_15m['adx'] > 35:
                return False, "15m_overbought"
        else:
            if row_15m['close'] < row_15m['e21']:
                conf_score += 25
                reasons.append("15m_trend_down")
            if row_15m['rsi'] < 30 and row_15m['adx'] > 35:
                return False, "15m_oversold"
        
        if direction == "LONG":
            if row_5m['close'] > row_5m['e9'] and row_5m['m5'] > 0:
                conf_score += 20
                reasons.append("5m_setup")
        else:
            if row_5m['close'] < row_5m['e9'] and row_5m['m5'] < 0:
                conf_score += 20
                reasons.append("5m_setup")
        
        if direction == "LONG":
            low_5 = df_1m['low'].iloc[-6:-1].min()
            current = row_1m['close']
            pullback = (current - low_5) / low_5 if low_5 > 0 else 0
            if MIN_PULLBACK_PCT < pullback < MAX_PULLBACK_PCT:
                conf_score += 30
                reasons.append(f"1m_pullback_{pullback:.3f}")
        else:
            high_5 = df_1m['high'].iloc[-6:-1].max()
            current = row_1m['close']
            pullback = (high_5 - current) / high_5 if high_5 > 0 else 0
            if MIN_PULLBACK_PCT < pullback < MAX_PULLBACK_PCT:
                conf_score += 30
                reasons.append(f"1m_pullback_{pullback:.3f}")
        
        if conf_score >= 50:
            return True, "|".join(reasons)
        return False, f"low_conf_{conf_score}"
        
    except Exception as e:
        return False, f"mtf_error"


# ═══════════════════════════════════════════════════════════════════════════
#  MARKET QUALITY FILTER
# ═══════════════════════════════════════════════════════════════════════════
def check_market_quality(symbol):
    try:
        orderbook = client.futures_order_book(symbol=symbol, limit=10)
        best_bid = float(orderbook['bids'][0][0])
        best_ask = float(orderbook['asks'][0][0])
        spread_pct = (best_ask - best_bid) / best_bid
        
        if spread_pct > MAX_SPREAD_PCT:
            return False, f"wide_spread_{spread_pct:.4f}"
        
        ticker = tickers_all().get(symbol, {})
        vol_24h = ticker.get('vol', 0)
        if vol_24h < MIN_VOLUME_24H:
            return False, f"low_volume_{vol_24h/1e6:.1f}M"
        
        return True, "good"
    except Exception as e:
        return False, f"orderbook_error"


# ═══════════════════════════════════════════════════════════════════════════
#  SIGNAL v21.1.0
# ═══════════════════════════════════════════════════════════════════════════
def signal_v21(df, btc_trend, loss_detector: LossClusterDetector = None):
    
    if df is None or len(df) < 55:
        return None, 0, [], 0.0, 0.0, 0.0
    
    row = df.iloc[-2]
    
    if row['vr'] < 0.8:
        return None, 0, ["low_volume"], 0, 0, 0
    
    if row['br2'] < 0.3:
        return None, 0, ["doji"], 0, 0, 0
    
    regime = get_regime(df)
    
    direction = None
    base_score = 0
    reasons = []
    pullback_pct = 0
    
    high_10 = df['high'].iloc[-12:-2].max()
    low_10 = df['low'].iloc[-12:-2].min()
    
    pullback_from_high = (high_10 - row['close']) / high_10 if high_10 > 0 else 0
    pullback_from_low = (row['close'] - low_10) / low_10 if low_10 > 0 else 0
    
    if MIN_PULLBACK_PCT < pullback_from_high < MAX_PULLBACK_PCT:
        if row['close'] > row['e21']:
            direction = "LONG"
            base_score = 60
            reasons.append(f"pullback_{pullback_from_high:.3f}")
            pullback_pct = pullback_from_high
    
    if MIN_PULLBACK_PCT < pullback_from_low < MAX_PULLBACK_PCT:
        if row['close'] < row['e21']:
            direction = "SHORT"
            base_score = 60
            reasons.append(f"pullback_{pullback_from_low:.3f}")
            pullback_pct = pullback_from_low
    
    if not direction:
        return None, 0, ["no_pullback"], 0, 0, 0
    
    quality_score, quality_reasons = calculate_quality_score(df, direction, btc_trend)
    reasons.extend(quality_reasons)
    
    final_score = base_score + (quality_score - 50)
    final_score = max(0, min(100, final_score))
    
    if final_score < MIN_QUALITY_SCORE:
        return None, 0, [f"low_score_{final_score}"], 0, 0, 0
    
    confirmed, confirm_reason = confirm_entry(df, direction)
    if not confirmed:
        return None, 0, [confirm_reason], 0, 0, 0
    reasons.append(confirm_reason)
    
    atr_pct = row['atr'] / row['close']
    sl_pct, tp_pct = calculate_adaptive_tp_sl(df, direction, btc_trend, regime, final_score)
    
    if sl_pct is None or tp_pct is None:
        return None, 0, ["invalid_risk"], 0, 0, 0
    
    return direction, final_score, reasons, atr_pct, sl_pct, tp_pct


# ═══════════════════════════════════════════════════════════════════════════
#  DRY RUN OPEN (MODIFIED: INVERSE STRATEGY)
# ═══════════════════════════════════════════════════════════════════════════
def live_open(sym, direction, score, sigs, price, atr_pct, sl_pct, tp_pct):
    with _lock:
        if sym in live_positions or len(live_positions) >= MAX_POSITIONS:
            return
        live_positions[sym] = {"_r": True}
    
    quality_ok, quality_msg = check_market_quality(sym)
    if not quality_ok:
        with _lock:
            live_positions.pop(sym, None)
        print(f"  ⚠️ [SKIP] {sym} market quality: {quality_msg}")
        return
    
    # Filter MTF tetap memvalidasi arah original agar filter tidak rusak
    mtf_ok, mtf_reason = multi_timeframe_confirmation(sym, direction, _macro["btc"])
    if not mtf_ok:
        with _lock:
            live_positions.pop(sym, None)
        print(f"  ⚠️ [SKIP] {sym} MTF: {mtf_reason}")
        return
    
    px_now = price_live(sym)
    if px_now <= 0:
        with _lock:
            live_positions.pop(sym, None)
        return
    
    slip = abs(px_now - price) / price
    if slip > SLIPPAGE_GUARD:
        with _lock:
            live_positions.pop(sym, None)
        print(f"  ⚠️ [SKIP] {sym} slippage: {slip:.4f}")
        return
    price = px_now
    
    try:
        q_val = qty(sym, price)
        if q_val <= 0:
            raise ValueError("invalid qty")
    except:
        with _lock:
            live_positions.pop(sym, None)
        return
    
    # =======================================================
    # INVERSE STRATEGY (Diterapkan setelah semua filter lolos)
    # =======================================================
    # 1. Balik arah entry
    inv_direction = "SHORT" if direction == "LONG" else "LONG"

    # 2. Tukar persentase TP dan SL
    inv_sl_pct = tp_pct  # ExtremeTP lama menjadi HardSL baru
    inv_tp_pct = sl_pct  # HardSL lama menjadi target TP baru

    # ─── VALIDASI MINIMUM TP INVERSE (BERSIH DARI FEE) ──────────────────
    # Setelah inverse, TP = sl_pct lama yang bisa saja lebih kecil dari
    # MIN_TP_GROSS_PCT. Enforce di sini agar profit bersih ≥ 0.1%.
    if inv_tp_pct < MIN_TP_GROSS_PCT:
        inv_tp_pct = MIN_TP_GROSS_PCT
        print(f"  ℹ️  [TP_FLOOR] {sym} inv_tp naik ke {inv_tp_pct*100:.2f}% (min net 0.1%)")

    # 3. Hitung harga berdasarkan arah inverse yang baru
    if inv_direction == "LONG":
        sl_price = price * (1 - inv_sl_pct)
        tp_price = price * (1 + inv_tp_pct)
    else:
        sl_price = price * (1 + inv_sl_pct)
        tp_price = price * (1 - inv_tp_pct)

    # Daftarkan posisi dengan properti yang sudah di-inverse
    pos = {
        "side": inv_direction,
        "entry": price,
        "qty": q_val,
        "open_time": time.time(),
        "score": score,
        "sigs": sigs,
        "atr_pct": atr_pct,
        "sl_price": sl_price,
        "tp_price": tp_price,
        "sl_pct": inv_sl_pct,
        "tp_pct": inv_tp_pct,
        "partial_hit": False,
        "peak": price,
        "trough": price
    }
    
    with _lock:
        live_positions[sym] = pos
    
    d = "🟢" if inv_direction == "LONG" else "🔴"
    print(f"\n  {d} [DRY INVERSE] {sym} {inv_direction} @{price:.6g} SL:{inv_sl_pct*100:.2f}% TP:{inv_tp_pct*100:.2f}% (net≥{MIN_TP_NET_PCT*100:.1f}%) [{' | '.join(sigs[:5])}]")
    print(f"        Quality Score: {score}, MTF: {mtf_reason} (Orig Signal: {direction})")
    _stats["trades"] += 1


# ═══════════════════════════════════════════════════════════════════════════
#  DRY RUN CLOSE
# ═══════════════════════════════════════════════════════════════════════════
def live_close(sym, reason, price=None, partial_qty=None):
    with _lock:
        pos = live_positions.pop(sym, None) if partial_qty is None else live_positions.get(sym)
    if pos is None or pos.get("_r"):
        return
    
    if price is None:
        price = price_live(sym)
    if price == 0:
        return
    
    side, entry, q_val = pos["side"], pos["entry"], pos["qty"]
    if partial_qty is not None:
        q_val = partial_qty
        with _lock:
            if sym in live_positions:
                live_positions[sym]["qty"] -= partial_qty
                live_positions[sym]["partial_hit"] = True
    
    gross_pnl = (price - entry) * q_val if side == "LONG" else (entry - price) * q_val
    open_fee = (entry * q_val) * FUTURES_FEE_PCT
    close_fee = (price * q_val) * FUTURES_FEE_PCT
    total_fee = open_fee + close_fee
    pnl = gross_pnl - total_fee
    
    pct = (price - entry) / entry * 100 if side == "LONG" else (entry - price) / entry * 100
    hold = time.time() - pos["open_time"]
    e = "🟢" if pnl >= 0 else "🔴"
    
    if partial_qty:
        print(f"  🎯 [PARTIAL] {sym} {side} @{price:.6g} (+{pct:.2f}%) PnL:{pnl:+.5f}U | Hold:{hold:.0f}s")
        _stats["pnl"] += pnl
        _stats["hist"].append(pnl)
        ks_upd(pnl)
        if pnl >= 0:
            _stats["wins"] += 0.5
        else:
            _stats["losses"] += 0.5
        return
    
    print(f"  {e} [DRY INVERSE] {sym} {side} CLOSE — {reason}")
    print(f"     {entry:.6g}→{price:.6g} ({pct:+.3f}%) hold:{hold:.0f}s | PnL Net:{pnl:+.5f}U (Fee:{total_fee:.5f}U)")
    
    _stats["pnl"] += pnl
    _stats["hist"].append(pnl)
    ks_upd(pnl)
    
    if pnl >= 0:
        _stats["wins"] += 1
        if pnl > _stats["best"]:
            _stats["best"] = pnl
    else:
        _stats["losses"] += 1
        if pnl < _stats["worst"]:
            _stats["worst"] = pnl
    
    if "ExtremeTP" in reason:
        _stats["extreme_tp"] += 1
    elif "HardSL" in reason:
        _stats["hard_sl"] += 1
    
    trade_log.append({
        "sym": sym, "side": side, "entry": round(entry, 7), "exit": round(price, 7),
        "pnl": round(pnl, 5), "reason": reason, "hold": int(hold),
    })
    _hot_syms.appendleft(sym)
    _rescan_q.put(1)
    print_inline()


# ═══════════════════════════════════════════════════════════════════════════
#  MONITOR POSITIONS (dengan TRAILING STOP & PARTIAL TP)
# ═══════════════════════════════════════════════════════════════════════════
def monitor_positions():
    for sym in list(live_positions.keys()):
        pos = live_positions.get(sym)
        if pos is None or pos.get("_r"):
            continue
        
        px = price_live(sym)
        if px == 0:
            continue
        
        side = pos["side"]
        entry = pos["entry"]
        sl_px = pos["sl_price"]
        tp_px = pos["tp_price"]
        hold = time.time() - pos["open_time"]
        
        # === HITUNG PROFIT PERSEN ===
        if side == "LONG":
            prof_pct = (px - entry) / entry
        else:
            prof_pct = (entry - px) / entry
        
        # === TRAILING STOP (setelah profit > TRAIL_ACTIVATE_PCT) ===
        if prof_pct > TRAIL_ACTIVATE_PCT:
            if side == "LONG":
                if px > pos.get("peak", entry):
                    pos["peak"] = px
                new_sl = pos["peak"] * (1 - TRAIL_DISTANCE_PCT)
                if new_sl > pos["sl_price"]:
                    pos["sl_price"] = new_sl
                    sl_px = new_sl
                    print(f"    🏃 Trailing SL {sym} -> {new_sl:.6g} (profit {prof_pct*100:.2f}%)")
            else:  # SHORT
                if px < pos.get("trough", entry):
                    pos["trough"] = px
                new_sl = pos["trough"] * (1 + TRAIL_DISTANCE_PCT)
                if new_sl < pos["sl_price"]:
                    pos["sl_price"] = new_sl
                    sl_px = new_sl
                    print(f"    🏃 Trailing SL {sym} -> {new_sl:.6g} (profit {prof_pct*100:.2f}%)")
        
        # === PARTIAL TAKE PROFIT (50% di 0.5R) ===
        if not pos.get("partial_hit", False) and prof_pct > (pos["tp_pct"] * PARTIAL_RR_RATIO):
            partial_qty = pos["qty"] * PARTIAL_CLOSE_PCT
            if partial_qty > 0:
                live_close(sym, "PartialTP", px, partial_qty)
                continue
        
        # === CEK SL/TP ===
        if side == "LONG":
            if px <= sl_px:
                live_close(sym, "HardSL", px)
                continue
            if px >= tp_px:
                live_close(sym, "ExtremeTP", px)
                continue
            pnl_now = ((px - entry) * pos["qty"]) - ((entry * pos["qty"] + px * pos["qty"]) * FUTURES_FEE_PCT)
            print(f"    📌 {sym} L@{entry:.5g}→{px:.5g}({prof_pct*100:+.2f}%) {pnl_now:+.4f}U {hold:.0f}s [DRY]")
        else:
            if px >= sl_px:
                live_close(sym, "HardSL", px)
                continue
            if px <= tp_px:
                live_close(sym, "ExtremeTP", px)
                continue
            pnl_now = ((entry - px) * pos["qty"]) - ((entry * pos["qty"] + px * pos["qty"]) * FUTURES_FEE_PCT)
            print(f"    📌 {sym} S@{entry:.5g}→{px:.5g}({prof_pct*100:+.2f}%) {pnl_now:+.4f}U {hold:.0f}s [DRY]")


# ═══════════════════════════════════════════════════════════════════════════
#  SCANNER
# ═══════════════════════════════════════════════════════════════════════════
def scan_one(sym, loss_detector: LossClusterDetector):
    try:
        time.sleep(SCAN_DELAY)
        df5 = run_ta(ohlcv(sym, Client.KLINE_INTERVAL_5MINUTE, 100).copy())
        if df5 is None:
            return None
        
        px = df5["close"].iloc[-2]
        if px == 0:
            return None
        
        dir_, sc, sigs, atr_pct_val, sl_p, tp_p = signal_v21(df5, _macro["btc"], loss_detector)
        if dir_ is None:
            return None
        
        px_live = price_live(sym)
        if px_live == 0:
            return None
        
        return (sym, dir_, sc, sigs, px_live, atr_pct_val, sl_p, tp_p)
    except Exception as e:
        return None


def scan_batch(syms, loss_detector: LossClusterDetector):
    res = []
    fut = {_executor.submit(scan_one, s, loss_detector): s for s in syms[:BATCH_SIZE]}
    try:
        for f in as_completed(fut, timeout=5):
            try:
                if r := f.result(timeout=1):
                    res.append(r)
            except:
                pass
    except:
        pass
    return res


def top_movers(syms, n=30):
    tk, ss = tickers_all(), set(syms)
    mv = [(s, abs(d["pct"])) for s, d in tk.items() if s in ss]
    return [s for s, _ in sorted(mv, key=lambda x: x[1], reverse=True)[:n]]


# ═══════════════════════════════════════════════════════════════════════════
#  PRINT UTILS
# ═══════════════════════════════════════════════════════════════════════════
def print_inline():
    n = _stats["wins"] + _stats["losses"]
    wr = _stats["wins"] / n * 100 if n else 0
    pnl, e = _stats["pnl"], "💚" if _stats["pnl"] >= 0 else "🔴"
    print(f"       ┌ [v21.1.0 DRY INVERSE] {n}T WR:{wr:.0f}% W:{_stats['wins']:.1f} L:{_stats['losses']:.1f} {e}PnL Net:{pnl:+.4f}U")
    print(f"       └ ExTP:{_stats['extreme_tp']} HardSL:{_stats['hard_sl']}")


def print_full():
    n = _stats["wins"] + _stats["losses"]
    wr = _stats["wins"] / n * 100 if n else 0
    pnl = _stats["pnl"]
    sess = (time.time() - _stats["start"]) / 3600
    tph = n / sess if sess > 0 else 0
    e = "💚" if pnl >= 0 else "🔴"
    
    print(f"\n  {'─'*70}")
    print(f"    ✅ DRY RUN v21.1.0 [INVERSE STRATEGY + TRAILING + PARTIAL]")
    print(f"    🎯 {n}T WR:{wr:.0f}% W:{_stats['wins']:.1f} L:{_stats['losses']:.1f} ({tph:.1f}T/hr)")
    print(f"    {e} PnL Net:{pnl:+.5f}U Best:{_stats['best']:+.5f} Worst:{_stats['worst']:+.5f}")
    print(f"    💰 ExtremeTP:{_stats['extreme_tp']} HardSL:{_stats['hard_sl']}")
    print(f"    ⚙️  Config: Leverage={LEVERAGE} Order={ORDER_USDT}U MaxPos={MAX_POSITIONS} MinScore={MIN_QUALITY_SCORE}")
    print(f"    🎯 Min TP Net: {MIN_TP_NET_PCT*100:.1f}% | Min TP Gross: {MIN_TP_GROSS_PCT*100:.2f}%")
    if trade_log:
        print(f"    📋 Last 5:")
        for t in trade_log[-5:]:
            em = "🟢" if t["pnl"] > 0 else "🔴"
            print(f"       {em} {t['sym']:<16} {t['side']} {t['pnl']:+.5f}U {t['hold']}s — {t['reason']}")
    print(f"  {'─'*70}")


# ═══════════════════════════════════════════════════════════════════════════
#  THREADS
# ═══════════════════════════════════════════════════════════════════════════
def t_monitor():
    while True:
        try:
            if live_positions:
                monitor_positions()
        except:
            pass
        time.sleep(MONITOR_INT)


def t_slot_filler(syms, loss_detector: LossClusterDetector, signal_ranker: SignalRanker):
    scan_idx = 0
    n_bat = math.ceil(len(syms) / BATCH_SIZE)
    
    while True:
        try:
            can_trade, reason = loss_detector.can_trade(time.time())
            if not can_trade:
                time.sleep(SLOT_FILL_INT)
                continue
            
            slots = MAX_POSITIONS - len(live_positions)
            if slots <= 0 or ks_check()[0]:
                time.sleep(SLOT_FILL_INT)
                continue
            
            hot = [s for s in _hot_syms if s not in live_positions]
            mv = top_movers(syms, 30)
            mv = [s for s in mv if s not in live_positions]
            
            bs = scan_idx * BATCH_SIZE
            reg = [s for s in syms[bs:bs+BATCH_SIZE] if s not in live_positions and s not in mv]
            scan_idx = (scan_idx + 1) % n_bat
            
            scan_list = list(dict.fromkeys(hot[:5] + mv[:20] + reg[:15]))[:BATCH_SIZE]
            if not scan_list:
                time.sleep(SLOT_FILL_INT)
                continue
            
            res = scan_batch(scan_list, loss_detector)
            if res:
                recent_loss = [t['sym'] for t in trade_log[-5:] if t['pnl'] < 0]
                ranked = signal_ranker.rank_and_filter(res, time.time(), recent_loss)
                
                for r in ranked[:slots]:
                    if len(live_positions) >= MAX_POSITIONS:
                        break
                    sym, d, sc, sg, px, atr, sl_p, tp_p = r
                    live_open(sym, d, sc, sg, px, atr, sl_p, tp_p)
                    
        except Exception as e:
            pass
        time.sleep(SLOT_FILL_INT)


def t_rescan(syms, loss_detector: LossClusterDetector, signal_ranker: SignalRanker):
    while True:
        try:
            _rescan_q.get(timeout=5)
            time.sleep(0.05)
            
            can_trade, reason = loss_detector.can_trade(time.time())
            if not can_trade:
                continue
                
            slots = MAX_POSITIONS - len(live_positions)
            if slots <= 0 or ks_check()[0]:
                continue
            
            hot = [s for s in _hot_syms if s not in live_positions]
            rest = [s for s in syms if s not in live_positions and s not in hot]
            res = scan_batch((hot + rest)[:30], loss_detector)
            if res:
                recent_loss = [t['sym'] for t in trade_log[-5:] if t['pnl'] < 0]
                ranked = signal_ranker.rank_and_filter(res, time.time(), recent_loss)
                for r in ranked[:slots]:
                    if len(live_positions) >= MAX_POSITIONS:
                        break
                    sym, d, sc, sg, px, atr, sl_p, tp_p = r
                    live_open(sym, d, sc, sg, px, atr, sl_p, tp_p)
        except:
            pass


def t_macro():
    while True:
        try:
            _macro["btc"] = btc_trend()
        except:
            pass
        time.sleep(10)


# ═══════════════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════════════
def run_bot():
    print("╔══════════════════════════════════════════════════════════════════════════╗")
    print("║  ✅ DRY RUN v21.1.0 — INVERSE STRATEGY + TRAILING + PARTIAL              ║")
    print("║  ✅ LOGIKA ENTRY & TP/SL DIBALIK (LONG->SHORT, SHORT->LONG)              ║")
    print("║  ✅ Quality Score 50+ | Loss Cluster 3 menit                             ║")
    print("║  ✅ Leverage 20 | Order 2 USDT | Max 3 posisi | Top 3 signals only       ║")
    print(f"║  ✅ Min TP Net {MIN_TP_NET_PCT*100:.1f}% (gross {MIN_TP_GROSS_PCT*100:.2f}%) setelah fee 2×{FUTURES_FEE_PCT*100:.2f}%          ║")
    print("╚══════════════════════════════════════════════════════════════════════════╝")
    
    try:
        valid = {s["symbol"] for s in client.futures_exchange_info()["symbols"] if s["status"] == "TRADING"}
        syms = list(dict.fromkeys([s for s in SYMBOLS if s in valid]))
    except:
        syms = list(dict.fromkeys(SYMBOLS))
    
    print(f"  📋 {len(syms)} simbol aktif terpantau")
    
    loss_detector = LossClusterDetector(
        max_consecutive_losses=MAX_CONSECUTIVE_LOSSES,
        pause_candles=PAUSE_CANDLES,
        cluster_window=CLUSTER_WINDOW_SECONDS
    )
    signal_ranker = SignalRanker(
        max_signals_per_cycle=MAX_SIGNALS_PER_CYCLE,
        min_score_threshold=MIN_QUALITY_SCORE
    )
    
    threading.Thread(target=t_monitor, daemon=True).start()
    threading.Thread(target=t_slot_filler, args=(syms, loss_detector, signal_ranker), daemon=True).start()
    threading.Thread(target=t_rescan, args=(syms, loss_detector, signal_ranker), daemon=True).start()
    threading.Thread(target=t_macro, daemon=True).start()
    
    time.sleep(2)
    tickers_all()
    
    cycle = 0
    while True:
        cycle += 1
        slots = MAX_POSITIONS - len(live_positions)
        cluster_status = loss_detector.get_status()
        
        print(f"\n{'═'*62}")
        print(f"  #{cycle} {time.strftime('%H:%M:%S')} BTC:{_macro['btc']} ({len(live_positions)}/{MAX_POSITIONS}) PnL:{_stats['pnl']:+.4f}U | {cluster_status}")
        
        if (k := ks_check())[0]:
            print(f"  🚨 KS:{k[1]}")
        elif slots == 0:
            print(f"  ✅ Slots full")
        else:
            print(f"  🔍 {slots} slot kosong — Scanning for pullback continuation...")
        
        if cycle % 30 == 0:
            print_full()
        
        time.sleep(SCAN_INTERVAL)


if __name__ == "__main__":
    run_bot()
