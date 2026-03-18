import os
import re
import time
import secrets
import requests
import threading
import numpy as np
import pandas as pd
import yfinance as yf
import xml.etree.ElementTree as ET
from datetime import date, timedelta, datetime
from concurrent.futures import ThreadPoolExecutor
from flask import Flask, send_file, request, jsonify, redirect, url_for, session, send_from_directory, make_response
import json
from werkzeug.security import generate_password_hash, check_password_hash
from scipy.stats import skew, kurtosis
from sklearn.linear_model import ElasticNetCV
from sklearn.covariance import LedoitWolf

BASE = os.path.dirname(os.path.abspath(__file__))
_DISK_CACHE_DIR = os.path.join(BASE, ".cache")
os.makedirs(_DISK_CACHE_DIR, exist_ok=True)


def _disk_cache_save(name: str, data: dict):
    """Save cache data to disk as JSON with timestamp."""
    try:
        path = os.path.join(_DISK_CACHE_DIR, f"{name}.json")
        with open(path, "w") as f:
            json.dump({"ts": time.time(), "data": data}, f)
        print(f"[DISK CACHE] Saved {name} ({os.path.getsize(path) // 1024}KB)")
    except Exception as e:
        print(f"[DISK CACHE] Error saving {name}: {e}")


def _disk_cache_load(name: str):
    """Load cache data from disk if not expired. Returns (data, ts) or (None, 0)."""
    try:
        path = os.path.join(_DISK_CACHE_DIR, f"{name}.json")
        if not os.path.exists(path):
            return None, 0
        with open(path) as f:
            obj = json.load(f)
        ts = obj.get("ts", 0)
        if _cache_expired(ts):
            print(f"[DISK CACHE] {name} expired")
            return None, 0
        print(f"[DISK CACHE] {name} loaded from disk (age: {(time.time()-ts)/60:.0f}min)")
        return obj["data"], ts
    except Exception as e:
        print(f"[DISK CACHE] Error loading {name}: {e}")
        return None, 0


def _cache_expired(ts: float) -> bool:
    """Return True if cache fetched at `ts` (epoch) is stale.
    Caches expire daily after NYSE close (4:00 PM New York).
    If fetched before today's 4pm NY and it's now past 4pm NY → stale.
    If fetched after today's 4pm NY → valid until tomorrow's 4pm NY."""
    if not ts:
        return True
    from zoneinfo import ZoneInfo
    ny = ZoneInfo("America/New_York")
    now = datetime.now(tz=ny)
    cutoff = now.replace(hour=16, minute=0, second=0, microsecond=0)
    if now < cutoff:
        # Before today's close: stale if fetched before yesterday's close
        cutoff -= timedelta(days=1)
    fetched = datetime.fromtimestamp(ts, tz=ny)
    return fetched < cutoff


app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY") or secrets.token_hex(32)
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['SESSION_COOKIE_SECURE'] = True
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(hours=12)


USERS = {
    "jvilla":  {"password": "scrypt:32768:8:1$8NSI0TYUylmMRgpu$f8f46e0116cc6d0de4af97d4f47b96fb8bd4ad2d71ceec46fe6ab4add66a07d8b1d6dec548919199351e06b880862b1a556eb90d5c186b15605f5b372e90cdd3", "nombre": "José Carlos Villa", "iniciales": "JV", "rol": "admin"},
    "admin":   {"password": "scrypt:32768:8:1$355dZMeOGJDWGAof$1f75a3d1918d39d199d3efcbabae55b30824d29f9da1d0e0b7a1da2c89bb130ea15e354a6dbbeae6c6825ec00ce8b5c52badc0c93a560b47547d2da1b77b463d", "nombre": "Administrador",      "iniciales": "AD", "rol": "admin"},
    "obernal": {"password": "scrypt:32768:8:1$EiyEQJ7JtcPqp0Um$1c7c793ae8583917403405e08f2f41f79e3d3643fe25d15f02bbc4f016a79ae9f15e42fc5312c0a2f3b2d4b2fb8d2d8a55be1eda0bef080b34a6d74b0244671c", "nombre": "Olivia Bernal",      "iniciales": "OB", "rol": "admin"},
}

# ── Persistent password overrides ──
_PASSWORDS_FILE = os.path.join(BASE, "passwords.json")

def _load_password_overrides():
    """Load user password overrides from persistent file."""
    try:
        if os.path.exists(_PASSWORDS_FILE):
            with open(_PASSWORDS_FILE, "r") as f:
                overrides = json.load(f)
            for username, pw_hash in overrides.items():
                if username in USERS:
                    USERS[username]["password"] = pw_hash
    except Exception as e:
        print(f"[SECURITY] Error loading password overrides: {e}")

def _save_password_override(username, pw_hash):
    """Save a password override to persistent file."""
    overrides = {}
    try:
        if os.path.exists(_PASSWORDS_FILE):
            with open(_PASSWORDS_FILE, "r") as f:
                overrides = json.load(f)
    except Exception:
        pass
    overrides[username] = pw_hash
    with open(_PASSWORDS_FILE, "w") as f:
        json.dump(overrides, f)

_load_password_overrides()

PERFILES = {
    "0": {"VXGUBCP": 14.00, "VXDEUDA": 81.00, "VXUDIMP": 5.00},
    "1": {"VXGUBCP": 33.00, "VXDEUDA": 10.00, "VXUDIMP": 8.75,  "VXGUBLP": 42.25, "VXTBILL": 6.00},
    "2": {"VXGUBCP": 32.34, "VXDEUDA": 15.00, "VXUDIMP": 7.29,  "VXGUBLP": 21.85, "VXTBILL": 3.52, "VALMX28": 17.00, "VALMX20": 3.00},
    "3": {"VXGUBCP": 23.37, "VXDEUDA": 4.80,  "VXUDIMP": 8.31,  "VXGUBLP": 20.40, "VXTBILL": 3.12, "VALMX28": 34.00, "VALMX20": 6.00},
    "4": {"VXGUBCP": 20.35, "VXDEUDA": 3.40,  "VXUDIMP": 6.71,  "VXGUBLP": 6.18,  "VXTBILL": 3.36, "VALMX28": 51.00, "VALMX20": 9.00},
}

FONDOS_DEUDA_MXN = {"VXREPO1", "VXGUBCP", "VXUDIMP", "VXDEUDA", "VXGUBLP", "VLMXETF"}
FONDOS_DEUDA_USD = {"VXTBILL", "VXCOBER", "VLMXDME"}
FONDOS_CRED_GLOBAL = {"VLMXETF"}
SP_RATING_MXN = "BBB"
SP_RATING_USD = "AA+"

FONDOS_DEUDA     = FONDOS_DEUDA_MXN | FONDOS_DEUDA_USD
FONDOS_RV        = {"VALMXA", "VALMX20", "VALMX28", "VALMXVL", "VALMXES", "VLMXTEC", "VLMXESG", "VALMXHC", "VXINFRA"}
FONDOS_CICLO     = {"VLMXJUB", "VLMXP24", "VLMXP31", "VLMXP38", "VLMXP45", "VLMXP52", "VLMXP59"}

# ── ETF: nombres simplificados → índice representado ──
ETF_INDEX_MAP = {
    "SPY": "S&P 500", "VOO": "S&P 500", "IVV": "S&P 500",
    "QQQ": "Nasdaq 100", "QQQM": "Nasdaq 100",
    "DIA": "Dow Jones 30", "DJIA": "Dow Jones 30",
    "IWM": "Russell 2000", "VTWO": "Russell 2000",
    "VTI": "US Total Market", "ITOT": "US Total Market",
    "EWW": "MSCI México", "EWZ": "MSCI Brasil", "EWJ": "MSCI Japón",
    "EFA": "MSCI EAFE", "EEM": "MSCI Emerging Markets", "VWO": "FTSE Emerging",
    "IEMG": "MSCI Core EM", "MCHI": "MSCI China",
    "VEA": "FTSE Developed ex-US", "VXUS": "FTSE All-World ex-US",
    "GLD": "Oro (Gold)", "SLV": "Plata (Silver)", "IAU": "Oro (Gold)",
    "USO": "WTI Crudo", "XLE": "Energy Select", "XLF": "Financial Select",
    "XLK": "Technology Select", "XLV": "Health Care Select",
    "XLI": "Industrial Select", "XLP": "Consumer Staples Select",
    "XLY": "Consumer Discretionary Select", "XLU": "Utilities Select",
    "ARKK": "ARK Innovation", "ARKW": "ARK Next Gen Internet",
    "TLT": "US Treasury 20+ Yr", "IEF": "US Treasury 7-10 Yr",
    "SHY": "US Treasury 1-3 Yr", "BND": "US Aggregate Bond",
    "AGG": "US Aggregate Bond", "LQD": "IG Corporate Bond",
    "HYG": "High Yield Bond", "JNK": "High Yield Bond",
    "VNQ": "US REITs", "VNQI": "Intl REITs",
    "NAFTRAC": "IPC México", "IVVPESO": "S&P 500 (MXN)",
}

_ETF_BRAND_PREFIXES = [
    "iShares ", "Vanguard ", "SPDR ", "Invesco ", "WisdomTree ",
    "ProShares ", "First Trust ", "Schwab ", "Global X ", "VanEck ",
    "ARK ", "JPMorgan ", "Fidelity ", "Franklin ", "PIMCO ",
    "BlackRock ", "State Street ", "Dimensional ",
]
_ETF_BRAND_SUFFIXES = [
    " ETF", " Trust", " Fund", " Index Fund", " Portfolio",
    " Shares", " UCITS", " Acc",
]

def simplificar_nombre_etf(ticker: str, nombre: str) -> str:
    clean = ticker.replace(".MX", "").upper()
    if clean in ETF_INDEX_MAP:
        return ETF_INDEX_MAP[clean]
    result = nombre
    for prefix in _ETF_BRAND_PREFIXES:
        if result.startswith(prefix):
            result = result[len(prefix):]
    for suffix in _ETF_BRAND_SUFFIXES:
        if result.endswith(suffix):
            result = result[:-len(suffix)]
    return result.strip() or nombre

# ── ETF: exposición geográfica — fuentes reales por proveedor ──
# Regiones canónicas en INGLÉS Morningstar (RE-RegionalExposure):
# United States, Canada, Latin America, Eurozone, Europe - ex Euro,
# United Kingdom, Japan, Australasia, Asia - Developed, Asia - Emerging,
# Europe - Emerging, Africa, Middle East

# iShares: product-id/slug para descargar CSV de holdings con Location
ISHARES_PRODUCTS = {
    "ACWI": "239600/ishares-msci-acwi-etf",
    "IVV":  "239726/ishares-core-sp-500-etf",
    "EEM":  "239637/ishares-msci-emerging-markets-etf",
    "EFA":  "239623/ishares-msci-eafe-etf",
    "IEFA": "244049/ishares-core-msci-eafe-etf",
    "IEMG": "244050/ishares-core-msci-emerging-markets-etf",
    "EWW":  "239676/ishares-msci-mexico-etf",
    "EWZ":  "239612/ishares-msci-brazil-etf",
    "EWJ":  "239665/ishares-msci-japan-etf",
    "EWG":  "239649/ishares-msci-germany-etf",
    "EWU":  "239690/ishares-msci-united-kingdom-etf",
    "EWA":  "239607/ishares-msci-australia-etf",
    "EWC":  "239615/ishares-msci-canada-etf",
    "EWT":  "239688/ishares-msci-taiwan-etf",
    "EWY":  "239681/ishares-msci-south-korea-etf",
    "FXI":  "239536/ishares-china-large-cap-etf",
    "MCHI": "239619/ishares-msci-china-etf",
    "INDA": "239659/ishares-msci-india-etf",
    "IWM":  "239710/ishares-russell-2000-etf",
    "URTH": "239750/ishares-msci-world-etf",
}

# Vanguard: tickers con endpoint /allocation que devuelve regiones
VANGUARD_TICKERS = {"VOO", "VTI", "VT", "VEA", "VXUS", "VWO", "VIG", "VUG", "VTV", "SCHD"}

# Mapeo Vanguard regiones → Morningstar regiones
VANGUARD_REGION_MAP = {
    "north america":     "United States",   # mayormente US
    "europe":            "Eurozone",
    "pacific":           "Japan",           # Japón + Asia Pac desarrollado
    "emerging markets":  "Asia - Emerging",
    "middle east":       "Middle East",
    "latin america":     "Latin America",
    "united kingdom":    "Europe - ex Euro",
    "other":             "Otros",
}

# Mapeo iShares Location → Morningstar región (complementa COUNTRY_TO_REGION)
ISHARES_LOCATION_MAP = {
    "korea (south)": "Asia - Developed",
    "korea": "Asia - Developed",
    "cayman islands": "Asia - Emerging",
    "bermuda": "United States",
    "jersey": "Europe - ex Euro",
    "guernsey": "Europe - ex Euro",
    "isle of man": "Europe - ex Euro",
    "macau": "Asia - Emerging",
    "curacao": "Latin America",
    "puerto rico": "United States",
    "virgin islands": "United States",
    "panama": "Latin America",
    "cyprus": "Eurozone",
    "estonia": "Eurozone",
    "latvia": "Eurozone",
    "lithuania": "Eurozone",
    "slovakia": "Eurozone",
    "slovenia": "Eurozone",
    "malta": "Eurozone",
    "croatia": "Eurozone",
    "romania": "Europe - Emerging",
    "kenya": "Africa",
    "morocco": "Africa",
    "mauritius": "Africa",
    "pakistan": "Asia - Emerging",
    "bangladesh": "Asia - Emerging",
    "sri lanka": "Asia - Emerging",
    "kuwait": "Middle East",
    "bahrain": "Middle East",
    "oman": "Middle East",
    "jordan": "Middle East",
    "iceland": "Europe - ex Euro",
}

# Mapeo iShares Sector → español (nombres del CSV de holdings)
ISHARES_SECTOR_MAP = {
    "information technology": "Tecnología",
    "financials":             "Financiero",
    "industrials":            "Industriales",
    "consumer discretionary": "Consumo Discrecional",
    "health care":            "Salud",
    "communication":          "Comunicaciones",
    "consumer staples":       "Consumo Básico",
    "materials":              "Materiales",
    "energy":                 "Energía",
    "utilities":              "Utilidades",
    "real estate":            "Bienes Raíces",
    "cash and/or derivatives": None,  # excluir del drilldown
}

# Fallback estático para commodities y ETFs sin holdings
ETF_GEO_STATIC = {
    "GLD":     {"Global": 100.0},
    "SLV":     {"Global": 100.0},
    "IAU":     {"Global": 100.0},
    "NAFTRAC": {"Latin America": 100.0},
    "IVVPESO": {"United States": 100.0},
    "SPY":     {"United States": 100.0},
    "QQQ":     {"United States": 100.0},
    "DIA":     {"United States": 100.0},
}

# Cache de geo+sec por ticker de ETF (evita re-fetches dentro de la sesión)
_ETF_DATA_CACHE = {}  # ticker → {"geo": dict, "sec": dict}

def _fetch_ishares_data(ticker: str) -> tuple:
    """Descarga CSV de iShares y extrae geo (Location) y sectores (Sector).
    Retorna (geo_dict, sec_dict) con regiones Morningstar y sectores en español."""
    import csv
    slug = ISHARES_PRODUCTS.get(ticker)
    if not slug:
        return {}, {}
    url = f"https://www.ishares.com/us/products/{slug}/1467271812596.ajax?tab=holdings&fileType=csv"
    try:
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
        if r.status_code != 200:
            return {}, {}
    except Exception:
        return {}, {}
    lines = r.text.strip().split("\n")
    header_idx = None
    for i, line in enumerate(lines[:15]):
        if "Weight" in line and ("Location" in line or "Sector" in line):
            header_idx = i
            break
    if header_idx is None:
        return {}, {}
    reader = csv.DictReader(lines[header_idx:])
    geo = {}
    sec = {}
    for row in reader:
        try:
            raw_w = row.get("Weight (%)")
            if raw_w is None:
                continue
            w = float(str(raw_w).replace(",", ""))
        except (ValueError, TypeError):
            continue
        if w <= 0:
            continue
        # Geografía
        loc = (row.get("Location") or "").strip()
        if loc:
            loc_lower = loc.lower()
            region = (COUNTRY_TO_REGION.get(loc_lower)
                      or ISHARES_LOCATION_MAP.get(loc_lower)
                      or "Otros")
            geo[region] = geo.get(region, 0) + w
        # Sectores
        raw_sec = (row.get("Sector") or "").strip()
        if raw_sec:
            sec_label = ISHARES_SECTOR_MAP.get(raw_sec.lower())
            if sec_label is None:  # None = excluir (cash/derivatives)
                continue
            if not sec_label:
                sec_label = raw_sec
            sec[sec_label] = sec.get(sec_label, 0) + w
    return geo, sec

def _fetch_vanguard_geo(ticker: str) -> dict:
    """Usa API de Vanguard /allocation para obtener regiones."""
    url = f"https://investor.vanguard.com/investment-products/etfs/profile/api/{ticker}/allocation"
    try:
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        if r.status_code != 200:
            return {}
        data = r.json()
        regions = data.get("region", {}).get("region", [])
        if not regions:
            return {}
    except Exception:
        return {}
    geo = {}
    for reg in regions:
        name = reg.get("name", "").strip().lower()
        try:
            pct = float(reg.get("percent", "0"))
        except (ValueError, TypeError):
            continue
        if pct <= 0:
            continue
        region = VANGUARD_REGION_MAP.get(name, "Otros")
        geo[region] = geo.get(region, 0) + pct
    return geo

def _fetch_holdings_geo(ticker: str) -> dict:
    """Fallback: usa top holdings de Yahoo Finance y busca país de cada uno."""
    import yfinance as yf
    try:
        t = yf.Ticker(ticker)
        fd = t.funds_data
        th = fd.top_holdings
        if th is None or th.empty:
            return {}
    except Exception:
        return {}

    geo = {}
    total_w = 0.0
    for symbol, row in th.iterrows():
        w = float(row.get("Holding Percent", 0))
        if w <= 0:
            continue
        try:
            info_h = yf.Ticker(symbol).info
            country_h = (info_h.get("country") or "").strip().lower()
        except Exception:
            country_h = ""
        region = (COUNTRY_TO_REGION.get(country_h)
                  or ISHARES_LOCATION_MAP.get(country_h)
                  or "Otros")
        geo[region] = geo.get(region, 0) + w * 100
        total_w += w

    # Escalar proporcionalmente a 100%
    if total_w > 0 and geo:
        scale = 100.0 / (total_w * 100)
        geo = {k: round(v * scale, 2) for k, v in geo.items()}
    return geo

def get_etf_data(ticker: str) -> dict:
    """Obtiene exposición geográfica y sectorial real de un ETF.
    Retorna {"geo": dict, "sec": dict}.
    Cascada:
      1. iShares CSV (geo + sec exactos)
      2. Vanguard API (geo por región, sec de Yahoo)
      3. Fallback estático (commodities)
      4. Yahoo Finance top holdings (geo escalada)
    """
    clean_tk = ticker.replace(".MX", "").upper()

    # Cache
    if clean_tk in _ETF_DATA_CACHE:
        c = _ETF_DATA_CACHE[clean_tk]
        return {"geo": dict(c["geo"]), "sec": dict(c["sec"])}

    geo = {}
    sec = {}

    # 1. iShares CSV (datos exactos de geo + sectores)
    if clean_tk in ISHARES_PRODUCTS:
        geo, sec = _fetch_ishares_data(clean_tk)

    # 2. Vanguard API (solo regiones; sectores vendrán de Yahoo)
    if not geo and clean_tk in VANGUARD_TICKERS:
        geo = _fetch_vanguard_geo(clean_tk)

    # 3. Fallback estático (commodities, NAFTRAC, etc.)
    if not geo and clean_tk in ETF_GEO_STATIC:
        geo = dict(ETF_GEO_STATIC[clean_tk])

    # 4. Yahoo Finance top holdings (geo escalada proporcional)
    if not geo:
        geo = _fetch_holdings_geo(ticker)

    result = {"geo": geo, "sec": sec}
    if geo or sec:
        _ETF_DATA_CACHE[clean_tk] = result
    return result

CREDIT_SCALE = ["AAA", "AA+", "AA", "AA-", "A+", "A", "A-", "BBB+", "BBB", "BBB-", "BB+", "BB", "BB-", "B+", "B", "B-", "<B", "NR"]
CREDIT_SCORE = {r: i for i, r in enumerate(CREDIT_SCALE)}

MX_LOCAL_TO_GLOBAL = {
    "AAA": "BBB", "AA": "BBB-", "A": "BB+", "BBB": "BB",
    "BB": "BB-", "B": "B+", "<B": "B", "NR": "NR",
}

def weighted_credit_rating(cred_acc: dict, local_to_global: bool = False) -> str:
    if local_to_global:
        converted = {}
        for rating, weight in cred_acc.items():
            global_rating = MX_LOCAL_TO_GLOBAL.get(rating, rating)
            converted[global_rating] = converted.get(global_rating, 0) + weight
        cred_acc = converted
    total_weight = sum(cred_acc.values())
    if total_weight <= 0:
        return "—"
    score = sum(CREDIT_SCORE.get(r, len(CREDIT_SCALE)-1) * v for r, v in cred_acc.items()) / total_weight
    idx = round(score)
    idx = max(0, min(idx, len(CREDIT_SCALE) - 1))
    return CREDIT_SCALE[idx]

ISIN_MAP = {
  "VXREPO1": {"A":"MXP800461008","B0CF":"MX51VA2J00C5","B0CO":"MX51VA2J0058","B0FI":"MX51VA2J0074","B0NC":"MX51VA2J0041","B1CF":"MX51VA2J00D3","B1CO":"MX51VA2J0082","B1FI":"MX51VA2J00F8","B1NC":"MX51VA2J0066","B2FI":"MX51VA2J0090"},
  "VXGUBCP": {"A":"MXP800501001","B0CF":"MX51VA2L00B3","B0CO":"MX51VA2L0054","B0FI":"MX51VA2L0039","B0NC":"MX51VA2L0047","B1CF":"MX51VA2L00C1","B1CO":"MX51VA2L0088","B1FI":"MX51VA2L0062","B1NC":"MX51VA2L0070","B2CF":"MX51VA2L00D9","B2FI":"MX51VA2L00E7","B2NC":"MX51VA2L0096"},
  "VXUDIMP": {"A":"MX51VA2S0008","B0CF":"MX51VA2S00D4","B0CO":"MX51VA2S0065","B0FI":"MX51VA2S0040","B0NC":"MX51VA2S0057","B1CO":"MX51VA2S0099","B1FI":"MX51VA2S0073","B1NC":"MX51VA2S0081","B2CO":"MX51VA2S00C6","B2FI":"MX51VA2S00A0","B2NC":"MX51VA2S00B8"},
  "VXDEUDA": {"A":"MXP800521009","B0CF":"MX51VA2M0046","B0CO":"MX51VA2M00D7","B0FI":"MX51VA2M0061","B0NC":"MX51VA2M0079","B1CF":"MX51VA2M0095","B1CO":"MX51VA2M00E5","B1FI":"MX51VA2M00A3","B1NC":"MX51VA2M00B1","B2FI":"MX51VA2M00C9","B2NC":"MX51VA2M00H8"},
  "VXGUBLP": {"A":"MX51VA2R0009","B0CF":"MX51VA2R00C8","B0CO":"MX51VA2R00F1","B0FI":"MX51VA2R0041","B0NC":"MX51VA2R0058","B1CF":"MX51VA2R00D6","B1CO":"MX51VA2R0082","B1FI":"MX51VA2R0066","B1NC":"MX51VA2R0074","B2CO":"MX51VA2R00B0","B2FI":"MX51VA2R0090","B2NC":"MX51VA2R00A2"},
  "VXTBILL": {"A":"MX51VA1F0004","B1CF":"MX51VA1F0087","B0CO":"MX51VA1F0020","B0FI":"MX51VA1F0012","B0NC":"MX51VA1F0053"},
  "VXCOBER": {"A":"MXP800621007","B0FI":"MX51VA2N0037","B0NC":"MX51VA2N0045","B1CF":"MX51VA2N00D5","B1CO":"MX51VA2N0086","B1FI":"MX51VA2N0060","B1NC":"MX52FM080076","B2FI":"MX51VA2N0094"},
  "VLMXETF": {"A":"MX52VL060004","B0FI":"MX52VL060038","B1CO":"MX52VL060061","B1FI":"MX52VL060079"},
  "VLMXDME": {"A":"MX52VL0D0002","B0CF":"MX52VL0D0010","B0CO":"MX52VL0D0028","B0FI":"MX52VL0D0036","B0NC":"MX52VL0D0044","B1CF":"MX52VL0D0051","B1FI":"MX52VL0D00B0","B2FI":"MX52VL0D0093"},
  "VALMXA":  {"A":"MX52VA2W0000","B0":"MX52VA2W0018","B1":"MX52VA2W0026","B2":"MX52VA2W0034"},
  "VALMX20": {"A":"MXP800541007","B0":"MX52VA2O0026","B1":"MX52VA2O0000"},
  "VALMX28": {"A":"MX52VA130008","B0CF":"MX52VA130040","B0CO":"MX52VA130032","B0FI":"MX52VA130065","B0NC":"MX52VA130099","B1CO":"MX52VA1300B0","B1FI":"MX52VA130016","B1NC":"MX52VA1300C8"},
  "VALMXVL": {"A":"MX52VA140007","B0":"MX52VA140015","B1":"MX52VA140023","B2":"MX52VA140031","B3":"MX52VA140049"},
  "VALMXES": {"A":"MX52VA190002","B0":"MX52VA190010","B1":"MX52VA190028","B2":"MX52VA190036","B3":"MX52VA190044"},
  "VLMXTEC": {"A":"MX52VL080002","B0CF":"MX52VL080010","B0CO":"MX52VL080028","B0FI":"MX52VL080036","B0NC":"MX52VL080044","B1CF":"MX52VL080051","B1CO":"MX52VL080077","B1FI":"MX52VL080069","B1NC":"MX52VL080085","B2FI":"MX52VL0800B7"},
  "VLMXESG": {"A":"MX52VL0B0004","B0CF":"MX52VL0B0038","B0CO":"MX52VL0B00D0","B0FI":"MX52VL0B0012","B1CF":"MX52VL0B0079","B1CO":"MX52VL0B0053","B1FI":"MX52VL0B0046","B1NC":"MX52VL0B0061","B2FI":"MX52VL0B0087"},
  "VALMXHC": {"A":"MX52VA1L0004","B0CF":"MX52VA1L0046","B0CO":"MX52VA1L0020","B0FI":"MX52VA1L0012","B0NC":"MX52VA1L0038","B1CF":"MX52VA1L0087","B1CO":"MX52VA1L0061","B1FI":"MX52VA1L00D0","B1NC":"MX52VA1L0079","B2FI":"MX52VA1L0095"},
  "VXINFRA": {"A":"MX52VL0E0001","B0CO":"MX52VL0E0019","B0FI":"MX52VL0E0027","B1FI":"MX52VL0E0050","B2FI":"MX52VL0E0084"},
  "VLMXJUB": {"A":"MX52VL070003","B0CF":"MX52VL070011","B0NC":"MX52VL070045","B1CF":"MX52VL070052","B1FI":"MX52VL070078","B1NC":"MX52VL070086","B2NC":"MX52VL0700C7","B3NC":"MX52VL0700D5"},
  "VLMXP24": {"A":"MX52VL010009","B0CF":"MX52VL0100A4","B0NC":"MX52VL010025","B1FI":"MX52VL010041","B1NC":"MX52VL010058","B2NC":"MX52VL010082","B3NC":"MX52VL0100D8"},
  "VLMXP31": {"A":"MX52VL030007","B0CF":"MX52VL0300A0","B0FI":"MX52VL030015","B0NC":"MX52VL030023","B1FI":"MX52VL030049","B1NC":"MX52VL030056","B2NC":"MX52VL030080","B3NC":"MX52VL0300D4"},
  "VLMXP38": {"A":"MX52VL000000","B0CF":"MX52VL0000A6","B0FI":"MX52VL000018","B0NC":"MX52VL000026","B1CF":"MX52VL0000B4","B1FI":"MX52VL000042","B1NC":"MX52VL000059","B2NC":"MX52VL000083","B3NC":"MX52VL0000D0"},
  "VLMXP45": {"A":"MX52VL040014","B0CF":"MX52VL040089","B0FI":"MX52VL040022","B0NC":"MX52VL040048","B1CF":"MX52VL040097","B1CO":"MX52VL0400C4","B1FI":"MX52VL040071","B1NC":"MX52VL0400B6","B2NC":"MX52VL040030","B3NC":"MX52VL0400D2"},
  "VLMXP52": {"A":"MX52VL050005","B0FI":"MX52VL050013","B0NC":"MX52VL050021","B1FI":"MX52VL050047","B1NC":"MX52VL050096","B2NC":"MX52VL050088","B3NC":"MX52VL0500B3"},
  "VLMXP59": {"A":"MX52VL0C0003","B0NC":"MX52VL0C0037","B1FI":"MX52VL0C0086","B1NC":"MX52VL0C0052","B2NC":"MX52VL0C0078","B3NC":"MX52VL0C0094"},
}

TIPO_KEY = {
    "Serie A":                             "A",
    "Persona Física - B1FI/B1":           "PF",
    "Persona Física con Fee - B0FI/B0":    "PF_fee",
    "Plan Personal de Retiro - B1NC/B1CF": "PPR",
    "Persona Moral - B1CO":                "PM",
    "Persona Moral con Fee - B0CO":        "PM_fee",
}

SERIE_MAP = {
    "VXREPO1": {"A":"A","PF":"B1FI","PF_fee":"B0FI","PPR":"B1CF","PM":"B1CO","PM_fee":"B0CO"},
    "VXGUBCP": {"A":"A","PF":"B1FI","PF_fee":"B0FI","PPR":"B1CF","PM":"B1CO","PM_fee":"B0CO"},
    "VXUDIMP": {"A":"A","PF":"B1FI","PF_fee":"B0FI","PPR":"B1NC","PM":"B1CO","PM_fee":"B0CO"},
    "VXDEUDA": {"A":"A","PF":"B1FI","PF_fee":"B0FI","PPR":"B1CF","PM":"B1CO","PM_fee":"B0CO"},
    "VXGUBLP": {"A":"A","PF":"B1FI","PF_fee":"B0FI","PPR":"B1CF","PM":"B1CO","PM_fee":"B0CO"},
    "VXTBILL": {"A":"A","PF":"B0FI","PF_fee":"B0FI","PPR":"B0CF","PM":"B0CO","PM_fee":"B0CO"},
    "VXCOBER": {"A":"A","PF":"B1FI","PF_fee":"B0FI","PPR":"B1CF","PM":"B1CO","PM_fee":"B0CO"},
    "VLMXETF": {"A":"A","PF":"B1FI","PF_fee":"B0FI","PPR":"B1CF","PM":"B1CO","PM_fee":"B0CO"},
    "VLMXDME": {"A":"A","PF":"B1FI","PF_fee":"B0FI","PPR":"B1CF","PM":"B1CO","PM_fee":"B0CO"},
    "VALMXA":  {"A":"A","PF":"B1",  "PF_fee":"B0",  "PPR":"B1",  "PM":"B1",  "PM_fee":"B0"},
    "VALMX20": {"A":"A","PF":"B1",  "PF_fee":"B0",  "PPR":"B1",  "PM":"B1",  "PM_fee":"B0"},
    "VALMX28": {"A":"A","PF":"B1FI","PF_fee":"B0FI","PPR":"B1NC","PM":"B1CO","PM_fee":"B0CO"},
    "VALMXVL": {"A":"A","PF":"B1",  "PF_fee":"B0",  "PPR":"B1",  "PM":"B1",  "PM_fee":"B0"},
    "VALMXES": {"A":"A","PF":"B1",  "PF_fee":"B0",  "PPR":"B1",  "PM":"B1",  "PM_fee":"B0"},
    "VLMXTEC": {"A":"A","PF":"B1FI","PF_fee":"B0FI","PPR":"B1CF","PM":"B1CO","PM_fee":"B0CO"},
    "VLMXESG": {"A":"A","PF":"B1FI","PF_fee":"B0FI","PPR":"B1CF","PM":"B1CO","PM_fee":"B0CO"},
    "VALMXHC": {"A":"A","PF":"B1FI","PF_fee":"B0FI","PPR":"B1CF","PM":"B1CO","PM_fee":"B0CO"},
    "VXINFRA": {"A":"A","PF":"B1FI","PF_fee":"B0FI","PPR":"B1FI","PM":None,  "PM_fee":"B0CO"},
    "VLMXJUB": {"A":"A","PF":"B1FI","PF_fee":"B0FI","PPR":"B1CF","PM":None,  "PM_fee":None},
    "VLMXP24": {"A":"A","PF":"B1FI","PF_fee":"B0FI","PPR":"B1NC","PM":None,  "PM_fee":None},
    "VLMXP31": {"A":"A","PF":"B1FI","PF_fee":"B0FI","PPR":"B1CF","PM":None,  "PM_fee":None},
    "VLMXP38": {"A":"A","PF":"B1FI","PF_fee":"B0FI","PPR":"B1CF","PM":None,  "PM_fee":None},
    "VLMXP45": {"A":"A","PF":"B1FI","PF_fee":"B0FI","PPR":"B1CF","PM":None,  "PM_fee":None},
    "VLMXP52": {"A":"A","PF":"B1FI","PF_fee":"B0FI","PPR":"B1NC","PM":None,  "PM_fee":None},
    "VLMXP59": {"A":"A","PF":"B1FI","PF_fee":"B0FI","PPR":"B1NC","PM":None,  "PM_fee":None},
}

_ms_cache = {}
_ms_cache_ts = 0.0          # epoch timestamp of last fetch
MS_URL    = os.environ.get("MS_URL", "https://api.morningstar.com/v2/service/mf/hlk0d0zmiy1b898b/universeid/txcm88fa8x3vxapp")
MS_ACCESS = os.environ.get("MS_ACCESS", "hwg0cty5re7araij32k035091f43wxd0")
MS_NAV_URL = os.environ.get("MS_NAV_URL", "https://api.morningstar.com/service/mf/UnadjustedNAV/ISIN")
_ms_session = requests.Session()          # reuse TCP connections to Morningstar


def load_ms_universe(force=False):
    global _ms_cache, _ms_cache_ts
    if _ms_cache and not force and not _cache_expired(_ms_cache_ts):
        return _ms_cache
    try:
        resp = _ms_session.get(MS_URL, params={"accesscode": MS_ACCESS, "format": "JSON"}, timeout=15)
        resp.raise_for_status()
        new_cache = {}
        for fund in resp.json().get("data", []):
            api    = fund.get("api", {})
            ticker = api.get("FSCBI-Ticker", "").strip()
            if ticker:
                new_cache[ticker] = api
        _ms_cache = new_cache
        _ms_cache_ts = time.time()
        print(f"[MS] Universo cargado: {len(_ms_cache)} fondos (refresh={'forced' if force else 'auto'})")
    except Exception as e:
        print(f"[MS ERROR] {e}")
        if not _ms_cache:
            _ms_cache_ts = 0.0
    return _ms_cache


# ── Morningstar NAV Histórico — precios diarios por ISIN ──
_ms_nav_cache: dict = {}   # "isin|start" → {"ts": float, "data": list}

def get_ms_nav(isin: str, start: str = "2000-01-01", end: str = None,
               expect_fund: str = None, expect_serie: str = None) -> list:
    """Obtiene precios históricos NAV de Morningstar por ISIN.
    Si expect_fund/expect_serie se proporcionan, valida que fundName coincida.
    Retorna lista de {"fecha": "yyyy-mm-dd", "nav": float}"""
    if end is None:
        end = date.today().isoformat()
    cache_key = f"{isin}|{start}"
    now = time.time()
    cached = _ms_nav_cache.get(cache_key)
    if cached and not _cache_expired(cached["ts"]):
        return cached["data"]
    try:
        r = _ms_session.get(
            f"{MS_NAV_URL}/{isin}",
            params={"startdate": start, "enddate": end, "accesscode": MS_ACCESS},
            timeout=15,
        )
        r.raise_for_status()
        root = ET.fromstring(r.text)

        # Validar que el ISIN corresponde al fondo+serie esperado
        if expect_fund and expect_serie:
            data_elem = root.find(".//data")
            api_fund_name = data_elem.get("fundName", "") if data_elem is not None else ""
            expected_name = f"{expect_fund} {expect_serie}"
            if api_fund_name and api_fund_name != expected_name:
                print(f"[MS NAV MISMATCH] {isin}: esperado '{expected_name}', "
                      f"API regresa '{api_fund_name}' — descartando datos")
                return []

        data = [{"fecha": elem.get("d"), "nav": float(elem.get("v"))}
                for elem in root.iter("r")]
        _ms_nav_cache[cache_key] = {"ts": now, "data": data}
        print(f"[MS NAV] {isin}: {len(data)} precios ({start} -> {end})")
        return data
    except Exception as e:
        print(f"[MS NAV ERROR] {isin}: {e}")
        return []


def get_fondo_backtesting(fondo: str, serie: str) -> list:
    """Construye serie mensual base-100 para un fondo Valmex usando NAV histórico.
    Valida fundName contra API y descarta series cerradas (>90 días sin precio).
    Retorna [{"fecha": "yyyy-mm-dd", "valor": float}]"""
    isin = ISIN_MAP.get(fondo, {}).get(serie)
    if not isin:
        return []
    navs = get_ms_nav(isin, expect_fund=fondo, expect_serie=serie)
    if len(navs) < 2:
        return []
    try:
        df = pd.DataFrame(navs)
        df["fecha"] = pd.to_datetime(df["fecha"])
        df = df.set_index("fecha").sort_index()

        # Detectar y ajustar resets de NAV (e.g. reestructuración de serie)
        # Un cambio diario >2x o <0.5x indica un reset, no un rendimiento real
        nav_vals = df["nav"].values.copy()
        for i in range(1, len(nav_vals)):
            if nav_vals[i - 1] > 0:
                ratio = nav_vals[i] / nav_vals[i - 1]
                if ratio > 2.0 or ratio < 0.5:
                    nav_vals[:i] *= ratio
                    print(f"[BT NAV RESET] {fondo} {serie}: ajuste {ratio:.1f}x en {df.index[i].strftime('%Y-%m-%d')}")
        df["nav"] = nav_vals

        # Descartar series cerradas: último precio > 90 días
        last_price_date = df.index[-1]
        if (pd.Timestamp.now() - last_price_date).days > 90:
            print(f"[BT FONDO SKIP] {fondo} {serie}: serie cerrada "
                  f"(ultimo precio {last_price_date.strftime('%Y-%m-%d')})")
            return []

        series = df["nav"].dropna()
        if len(series) < 2:
            return []
        base = float(series.iloc[0])
        return [{"fecha": dt.strftime("%Y-%m-%d"), "valor": round(float(px) / base * 100, 4)}
                for dt, px in series.items()]
    except Exception as e:
        print(f"[BT FONDO ERROR] {fondo} {serie}: {e}")
        return []


def calc_rend_from_nav(fondo: str, serie: str) -> dict:
    """Calcula rendimientos (MTD, 3M, 6M, YTD, 1Y, 2Y, 3Y) desde el NAV histórico.
    Retorna dict con claves r1m, r3m, r6m, ytd, r1y, r2y, r3y (ya en porcentaje)."""
    isin = ISIN_MAP.get(fondo, {}).get(serie)
    if not isin:
        return {}
    navs = get_ms_nav(isin, expect_fund=fondo, expect_serie=serie)
    if len(navs) < 2:
        return {}
    try:
        df = pd.DataFrame(navs)
        df["fecha"] = pd.to_datetime(df["fecha"])
        df = df.set_index("fecha").sort_index()
        prices = df["nav"].dropna()
        if len(prices) < 2:
            return {}

        last_date = prices.index[-1]
        last_val = float(prices.iloc[-1])

        def find_price(target):
            mask = prices.index <= target
            if mask.any():
                return float(prices[mask].iloc[-1])
            return None

        today = last_date
        targets = {
            "r1m":  pd.Timestamp(today.year, today.month, 1),
            "r3m":  today - pd.DateOffset(months=3),
            "r6m":  today - pd.DateOffset(months=6),
            "ytd":  pd.Timestamp(today.year, 1, 1),
            "r1y":  today - pd.DateOffset(years=1),
            "r2y":  today - pd.DateOffset(years=2),
            "r3y":  today - pd.DateOffset(years=3),
        }
        result = {}
        for key, target_dt in targets.items():
            base = find_price(target_dt)
            if base and base > 0:
                base_date = prices[prices.index <= target_dt].index[-1]
                raw = last_val / base - 1
                # All returns are effective (cumulative) — annualization done in frontend
                result[key] = round(raw * 100, 6)
                # Send actual calendar days for annualization
                actual_days = (last_date - base_date).days
                if actual_days > 0:
                    result[f"days_{key}"] = actual_days
        return result
    except Exception as e:
        print(f"[REND NAV ERR] {fondo} {serie}: {e}")
        return {}


# ─────────────────────────────────────────────────────────────────────────────
# ACCIONES MX — DataBursatil (fuente principal para BMV/BIVA)
# ─────────────────────────────────────────────────────────────────────────────
DB_TOKEN = os.environ.get("DATABURSATIL_TOKEN", "")
DB_BASE  = "https://api.databursatil.com/v2"

# Casos especiales DataBursatil → Yahoo Finance
# (la Ñ se omite en DataBursatil pero se escribe como & en Yahoo Finance)
_YF_OVERRIDES = {"PENOLES": "PE&OLES"}

def _db_to_yf(ticker_db: str) -> str:
    """Convierte ticker de DataBursatil al formato Yahoo Finance (.MX)."""
    t = ticker_db.rstrip("*")                  # WALMEX* → WALMEX
    t = _YF_OVERRIDES.get(t, t)               # PENOLES → PE&OLES
    return t + ".MX"

_db_cache: dict    = {}
_db_cache_ts: dict = {}

# Catálogo completo de emisoras (BMV local + SIC/global + BIVA)
# Cargado una vez al inicio; estructura: {ticker: {nombre, bolsa, tipo, mercado}}
_catalogo_emisoras: dict = {}
_catalogo_ts: float      = 0


def cargar_catalogo_emisoras(forzar: bool = False) -> dict:
    """
    Descarga el catálogo completo de emisoras de DataBursatil.
    Incluye BMV local, SIC (global) y BIVA.
    Retorna dict {ticker_db: {nombre, bolsa, tipo, mercado, yf_ticker}}
    """
    global _catalogo_emisoras, _catalogo_ts
    now = time.time()
    if not forzar and _catalogo_emisoras and not _cache_expired(_catalogo_ts):
        return _catalogo_emisoras
    if not DB_TOKEN:
        return {}

    catalogo = {}
    # Respuesta de DataBursatil: {emisora: {serie: {campos}}}
    for mercado in ["local", "global"]:
        try:
            r = requests.get(
                f"{DB_BASE}/emisoras",
                params={"token": DB_TOKEN, "mercado": mercado},
                timeout=30,
            )
            r.raise_for_status()
            data  = r.json()  # {emisora: {serie: {campos}}}
            count = 0
            for emisora, series in data.items():
                if not isinstance(series, dict):
                    continue
                for serie, campos in series.items():
                    if not isinstance(campos, dict):
                        continue
                    ticker_db = emisora.strip().upper() + serie.strip().upper()
                    yf_ticker = _db_to_yf(ticker_db)
                    tv = (campos.get("tipo_valor_descripcion") or "").upper()
                    if "FIBRA" in tv or "FIDEICOMISO" in tv:
                        tipo = "FIBRA"
                    elif "ETF" in tv or "TRAC" in tv or "FONDO" in tv:
                        tipo = "ETF"
                    elif "SIC" in tv or "SISTEMA INTERNACIONAL" in tv:
                        tipo = "SIC"
                    else:
                        tipo = "Acción"
                    catalogo[ticker_db] = {
                        "ticker_db": ticker_db,
                        "yf_ticker": yf_ticker,
                        "nombre":    campos.get("razon_social") or ticker_db,
                        "bolsa":     (campos.get("bolsa") or "").upper(),
                        "tipo":      tipo,
                        "mercado":   mercado,
                        "isin":      campos.get("isin", ""),
                        "estatus":   campos.get("estatus", ""),
                    }
                    count += 1
            print(f"[CATALOGO] {mercado}: {count} emisoras cargadas")
        except Exception as e:
            print(f"[CATALOGO ERROR] mercado={mercado}: {e}")

    if catalogo:
        _catalogo_emisoras = catalogo
        _catalogo_ts       = now
        print(f"[CATALOGO] Total: {len(catalogo)} emisoras (BMV + SIC + BIVA)")
    return _catalogo_emisoras


def get_accion_db(emisora_serie: str) -> dict | None:
    """
    Obtiene datos de una emisora mexicana desde DataBursatil.
    emisora_serie: p.ej. "AMXL", "GMEXICOB", "VOLARA"
    """
    if not DB_TOKEN:
        return None

    now = time.time()
    key = emisora_serie.upper().strip()
    if key in _db_cache and not _cache_expired(_db_cache_ts.get(key, 0)):
        return _db_cache[key]

    hoy = date.today()
    ini = "2000-01-01"
    fin = hoy.isoformat()

    # 1. Historial de precios
    try:
        r = requests.get(
            f"{DB_BASE}/historicos",
            params={"token": DB_TOKEN, "emisora_serie": key, "inicio": ini, "final": fin},
            timeout=20,
        )
        r.raise_for_status()
        hist_raw = r.json()
    except Exception as e:
        print(f"[DB HIST ERROR] {key}: {e}")
        return None

    # El response puede venir como dict directo {fecha: {precio, importe}}
    # o envuelto en {"data": {...}}
    if isinstance(hist_raw, dict) and "data" in hist_raw and isinstance(hist_raw["data"], dict):
        hist_raw = hist_raw["data"]

    if not hist_raw or not isinstance(hist_raw, dict):
        print(f"[DB] {key}: sin datos históricos")
        return None

    precios: list[tuple[date, float]] = []
    for fecha_str, vals in hist_raw.items():
        try:
            d = date.fromisoformat(fecha_str[:10])
            p = float(vals.get("precio", 0) if isinstance(vals, dict) else vals)
            if p > 0:
                precios.append((d, p))
        except Exception:
            pass

    if not precios:
        return None

    precios.sort(key=lambda x: x[0])

    def precio_en(target: date):
        candidates = [p for d, p in precios if d <= target]
        return candidates[-1] if candidates else None

    p_hoy = precio_en(hoy)
    if p_hoy is None:
        return None

    precio_cierre = round(precios[-1][1], 2)
    p_mtd = precio_en(date(hoy.year, hoy.month, 1))
    p_3m  = precio_en(hoy - timedelta(days=91))
    p_6m  = precio_en(hoy - timedelta(days=182))
    p_ytd = precio_en(date(hoy.year, 1, 1))
    p_1y  = precio_en(hoy - timedelta(days=365))
    p_2y  = precio_en(hoy - timedelta(days=730))
    p_3y  = precio_en(hoy - timedelta(days=1095))

    def rend_efectivo(p_ini):
        if p_ini and p_ini > 0:
            return round((p_hoy / p_ini - 1) * 100, 6)
        return None

    def rend_anual(p_ini, years):
        if p_ini and p_ini > 0:
            return round(((p_hoy / p_ini) ** (1 / years) - 1) * 100, 6)
        return None

    # 2. Info de la emisora (nombre, tipo) — desde catálogo en memoria
    catalogo = cargar_catalogo_emisoras()
    em_info  = catalogo.get(key, {})
    nombre   = em_info.get("nombre", key)
    tipo     = em_info.get("tipo",   "Acción")
    if tipo == "ETF":
        nombre = simplificar_nombre_etf(key, nombre)

    # Geo + Sectores: cascada real (iShares → Vanguard → estático → Yahoo)
    geo_db = {}
    sec_db = {}
    if tipo == "ETF":
        etf_d = get_etf_data(key)
        geo_db = etf_d.get("geo", {})
        sec_db = etf_d.get("sec", {})
    if not geo_db:
        geo_db = {"Latin America": 100.0}

    # ── Backtesting: serie diaria base 100 ──
    historico_bt = []
    try:
        df = pd.DataFrame(precios, columns=["fecha", "precio"])
        df["fecha"] = pd.to_datetime(df["fecha"])
        df = df.set_index("fecha").sort_index()
        daily = df["precio"].dropna()
        if len(daily) > 1:
            base = float(daily.iloc[0])
            for dt, px in daily.items():
                historico_bt.append({
                    "fecha": dt.strftime("%Y-%m-%d"),
                    "valor": round(float(px) / base * 100, 4)
                })
    except Exception:
        pass

    result = {
        "ticker":        key,
        "nombre":        nombre,
        "tipo":          tipo,
        "sector":        "",
        "pais":          "México",
        "moneda":        "MXN",
        "precio_cierre": precio_cierre,
        "moneda_precio": "MXN",
        "r1m":           rend_efectivo(p_mtd),
        "r3m":           rend_efectivo(p_3m),
        "r6m":           rend_efectivo(p_6m),
        "ytd":           rend_efectivo(p_ytd),
        "r1y":           rend_anual(p_1y, 1),
        "r2y":           rend_anual(p_2y, 2),
        "r3y":           rend_anual(p_3y, 3),
        "sectores":      sec_db,
        "geo":           geo_db,
        "historico":     historico_bt,
    }

    _db_cache[key]    = result
    _db_cache_ts[key] = now
    print(f"[DB OK] {key}: {nombre} | p={precio_cierre:.2f} | tipo={tipo}")
    return result


# ─────────────────────────────────────────────────────────────────────────────
# ACCIONES & ETFs — Yahoo Finance con cookie/crumb
# ─────────────────────────────────────────────────────────────────────────────
_accion_cache: dict = {}
_accion_cache_ts: dict = {}
def _accion_cache_valid(ticker: str) -> bool:
    """Cache válido hasta las 4:00pm hora Nueva York (cierre NYSE/NASDAQ)."""
    ts = _accion_cache_ts.get(ticker, 0)
    if not ts:
        return False
    from zoneinfo import ZoneInfo
    ny = ZoneInfo("America/New_York")
    now = datetime.now(tz=ny)
    cutoff = now.replace(hour=16, minute=0, second=0, microsecond=0)
    if now >= cutoff:
        next_cutoff = cutoff + timedelta(days=1)
    else:
        next_cutoff = cutoff
    fetched = datetime.fromtimestamp(ts, tz=ny)
    return fetched < next_cutoff and now < next_cutoff
_yf_rate_limit_until: float = 0  # timestamp hasta el cual no hacer requests a Yahoo

# ── Mapeo estático ticker → (country, sector) — para cuando quoteSummary no funciona ──
_TICKER_INFO_STATIC = {
    # ── BMV (mexicanas) ──
    "GFNORTEO": {"country": "Mexico", "sector": "Financial Services"},
    "BIMBOA": {"country": "Mexico", "sector": "Consumer Defensive"},
    "CEMEXCPO": {"country": "Mexico", "sector": "Basic Materials"},
    "FEMSAUBD": {"country": "Mexico", "sector": "Consumer Defensive"},
    "WALMEX": {"country": "Mexico", "sector": "Consumer Defensive"},
    "AMXB": {"country": "Mexico", "sector": "Communication Services"},
    "AMXL": {"country": "Mexico", "sector": "Communication Services"},
    "TABORB": {"country": "Mexico", "sector": "Financial Services"},
    "GCARSOA1": {"country": "Mexico", "sector": "Consumer Cyclical"},
    "GMEXICOB": {"country": "Mexico", "sector": "Industrials"},
    "GRUMAB": {"country": "Mexico", "sector": "Consumer Defensive"},
    "ASURB": {"country": "Mexico", "sector": "Industrials"},
    "GAPB": {"country": "Mexico", "sector": "Industrials"},
    "OMAB": {"country": "Mexico", "sector": "Industrials"},
    "LIVEPOLC-1": {"country": "Mexico", "sector": "Consumer Cyclical"},
    "KIMBERA": {"country": "Mexico", "sector": "Consumer Defensive"},
    "PE&OLES": {"country": "Mexico", "sector": "Basic Materials"},
    "ALSEA": {"country": "Mexico", "sector": "Consumer Cyclical"},
    "GENTERA": {"country": "Mexico", "sector": "Financial Services"},
    "MEGACPO": {"country": "Mexico", "sector": "Communication Services"},
    "TABORUSD": {"country": "Mexico", "sector": "Financial Services"},
    "AC": {"country": "Mexico", "sector": "Consumer Defensive"},
    "ORBIA": {"country": "Mexico", "sector": "Basic Materials"},
    "ELEKTRA": {"country": "Mexico", "sector": "Consumer Cyclical"},
    "GFINBURO": {"country": "Mexico", "sector": "Financial Services"},
    "BBAJIOO": {"country": "Mexico", "sector": "Financial Services"},
    "RA": {"country": "Mexico", "sector": "Financial Services"},
    "CABORB": {"country": "Mexico", "sector": "Industrials"},
    "Q": {"country": "Mexico", "sector": "Financial Services"},
    "VOLAR": {"country": "Mexico", "sector": "Industrials"},
    "PINFRA": {"country": "Mexico", "sector": "Industrials"},
    # ── SIC (extranjeras en BMV) ──
    "AMZN": {"country": "United States", "sector": "Consumer Cyclical"},
    "AAPL": {"country": "United States", "sector": "Technology"},
    "MSFT": {"country": "United States", "sector": "Technology"},
    "GOOGL": {"country": "United States", "sector": "Communication Services"},
    "GOOG": {"country": "United States", "sector": "Communication Services"},
    "META": {"country": "United States", "sector": "Communication Services"},
    "TSLA": {"country": "United States", "sector": "Consumer Cyclical"},
    "NVDA": {"country": "United States", "sector": "Technology"},
    "NFLX": {"country": "United States", "sector": "Communication Services"},
    "JPM": {"country": "United States", "sector": "Financial Services"},
    "V": {"country": "United States", "sector": "Financial Services"},
    "MA": {"country": "United States", "sector": "Financial Services"},
    "BAC": {"country": "United States", "sector": "Financial Services"},
    "WMT": {"country": "United States", "sector": "Consumer Defensive"},
    "JNJ": {"country": "United States", "sector": "Healthcare"},
    "PG": {"country": "United States", "sector": "Consumer Defensive"},
    "KO": {"country": "United States", "sector": "Consumer Defensive"},
    "PEP": {"country": "United States", "sector": "Consumer Defensive"},
    "DIS": {"country": "United States", "sector": "Communication Services"},
    "COST": {"country": "United States", "sector": "Consumer Defensive"},
    "HD": {"country": "United States", "sector": "Consumer Cyclical"},
    "CRM": {"country": "United States", "sector": "Technology"},
    "AMD": {"country": "United States", "sector": "Technology"},
    "INTC": {"country": "United States", "sector": "Technology"},
    "AVGO": {"country": "United States", "sector": "Technology"},
    "ADBE": {"country": "United States", "sector": "Technology"},
    "CSCO": {"country": "United States", "sector": "Technology"},
    "ORCL": {"country": "United States", "sector": "Technology"},
    "IBM": {"country": "United States", "sector": "Technology"},
    "XOM": {"country": "United States", "sector": "Energy"},
    "CVX": {"country": "United States", "sector": "Energy"},
    "LLY": {"country": "United States", "sector": "Healthcare"},
    "UNH": {"country": "United States", "sector": "Healthcare"},
    "ABBV": {"country": "United States", "sector": "Healthcare"},
    "MRK": {"country": "United States", "sector": "Healthcare"},
    "PFE": {"country": "United States", "sector": "Healthcare"},
    "ABT": {"country": "United States", "sector": "Healthcare"},
    "TMO": {"country": "United States", "sector": "Healthcare"},
    "CRCL": {"country": "United States", "sector": "Technology"},
    "BRK-B": {"country": "United States", "sector": "Financial Services"},
    "GS": {"country": "United States", "sector": "Financial Services"},
    "MS": {"country": "United States", "sector": "Financial Services"},
    "C": {"country": "United States", "sector": "Financial Services"},
    "AXP": {"country": "United States", "sector": "Financial Services"},
    "BLK": {"country": "United States", "sector": "Financial Services"},
    "T": {"country": "United States", "sector": "Communication Services"},
    "VZ": {"country": "United States", "sector": "Communication Services"},
    "BA": {"country": "United States", "sector": "Industrials"},
    "CAT": {"country": "United States", "sector": "Industrials"},
    "UPS": {"country": "United States", "sector": "Industrials"},
    "GE": {"country": "United States", "sector": "Industrials"},
    "NKE": {"country": "United States", "sector": "Consumer Cyclical"},
    "SBUX": {"country": "United States", "sector": "Consumer Cyclical"},
    "MCD": {"country": "United States", "sector": "Consumer Cyclical"},
    "F": {"country": "United States", "sector": "Consumer Cyclical"},
    "GM": {"country": "United States", "sector": "Consumer Cyclical"},
    "PLTR": {"country": "United States", "sector": "Technology"},
    "UBER": {"country": "United States", "sector": "Technology"},
    "SQ": {"country": "United States", "sector": "Technology"},
    "PYPL": {"country": "United States", "sector": "Financial Services"},
    "COIN": {"country": "United States", "sector": "Financial Services"},
    "SNOW": {"country": "United States", "sector": "Technology"},
    "SHOP": {"country": "Canada", "sector": "Technology"},
    "BABA": {"country": "China", "sector": "Consumer Cyclical"},
    "TSM": {"country": "Taiwan", "sector": "Technology"},
    "NVO": {"country": "Denmark", "sector": "Healthcare"},
    "ASML": {"country": "Netherlands", "sector": "Technology"},
    "SAP": {"country": "Germany", "sector": "Technology"},
    "TM": {"country": "Japan", "sector": "Consumer Cyclical"},
    "SONY": {"country": "Japan", "sector": "Technology"},
}

SEC_TRANSLATE_YF = {
    # Keys con espacios (info.sector de YF)
    "technology": "Tecnología", "financial services": "Financiero",
    "healthcare": "Salud", "consumer cyclical": "Consumo Discrecional",
    "industrials": "Industriales", "communication services": "Comunicaciones",
    "consumer defensive": "Consumo Básico", "energy": "Energía",
    "basic materials": "Materiales", "real estate": "Bienes Raíces",
    "utilities": "Utilidades",
    # Keys con underscores (funds_data.sector_weightings de YF)
    "financial_services": "Financiero", "consumer_cyclical": "Consumo Discrecional",
    "communication_services": "Comunicaciones", "consumer_defensive": "Consumo Básico",
    "basic_materials": "Materiales", "realestate": "Bienes Raíces",
}

# País → Región Morningstar EN INGLÉS (mismas keys que RE-RegionalExposure)
COUNTRY_TO_REGION = {
    "united states": "United States", "canada": "Canada",
    "mexico": "Latin America", "brazil": "Latin America", "chile": "Latin America",
    "colombia": "Latin America", "peru": "Latin America", "argentina": "Latin America",
    "united kingdom": "Europe - ex Euro",
    "germany": "Eurozone", "france": "Eurozone", "netherlands": "Eurozone",
    "spain": "Eurozone", "italy": "Eurozone", "belgium": "Eurozone",
    "austria": "Eurozone", "finland": "Eurozone", "ireland": "Eurozone",
    "portugal": "Eurozone", "greece": "Eurozone", "luxembourg": "Eurozone",
    "switzerland": "Europe - ex Euro", "sweden": "Europe - ex Euro",
    "norway": "Europe - ex Euro", "denmark": "Europe - ex Euro",
    "poland": "Europe - Emerging", "czech republic": "Europe - Emerging",
    "hungary": "Europe - Emerging", "turkey": "Europe - Emerging", "russia": "Europe - Emerging",
    "japan": "Japan",
    "australia": "Australasia", "new zealand": "Australasia",
    "hong kong": "Asia - Developed", "singapore": "Asia - Developed",
    "south korea": "Asia - Developed", "taiwan": "Asia - Developed",
    "china": "Asia - Emerging", "india": "Asia - Emerging",
    "indonesia": "Asia - Emerging", "thailand": "Asia - Emerging",
    "malaysia": "Asia - Emerging", "philippines": "Asia - Emerging", "vietnam": "Asia - Emerging",
    "saudi arabia": "Middle East", "israel": "Middle East",
    "united arab emirates": "Middle East", "qatar": "Middle East",
    "south africa": "Africa", "nigeria": "Africa", "egypt": "Africa",
}


# ── Yahoo Finance direct API con curl_cffi (Chrome TLS fingerprint) ──
_YF_DIRECT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "application/json,text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://finance.yahoo.com/",
    "Origin": "https://finance.yahoo.com",
}

_yf_direct_session = {"s": None, "crumb": None, "ts": 0, "lib": None}

def _yf_direct_chart(ticker: str):
    """Llama directo al Yahoo Chart API con curl_cffi (impersonate Chrome).
    Fallback a requests si curl_cffi no está disponible."""
    try:
        now = time.time()
        # Reutilizar sesión+crumb por 1 hora
        if not _yf_direct_session["s"] or (now - _yf_direct_session["ts"]) > 3600:
            # Preferir curl_cffi (Chrome TLS fingerprint, pasa detección cloud)
            try:
                from curl_cffi import requests as cffi_req
                s = cffi_req.Session(impersonate="chrome")
                _yf_direct_session["lib"] = "curl_cffi"
            except ImportError:
                s = requests.Session()
                s.headers.update(_YF_DIRECT_HEADERS)
                _yf_direct_session["lib"] = "requests"

            # Obtener cookies
            s.get("https://fc.yahoo.com", timeout=10, allow_redirects=True)
            # Obtener crumb
            cr = s.get("https://query2.finance.yahoo.com/v1/test/getcrumb", timeout=10)
            crumb = cr.text.strip() if cr.status_code == 200 else ""
            _yf_direct_session["s"] = s
            _yf_direct_session["crumb"] = crumb
            _yf_direct_session["ts"] = now
            print(f"[YF DIRECT] Sesión {_yf_direct_session['lib']} creada, crumb={'OK' if crumb else 'NONE'}")

        s = _yf_direct_session["s"]
        crumb = _yf_direct_session["crumb"]

        # Intentar v8 chart API con crumb — period1=0 para historial completo
        url = f"https://query2.finance.yahoo.com/v8/finance/chart/{ticker}"
        params = {"period1": "0", "period2": str(int(time.time())),
                  "interval": "1d", "includeAdjustedClose": "true"}
        if crumb:
            params["crumb"] = crumb
        r = s.get(url, params=params, timeout=20)

        # Si falla, reintentar con query1
        if r.status_code != 200:
            url2 = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
            r = s.get(url2, params=params, timeout=20)

        if r.status_code != 200:
            print(f"[YF DIRECT] {ticker} HTTP {r.status_code} (vía {_yf_direct_session['lib']})")
            _yf_direct_session["ts"] = 0
            return None, None

        data = r.json()
        result = data.get("chart", {}).get("result", [])
        if not result:
            print(f"[YF DIRECT] {ticker} sin result en respuesta")
            return None, None

        meta = result[0].get("meta", {})
        timestamps = result[0].get("timestamp", [])
        quotes = result[0].get("indicators", {}).get("quote", [{}])[0]
        closes = quotes.get("close", [])
        if not timestamps or not closes:
            print(f"[YF DIRECT] {ticker} sin timestamps/closes")
            return None, None

        # Build DataFrame compatible con el resto del código
        dates = pd.to_datetime(timestamps, unit="s", utc=True).tz_convert("America/New_York")
        df = pd.DataFrame({"Close": closes}, index=dates)
        df.index.name = "Date"
        df = df.dropna()

        # Build info dict
        info = {
            "shortName": meta.get("shortName", ticker),
            "longName": meta.get("longName", ""),
            "quoteType": meta.get("instrumentType", meta.get("quoteType", "")),
            "regularMarketPrice": meta.get("regularMarketPrice"),
            "currentPrice": meta.get("regularMarketPrice"),
            "currency": meta.get("currency", "USD"),
            "exchangeName": meta.get("exchangeName", ""),
        }
        print(f"[YF DIRECT] {ticker} OK ({_yf_direct_session['lib']}): {len(df)} puntos, p={meta.get('regularMarketPrice')}")
        return info, df
    except Exception as e:
        print(f"[YF DIRECT] {ticker} error: {e}")
        _yf_direct_session["ts"] = 0
        return None, None


def _yf_quote_summary(ticker: str) -> dict:
    """Obtiene info básica (country, sector, quoteType, nombre) vía Yahoo quoteSummary.
    Usa la sesión curl_cffi existente para evitar rate limits."""
    try:
        # Asegurar que tenemos sesión curl_cffi
        if not _yf_direct_session["s"]:
            _yf_direct_chart("AAPL")  # inicializa sesión
        s = _yf_direct_session["s"]
        if not s:
            return {}

        url = f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{ticker}"
        params = {"modules": "assetProfile,quoteType"}
        crumb = _yf_direct_session.get("crumb", "")
        if crumb:
            params["crumb"] = crumb
        r = s.get(url, params=params, timeout=15)
        if r.status_code != 200:
            # Retry with query1
            r = s.get(f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{ticker}",
                       params=params, timeout=15)
        if r.status_code != 200:
            return {}

        data = r.json()
        result = data.get("quoteSummary", {}).get("result", [])
        if not result:
            return {}

        profile = result[0].get("assetProfile", {})
        qt = result[0].get("quoteType", {})
        return {
            "country": profile.get("country", ""),
            "sector": profile.get("sector", ""),
            "quoteType": qt.get("quoteType", ""),
            "shortName": qt.get("shortName", ""),
            "longName": qt.get("longName", ""),
        }
    except Exception as e:
        print(f"[YF SUMMARY] {ticker} error: {e}")
        return {}




def get_accion_yf(ticker: str) -> dict | None:
    now = time.time()
    if ticker in _accion_cache and _accion_cache_valid(ticker):
        return _accion_cache[ticker]

    global _yf_rate_limit_until
    hist = None
    info = {}
    t    = None

    # ── Intento 1: curl_cffi directo (Chrome TLS fingerprint — funciona en cloud) ──
    direct_info, direct_df = _yf_direct_chart(ticker)
    if direct_df is not None and not direct_df.empty:
        hist = direct_df
        if direct_info:
            info = direct_info

    # ── Intento 2: yfinance nativo (funciona bien en local) ──
    if hist is None or hist.empty:
        if now < _yf_rate_limit_until:
            print(f"[YF] {ticker} yfinance en cooldown ({int(_yf_rate_limit_until - now)}s)")
        else:
            try:
                t    = yf.Ticker(ticker)
                hist = t.history(start="2000-01-01", auto_adjust=False)
                if hist is not None and not hist.empty:
                    print(f"[YF] {ticker} OK vía yfinance nativo ({len(hist)} filas)")
            except Exception as e:
                err_str = str(e)
                print(f"[YF] {ticker} yfinance falló: {e}")
                if "Too Many Requests" in err_str or "Rate limit" in err_str:
                    _yf_rate_limit_until = now + 300
                    print(f"[YF] Rate limited — cooldown 5 min")

    # ── Intento 3: yf.download (backup) ──
    if hist is None or hist.empty:
        if now >= _yf_rate_limit_until:
            try:
                hist = yf.download(ticker, start="2000-01-01", auto_adjust=False,
                                   progress=False, threads=False)
                if isinstance(hist.columns, pd.MultiIndex):
                    hist.columns = hist.columns.get_level_values(0)
                if hist is not None and not hist.empty:
                    print(f"[YF] {ticker} OK vía yf.download ({len(hist)} filas)")
            except Exception as e:
                print(f"[YF] {ticker} yf.download falló: {e}")

    if hist is None or hist.empty:
        print(f"[YF] {ticker}: sin datos después de 3 intentos")
        return None

    # ── Obtener info (nombre, sector, país) ──
    if t is not None:
        try:
            info = t.info or {}
        except Exception:
            info = {}
        # fast_info como respaldo para precio de mercado
        try:
            fi = t.fast_info
            if fi:
                if not info.get("regularMarketPrice") and hasattr(fi, "last_price"):
                    info["regularMarketPrice"] = fi.last_price
                if not info.get("previousClose") and hasattr(fi, "previous_close"):
                    info["previousClose"] = fi.previous_close
        except Exception:
            pass

    # ── Enriquecer info si falta country/sector (direct API no los trae) ──
    if not info.get("country"):
        clean = ticker.replace(".MX", "").upper()
        # 1. Mapeo estático (confiable, no depende de API)
        static = _TICKER_INFO_STATIC.get(clean)
        if static:
            for key in ("country", "sector", "quoteType"):
                if static.get(key):
                    info.setdefault(key, static[key])
            print(f"[YF INFO] {ticker} from static: country={static.get('country')}, sector={static.get('sector')}")
        else:
            # 2. quoteSummary API (puede fallar sin crumb en cloud)
            tickers_to_try = [ticker]
            if ticker.endswith(".MX"):
                tickers_to_try.append(clean)
            for try_tk in tickers_to_try:
                try:
                    gi = _yf_quote_summary(try_tk)
                    if gi and gi.get("country"):
                        for key in ("country", "sector", "quoteType", "shortName", "longName"):
                            if gi.get(key):
                                info.setdefault(key, gi[key])
                        print(f"[YF INFO] {ticker} from {try_tk}: country={gi.get('country')}, sector={gi.get('sector')}")
                        break
                except Exception as e:
                    print(f"[YF INFO] {ticker} via {try_tk} failed: {e}")

    try:
        today  = datetime.now().date()
        prices = hist["Close"].dropna()
        if prices.empty:
            return None

        # ── Limpiar serie bimodal (SIC .MX mezcla precios USD y MXN) ──
        # Detectar por daily returns >+100% (imposibles en mercado real)
        if len(prices) > 20:
            daily_ret = prices.pct_change().dropna()
            extreme_jumps = (daily_ret.abs() > 1.0).sum()  # >±100% diario
            if extreme_jumps > 3:
                median_price = prices.median()
                last_price = float(prices.iloc[-1])
                if last_price > median_price:
                    prices = prices[prices > median_price * 0.3]
                else:
                    prices = prices[prices < median_price * 3]

        idx = prices.index

        def precio_en(d: date):
            ts = [i for i in idx if i.date() <= d]
            return float(prices[ts[-1]]) if ts else None

        p_hoy = precio_en(today)
        if p_hoy is None:
            return None

        # Usar regularMarketPrice para todo (más preciso que history para SIC)
        raw_price = info.get("regularMarketPrice") or info.get("currentPrice")
        if raw_price and float(raw_price) > 0:
            p_hoy = float(raw_price)
        precio_cierre = round(p_hoy, 2)

        p_mtd = precio_en(date(today.year, today.month, 1))
        p_3m  = precio_en(today - timedelta(days=91))
        p_6m  = precio_en(today - timedelta(days=182))
        p_ytd = precio_en(date(today.year, 1, 1))
        p_1y  = precio_en(today - timedelta(days=365))
        p_2y  = precio_en(today - timedelta(days=730))
        p_3y  = precio_en(today - timedelta(days=1095))

        def rend_efectivo(p_ini):
            if p_ini and p_ini > 0:
                return round((p_hoy / p_ini - 1) * 100, 6)
            return None

        def rend_anual(p_ini, years):
            if p_ini and p_ini > 0:
                return round(((p_hoy / p_ini) ** (1 / years) - 1) * 100, 6)
            return None

        quote_type = info.get("quoteType", "").upper()
        is_index   = quote_type == "INDEX" or ticker.startswith("^")
        tipo       = "Índice" if is_index else ("ETF" if quote_type == "ETF" else "Acción")
        sector_en  = (info.get("sector") or "").strip().lower()
        sector     = SEC_TRANSLATE_YF.get(sector_en, info.get("sector") or "")
        pais_en    = (info.get("country") or "").strip().lower()
        pais       = COUNTRY_TO_REGION.get(pais_en, info.get("country") or "")
        moneda     = "MXN" if ticker.endswith(".MX") else "USD"
        nombre     = info.get("shortName") or info.get("longName") or ticker

        # Índices: moneda y geo según el índice
        if is_index:
            idx_cfg = INDEX_META.get(ticker, {})
            moneda  = idx_cfg.get("moneda", moneda)

        if quote_type == "ETF":
            nombre = simplificar_nombre_etf(ticker, nombre)

        sectores_etf = {}
        geo_etf      = {}

        # Índices: geo y sectores estáticos de INDEX_META
        if is_index:
            idx_cfg = INDEX_META.get(ticker, {})
            geo_etf = dict(idx_cfg.get("geo", {}))
            sectores_etf = dict(idx_cfg.get("sec", {}))
        elif quote_type == "ETF":
            # Geo + Sectores: cascada proveedor (iShares/Vanguard) → estático → Yahoo
            etf_data = get_etf_data(ticker)
            geo_etf = etf_data.get("geo", {})
            sectores_etf = etf_data.get("sec", {})
            # Sectores fallback: Yahoo Finance sector_weightings
            if not sectores_etf:
                try:
                    _t = t if t else yf.Ticker(ticker)
                    holdings = _t.funds_data
                    if holdings and hasattr(holdings, "sector_weightings"):
                        for s, v in (holdings.sector_weightings or {}).items():
                            lbl = SEC_TRANSLATE_YF.get(s.lower(), s)
                            if v > 0:
                                sectores_etf[lbl] = round(v * 100, 2)
                except Exception:
                    pass
            if not sectores_etf and sector:
                sectores_etf[sector] = 100.0
            if not geo_etf and pais:
                geo_etf[pais] = 100.0
        else:
            if sector:
                sectores_etf[sector] = 100.0
            if pais:
                region = COUNTRY_TO_REGION.get(pais_en, pais)
                geo_etf[region] = 100.0

        # ── Backtesting: serie diaria base 100 ──
        # For USD assets: convert to MXN using Banxico FIX daily rate
        # This ensures consistent currency when mixing with MXN-denominated funds
        historico_bt = []
        try:
            daily = prices.dropna()
            if len(daily) > 1:
                needs_fx = moneda == "USD"
                fx_rates = _get_fx_daily() if needs_fx else {}
                base = float(daily.iloc[0])
                # Get FX at base date for proper rebasing
                base_fx = 1.0
                if needs_fx:
                    base_date = daily.index[0].strftime("%Y-%m-%d")
                    base_fx = fx_rates.get(base_date, 0)
                    if base_fx == 0:
                        # Find nearest FX rate
                        for offset in range(1, 10):
                            for delta in [timedelta(days=-offset), timedelta(days=offset)]:
                                d = (daily.index[0] + delta).strftime("%Y-%m-%d")
                                if d in fx_rates:
                                    base_fx = fx_rates[d]
                                    break
                            if base_fx > 0:
                                break
                    if base_fx == 0:
                        base_fx = 1.0  # fallback: no conversion
                        needs_fx = False

                last_fx = base_fx
                for dt, px in daily.items():
                    fecha_str = dt.strftime("%Y-%m-%d")
                    if needs_fx:
                        fx = fx_rates.get(fecha_str)
                        if fx:
                            last_fx = fx
                        # Price in MXN = USD price * FX, rebased to 100
                        valor = round((float(px) * last_fx) / (base * base_fx) * 100, 4)
                    else:
                        valor = round(float(px) / base * 100, 4)
                    historico_bt.append({"fecha": fecha_str, "valor": valor})
                if needs_fx:
                    print(f"[BT] {ticker}: USD→MXN conversion applied ({len(historico_bt)} pts, base_fx={base_fx:.4f})")
        except Exception as e:
            print(f"[BT] {ticker} historico error: {e}")

        # Keep USD-denominated series for beta calculations (avoids FX-driven spurious correlations)
        historico_usd = []
        if moneda == "USD":
            try:
                d_clean = prices.dropna()
                if len(d_clean) > 1:
                    b = float(d_clean.iloc[0])
                    for dt, px in d_clean.items():
                        historico_usd.append({"fecha": dt.strftime("%Y-%m-%d"), "valor": round(float(px) / b * 100, 4)})
            except Exception:
                pass

        # Days elapsed for each period (for frontend annualization)
        def _days_since(p_ref, target_days):
            if p_ref is not None:
                return target_days
            return None

        result = {
            "ticker":        ticker,
            "nombre":        nombre,
            "tipo":          tipo,
            "sector":        sector,
            "pais":          pais,
            "moneda":        moneda,
            "precio_cierre": precio_cierre,
            "moneda_precio": moneda,
            "r1m":           rend_efectivo(p_mtd),
            "r3m":           rend_efectivo(p_3m),
            "r6m":           rend_efectivo(p_6m),
            "ytd":           rend_efectivo(p_ytd),
            "r1y":           rend_efectivo(p_1y),
            "r2y":           rend_efectivo(p_2y),
            "r3y":           rend_efectivo(p_3y),
            "days_r1m":      (today - date(today.year, today.month, 1)).days or 1,
            "days_r3m":      91,
            "days_r6m":      182,
            "days_ytd":      (today - date(today.year, 1, 1)).days or 1,
            "days_r1y":      365,
            "days_r2y":      730,
            "days_r3y":      1095,
            "sectores":      sectores_etf,
            "geo":           geo_etf,
            "historico":     historico_bt,
            "historico_usd": historico_usd,
        }

        _accion_cache[ticker]    = result
        _accion_cache_ts[ticker] = now
        print(f"[YF OK] {ticker}: {nombre} | p={precio_cierre:.2f} | tipo={tipo} | pais={pais}")
        return result

    except Exception as e:
        print(f"[YF ERROR] {ticker}: {e}")
        return None


def get_accion(ticker: str) -> dict | None:
    """
    Fuente unificada para acciones/ETFs.
    Prioridad: Yahoo Finance SIC (.MX) → DataBursatil (BMV local + SIC en MXN).
    YF primero para que precio actual (regularMarketPrice) e histórico sean consistentes.
    Cross-valida: si el .MX tiene backtesting sospechoso (caída >35% desde inception
    en un listing reciente <500 días), prueba el ticker global y usa el más consistente.
    """
    db_key = ticker.upper().replace(".MX", "")
    # Normalizar caracteres especiales BMV (ñ/Ñ → & para Yahoo Finance)
    db_key = db_key.replace("Ñ", "&").replace("ñ", "&")

    # 0. Resolver aliases de índices (IPC → ^MXX, SPX → ^GSPC, etc.)
    idx_alias = INDEX_ALIASES.get(db_key)
    if idx_alias:
        data = get_accion_yf(idx_alias)
        if data:
            return data
    if ticker.startswith("^"):
        data = get_accion_yf(ticker)
        if data:
            return data

    # 1. Yahoo Finance SIC — ticker con .MX (MXN), precio más preciso
    mx_ticker = db_key + ".MX"
    data = get_accion_yf(mx_ticker)
    if data:
        hist = data.get("historico", [])

        # Cross-validar backtesting de SIC recientes (caídas sospechosas)
        if hist and len(hist) < 500:
            last_bt = hist[-1]["valor"]
            first_bt = hist[0]["valor"]
            bt_return = (last_bt / first_bt - 1) if first_bt > 0 else 0
            if bt_return < -0.35:
                global_data = get_accion_yf(db_key) if db_key != mx_ticker else None
                if global_data:
                    g_hist = global_data.get("historico", [])
                    if g_hist:
                        g_return = (g_hist[-1]["valor"] / g_hist[0]["valor"] - 1) if g_hist[0]["valor"] > 0 else 0
                        if g_return > bt_return + 0.30:
                            print(f"[SIC FIX] {mx_ticker} bt={bt_return*100:.1f}% vs {db_key} bt={g_return*100:.1f}% → usando global")
                            return global_data

        # ── Enriquecer con datos del ticker global (risk/geo/sectors) ──
        # .MX = precio + rendimientos + backtesting MXN (lo que ve el cliente)
        # Global = geo, sectores, historico_usd (para risk drivers y factor betas)
        if not ticker.endswith(".MX"):
            try:
                global_data = get_accion_yf(db_key)
                if global_data:
                    # Geo y sectores del global (más precisos para risk)
                    if global_data.get("geo"):
                        data["geo"] = global_data["geo"]
                        print(f"[RISK-ENRICH] {mx_ticker} → geo from {db_key}: {list(global_data['geo'].keys())}")
                    if global_data.get("sectores"):
                        data["sectores"] = global_data["sectores"]
                    # USD historico para factor betas (sin distorsión FX)
                    g_usd = global_data.get("historico_usd") or global_data.get("historico", [])
                    if g_usd:
                        data["historico_usd"] = g_usd
                    # Enriquecer backtesting MXN si .MX tiene historia corta
                    if hist:
                        first_date = hist[0]["fecha"]
                        g_hist = global_data.get("historico", [])
                        if g_hist and g_hist[0]["fecha"] < first_date and first_date > "2005-01-01":
                            print(f"[BT-ENRICH] {mx_ticker} starts {first_date}, {db_key} starts {g_hist[0]['fecha']} → extending BT")
                            data["historico"] = g_hist
            except Exception as e:
                print(f"[RISK-ENRICH] {db_key} global fetch failed: {e}")

        return data

    # 2. DataBursatil — fallback para emisoras que YF no tenga
    if DB_TOKEN:
        data = get_accion_db(db_key)
        if data:
            return data

    # 3. Último recurso: Yahoo Finance global (solo si los anteriores fallaron)
    if ticker.upper() != mx_ticker:
        return get_accion_yf(ticker)
    return None


def safe_float(val, default=0.0):
    try:    return float(val)
    except: return default


def resolve_serie(fondo, tipo_cliente):
    tipo_key    = TIPO_KEY.get(tipo_cliente, "PF")
    fondo_map   = SERIE_MAP.get(fondo, {})
    deseada     = fondo_map.get(tipo_key)
    disponibles = ISIN_MAP.get(fondo, {})
    if deseada and deseada in disponibles:
        return deseada
    for fb in ["B1FI", "B0FI", "B1CF", "B1NC", "B1CO", "B0CO", "B1", "B0", "A"]:
        if fb in disponibles:
            return fb
    return list(disponibles.keys())[0] if disponibles else "A"


# ─────────────────────────────────────────────────────────────────────────────
# FACTOR BETAS — regression of portfolio returns vs factor returns
# ─────────────────────────────────────────────────────────────────────────────
_fx_daily_cache = {}
_fx_daily_cache_ts = 0

def _get_fx_daily():
    """Get daily USD/MXN FIX from Banxico SF43718. Cached 6h. Returns dict {date_str: fx_rate}."""
    global _fx_daily_cache, _fx_daily_cache_ts
    now = time.time()
    if _fx_daily_cache and not _cache_expired(_fx_daily_cache_ts):
        return _fx_daily_cache
    try:
        start, end = "1990-01-01", date.today().isoformat()
        url = f"https://www.banxico.org.mx/SieAPIRest/service/v1/series/SF43718/datos/{start}/{end}"
        r = requests.get(url, headers={"Bmx-Token": BANXICO_TOKEN, "Accept": "application/json"}, timeout=30)
        r.raise_for_status()
        datos = r.json()["bmx"]["series"][0]["datos"]
        fx = {}
        for d in datos:
            try:
                f = datetime.strptime(d["fecha"], "%d/%m/%Y").date()
                fx[f.isoformat()] = float(d["dato"].replace(",", ""))
            except:
                pass
        _fx_daily_cache = fx
        _fx_daily_cache_ts = now
        print(f"[FX] Loaded {len(fx)} daily USD/MXN rates for BT conversion")
        return fx
    except Exception as e:
        print(f"[FX] Failed to load USD/MXN: {e}")
        return {}


_factor_cache = {}
_factor_cache_ts = 0

def _fetch_factor_series():
    """Fetch daily price series for all sensitivity factors. Cached daily."""
    global _factor_cache, _factor_cache_ts
    now = time.time()
    if _factor_cache and not _cache_expired(_factor_cache_ts):
        return _factor_cache

    from concurrent.futures import ThreadPoolExecutor, as_completed

    today = date.today()
    start = "1990-01-01"
    end = today.isoformat()
    factors = {}
    results = {}  # name → pd.Series

    # ── Helper: fetch Banxico series ──
    def _banxico(name, serie_id):
        try:
            url = f"https://www.banxico.org.mx/SieAPIRest/service/v1/series/{serie_id}/datos/{start}/{end}"
            r = requests.get(url, headers={"Bmx-Token": BANXICO_TOKEN, "Accept": "application/json"}, timeout=30)
            r.raise_for_status()
            datos = r.json()["bmx"]["series"][0]["datos"]
            vals = {}
            for d in datos:
                try:
                    f = datetime.strptime(d["fecha"], "%d/%m/%Y")
                    vals[f] = float(d["dato"].replace(",", ""))
                except: pass
            s = pd.Series(vals).sort_index()
            print(f"[BETAS] {name} (Banxico {serie_id}): {len(s)} obs")
            return (name, s)
        except Exception as e:
            print(f"[BETAS] {name} error: {e}")
            return (name, pd.Series(dtype=float))

    # ── Helper: fetch FRED series ──
    def _fred(name, serie_id, label):
        try:
            params = {"series_id": serie_id, "api_key": FRED_API_KEY, "file_type": "json",
                      "observation_start": start, "observation_end": end}
            r = requests.get(FRED_BASE, params=params, timeout=15)
            obs = r.json().get("observations", [])
            vals = {}
            for o in obs:
                try: vals[datetime.strptime(o["date"], "%Y-%m-%d")] = float(o["value"])
                except: pass
            s = pd.Series(vals).sort_index()
            if len(s) >= 60:
                print(f"[BETAS] {label} ({serie_id}): {len(s)} obs")
                return (name, s)
            else:
                print(f"[BETAS] {label} ({serie_id}): only {len(s)} obs, skipped")
                return (name, pd.Series(dtype=float))
        except Exception as e:
            print(f"[BETAS] {label} error: {e}")
            return (name, pd.Series(dtype=float))

    # ── Helper: fetch Yahoo Finance series ──
    def _yf(name, tickers):
        for ticker in tickers:
            try:
                df = yf.download(ticker, start=start, end=end, auto_adjust=True, progress=False)
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.get_level_values(0)
                s = df["Close"].dropna()
                if len(s) >= 60:
                    print(f"[BETAS] {name} ({ticker}): {len(s)} prices")
                    return (name, s)
            except Exception as e:
                print(f"[BETAS] {name} ({ticker}) failed: {e}")
        print(f"[BETAS] {name}: ALL tickers failed")
        return (name, pd.Series(dtype=float))

    # ── Submit all fetches in parallel ──
    tasks = []
    with ThreadPoolExecutor(max_workers=15) as executor:
        # Banxico (5 series)
        tasks.append(executor.submit(_banxico, "fx", "SF43718"))
        tasks.append(executor.submit(_banxico, "bono_m10", "SF44071"))
        tasks.append(executor.submit(_banxico, "tiie_28d", "SF43783"))
        tasks.append(executor.submit(_banxico, "udibono_10y", "SF43924"))
        tasks.append(executor.submit(_banxico, "bono_m30", "SF60696"))
        # FRED (6 series)
        tasks.append(executor.submit(_fred, "ust_10y", "DGS10", "UST 10Y"))
        tasks.append(executor.submit(_fred, "em_spread", "BAMLEMCBPIOAS", "EM Corp OAS"))
        tasks.append(executor.submit(_fred, "latam_oas", "BAMLEMRLCRPILAOAS", "LatAm EM Corp OAS"))
        tasks.append(executor.submit(_fred, "hy_spread", "BAMLH0A0HYM2", "US HY OAS"))
        tasks.append(executor.submit(_fred, "breakeven", "T10YIE", "US 10Y Breakeven"))
        tasks.append(executor.submit(_fred, "term_premium", "THREEFYTP10", "US 10Y Term Premium"))
        # Yahoo Finance (8 series)
        tasks.append(executor.submit(_yf, "ipc", ["^MXX", "NAFTRACISHRS.MX"]))
        tasks.append(executor.submit(_yf, "sp500", ["^GSPC"]))
        tasks.append(executor.submit(_yf, "gold", ["GC=F"]))
        tasks.append(executor.submit(_yf, "oil", ["CL=F"]))
        tasks.append(executor.submit(_yf, "vix", ["^VIX"]))
        tasks.append(executor.submit(_yf, "copper", ["HG=F"]))
        tasks.append(executor.submit(_yf, "dxy", ["DX-Y.NYB"]))
        tasks.append(executor.submit(_yf, "eww", ["EWW"]))

        for future in as_completed(tasks):
            try:
                name, series = future.result()
                if len(series) > 0:
                    factors[name] = series
                else:
                    factors[name] = pd.Series(dtype=float)
            except Exception as e:
                print(f"[BETAS] Future error: {e}")

    # ── Bono M10 FRED fallback if Banxico failed ──
    if "bono_m10" not in factors or len(factors.get("bono_m10", [])) < 12:
        print("[BETAS] Banxico Bono M10 insufficient, falling back to FRED monthly")
        _, s = _fred("bono_m10", "IRLTLT01MXM156N", "Bono M10 MX (FRED fallback)")
        factors["bono_m10"] = s

    # ── Derived: filter low-obs Banxico series ──
    for k in ["tiie_28d", "udibono_10y", "bono_m30"]:
        if k in factors and len(factors[k]) < 60:
            print(f"[BETAS] {k}: only {len(factors[k])} obs, removed")
            factors[k] = pd.Series(dtype=float)

    # ── Derived: MX breakeven inflation ──
    if "udibono_10y" in factors and len(factors["udibono_10y"]) >= 60:
        if "bono_m10" in factors and len(factors["bono_m10"]) >= 60:
            b10 = factors["bono_m10"].reindex(factors["udibono_10y"].index, method="ffill")
            mx_be = (b10 - factors["udibono_10y"]).dropna()
            if len(mx_be) >= 60:
                factors["mx_breakeven"] = mx_be
                print(f"[BETAS] MX Breakeven Inflation (derived): {len(mx_be)} obs")

    # ── Derived: MX curve slope 30-10 ──
    if "bono_m30" in factors and len(factors["bono_m30"]) >= 60:
        if "bono_m10" in factors and len(factors["bono_m10"]) >= 60:
            b10_a = factors["bono_m10"].reindex(factors["bono_m30"].index, method="ffill")
            mx_slope = (factors["bono_m30"] - b10_a).dropna()
            if len(mx_slope) >= 60:
                factors["mx_slope"] = mx_slope
                print(f"[BETAS] MX Curve Slope 30-10 (derived): {len(mx_slope)} obs")

    _factor_cache = factors
    _factor_cache_ts = now
    return factors


# ── Cache for compute_factor_betas ──
_betas_cache = {}   # hash(bt_dict) -> result dict
_betas_cache_ts = 0

def compute_factor_betas(bt_portafolio_dict):
    """Aladdin-style factor risk engine with:
    - Elastic Net (L1+L2) with cross-validation for automatic factor selection
    - EWMA weighting (recent regime weighted more heavily)
    - Ledoit-Wolf shrinkage on EWMA covariance (PCA-shrinkage, Bloomberg MAC3 style)
    - Cornish-Fisher VaR (adjusts for skewness/kurtosis, not just normal)
    - Factor risk contribution decomposition (Euler decomposition)
    - Full covariance cascade for scenario impacts
    - 19 factors: MX rates, US rates, equities, FX, commodities, credit, vol, term premium, MX breakeven, MX slope, LatAm OAS
    """
    if not bt_portafolio_dict or len(bt_portafolio_dict) < 60:
        return {}

    # ── Cache: hash portfolio series → reuse betas for same portfolio (6h) ──
    global _betas_cache, _betas_cache_ts
    import hashlib
    _now = time.time()
    if _now - _betas_cache_ts > 21600:  # 6h
        _betas_cache = {}
        _betas_cache_ts = _now
    _sorted_keys = sorted(bt_portafolio_dict.keys())
    _hash_input = "|".join(f"{k}:{bt_portafolio_dict[k]:.6f}" for k in _sorted_keys[-60:])
    _cache_key = hashlib.md5(_hash_input.encode()).hexdigest()
    if _cache_key in _betas_cache:
        print(f"[BETAS] cache HIT ({_cache_key[:8]})")
        return _betas_cache[_cache_key]
    print(f"[BETAS] cache MISS ({_cache_key[:8]}), computing...")

    try:
        factors = _fetch_factor_series()
    except Exception as e:
        print(f"[BETAS] fetch error: {e}")
        return {}

    # Build portfolio daily series, then resample to month-end
    port_series = pd.Series(
        {pd.Timestamp(f): v for f, v in bt_portafolio_dict.items()}
    ).sort_index()
    port_monthly = port_series.resample('ME').last().dropna()
    port_mret = port_monthly.pct_change().dropna()

    if len(port_mret) < 12:
        print(f"[BETAS] Only {len(port_mret)} monthly returns, need >= 12")
        return {}

    # ── Build factor monthly returns/changes ──
    factor_mrets = {}

    # Price-based factors → monthly returns
    price_factors = ["fx", "ipc", "sp500", "gold", "oil", "vix", "copper", "dxy", "eww"]
    for name in price_factors:
        s = factors.get(name)
        if s is None or len(s) < 60:
            continue
        f_monthly = s.resample('ME').last().dropna()
        f_mret = f_monthly.pct_change().dropna()
        if len(f_mret) >= 12:
            factor_mrets[name] = f_mret

    # Yield/rate-based factors → monthly yield change (decimal: 0.01 = 1pp)
    for yld_name in ["bono_m10", "ust_10y", "tiie_28d"]:
        yld_s = factors.get(yld_name)
        if yld_s is None or len(yld_s) < 6:
            continue
        yld_monthly = yld_s.resample('ME').last().dropna()
        yld_chg = yld_monthly.diff().dropna() / 100
        if len(yld_chg) >= 6:
            factor_mrets[yld_name] = yld_chg

    # Derived spread factors → monthly change (decimal: breakeven, slope)
    for derived_name in ["mx_breakeven", "mx_slope"]:
        d_s = factors.get(derived_name)
        if d_s is None or len(d_s) < 60:
            continue
        d_monthly = d_s.resample('ME').last().dropna()
        d_chg = d_monthly.diff().dropna() / 100  # percentage point change
        if len(d_chg) >= 12:
            factor_mrets[derived_name] = d_chg

    # Spread/premium-based factors → monthly change (decimal)
    for spd_name in ["em_spread", "latam_oas", "hy_spread", "breakeven", "term_premium"]:
        spd_s = factors.get(spd_name)
        if spd_s is None or len(spd_s) < 60:
            continue
        spd_monthly = spd_s.resample('ME').last().dropna()
        spd_chg = spd_monthly.diff().dropna() / 100
        if len(spd_chg) >= 12:
            factor_mrets[spd_name] = spd_chg

    # ── Align factors to portfolio date range (Aladdin-style: max history) ──
    # Instead of intersecting ALL factors (which truncates to shortest),
    # use the portfolio's full range and fill missing factor months with 0.
    # EWMA naturally downweights older months, so early zeros don't distort.
    # Only keep factors that cover at least 50% of portfolio months.
    factor_names = []
    port_idx = port_mret.index
    for name, s in factor_mrets.items():
        overlap = port_idx.intersection(s.index)
        coverage = len(overlap) / len(port_idx) if len(port_idx) > 0 else 0
        if coverage >= 0.5 and len(overlap) >= 12:
            factor_names.append(name)
        else:
            print(f"[BETAS] {name}: coverage {coverage:.0%} ({len(overlap)}/{len(port_idx)}), dropped")

    n = len(port_idx)
    k = len(factor_names)

    if n < 12 or k == 0:
        print(f"[BETAS] Insufficient data: n={n}, k={k}")
        return {}

    y = port_mret.values
    X = np.column_stack([
        factor_mrets[name].reindex(port_idx, fill_value=0.0).values
        for name in factor_names
    ])

    # Clean NaN/Inf
    mask = np.isfinite(y)
    for col in range(k):
        mask &= np.isfinite(X[:, col])
    y, X = y[mask], X[mask]
    n = len(y)

    if n < 12:
        return {}

    try:
        # ══════════════════════════════════════════════════════════════
        # STEP 1: EWMA WEIGHTS (Aladdin-style: recent data matters more)
        # ══════════════════════════════════════════════════════════════
        hl = min(36, max(n // 3, 12))
        decay = 1 - np.log(2) / hl
        ewma_w = np.array([decay ** (n - 1 - t) for t in range(n)])
        ewma_w /= ewma_w.sum()

        # ══════════════════════════════════════════════════════════════
        # STEP 2: ELASTIC NET with CV (replaces Ridge — automatic factor selection)
        # L1 penalty zeros out irrelevant factors, L2 handles multicollinearity
        # ══════════════════════════════════════════════════════════════
        # Standardize X for Elastic Net (sklearn requires it)
        X_mean = np.average(X, axis=0, weights=ewma_w)
        X_wm = X - X_mean
        X_std = np.sqrt(np.average(X_wm ** 2, axis=0, weights=ewma_w))
        X_std[X_std < 1e-12] = 1.0
        X_scaled = X_wm / X_std

        y_mean = np.average(y, weights=ewma_w)
        y_centered = y - y_mean

        # Apply EWMA as sample_weight in Elastic Net CV
        sample_weights = ewma_w * n  # sklearn expects unnormalized weights

        # ElasticNetCV: 5-fold CV, l1_ratio from 0.1 (mostly Ridge) to 0.9 (mostly Lasso)
        n_cv = min(5, max(3, n // 10))
        enet = ElasticNetCV(
            l1_ratio=[0.1, 0.3, 0.5, 0.7, 0.9],
            alphas=50,
            cv=n_cv,
            max_iter=10000,
            fit_intercept=False,  # we centered manually
        )
        enet.fit(X_scaled, y_centered, sample_weight=sample_weights)

        # Unscale coefficients back to original factor space
        beta_scaled = enet.coef_
        beta_original = beta_scaled / X_std
        intercept = y_mean - np.dot(X_mean, beta_original)

        coeffs_full = np.concatenate([[intercept], beta_original])
        l1_ratio_best = enet.l1_ratio_
        alpha_best = enet.alpha_
        n_nonzero = int(np.sum(np.abs(beta_scaled) > 1e-8))

        # R² (unweighted)
        y_pred = X @ beta_original + intercept
        ss_res = np.sum((y - y_pred) ** 2)
        ss_tot = np.sum((y - y.mean()) ** 2)
        r_squared = 1 - ss_res / ss_tot if ss_tot > 0 else 0

        # Adjusted R² (penalizes for number of active factors)
        k_active = max(n_nonzero, 1)
        adj_r_squared = 1 - (1 - r_squared) * (n - 1) / (n - k_active - 1) if n > k_active + 1 else r_squared

        # Standard errors via OLS on the selected factors (for t-stats)
        X_aug = np.column_stack([np.ones(n), X])
        dof = n - k - 1
        sigma2 = ss_res / dof if dof > 0 else ss_res / max(n - 1, 1)
        try:
            XtX = X_aug.T @ X_aug
            se = np.sqrt(np.maximum(np.diag(sigma2 * np.linalg.inv(XtX + 1e-8 * np.eye(k + 1))), 0))
        except Exception:
            se = np.zeros(k + 1)
        n_significant = int(sum(1 for i in range(k) if se[i+1] > 1e-12 and abs(coeffs_full[i+1] / se[i+1]) > 1.96))

        betas = {}
        for i, name in enumerate(factor_names):
            betas[name] = round(float(beta_original[i]), 6)

        betas["alpha"] = round(float(intercept) * 12, 6)  # annualized
        betas["r_squared"] = round(float(r_squared), 4)
        betas["adj_r_squared"] = round(float(adj_r_squared), 4)
        betas["n_months"] = int(n)
        betas["n_factors"] = int(k)
        betas["n_active"] = n_nonzero
        betas["n_significant"] = n_significant
        betas["l1_ratio"] = round(float(l1_ratio_best), 2)
        betas["enet_alpha"] = round(float(alpha_best), 6)
        betas["ewma_halflife"] = int(hl)

        # ══════════════════════════════════════════════════════════════
        # STEP 3: DUAL-DECAY EWMA + LEDOIT-WOLF (Bloomberg MAC3 style)
        #
        # Bloomberg MAC3 uses SEPARATE half-lives:
        #   - SHORT half-life for VOLATILITIES (captures recent vol regime quickly)
        #   - LONG half-life for CORRELATIONS (more stable, less estimation noise)
        # Then applies PCA/LW shrinkage to the correlation matrix.
        # Final covariance: Σ = D_vol × R_shrunk × D_vol
        # ══════════════════════════════════════════════════════════════
        hl_vol = max(6, min(hl, n // 6))       # short: ~6m for monthly data
        hl_corr = max(12, min(36, n // 2))      # long: ~12-36m for correlations

        # EWMA weights for volatilities (short decay — reacts fast to vol regime)
        decay_vol = 1 - np.log(2) / hl_vol
        w_vol = np.array([decay_vol ** (n - 1 - t) for t in range(n)])
        w_vol /= w_vol.sum()

        # EWMA weights for correlations (long decay — stable structure)
        decay_corr = 1 - np.log(2) / hl_corr
        w_corr = np.array([decay_corr ** (n - 1 - t) for t in range(n)])
        w_corr /= w_corr.sum()

        # EWMA volatilities (short half-life)
        X_dm_vol = X - np.average(X, axis=0, weights=w_vol)
        ewma_var = np.average(X_dm_vol ** 2, axis=0, weights=w_vol)
        ewma_std = np.sqrt(np.maximum(ewma_var, 1e-20))

        # EWMA correlation matrix (long half-life)
        X_dm_corr = X - np.average(X, axis=0, weights=w_corr)
        corr_std = np.sqrt(np.maximum(np.average(X_dm_corr ** 2, axis=0, weights=w_corr), 1e-20))
        X_standardized = X_dm_corr / corr_std
        ewma_corr_raw = (X_standardized * w_corr[:, None]).T @ X_standardized * (n / (n - 1))
        np.fill_diagonal(ewma_corr_raw, 1.0)  # ensure diagonal = 1

        # Apply Ledoit-Wolf shrinkage to the CORRELATION matrix (more principled)
        # LW identity target makes more sense for correlations (shrink toward uncorrelated)
        try:
            lw = LedoitWolf()
            X_for_lw = X_standardized * np.sqrt(w_corr * n)[:, None]
            lw.fit(X_for_lw)
            corr_shrunk = lw.covariance_
            # Re-normalize to valid correlation (diag=1)
            d_inv = 1.0 / np.sqrt(np.maximum(np.diag(corr_shrunk), 1e-20))
            corr_shrunk = corr_shrunk * np.outer(d_inv, d_inv)
            np.fill_diagonal(corr_shrunk, 1.0)
            shrinkage_coef = round(float(lw.shrinkage_), 4)
        except Exception:
            corr_shrunk = ewma_corr_raw
            shrinkage_coef = 0.0

        # Reconstruct covariance: Σ = D_vol × R_shrunk × D_vol (Bloomberg MAC3 formula)
        D = np.diag(ewma_std)
        cov_matrix = D @ corr_shrunk @ D

        betas["shrinkage"] = shrinkage_coef
        betas["hl_vol"] = int(hl_vol)
        betas["hl_corr"] = int(hl_corr)

        beta_vec = np.array([betas.get(name, 0) for name in factor_names])

        # ══════════════════════════════════════════════════════════════
        # STEP 4: FACTOR RISK CONTRIBUTION (Euler decomposition)
        # RC_i = β_i × (Σ × β)_i / σ_portfolio — "which factor drives your risk?"
        # ══════════════════════════════════════════════════════════════
        try:
            port_factor_vol = np.sqrt(float(beta_vec @ cov_matrix @ beta_vec))
            if port_factor_vol > 1e-10:
                marginal_risk = cov_matrix @ beta_vec / port_factor_vol
                risk_contrib = beta_vec * marginal_risk
                total_rc = risk_contrib.sum()
                risk_pct = {}
                for i, name in enumerate(factor_names):
                    pct = risk_contrib[i] / total_rc if abs(total_rc) > 1e-10 else 0
                    if abs(pct) > 0.005:  # only report factors contributing >0.5%
                        risk_pct[name] = round(float(pct), 4)
                betas["risk_contrib"] = risk_pct
                betas["factor_vol"] = round(float(port_factor_vol) * np.sqrt(12), 4)  # annualized
        except Exception as rc_err:
            print(f"[RISK-CONTRIB] Error: {rc_err}")

        # ══════════════════════════════════════════════════════════════
        # STEP 5: SCENARIO IMPACTS (Aladdin-style covariance cascade)
        # ══════════════════════════════════════════════════════════════
        scenario_defs = [
            ("bonoMxUp",     "bono_m10",      0.01),     # +100bp MX yield rise
            ("ustUp",        "ust_10y",       0.01),      # +100bp UST yield rise
            ("mxnUp",       "fx",            -0.10),      # MXN appreciates 10%
            ("mxnDown",     "fx",             0.10),      # MXN depreciates 10%
            ("ipcDown",     "ipc",           -0.10),      # IPC -10%
            ("sp500Down",   "sp500",         -0.10),      # SP500 -10%
            ("oilDown",     "oil",           -0.10),      # Oil -10%
            ("goldDown",    "gold",          -0.10),      # Gold -10%
            ("vixSpike",    "vix",            0.50),      # VIX +50%
            ("emSpreadUp",  "em_spread",      0.02),      # EM spread +200bp
            ("hySpreadUp",  "hy_spread",      0.03),      # US HY spread +300bp
            ("copperDown",  "copper",        -0.15),      # Copper -15%
            ("tiieUp",      "tiie_28d",       0.01),      # TIIE +100bp (Banxico hike)
            ("termPremUp",  "term_premium",   0.01),      # US term premium +100bp
            ("mxBeUp",      "mx_breakeven",   0.01),      # MX breakeven inflation +100bp
            ("mxSlopeUp",   "mx_slope",       0.01),      # MX curve steepens +100bp (30Y-10Y)
            ("latamOasUp",  "latam_oas",      0.02),      # LatAm credit spread +200bp
        ]

        scenarios = {}
        for skey, sfactor, shock in scenario_defs:
            if sfactor not in factor_names:
                continue
            idx = factor_names.index(sfactor)
            var_i = cov_matrix[idx, idx]
            if var_i <= 1e-20:
                continue
            # Conditional expected moves for ALL factors
            cond_moves = np.zeros(k)
            for j in range(k):
                if j == idx:
                    cond_moves[j] = shock
                else:
                    cond_moves[j] = (cov_matrix[j, idx] / var_i) * shock
            impact = float(np.dot(beta_vec, cond_moves))
            scenarios[skey] = round(impact, 6)

        # Combined crisis scenario (GFC/COVID calibrated)
        combined_shocks = {
            "sp500": -0.20, "ipc": -0.20, "fx": 0.20, "eww": -0.25,
            "em_spread": 0.04, "hy_spread": 0.05,
            "vix": 1.00, "oil": -0.25, "copper": -0.20,
            "bono_m10": 0.02, "ust_10y": -0.01, "tiie_28d": 0.02,
            "gold": 0.08, "dxy": 0.05,
            "breakeven": -0.01, "term_premium": 0.005,
            "mx_breakeven": -0.005, "mx_slope": 0.01, "latam_oas": 0.04,
        }
        combined_impact = sum(beta_vec[j] * combined_shocks.get(fname, 0)
                              for j, fname in enumerate(factor_names))
        scenarios["combinedCrisis"] = round(float(combined_impact), 6)

        # ══════════════════════════════════════════════════════════════
        # STEP 6: MONTE CARLO VaR + CORNISH-FISHER ADJUSTMENT
        # MC from Ledoit-Wolf shrunk covariance, then CF for fat tails
        # ══════════════════════════════════════════════════════════════
        try:
            n_sims = 10000
            np.random.seed(42)
            mc_factors = np.random.multivariate_normal(np.zeros(k), cov_matrix, size=n_sims)
            mc_port = mc_factors @ beta_vec + float(intercept)
            mc_sorted = np.sort(mc_port)

            # Standard MC VaR
            var_95_mc = round(float(np.percentile(mc_port, 5)), 6)
            var_99_mc = round(float(np.percentile(mc_port, 1)), 6)
            es_95 = round(float(np.mean(mc_sorted[:max(int(0.05 * n_sims), 1)])), 6)

            # Cornish-Fisher adjustment on portfolio residuals
            # Winsorize residuals at 1st/99th percentile to prevent outlier-driven divergence
            resid = y - y_pred
            p01, p99 = np.percentile(resid, [1, 99])
            resid_w = np.clip(resid, p01, p99)
            S = float(skew(resid_w))
            K = float(kurtosis(resid_w, fisher=True))  # excess kurtosis
            mu_port = float(np.mean(mc_port))
            sig_port = float(np.std(mc_port))

            # CF expansion is valid for mild non-normality (|S|<3, |K|<10)
            # Beyond that, the polynomial diverges — fall back to MC VaR
            cf_valid = sig_port > 1e-10 and abs(S) < 3.0 and abs(K) < 10.0
            if cf_valid:
                # CF expansion: z_cf = z + (z²-1)/6 × S + (z³-3z)/24 × K - (2z³-5z)/36 × S²
                z95 = -1.6449
                z99 = -2.3263
                z95_cf = z95 + (z95**2 - 1)/6 * S + (z95**3 - 3*z95)/24 * K - (2*z95**3 - 5*z95)/36 * S**2
                z99_cf = z99 + (z99**2 - 1)/6 * S + (z99**3 - 3*z99)/24 * K - (2*z99**3 - 5*z99)/36 * S**2
                var_95_cf = round(float(mu_port + sig_port * z95_cf), 6)
                var_99_cf = round(float(mu_port + sig_port * z99_cf), 6)
            else:
                # Fallback: MC VaR (already incorporates covariance structure)
                var_95_cf = var_95_mc
                var_99_cf = var_99_mc
                if abs(S) >= 3.0 or abs(K) >= 10.0:
                    print(f"[VaR] CF divergence guard: |S|={abs(S):.1f}, |K|={abs(K):.1f} → MC fallback")

            betas["mc_var95"] = var_95_mc
            betas["mc_var99"] = var_99_mc
            betas["mc_es95"] = es_95
            betas["cf_var95"] = var_95_cf  # Cornish-Fisher adjusted
            betas["cf_var99"] = var_99_cf
            betas["skewness"] = round(S, 4)
            betas["kurtosis"] = round(K, 4)
            print(f"[VaR] MC95={var_95_mc*100:+.2f}%, CF95={var_95_cf*100:+.2f}%, MC99={var_99_mc*100:+.2f}%, CF99={var_99_cf*100:+.2f}%, ES95={es_95*100:+.2f}%")
            print(f"[VaR] Skew={S:.3f}, ExKurt={K:.3f}")
        except Exception as mc_err:
            print(f"[VaR] Error: {mc_err}")

        betas["scenarios"] = scenarios

        # ── Logging ──
        betas["method"] = "ElasticNet+DualEWMA+LW+CF"
        print(f"[BETAS] EN+DualEWMA+LW (n={n}, k={k}, active={n_nonzero}, "
              f"hl_reg={hl}, hl_vol={hl_vol}, hl_corr={hl_corr}, "
              f"l1={l1_ratio_best:.1f}, α={alpha_best:.5f}, shrink={shrinkage_coef:.3f}, "
              f"R²={r_squared:.3f}, AdjR²={adj_r_squared:.3f}, sig={n_significant}/{k}):")
        for i, name in enumerate(factor_names):
            b = beta_original[i]
            t_stat = coeffs_full[i + 1] / se[i + 1] if se[i + 1] > 1e-12 else 0
            active = "●" if abs(b) > 1e-8 else "○"
            sig = "***" if abs(t_stat) > 2.58 else "**" if abs(t_stat) > 1.96 else "*" if abs(t_stat) > 1.65 else ""
            print(f"  {active} {name:>14s}: {b:+.6f}  (t={t_stat:+.2f}{sig})")
        print(f"  {'alpha':>16s}: {betas['alpha']:+.6f} (annualized)")
        print(f"[SCENARIOS] Aladdin cascade (LW-shrunk cov):")
        for skey, impact in scenarios.items():
            print(f"  {skey:>20s}: {impact*100:+.4f}%")

        _betas_cache[_cache_key] = betas
        return betas

    except Exception as e:
        import traceback
        print(f"[BETAS] Factor model error: {e}")
        traceback.print_exc()
        return {}


_factor_beta_cache = {}   # (fondo, serie, factor_key) -> beta, cached per request cycle
_factor_beta_cache_ts = 0

def _factor_beta_for_fund(fondo: str, serie: str, factor_key: str,
                          fallback: float = 0.0) -> float:
    """Calculate beta of a Morningstar fund vs a factor using last 2yr monthly returns.
    Controls for FX (USD/MXN) to avoid spurious correlations with USD-priced factors.
    Cached 6h to ensure consistent results across portfolio comparisons.
    For gold/oil: uses max(beta, 0) — negative beta = hedge, not real exposure.
    For vix: uses abs(beta) since VIX is inverse by nature.
    Other factors: abs(beta). All capped at 2.0."""
    global _factor_beta_cache, _factor_beta_cache_ts
    now = time.time()
    if now - _factor_beta_cache_ts > 21600:   # 6h
        _factor_beta_cache = {}
        _factor_beta_cache_ts = now
    cache_key = (fondo, serie, factor_key)
    if cache_key in _factor_beta_cache:
        return _factor_beta_cache[cache_key]
    try:
        factors = _fetch_factor_series()
        fac_s = factors.get(factor_key)
        if fac_s is None or len(fac_s) < 60:
            return fallback
        isin = ISIN_MAP.get(fondo, {}).get(serie)
        if not isin:
            return fallback
        navs = get_ms_nav(isin)
        if not navs or len(navs) < 60:
            return fallback
        fund_s = pd.Series({pd.Timestamp(n["fecha"]): n["nav"] for n in navs}).sort_index()
        fund_m = fund_s.resample("ME").last().pct_change().dropna()
        fac_m = fac_s.resample("ME").last().pct_change().dropna()

        # Control for FX to isolate factor effect from currency moves
        fx_s = factors.get("fx")
        fx_m = fx_s.resample("ME").last().pct_change().dropna() if fx_s is not None and len(fx_s) >= 60 else None

        common = sorted(fund_m.index.intersection(fac_m.index))[-24:]
        if len(common) < 12:
            return fallback
        fr = fund_m.loc[common].values
        fac_r = fac_m.loc[common].values

        if fx_m is not None and factor_key not in ("fx", "bono_m10", "tiie_28d"):
            # Bivariate: regress fund on [factor, fx] → use factor coefficient
            fx_common = fx_m.reindex(pd.DatetimeIndex(common), method=None).fillna(0).values
            X = np.column_stack([fac_r, fx_common])
            XtX = X.T @ X
            try:
                beta_vec = np.linalg.solve(XtX + 1e-8 * np.eye(2), X.T @ fr)
                beta = beta_vec[0]  # factor coefficient, controlling for FX
            except np.linalg.LinAlgError:
                cov = float(np.cov(fr, fac_r)[0, 1])
                var_fac = float(np.var(fac_r, ddof=1))
                beta = cov / var_fac if var_fac > 1e-10 else 0
        else:
            cov = float(np.cov(fr, fac_r)[0, 1])
            var_fac = float(np.var(fac_r, ddof=1))
            if var_fac < 1e-10:
                return fallback
            beta = cov / var_fac

        # Gold/Oil: only positive beta = real exposure; negative = hedge → 0
        # VIX: abs() because it's inversely correlated with equities by nature
        # Others: abs()
        if factor_key in ("gold", "oil"):
            result = round(min(max(beta, 0), 2.0), 4)
        elif factor_key == "vix":
            result = round(min(abs(beta), 2.0), 4)
        else:
            result = round(min(abs(beta), 2.0), 4)
        _factor_beta_cache[cache_key] = result
        return result
    except Exception as e:
        print(f"[FACTOR-BETA] {fondo} {serie} vs {factor_key}: {e}")
        _factor_beta_cache[cache_key] = fallback
        return fallback


def calcular_portafolio(fondos_pct: dict, tipo_cliente: str,
                        repo_mxn: dict = None, repo_usd: dict = None,
                        acciones: list = None,
                        bt_fecha_ini: str = None, bt_fecha_fin: str = None) -> dict:
    universe = load_ms_universe()

    r1m = r3m = r6m = ytd = r1y = r2y = r3y = 0.0
    stock_t = bond_t = cash_t = 0.0
    accion_t = etf_t = ciclo_t = indice_t = 0.0
    geo_acc = {}; sec_acc = {}; supersec_acc = {}
    lista = []
    fund_lookthrough = {}  # per-fund risk driver look-through

    dur_mxn_num = ytm_mxn_num = bond_mxn_denom = 0.0
    dur_usd_num = ytm_usd_num = bond_usd_denom = 0.0
    cred_mxn = {}; cred_usd = {}
    bt_components = []  # {"weight": float, "series": {fecha: valor_base100}, "is_repo": bool}

    # ── Pre-fetch NAVs en paralelo para todos los fondos ──
    _prefetch_items = []
    for _f, _p in fondos_pct.items():
        if _p <= 0:
            continue
        _s = resolve_serie(_f, tipo_cliente)
        _tk = f"{_f} {_s}"
        _dd = universe.get(_tk, {})
        if not _dd:
            for _ss in ["B1FI", "B0FI", "B1CF", "B1NC", "B1CO", "B0CO", "B1", "B0", "A"]:
                if f"{_f} {_ss}" in universe:
                    _s = _ss; break
        _isin = ISIN_MAP.get(_f, {}).get(_s)
        if _isin:
            _prefetch_items.append((_isin, _f, _s))
    from concurrent.futures import ThreadPoolExecutor
    # Pre-warm factor series cache + NAV cache in parallel
    def _fetch_nav(args):
        isin, f, s = args
        get_ms_nav(isin, expect_fund=f, expect_serie=s)
    with ThreadPoolExecutor(max_workers=12) as executor:
        # Factor series: 15-30s uncached, runs alongside NAV fetches
        _factor_future = executor.submit(_fetch_factor_series)
        if _prefetch_items:
            list(executor.map(_fetch_nav, _prefetch_items))
        _factor_future.result()  # ensure factor cache is warm

    for fondo, pct in fondos_pct.items():
        if pct <= 0:
            continue
        serie  = resolve_serie(fondo, tipo_cliente)
        ticker = f"{fondo} {serie}"
        d      = universe.get(ticker, {})

        if not d:
            for s in ["B1FI", "B0FI", "B1CF", "B1NC", "B1CO", "B0CO", "B1", "B0", "A"]:
                t2 = f"{fondo} {s}"
                if t2 in universe:
                    d = universe[t2]; serie = s; break

        w = pct / 100.0

        # Rendimientos calculados desde NAV histórico (no desde TTR-Return* de API)
        nav_rend = calc_rend_from_nav(fondo, serie)
        r1m += (nav_rend.get("r1m", 0)) * w
        r3m += (nav_rend.get("r3m", 0)) * w
        r6m += (nav_rend.get("r6m", 0)) * w
        ytd += (nav_rend.get("ytd", 0)) * w
        r1y += (nav_rend.get("r1y", 0)) * w
        r2y += (nav_rend.get("r2y", 0)) * w
        r3y += (nav_rend.get("r3y", 0)) * w

        stock = safe_float(d.get("AAB-StockNet"))
        bond  = safe_float(d.get("AAB-BondNet"))
        cash  = safe_float(d.get("AAB-CashNet"))

        is_usd       = fondo in FONDOS_DEUDA_USD
        is_deuda_mxn = fondo in FONDOS_DEUDA_MXN
        is_deuda     = fondo in FONDOS_DEUDA
        is_rv        = fondo in FONDOS_RV
        is_ciclo     = fondo in FONDOS_CICLO

        # Clase de activo: clasificar fondo entero por tipo (no split Morningstar)
        if fondo == "VXREPO1":
            cash_t += 100.0 * w          # VXREPO1 = 100% Reporto
        elif is_deuda:
            bond_t += 100.0 * w          # Deuda = 100% Deuda
        elif is_rv:
            stock_t += 100.0 * w         # RV = 100% Renta Variable
        elif is_ciclo:
            ciclo_t += 100.0 * w         # Ciclo = 100% Ciclo de Vida

        if (is_deuda or is_ciclo) and bond > 0:
            bond_w = (bond / 100.0) * w
            if bond_w > 0:
                dur_val = safe_float(d.get("PS-EffectiveDuration"))
                ytm_val = safe_float(d.get("PS-YieldToMaturity"))
                if is_usd:
                    dur_usd_num    += dur_val * w
                    ytm_usd_num    += ytm_val * w
                    bond_usd_denom += w
                else:
                    dur_mxn_num    += dur_val * w
                    ytm_mxn_num    += ytm_val * w
                    bond_mxn_denom += w

                if fondo in FONDOS_CRED_GLOBAL:
                    cred_mxn[SP_RATING_USD] = cred_mxn.get(SP_RATING_USD, 0) + 100 * w
                elif is_usd:
                    cred_usd[SP_RATING_USD] = cred_usd.get(SP_RATING_USD, 0) + 100 * w
                else:
                    cred_mxn[SP_RATING_MXN] = cred_mxn.get(SP_RATING_MXN, 0) + 100 * w

                supersector_map = {
                    "GBSR-SuperSectorCashandEquivalentsNet": "Reporto",
                    "GBSR-SuperSectorCorporateNet":          "Corporativo",
                    "GBSR-SuperSectorGovernmentNet":         "Gubernamental",
                    "GBSR-SuperSectorMunicipalNet":          "Municipal",
                    "GBSR-SuperSectorSecuritizedNet":        "Bursatilizado",
                    "GBSR-SuperSectorDerivativeNet":         "Derivados",
                }
                for ss_key, ss_lbl in supersector_map.items():
                    v = safe_float(d.get(ss_key))
                    if v > 0:
                        supersec_acc[ss_lbl] = supersec_acc.get(ss_lbl, 0) + v * w

        if (is_rv or is_ciclo) and stock > 0:
            geo_raw = d.get("RE-RegionalExposure", [])
            GEO_EXCLUDE = {"emerging market", "developing country", "emerging markets", "developed countries", "developed country"}
            GEO_MERGE = {"United Kingdom": "Europe - ex Euro"}
            if isinstance(geo_raw, list):
                for item in geo_raw:
                    region = item.get("Region", "")
                    val    = safe_float(item.get("Value", 0))
                    if region and val > 0 and region.lower() not in GEO_EXCLUDE:
                        region = GEO_MERGE.get(region, region)
                        geo_acc[region] = geo_acc.get(region, 0) + val * (stock * w / 100)

            sector_map = {
                "GR-TechnologyNet":           "Tecnología",
                "GR-FinancialServicesNet":    "Financiero",
                "GR-HealthcareNet":           "Salud",
                "GR-CommunicationServicesNet":"Comunicaciones",
                "GR-IndustrialsNet":          "Industriales",
                "GR-ConsumerCyclicalNet":     "Consumo Discrecional",
                "GR-ConsumerDefensiveNet":    "Consumo Básico",
                "GR-BasicMaterialsNet":       "Materiales",
                "GR-EnergyNet":               "Energía",
                "GR-RealEstateNet":           "Bienes Raíces",
                "GR-UtilitiesNet":            "Utilidades",
            }
            for key, nombre in sector_map.items():
                v = safe_float(d.get(key))
                if v > 0:
                    sec_acc[nombre] = sec_acc.get(nombre, 0) + v * (stock * w / 100)

        lista.append({
            "fondo": fondo, "serie": serie, "pct": round(pct, 2),
            "tipo_fondo": "deuda" if (fondo in FONDOS_DEUDA) else ("rv" if fondo in FONDOS_RV else "ciclo"),
            "r1m": nav_rend.get("r1m", 0),
            "r3m": nav_rend.get("r3m", 0),
            "r6m": nav_rend.get("r6m", 0),
            "ytd": nav_rend.get("ytd", 0),
            "r1y": nav_rend.get("r1y", 0),
            "r2y": nav_rend.get("r2y", 0),
            "r3y": nav_rend.get("r3y", 0),
            "days_r1m": nav_rend.get("days_r1m"),
            "days_r3m": nav_rend.get("days_r3m"),
            "days_r6m": nav_rend.get("days_r6m"),
            "days_ytd": nav_rend.get("days_ytd"),
            "days_r1y": nav_rend.get("days_r1y"),
            "days_r2y": nav_rend.get("days_r2y"),
            "days_r3y": nav_rend.get("days_r3y"),
        })

        # ── Look-through: per-fund risk driver mapping ──
        _dur = safe_float(d.get("PS-EffectiveDuration"))
        _ytm = safe_float(d.get("PS-YieldToMaturity"))
        _fund_geo = {}
        _geo_raw = d.get("RE-RegionalExposure", [])
        _GEO_MX = {"latin america", "latinoamérica", "méxico", "mexico"}
        _geo_mx_pct = 0.0
        if isinstance(_geo_raw, list):
            for item in _geo_raw:
                region = item.get("Region", "")
                val = safe_float(item.get("Value", 0))
                if region and val > 0:
                    _fund_geo[region] = round(val, 2)
                    if region.lower() in _GEO_MX:
                        _geo_mx_pct += val
        _geo_us_pct = _fund_geo.get("United States", 0)
        _geo_nonmx_pct = max(0, 100 - _geo_mx_pct)  # % fuera de MX/LatAm = FX exposure

        _fund_sectors = {}
        _sector_map_lt = {
            "GR-TechnologyNet": "Tecnologia", "GR-FinancialServicesNet": "Financiero",
            "GR-HealthcareNet": "Salud", "GR-CommunicationServicesNet": "Comunicaciones",
            "GR-IndustrialsNet": "Industriales", "GR-ConsumerCyclicalNet": "Consumo Disc.",
            "GR-ConsumerDefensiveNet": "Consumo Bas.", "GR-BasicMaterialsNet": "Materiales",
            "GR-EnergyNet": "Energia", "GR-RealEstateNet": "Bienes Raices",
            "GR-UtilitiesNet": "Utilidades",
        }
        for key, nombre in _sector_map_lt.items():
            v = safe_float(d.get(key))
            if v > 0:
                _fund_sectors[nombre] = round(v, 2)

        _drivers = {}
        if is_deuda_mxn or fondo == "VXREPO1":
            # Rate drivers must sum to fund weight: bono_m10 (long-end) + tiie (short-end) = w
            _dur_scale = min(_dur / 5.0, 1.0) if _dur > 0 else 0.0
            if fondo == "VXUDIMP":
                # UDIBONO: split long-end between nominal rates + inflation breakeven
                _drivers["bono_m10"] = round(w * _dur_scale * 0.5, 4)
                _drivers["mx_breakeven"] = round(w * _dur_scale * 0.5, 4)
            else:
                _drivers["bono_m10"] = round(w * _dur_scale, 4)
            _tiie_r = w * (1.0 - _dur_scale)
            if _tiie_r > 0.0001:
                _drivers["tiie_28d"] = round(_tiie_r, 4)
        elif is_usd:
            # Rate drivers must sum to fund weight: ust_10y (long) + remainder = w
            # FX is a separate overlay dimension (always = w for full USD exposure)
            _dur_scale = min(_dur / 5.0, 1.0) if _dur > 0 else 0.0
            _drivers["ust_10y"] = round(w * _dur_scale, 4)
            _ust_short = w * (1.0 - _dur_scale)
            if _ust_short > 0.0001:
                _drivers["ust_10y_short"] = round(_ust_short, 4)
            _drivers["fx"] = round(w, 4)
            if fondo in ("VLMXDME", "VXCOBER"):
                _em_b = _factor_beta_for_fund(fondo, serie, "em_spread")
                if _em_b > 0.001:
                    _drivers["em_spread"] = round(w * _em_b, 4)
        if is_rv or is_ciclo:
            _stk = stock / 100.0 if stock > 0 else 0
            _drivers["sp500"] = round(w * _stk * _geo_us_pct / 100, 4) if _geo_us_pct > 0 else 0
            _drivers["ipc"] = round(w * _stk * _geo_mx_pct / 100, 4) if _geo_mx_pct > 0 else 0
            _drivers["fx"] = round(w * _stk * _geo_nonmx_pct / 100, 4)
            _vix_b = _factor_beta_for_fund(fondo, serie, "vix")
            _drivers["vix"] = round(w * _stk * _vix_b, 4)
            # Gold/Oil: only show if fund has real sector exposure (Morningstar)
            # Energy >10% → oil driver; Materials >15% → gold driver
            # Broad equity funds (VALMX28, ACWI, etc.) don't have meaningful commodity exposure
            _energy_pct = safe_float(d.get("GR-EnergyNet"))
            _materials_pct = safe_float(d.get("GR-BasicMaterialsNet"))
            if _energy_pct > 10:
                _oil_b = _factor_beta_for_fund(fondo, serie, "oil")
                if _oil_b > 0.001:
                    _drivers["oil"] = round(w * _stk * _oil_b, 4)
            if _materials_pct > 15:
                _gold_b = _factor_beta_for_fund(fondo, serie, "gold")
                if _gold_b > 0.001:
                    _drivers["gold"] = round(w * _stk * _gold_b, 4)

        # Remove zero drivers
        _drivers = {k: v for k, v in _drivers.items() if abs(v) > 0.0001}

        fund_lookthrough[fondo] = {
            "weight": round(pct, 2),
            "tipo": "deuda_mxn" if (is_deuda_mxn or fondo == "VXREPO1") else ("deuda_usd" if is_usd else ("rv" if is_rv else "ciclo")),
            "asset_alloc": {"stock": round(stock, 1), "bond": round(bond, 1), "cash": round(cash, 1)},
            "ccy": "USD" if is_usd else "MXN",
            "duration": round(_dur, 2),
            "ytm": round(_ytm, 2),
            "geo": _fund_geo,
            "sectors": _fund_sectors,
            "drivers": _drivers,
        }

        # Backtesting: serie histórica NAV de Morningstar
        fondo_bt = get_fondo_backtesting(fondo, serie)
        if fondo_bt:
            fondo_bt_series = {pt["fecha"]: pt["valor"] for pt in fondo_bt}
            bt_components.append({"weight": w, "series": fondo_bt_series, "is_repo": False, "name": fondo})

    # ── Reporto directo ──
    for repo_cfg, es_usd, label_corto in [
        (repo_mxn, False, "Reporto MXN"),
        (repo_usd, True,  "Reporto USD"),
    ]:
        if not repo_cfg:
            continue
        pct  = float(repo_cfg.get("pct", 0))
        tasa = float(repo_cfg.get("tasa", 0))
        if pct <= 0:
            continue
        w = pct / 100.0
        rend = get_repo_rendimientos(tasa, es_usd)
        r1m += rend["r1m"] * w; r3m += rend["r3m"] * w
        r6m += rend["r6m"] * w; ytd += rend["ytd"] * w
        r1y += rend["r1y"] * w; r2y += rend["r2y"] * w; r3y += rend["r3y"] * w
        repo_bt_series = {pt["fecha"]: pt["valor"] for pt in rend.get("backtesting", [])}
        if repo_bt_series:
            bt_components.append({"weight": w, "series": repo_bt_series, "is_repo": True, "name": label_corto})
        cash_t += 100.0 * w
        if es_usd:
            dur_usd_num += 0.0 * w; ytm_usd_num += tasa * w; bond_usd_denom += w
            cred_usd[SP_RATING_USD] = cred_usd.get(SP_RATING_USD, 0) + 100 * w
        else:
            dur_mxn_num += 0.0 * w; ytm_mxn_num += tasa * w; bond_mxn_denom += w
            cred_mxn[SP_RATING_MXN] = cred_mxn.get(SP_RATING_MXN, 0) + 100 * w
        supersec_acc["Reporto"] = supersec_acc.get("Reporto", 0) + 100 * w
        _today = date.today()
        _days_ytd = (_today - date(_today.year, 1, 1)).days or 1
        lista.append({"fondo": label_corto, "serie": "—", "pct": round(pct, 2),
                      "tipo_fondo": "deuda",
                      "r1m": round(rend["r1m"], 6), "r3m": round(rend["r3m"], 6),
                      "r6m": round(rend["r6m"], 6), "ytd": round(rend["ytd"], 6),
                      "r1y": round(rend["r1y"], 6), "r2y": round(rend["r2y"], 6),
                      "r3y": round(rend["r3y"], 6),
                      "days_r1m": 30, "days_r3m": 91, "days_r6m": 182,
                      "days_ytd": _days_ytd, "days_r1y": 365,
                      "days_r2y": 730, "days_r3y": 1095})
        # Look-through for Reporto — rate driver = full weight
        _repo_drivers = {}
        if es_usd:
            _repo_drivers["fx"] = round(w, 4)
            _repo_drivers["ust_10y_short"] = round(w, 4)
        else:
            _repo_drivers["tiie_28d"] = round(w, 4)
        fund_lookthrough[label_corto] = {
            "weight": round(pct, 2), "tipo": "reporto",
            "asset_alloc": {"stock": 0, "bond": 0, "cash": 100},
            "ccy": "USD" if es_usd else "MXN",
            "duration": 0, "ytm": round(tasa, 2),
            "geo": {}, "sectors": {},
            "drivers": {k: v for k, v in _repo_drivers.items() if abs(v) > 0.0001},
        }

    # ── Acciones & ETFs (Yahoo Finance) ──
    for acc in (acciones or []):
        ticker = acc.get("ticker", "").upper()
        pct    = float(acc.get("pct", 0))
        if pct <= 0 or not ticker:
            continue
        w   = pct / 100.0
        yfd = get_accion(ticker)
        if not yfd:
            continue

        r1m += (yfd.get("r1m") or 0) * w
        r3m += (yfd.get("r3m") or 0) * w
        r6m += (yfd.get("r6m") or 0) * w
        ytd += (yfd.get("ytd") or 0) * w
        r1y += (yfd.get("r1y") or 0) * w
        r2y += (yfd.get("r2y") or 0) * w
        r3y += (yfd.get("r3y") or 0) * w
        if yfd.get("tipo") == "ETF":
            etf_t += 100 * w
        elif yfd.get("tipo") == "Índice":
            indice_t += 100 * w
        else:
            accion_t += 100 * w

        # Use original ticker for index aliases (IPC stays as "IPC", not "MXX")
        _raw_tk = ticker.upper().replace(".MX", "")
        if _raw_tk in INDEX_ALIASES:
            display_tk = _raw_tk
        else:
            display_tk = yfd.get("ticker", ticker).replace(".MX", "").lstrip("^")
        lista.append({
            "fondo": display_tk, "serie": yfd.get("tipo", "Acción"), "pct": round(pct, 2),
            "r1m": round(yfd.get("r1m") or 0, 6), "r3m": round(yfd.get("r3m") or 0, 6),
            "r6m": round(yfd.get("r6m") or 0, 6), "ytd": round(yfd.get("ytd") or 0, 6),
            "r1y": round(yfd.get("r1y") or 0, 6), "r2y": round(yfd.get("r2y") or 0, 6),
            "r3y": round(yfd.get("r3y") or 0, 6),
            "days_r1m": yfd.get("days_r1m"), "days_r3m": yfd.get("days_r3m"),
            "days_r6m": yfd.get("days_r6m"), "days_ytd": yfd.get("days_ytd"),
            "days_r1y": yfd.get("days_r1y"), "days_r2y": yfd.get("days_r2y"),
            "days_r3y": yfd.get("days_r3y"),
        })

        # Sectores
        if yfd.get("sectores"):
            for s, v in yfd["sectores"].items():
                sec_acc[s] = sec_acc.get(s, 0) + v * w
        elif yfd.get("sector"):
            sec_acc[yfd["sector"]] = sec_acc.get(yfd["sector"], 0) + 100 * w

        # Geografía
        _GEO_MERGE = {"United Kingdom": "Europe - ex Euro"}
        if yfd.get("geo"):
            for g, v in yfd["geo"].items():
                g = _GEO_MERGE.get(g, g)
                geo_acc[g] = geo_acc.get(g, 0) + v * w
        elif yfd.get("pais"):
            p = _GEO_MERGE.get(yfd["pais"], yfd["pais"])
            geo_acc[p] = geo_acc.get(p, 0) + 100 * w

        # Look-through for Acciones/ETFs
        _acc_is_usd = not ticker.endswith(".MX")
        # Override for index aliases: use actual index currency
        _raw_upper = ticker.upper().replace(".MX", "")
        if _raw_upper in INDEX_ALIASES:
            _idx_tk = INDEX_ALIASES[_raw_upper]
            _idx_meta = INDEX_META.get(_idx_tk, {})
            _acc_is_usd = _idx_meta.get("moneda", "USD") == "USD"
        _acc_geo = yfd.get("geo", {})
        _acc_mx = sum(v for k, v in _acc_geo.items() if k.lower() in ("méxico", "mexico", "latin america")) if _acc_geo else (100 if ticker.endswith(".MX") else 0)
        _acc_us = _acc_geo.get("United States", _acc_geo.get("united states", 0)) if _acc_geo else (100 if not ticker.endswith(".MX") else 0)
        # Factor betas via multivariate regression: [gold, oil, (fx)]
        # Gold and oil compete with each other (avoiding cross-attribution),
        # but sp500 is NOT included — it would absorb all variance for equity ETFs
        # and hide real commodity exposure embedded in the index constituents.
        # VIX computed univariate (abs beta) since it's inversely correlated with equities.
        _acc_vix_beta = 0.0
        _acc_oil_beta = 0.0
        _acc_gold_beta = 0.0
        try:
            _use_usd = bool(yfd.get("historico_usd"))
            _acc_hist = yfd.get("historico_usd") if _use_usd else yfd.get("historico", [])
            if not _acc_hist or len(_acc_hist) < 60:
                _acc_hist = yfd.get("historico", [])
                _use_usd = False
            if _acc_hist and len(_acc_hist) >= 60:
                _facs = _fetch_factor_series()
                _ps = pd.Series({pd.Timestamp(p["fecha"]): p["valor"] for p in _acc_hist}).sort_index()
                _mr = _ps.resample("ME").last().pct_change().dropna()
                # Multivariate for gold/oil: [gold, oil, (fx for MXN assets)]
                _fkeys = ["gold", "oil"]
                if not _use_usd:
                    _fkeys.append("fx")
                _fm_dict = {}
                for fk in _fkeys + ["vix"]:
                    fs = _facs.get(fk)
                    if fs is not None and len(fs) >= 60:
                        _fm_dict[fk] = fs.resample("ME").last().pct_change().dropna()
                if "gold" in _fm_dict and "oil" in _fm_dict:
                    _common = _mr.index
                    for fk in _fkeys:
                        if fk in _fm_dict:
                            _common = _common.intersection(_fm_dict[fk].index)
                    _common = sorted(_common)[-24:]
                    if len(_common) >= 12:
                        _y = _mr.loc[_common].values
                        _X = np.column_stack([_fm_dict[fk].loc[_common].values for fk in _fkeys])
                        try:
                            _betas = np.linalg.solve(_X.T @ _X + 1e-8 * np.eye(len(_fkeys)), _X.T @ _y)
                            # gold=0, oil=1 in _fkeys
                            _acc_gold_beta = round(min(max(_betas[0], 0), 2.0), 4)
                            _acc_oil_beta = round(min(max(_betas[1], 0), 2.0), 4)
                        except np.linalg.LinAlgError:
                            pass
                        # VIX: univariate
                        if "vix" in _fm_dict:
                            _vix_common = sorted(_mr.index.intersection(_fm_dict["vix"].index))[-24:]
                            if len(_vix_common) >= 12:
                                _vy = _mr.loc[_vix_common].values
                                _vx = _fm_dict["vix"].loc[_vix_common].values
                                _vcov = float(np.cov(_vy, _vx)[0, 1])
                                _vvar = float(np.var(_vx, ddof=1))
                                if _vvar > 1e-10:
                                    _acc_vix_beta = round(min(abs(_vcov / _vvar), 2.0), 4)
        except Exception as _e:
            print(f"[ACC-BETA-MV] {ticker}: {_e}")
        # sp500/ipc use geographic allocation (ETF provider data, domicile-based)
        # Beta regression is distorted by FX conversion (MXN) creating spurious correlations
        # Gold/Oil: only show if real sector exposure (Energy >10% for oil, Materials >15% for gold)
        # Broad equity ETFs (ACWI, SPY, QQQ) don't have meaningful commodity exposure
        _acc_sectors = yfd.get("sectores") or {}
        _acc_energy = sum(v for s, v in _acc_sectors.items() if "energ" in s.lower()) if _acc_sectors else 0
        _acc_materials = sum(v for s, v in _acc_sectors.items() if "material" in s.lower() or "miner" in s.lower()) if _acc_sectors else 0
        # For single stocks, check sector directly
        _acc_sector_single = (yfd.get("sector") or "").lower()
        _show_oil = _acc_energy > 10 or "energ" in _acc_sector_single
        _show_gold = _acc_materials > 15 or "miner" in _acc_sector_single or "metal" in _acc_sector_single or "basic material" in _acc_sector_single
        _acc_drivers = {
            "sp500": round(w * _acc_us / 100, 4) if _acc_us > 0 else 0,
            "ipc": round(w * _acc_mx / 100, 4) if _acc_mx > 0 else 0,
            "fx": round(w * (100 - _acc_mx) / 100, 4) if _acc_is_usd or _acc_mx < 100 else 0,
            "vix": round(w * _acc_vix_beta, 4),
            "oil": round(w * _acc_oil_beta, 4) if _show_oil else 0,
            "gold": round(w * _acc_gold_beta, 4) if _show_gold else 0,
        }
        fund_lookthrough[display_tk] = {
            "weight": round(pct, 2),
            "tipo": yfd.get("tipo", "Accion").lower(),
            "asset_alloc": {"stock": 100, "bond": 0, "cash": 0},
            "ccy": "USD" if _acc_is_usd else "MXN",
            "duration": 0, "ytm": 0,
            "geo": {k: round(v, 1) for k, v in (_acc_geo or {}).items() if v > 0},
            "sectors": {k: round(v, 1) for k, v in (yfd.get("sectores") or {}).items() if v > 0},
            "drivers": {k: v for k, v in _acc_drivers.items() if abs(v) > 0.0001},
        }

        # Backtesting: serie individual del componente
        acc_bt_series = {pt["fecha"]: pt["valor"] for pt in yfd.get("historico", [])}
        if acc_bt_series:
            bt_components.append({"weight": w, "series": acc_bt_series, "is_repo": False, "name": display_tk})

    def filter_pct(d, min_pct=1.0, translate=None):
        t = sum(d.values()) or 1
        main  = []
        otros = 0.0
        for k, v in sorted(d.items(), key=lambda x: -x[1]):
            pct = v / t * 100
            label = (translate or {}).get(k.lower(), k)
            if pct >= min_pct:
                main.append((label, pct))
            else:
                otros += pct
        if otros > 0:
            main.append(("Otros", round(otros, 2)))
        return {"labels":[i[0] for i in main],"values":[round(i[1],2) for i in main]}

    GEO_TRANSLATE = {
        "united states":"Estados Unidos","canada":"Canadá","latin america":"América Latina",
        "eurozone":"Eurozona","europe - ex euro":"Europa ex-Euro",
        "europe - emerging":"Europa Emergente","africa":"África","middle east":"Medio Oriente",
        "japan":"Japón","australasia":"Australasia","asia - developed":"Asia Desarrollada",
        "asia - emerging":"Asia Emergente","greater asia":"Gran Asia","greater europe":"Gran Europa",
        "americas":"Américas","north america":"Norteamérica",
    }

    has_mxn = bond_mxn_denom > 0
    has_usd = bond_usd_denom > 0

    # ── Combinar backtesting dinámico ──
    # Cada componente entra cuando alcanza su inception. Pesos se re-normalizan
    # entre componentes activos. Todo arranca base 100.
    bt_portafolio = {}
    bt_repo_filtered = {}
    historical_scenarios = {}

    if bt_components:
        # Interpolar series mensuales a diarias para suavizar backtesting
        for comp in bt_components:
            dates_sorted = sorted(comp["series"].keys())
            if len(dates_sorted) >= 2:
                # Keys pueden ser str "yyyy-mm-dd" o date objects
                def _to_date(d):
                    return date.fromisoformat(d) if isinstance(d, str) else d
                d_objs = [_to_date(d) for d in dates_sorted]
                gaps = [(d_objs[i+1] - d_objs[i]).days for i in range(min(5, len(d_objs)-1))]
                avg_gap = sum(gaps) / len(gaps) if gaps else 1
                if avg_gap > 20:
                    new_series = {}
                    for k in range(len(dates_sorted) - 1):
                        key0, key1 = dates_sorted[k], dates_sorted[k+1]
                        d0, d1 = d_objs[k], d_objs[k+1]
                        v0, v1 = comp["series"][key0], comp["series"][key1]
                        delta_days = (d1 - d0).days
                        for day_offset in range(delta_days):
                            d = d0 + timedelta(days=day_offset)
                            # Usar mismo tipo de key que el original
                            dk = d.isoformat() if isinstance(key0, str) else d
                            new_series[dk] = round(v0 + (v1 - v0) * day_offset / delta_days, 6)
                    new_series[dates_sorted[-1]] = comp["series"][dates_sorted[-1]]
                    comp["series"] = new_series

        # Todas las fechas únicas de todos los componentes
        all_dates = sorted(set(d for c in bt_components for d in c["series"]))
        has_any_repo = any(c["is_repo"] for c in bt_components)

        # ── PASO 1: Compute FULL bt (all dates, no filter) ──
        # Needed for historical scenarios that must always be available
        bt_full = {}
        repo_full = {}
        if all_dates:
            pv_full = 100.0
            rv_full = 100.0
            cp_full = {}

            for i, fecha in enumerate(all_dates):
                cn = {}
                for j, comp in enumerate(bt_components):
                    if fecha in comp["series"]:
                        cn[j] = comp["series"][fecha]
                    elif j in cp_full:
                        cn[j] = cp_full[j]

                if i == 0:
                    bt_full[fecha] = 100.0
                    if has_any_repo:
                        repo_full[fecha] = 100.0
                    cp_full = cn
                    continue

                ra = []
                rr = []
                for j in cn:
                    if j in cp_full and cp_full[j] > 0:
                        ret = (cn[j] / cp_full[j]) - 1
                        ra.append((j, ret))
                        if bt_components[j]["is_repo"]:
                            rr.append((j, ret))

                if ra:
                    tw = sum(bt_components[j]["weight"] for j, _ in ra)
                    if tw > 0:
                        wr = sum((bt_components[j]["weight"] / tw) * r for j, r in ra)
                        pv_full *= (1 + wr)
                bt_full[fecha] = round(pv_full, 4)

                if rr and has_any_repo:
                    trw = sum(bt_components[j]["weight"] for j, _ in rr)
                    if trw > 0:
                        rwr = sum((bt_components[j]["weight"] / trw) * r for j, r in rr)
                        rv_full *= (1 + rwr)
                    repo_full[fecha] = round(rv_full, 4)

                cp_full = cn

        # ── PASO 2: Historical scenarios from FULL bt (always available) ──
        def _full_period_ret(start, end):
            fdates = sorted(bt_full.keys())
            sp = next(((f, bt_full[f]) for f in fdates if f >= start), None)
            ep = next(((f, bt_full[f]) for f in reversed(fdates) if f <= end), None)
            if not sp or not ep or sp[1] <= 0:
                return None
            return round(ep[1] / sp[1] - 1, 6)

        historical_scenarios = {
            "gfc2008":      _full_period_ret('2008-09-01', '2009-03-09'),
            "mxDowngrade":  _full_period_ret('2019-12-01', '2020-04-30'),
            "covid":        _full_period_ret('2020-02-19', '2020-03-23'),
            "bankCrisis":   _full_period_ret('2023-03-06', '2023-03-15'),
            "bankRecovery": _full_period_ret('2023-03-15', '2023-06-30'),
            "tariffCrisis": _full_period_ret('2025-02-01', '2025-04-30'),
        }

        # ── PASO 2b: Fund-level risk contribution (Euler decomposition) ──
        fund_risk_contrib = {}
        try:
            # Build monthly returns per component
            comp_monthly = {}  # {comp_idx: {yyyy-mm: monthly_ret}}
            for ci, comp in enumerate(bt_components):
                sd = sorted(comp["series"].keys())
                if len(sd) < 2:
                    continue
                monthly_vals = {}
                for d in sd:
                    ym = d[:7]
                    monthly_vals[ym] = comp["series"][d]
                months_sorted = sorted(monthly_vals.keys())
                rets = {}
                for mi in range(1, len(months_sorted)):
                    prev_v = monthly_vals[months_sorted[mi - 1]]
                    curr_v = monthly_vals[months_sorted[mi]]
                    if prev_v > 0:
                        rets[months_sorted[mi]] = curr_v / prev_v - 1
                if rets:
                    comp_monthly[ci] = rets

            # Use only components with return data; find common months
            valid_idx = sorted(comp_monthly.keys())
            if len(valid_idx) >= 2:
                common_months = sorted(set.intersection(*[set(comp_monthly[ci].keys()) for ci in valid_idx]))
                if len(common_months) >= 6:
                    n = len(valid_idx)
                    weights = np.array([bt_components[ci]["weight"] for ci in valid_idx])
                    weights = weights / weights.sum()
                    ret_matrix = np.array([[comp_monthly[ci][m] for m in common_months] for ci in valid_idx])
                    # Ledoit-Wolf shrinkage for robust covariance estimation
                    try:
                        lw = LedoitWolf().fit(ret_matrix.T)  # expects (n_samples, n_features)
                        cov = lw.covariance_ * 12
                    except Exception:
                        cov = np.cov(ret_matrix) * 12  # fallback to sample covariance
                    port_vol = np.sqrt(float(weights @ cov @ weights))
                    if port_vol > 1e-10:
                        marginal = cov @ weights / port_vol
                        rc = weights * marginal
                        total_rc = rc.sum()
                        if abs(total_rc) > 1e-10:
                            raw_pcts = {}
                            for i, ci in enumerate(valid_idx):
                                name = bt_components[ci].get("name", f"Comp {ci}")
                                raw_pcts[name] = float(rc[i] / total_rc)
                            # Normalize rounding: ensure values sum to exactly 1.0
                            rounded = {n: round(v, 4) for n, v in raw_pcts.items()}
                            diff = round(1.0 - sum(rounded.values()), 4)
                            if abs(diff) > 0:
                                largest = max(rounded, key=lambda n: abs(rounded[n]))
                                rounded[largest] = round(rounded[largest] + diff, 4)
                            fund_risk_contrib = rounded
            elif len(valid_idx) == 1:
                # Single component = 100% risk
                ci = valid_idx[0]
                name = bt_components[ci].get("name", f"Comp {ci}")
                fund_risk_contrib[name] = 1.0
        except Exception as frc_err:
            print(f"[FUND-RISK-CONTRIB] Error: {frc_err}")
            import traceback; traceback.print_exc()

        # ── PASO 3: Apply date filter + rebase for main bt_portafolio ──
        f_ini = bt_fecha_ini or all_dates[0]
        f_fin = bt_fecha_fin or all_dates[-1]
        filtered_dates = [d for d in all_dates if d <= f_fin]
        if filtered_dates and f_ini < filtered_dates[0]:
            f_ini = filtered_dates[0]
        filtered_dates = [d for d in filtered_dates if d >= f_ini]

        if filtered_dates:
            base_port = bt_full.get(filtered_dates[0], 100)
            base_repo = None
            for d in filtered_dates:
                if base_port > 0:
                    bt_portafolio[d] = round(bt_full[d] / base_port * 100, 4)
                if has_any_repo and d in repo_full:
                    if base_repo is None:
                        base_repo = repo_full[d]
                    if base_repo and base_repo > 0:
                        bt_repo_filtered[d] = round(repo_full[d] / base_repo * 100, 4)

    # ── Build risk driver matrix from fund_lookthrough ──
    _driver_labels = {
        "fx": "FX (MXN/USD)", "sp500": "S&P 500", "ipc": "IPC",
        "bono_m10": "Bono M10", "ust_10y": "UST 10Y", "ust_10y_short": "UST CP",
        "tiie_28d": "TIIE", "vix": "VIX", "oil": "Petroleo", "gold": "Oro",
        "em_spread": "EM Spread", "mx_breakeven": "Breakeven MX",
    }
    _all_drivers = set()
    for fl in fund_lookthrough.values():
        _all_drivers.update(fl.get("drivers", {}).keys())
    _driver_order = [d for d in ["fx","sp500","ipc","bono_m10","ust_10y","ust_10y_short","tiie_28d","vix","oil","gold","em_spread","mx_breakeven"] if d in _all_drivers]
    _fund_order = sorted(fund_lookthrough.keys(), key=lambda f: -fund_lookthrough[f]["weight"])
    risk_driver_matrix = {
        "funds": _fund_order,
        "drivers": [_driver_labels.get(d, d) for d in _driver_order],
        "driver_keys": _driver_order,
        "values": [[fund_lookthrough[f]["drivers"].get(d, 0) for d in _driver_order] for f in _fund_order],
    }

    result = {
        "ok": True,
        "rendimientos": {
            "mtd":round(r1m,6),"r3m":round(r3m,6),
            "r6m":round(r6m,6),
            "ytd":round(ytd,6),"r1y":round(r1y,6),
            "r2y":round(r2y,6),"r3y":round(r3y,6),
        },
        "clase_activos": (lambda: {
            "labels": [l for l, v in [
                ("Reporto", round(cash_t, 2)),
                ("Deuda", round(bond_t, 2)),
                ("Renta Variable", round(stock_t, 2)),
                ("Ciclo de Vida", round(ciclo_t, 2)),
                ("Acciones", round(accion_t, 2)),
                ("ETF", round(etf_t, 2)),
                ("Índice", round(indice_t, 2)),
            ] if v > 0],
            "values": [v for _, v in [
                ("Reporto", round(cash_t, 2)),
                ("Deuda", round(bond_t, 2)),
                ("Renta Variable", round(stock_t, 2)),
                ("Ciclo de Vida", round(ciclo_t, 2)),
                ("Acciones", round(accion_t, 2)),
                ("ETF", round(etf_t, 2)),
                ("Índice", round(indice_t, 2)),
            ] if v > 0],
        })(),
        "composicion": sorted(lista, key=lambda x: -x["pct"]),
        "geo":           filter_pct(geo_acc, translate=GEO_TRANSLATE),
        "sectores":      filter_pct(sec_acc),
        "supersectores": filter_pct(supersec_acc),
        "has_rv":        stock_t + accion_t + etf_t + indice_t > 0,
        "pct_rv":        round(stock_t + accion_t + etf_t + indice_t, 2),
        "has_deuda":     has_mxn or has_usd,
        "bt_repo":       sorted(
            [{"fecha": f, "valor": round(v, 4)} for f, v in bt_repo_filtered.items()],
            key=lambda x: x["fecha"]
        ) if bt_repo_filtered else [],
        "bt_portafolio": sorted(
            [{"fecha": f, "valor": round(v, 4)} for f, v in bt_portafolio.items()],
            key=lambda x: x["fecha"]
        ) if bt_portafolio else [],
        "deuda": {
            "has_mxn":  has_mxn,
            "dur_mxn":  round(dur_mxn_num / bond_mxn_denom, 2) if has_mxn else 0,
            "ytm_mxn":  round(ytm_mxn_num / bond_mxn_denom, 2) if has_mxn else 0,
            "cred_mxn": weighted_credit_rating(cred_mxn) if cred_mxn else "—",
            "pct_mxn":  round(bond_mxn_denom * 100, 2) if has_mxn else 0,
            "fondos_mxn": sorted(
                [{"fondo": f, "pct": round(lt["weight"], 2)} for f, lt in fund_lookthrough.items() if lt.get("tipo") == "deuda_mxn"],
                key=lambda x: -x["pct"]
            ),
            "has_usd":  has_usd,
            "dur_usd":  round(dur_usd_num / bond_usd_denom, 2) if has_usd else 0,
            "ytm_usd":  round(ytm_usd_num / bond_usd_denom, 2) if has_usd else 0,
            "cred_usd": weighted_credit_rating(cred_usd) if cred_usd else "—",
            "pct_usd":  round(bond_usd_denom * 100, 2) if has_usd else 0,
            "fondos_usd": sorted(
                [{"fondo": f, "pct": round(lt["weight"], 2)} for f, lt in fund_lookthrough.items() if lt.get("tipo") == "deuda_usd"],
                key=lambda x: -x["pct"]
            ),
        },
        "historical_scenarios": historical_scenarios,
        "fund_risk_contrib": fund_risk_contrib,
        "fund_lookthrough": fund_lookthrough,
        "risk_driver_matrix": risk_driver_matrix,
    }

    # ACWI benchmark for overweight/underweight visualization
    if stock_t + accion_t + etf_t + indice_t > 0:
        try:
            _acwi = get_etf_data("ACWI")
            _acwi_geo = _acwi.get("geo", {})
            _acwi_sec = _acwi.get("sec", {})
            result["acwi_benchmark"] = {
                "geo": filter_pct(_acwi_geo, min_pct=0.5, translate=GEO_TRANSLATE),
                "sectores": filter_pct(_acwi_sec, min_pct=0.5),
            }
            print(f"[ACWI] geo={len(result['acwi_benchmark']['geo'].get('labels',[]))} sec={len(result['acwi_benchmark']['sectores'].get('labels',[]))}")
        except Exception as e:
            print(f"[ACWI] benchmark error: {e}")
            result["acwi_benchmark"] = {}

    # Compute real factor betas from regression
    # Use bt_full (full history) for maximum data → most stable betas and covariance
    try:
        _bt_src = bt_full if bt_full else bt_portafolio
        print(f"[BETAS] bt_src type={type(_bt_src).__name__} len={len(_bt_src)}")
        result["betas"] = compute_factor_betas(_bt_src)
        print(f"[BETAS] result keys={list(result['betas'].keys())[:5]}... len={len(result['betas'])}")
    except Exception as e:
        import traceback
        print(f"[BETAS] Error in calcular_portafolio: {e}")
        traceback.print_exc()
        result["betas"] = {}

    # Compute average CETES 28d rate over BT period as Rf
    try:
        bt_dates = sorted(bt_portafolio.keys()) if bt_portafolio else []
        if bt_dates:
            cetes_raw = _banxico_serie_rango(SERIE_CETES28, bt_dates[0], bt_dates[-1])
            cetes_vals = [d["valor"] for d in cetes_raw if d.get("valor") is not None]
            if cetes_vals:
                result["rf_annual"] = round(sum(cetes_vals) / len(cetes_vals) / 100, 6)
                print(f"[RF] CETES avg {result['rf_annual']*100:.2f}% over {bt_dates[0]} to {bt_dates[-1]} ({len(cetes_vals)} obs)")
            else:
                result["rf_annual"] = 0.10
        else:
            result["rf_annual"] = 0.10
    except Exception as e:
        print(f"[RF] Error computing CETES avg: {e}")
        result["rf_annual"] = 0.10

    return result


# ─────────────────────────────────────────────────────────────────────────────
# SECURITY: headers, rate limiting, input validation
# ─────────────────────────────────────────────────────────────────────────────

@app.after_request
def set_security_headers(response):
    response.headers['X-Content-Type-Options'] = 'nosniff'
    response.headers['X-Frame-Options'] = 'SAMEORIGIN'
    response.headers['X-XSS-Protection'] = '1; mode=block'
    response.headers['Referrer-Policy'] = 'strict-origin-when-cross-origin'
    response.headers['Cross-Origin-Opener-Policy'] = 'same-origin'
    response.headers['X-Permitted-Cross-Domain-Policies'] = 'none'
    response.headers['Permissions-Policy'] = 'geolocation=(), camera=(), microphone=(), usb=(), bluetooth=()'
    response.headers['Content-Security-Policy'] = (
        "default-src 'self'; "
        "script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net https://cdnjs.cloudflare.com https://html2canvas.hertzen.com; "
        "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com https://cdn.jsdelivr.net; "
        "font-src 'self' https://fonts.gstatic.com https://cdn.jsdelivr.net; "
        "img-src 'self' data: blob:; "
        "connect-src 'self'; "
        "frame-ancestors 'self'; "
        "base-uri 'self'; "
        "form-action 'self';"
    )
    response.headers['Strict-Transport-Security'] = 'max-age=31536000; includeSubDomains; preload'
    response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, private'
    response.headers['Pragma'] = 'no-cache'
    return response

_login_attempts = {}
_LOGIN_MAX_ATTEMPTS = 10
_LOGIN_WINDOW = 300
_login_cleanup_ts = 0

def _check_login_rate_limit(ip):
    global _login_cleanup_ts
    now = time.time()
    # Cleanup stale entries
    if now - _login_cleanup_ts > 600:
        stale = [k for k, (_, t) in _login_attempts.items() if now - t > _LOGIN_WINDOW * 2]
        for k in stale:
            del _login_attempts[k]
        _login_cleanup_ts = now
    if ip in _login_attempts:
        count, first = _login_attempts[ip]
        if now - first > _LOGIN_WINDOW:
            _login_attempts[ip] = (1, now)
            return True
        if count >= _LOGIN_MAX_ATTEMPTS:
            return False
        _login_attempts[ip] = (count + 1, first)
        return True
    _login_attempts[ip] = (1, now)
    return True

# ── Global API rate limiting (with periodic cleanup) ──
_api_calls = {}
_API_MAX_RPM = 120  # max requests per minute per IP
_API_WINDOW  = 60
_api_cleanup_ts = 0

def _check_api_rate_limit():
    global _api_cleanup_ts
    ip = request.headers.get('X-Forwarded-For', '').split(',')[0].strip() or request.remote_addr or "unknown"
    now = time.time()
    # Periodic cleanup: purge stale entries every 10 min to prevent memory leak
    if now - _api_cleanup_ts > 600:
        stale = [k for k, (_, t) in _api_calls.items() if now - t > _API_WINDOW * 2]
        for k in stale:
            del _api_calls[k]
        _api_cleanup_ts = now
    if ip in _api_calls:
        calls, first = _api_calls[ip]
        if now - first > _API_WINDOW:
            _api_calls[ip] = (1, now)
            return True
        if calls >= _API_MAX_RPM:
            return False
        _api_calls[ip] = (calls + 1, first)
        return True
    _api_calls[ip] = (1, now)
    return True

@app.before_request
def global_rate_limit():
    if request.path.startswith('/api/'):
        if not _check_api_rate_limit():
            return jsonify({"error": "Rate limit exceeded"}), 429

_TICKER_RE = re.compile(r'^[A-Za-z0-9.&Ññ^]{1,20}$')

# Aliases para índices — el usuario escribe sin ^ y el sistema lo resuelve
INDEX_ALIASES = {
    "IPC":   "^MXX",    # S&P/BMV IPC (alias común)
    "MXX":   "^MXX",    # S&P/BMV IPC
    "GSPC":  "^GSPC",   # S&P 500
    "SPX":   "^GSPC",   # S&P 500 (alias Bloomberg)
    "DJI":   "^DJI",    # Dow Jones
    "IXIC":  "^IXIC",   # NASDAQ Composite
    "RUT":   "^RUT",    # Russell 2000
    "VIX":   "^VIX",    # CBOE Volatility
    "FTSE":  "^FTSE",   # FTSE 100
    "N225":  "^N225",   # Nikkei 225
    "HSI":   "^HSI",    # Hang Seng
}

# Metadata de índices — moneda, geo y sectores aproximados
INDEX_META = {
    "^MXX": {
        "moneda": "MXN",
        "geo": {"Latin America": 100.0},
        "sec": {
            "Financiero": 25.0, "Consumo Básico": 20.0, "Materiales": 15.0,
            "Comunicaciones": 12.0, "Industriales": 10.0, "Consumo Discrecional": 8.0,
            "Energía": 5.0, "Salud": 3.0, "Bienes Raíces": 2.0,
        },
    },
    "^GSPC": {
        "moneda": "USD",
        "geo": {"United States": 100.0},
        "sec": {
            "Tecnología": 33.0, "Financiero": 13.0, "Salud": 12.0,
            "Consumo Discrecional": 10.0, "Comunicaciones": 9.0, "Industriales": 9.0,
            "Consumo Básico": 6.0, "Energía": 3.5, "Utilidades": 2.5, "Bienes Raíces": 2.0,
        },
    },
    "^DJI": {
        "moneda": "USD",
        "geo": {"United States": 100.0},
        "sec": {"Financiero": 22.0, "Tecnología": 20.0, "Salud": 18.0, "Industriales": 15.0, "Consumo Discrecional": 13.0, "Energía": 5.0, "Otros": 7.0},
    },
    "^IXIC": {
        "moneda": "USD",
        "geo": {"United States": 100.0},
        "sec": {"Tecnología": 55.0, "Comunicaciones": 15.0, "Consumo Discrecional": 13.0, "Salud": 8.0, "Financiero": 4.0, "Otros": 5.0},
    },
}

def _valid_ticker(t: str) -> bool:
    return bool(_TICKER_RE.match(t))


# ─────────────────────────────────────────────────────────────────────────────
# RUTAS
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        ip = request.headers.get('X-Forwarded-For', '').split(',')[0].strip() or request.remote_addr or "unknown"
        if not _check_login_rate_limit(ip):
            return jsonify({"ok": False, "error": "Demasiados intentos. Espera 5 minutos."}), 429
        data = request.get_json(force=True)
        u    = data.get("usuario", "").strip().lower()
        p    = data.get("password", "").strip()
        user = USERS.get(u)
        if user and check_password_hash(user["password"], p):
            session.clear()
            session["usuario"] = u
            session.permanent = True
            print(f"[AUDIT] LOGIN_OK user={u} ip={ip}", flush=True)
            return jsonify({"ok":True,"nombre":user["nombre"],"iniciales":user["iniciales"],"rol":user["rol"]})
        print(f"[AUDIT] LOGIN_FAIL user={u} ip={ip}", flush=True)
        return jsonify({"ok": False}), 401
    return send_file(os.path.join(BASE, "login.html"))

@app.route("/logout")
def logout():
    u = session.get("usuario", "?")
    session.clear()
    print(f"[AUDIT] LOGOUT user={u} ip={request.remote_addr}", flush=True)
    return redirect(url_for("login"))

@app.route("/me")
def me():
    u = session.get("usuario")
    if not u or u not in USERS:
        return jsonify({"ok": False}), 401
    user = USERS[u]
    return jsonify({"ok":True,"nombre":user["nombre"],"iniciales":user["iniciales"],"rol":user["rol"]})

@app.route("/api/change-password", methods=["POST"])
def change_password():
    u = session.get("usuario")
    if not u or u not in USERS:
        return jsonify({"ok": False, "error": "No autenticado"}), 401
    ip = request.remote_addr or "unknown"
    if not _check_login_rate_limit(ip):
        return jsonify({"ok": False, "error": "Demasiados intentos. Espera 5 minutos."}), 429
    data = request.get_json(force=True)
    current  = (data.get("current") or "").strip()
    new_pass = (data.get("new_password") or "").strip()
    confirm  = (data.get("confirm") or "").strip()
    user = USERS[u]
    if not check_password_hash(user["password"], current):
        return jsonify({"ok": False, "error": "Contraseña actual incorrecta"}), 401
    if len(new_pass) < 8:
        return jsonify({"ok": False, "error": "Mínimo 8 caracteres"}), 400
    if new_pass != confirm:
        return jsonify({"ok": False, "error": "Las contraseñas no coinciden"}), 400
    if new_pass == current:
        return jsonify({"ok": False, "error": "La nueva contraseña debe ser diferente"}), 400
    new_hash = generate_password_hash(new_pass)
    USERS[u]["password"] = new_hash
    _save_password_override(u, new_hash)
    print(f"[AUDIT] PASSWORD_CHANGED user={u} ip={ip}", flush=True)
    session.clear()
    session["usuario"] = u
    session.permanent = True
    return jsonify({"ok": True, "message": "Contraseña actualizada"})

@app.route("/PC.pdf")
def pc_pdf():
    if "usuario" not in session:
        return redirect(url_for("login"))
    return send_from_directory(BASE, "PC.pdf")

@app.route("/VALMEX.png")
def valmex_logo():
    # No auth — needed by login page before user is authenticated
    return send_from_directory(BASE, "VALMEX.png")

@app.route("/VALMEX2.png")
def valmex_logo2():
    if "usuario" not in session:
        return redirect(url_for("login"))
    return send_from_directory(BASE, "VALMEX2.png")

@app.route("/")
def index():
    if "usuario" not in session:
        return redirect(url_for("login"))
    with open(os.path.join(BASE, "valmex_dashboard.html"), "r", encoding="utf-8") as f:
        html = f.read()
    return make_response(html)


@app.route("/api/accion/validate", methods=["POST"])
def api_accion_validate():
    if "usuario" not in session:
        return jsonify({"ok": False, "error": "No autenticado"}), 401
    body   = request.get_json(force=True)
    ticker = (body.get("ticker") or "").strip().upper()
    if not ticker or not _valid_ticker(ticker):
        return jsonify({"ok": False, "error": "Ticker inválido"}), 400

    db_key    = ticker.replace(".MX", "")
    # Normalizar caracteres especiales BMV (ñ/Ñ → & para Yahoo Finance)
    db_key    = db_key.replace("Ñ", "&").replace("ñ", "&")

    # Índices: resolver alias (MXX → ^MXX, SPX → ^GSPC, etc.)
    if db_key in INDEX_ALIASES:
        idx_ticker = INDEX_ALIASES[db_key]
        data = get_accion_yf(idx_ticker)
        if data:
            return jsonify({"ok": True, "data": data, "fuente": "yahoo_index"})
    # Índices con ^ explícito (^MXX, ^GSPC, etc.)
    if ticker.startswith("^"):
        data = get_accion_yf(ticker)
        if data:
            return jsonify({"ok": True, "data": data, "fuente": "yahoo_index"})

    mx_ticker = db_key + ".MX"

    # 1. Yahoo Finance SIC — ticker con .MX (MXN), precio más preciso
    data = get_accion_yf(mx_ticker)
    if data:
        return jsonify({"ok": True, "data": data, "fuente": "yahoo_sic"})

    # 2. DataBursatil — fallback para emisoras que YF no tenga
    if DB_TOKEN:
        data = get_accion_db(db_key)
        if data:
            return jsonify({"ok": True, "data": data, "fuente": "databursatil"})

    # 3. Último recurso: Yahoo Finance global (solo si los anteriores fallaron)
    if ticker != mx_ticker:
        data = get_accion_yf(ticker)
        if data:
            return jsonify({"ok": True, "data": data, "fuente": "yahoo_global"})

    return jsonify({"ok": False, "error": f"'{db_key}' no encontrado en BMV/SIC. Verifica el ticker."}), 404


_compute_semaphore = threading.Semaphore(3)  # Max 3 concurrent portfolio computations

@app.route("/api/propuesta", methods=["POST"])
def api_propuesta():
    if "usuario" not in session:
        return jsonify({"ok": False, "error": "No autenticado"}), 401

    body         = request.get_json(force=True)
    tipo_cliente = body.get("tipo_cliente", "Serie A")
    modo         = body.get("modo", "propuesta")

    if modo == "perfil":
        pid = str(body.get("perfil_id", "3"))
        fondos_pct = PERFILES.get(pid)
        if not fondos_pct:
            return jsonify({"ok": False, "error": f"Perfil {pid} no existe"}), 400
    else:
        raw = body.get("fondos", {})
        try:
            fondos_pct = {k: float(v) for k, v in raw.items() if float(v) > 0}
            if any(v > 100 or v < 0 for v in fondos_pct.values()):
                return jsonify({"ok": False, "error": "Porcentaje fuera de rango"}), 400
            if len(fondos_pct) > 25:
                return jsonify({"ok": False, "error": "Máximo 25 fondos"}), 400
        except (ValueError, TypeError):
            return jsonify({"ok": False, "error": "Datos numéricos inválidos"}), 400
        repo_mxn = body.get("repo_mxn")
        repo_usd = body.get("repo_usd")
        acciones_raw = body.get("acciones", [])
        if not fondos_pct and not repo_mxn and not repo_usd and not acciones_raw:
            return jsonify({"ok": False, "error": "Sin fondos con % > 0"}), 400
        # Validate date range
        bt_ini = body.get("bt_fecha_ini")
        bt_fin = body.get("bt_fecha_fin")
        if bt_ini and bt_fin:
            try:
                from datetime import date as _d
                _di = _d.fromisoformat(str(bt_ini)[:10])
                _df = _d.fromisoformat(str(bt_fin)[:10])
                if (_df - _di).days > 7300:
                    return jsonify({"ok": False, "error": "Rango máximo 20 años"}), 400
                if _di > _df:
                    return jsonify({"ok": False, "error": "Fecha inicio > fin"}), 400
            except (ValueError, TypeError):
                pass

    if not _compute_semaphore.acquire(timeout=0.5):
        return jsonify({"ok": False, "error": "Servidor ocupado, intenta de nuevo en unos segundos."}), 503

    try:
        return jsonify(calcular_portafolio(fondos_pct, tipo_cliente,
                                            repo_mxn=body.get("repo_mxn"),
                                            repo_usd=body.get("repo_usd"),
                                            acciones=body.get("acciones", []),
                                            bt_fecha_ini=body.get("bt_fecha_ini"),
                                            bt_fecha_fin=body.get("bt_fecha_fin")))
    except Exception as e:
        print(f"[ERROR] api_propuesta: {e}")
        return jsonify({"ok": False, "error": "Error interno en cálculo"}), 500
    finally:
        _compute_semaphore.release()


# ─────────────────────────────────────────────────────────────────────────────
# MACRO
# ─────────────────────────────────────────────────────────────────────────────
BANXICO_TOKEN = os.environ.get("BANXICO_TOKEN", "") or "592b06934a31710cba9e9a6efebec12c1fe432f5459fc87e7f473380fa0a1d3a"
BANXICO_BASE  = "https://www.banxico.org.mx/SieAPIRest/service/v1/series"
FRED_API_KEY  = os.environ.get("FRED_API_KEY", "") or "1a6dadbec2267dd21b3ad5d6447ed711"
FRED_BASE     = "https://api.stlouisfed.org/fred/series/observations"
# Warn if using hardcoded fallback keys (should be set via env vars in production)
if not os.environ.get("BANXICO_TOKEN"):
    print("[SECURITY] WARNING: Using fallback Banxico token — set BANXICO_TOKEN env var in production")
if not os.environ.get("FRED_API_KEY"):
    print("[SECURITY] WARNING: Using fallback FRED key — set FRED_API_KEY env var in production")
if not os.environ.get("MS_ACCESS"):
    print("[SECURITY] WARNING: Using fallback Morningstar access — set MS_ACCESS env var in production")

SERIE_TIIE28  = "SF43783"
SERIE_USDMXN  = "SF43718"
SERIE_CETES28 = "SF60633"
SERIE_FONDEO  = "SF43936"
SERIE_USD_REPO = "SOFR"

_hist_cache = {}; _hist_cache_ts = 0


_banxico_rango_cache = {}  # (serie, ini, fin) -> result, daily

def _banxico_serie_rango(serie_id, fecha_ini, fecha_fin):
    _bk = (serie_id, fecha_ini, fecha_fin)
    if _bk in _banxico_rango_cache:
        return _banxico_rango_cache[_bk]
    try:
        url  = f"{BANXICO_BASE}/{serie_id}/datos/{fecha_ini}/{fecha_fin}"
        hdrs = {"Bmx-Token": BANXICO_TOKEN, "Accept": "application/json"}
        r    = requests.get(url, headers=hdrs, timeout=15)
        r.raise_for_status()
        datos = r.json()["bmx"]["series"][0].get("datos", [])
        result = []
        for d in datos:
            try:
                result.append({"fecha": d["fecha"], "valor": float(d["dato"].replace(",", "."))})
            except Exception:
                pass
        _banxico_rango_cache[_bk] = result
        return result
    except Exception as e:
        print(f"[BANXICO HIST ERROR] {serie_id}: {e}")
        return []


def _parse_fecha(s):
    try:
        if "/" in s:
            d, m, y = s.split("/")
            return date(int(y), int(m), int(d))
        return date.fromisoformat(s[:10])
    except Exception:
        return None


def _get_datos_hist(es_usd):
    global _hist_cache, _hist_cache_ts
    cache_key = "usd" if es_usd else "mxn"
    now = time.time()
    if cache_key in _hist_cache and not _cache_expired(_hist_cache_ts):
        return _hist_cache[cache_key]
    hoy = date.today(); ini = "2000-01-01"; fin = hoy.isoformat()
    if es_usd:
        # Combinar DFF (Fed Funds, desde 2000) + SOFR (desde 2018-04) para historia completa
        datos_dff  = []
        datos_sofr = []
        for serie in ["DFF", SERIE_USD_REPO]:
            try:
                params = {"series_id": serie, "observation_start": ini, "observation_end": fin,
                          "api_key": FRED_API_KEY, "file_type": "json"}
                r = requests.get(FRED_BASE, params=params, timeout=15)
                r.raise_for_status()
                obs = [o for o in r.json().get("observations", []) if o["value"] != "."]
                parsed = [{"fecha": _parse_fecha(o["date"]), "valor": float(o["value"])} for o in obs]
                parsed = [d for d in parsed if d["fecha"] is not None]
                if serie == "DFF":
                    datos_dff = parsed
                    print(f"[FRED] DFF: {len(parsed)} registros OK")
                else:
                    datos_sofr = parsed
                    print(f"[FRED] SOFR: {len(parsed)} registros OK")
            except Exception as e:
                print(f"[FRED {serie} ERROR] {e}")
        # SOFR desde su inicio (2018-04), DFF para antes
        if datos_sofr:
            sofr_start = datos_sofr[0]["fecha"]
            datos = [d for d in datos_dff if d["fecha"] < sofr_start] + datos_sofr
            print(f"[HIST USD] Combinado: DFF hasta {sofr_start} + SOFR desde {sofr_start}")
        elif datos_dff:
            datos = datos_dff
        else:
            datos = []
    else:
        raw   = _banxico_serie_rango(SERIE_FONDEO, ini, fin)
        datos = [{"fecha": _parse_fecha(d["fecha"]), "valor": d["valor"]} for d in raw if _parse_fecha(d["fecha"])]
    datos = sorted([d for d in datos if d["fecha"] is not None], key=lambda x: x["fecha"])
    _hist_cache[cache_key] = datos; _hist_cache_ts = now
    print(f"[HIST {'USD' if es_usd else 'MXN'}] {len(datos)} registros desde {datos[0]['fecha'] if datos else 'N/A'}")
    return datos


def get_repo_rendimientos(tasa_neta, es_usd):
    datos = _get_datos_hist(es_usd)
    if not datos:
        anual = tasa_neta
        # Effective cumulative approximation from annual rate
        r1y_eff = round(anual, 6)
        r2y_eff = round(((1 + anual/100)**2 - 1) * 100, 6)
        r3y_eff = round(((1 + anual/100)**3 - 1) * 100, 6)
        return {"r1m":round(anual/12,6),"r3m":round(anual/4,6),"r6m":round(anual/2,6),
                "ytd":round(anual/12,6),"r1y":r1y_eff,"r2y":r2y_eff,"r3y":r3y_eff,"backtesting":[]}
    hoy = date.today(); tasa_ref_hoy = datos[-1]["valor"]; spread = tasa_ref_hoy - tasa_neta
    def componer_acum(desde):
        acum = 1.0; ultimo = None; rango = [d for d in datos if d["fecha"] >= desde]
        if not rango: return 0.0
        d_actual = desde; idx = 0
        while d_actual <= hoy:
            while idx < len(rango) and rango[idx]["fecha"] <= d_actual:
                ultimo = rango[idx]["valor"]; idx += 1
            if ultimo is not None:
                tasa_dia = max(0.0, ultimo - spread)
                acum *= (1 + tasa_dia / 360 / 100)
            d_actual += timedelta(days=1)
        return acum - 1
    def anualizar(acum_dec, años):
        if acum_dec <= -1: return -100.0
        return round(((1 + acum_dec) ** (1 / años) - 1) * 100, 6)
    def efectivo(acum_dec): return round(acum_dec * 100, 6)
    inicio_ytd = date(hoy.year, 1, 1)
    ini_back = date(2000, 1, 1)
    if datos and datos[0]["fecha"] > ini_back: ini_back = datos[0]["fecha"]
    bt_puntos = []; cur = date(ini_back.year, ini_back.month, 1)
    acum_bt = 1.0; ultimo = None; idx_bt = 0
    datos_bt = [d for d in datos if d["fecha"] >= ini_back]; d_cur = ini_back
    while d_cur <= hoy:
        while idx_bt < len(datos_bt) and datos_bt[idx_bt]["fecha"] <= d_cur:
            ultimo = datos_bt[idx_bt]["valor"]; idx_bt += 1
        if ultimo is not None:
            tasa_dia = max(0.0, ultimo - spread)
            acum_bt *= (1 + tasa_dia / 360 / 100)
        if d_cur.day == 1 or d_cur == ini_back:
            bt_puntos.append({"fecha": d_cur.isoformat(), "valor": round(acum_bt * 100, 4)})
        d_cur += timedelta(days=1)
    return {"r1m":efectivo(componer_acum(hoy-timedelta(days=30))),"r3m":efectivo(componer_acum(hoy-timedelta(days=91))),
            "r6m":efectivo(componer_acum(hoy-timedelta(days=182))),"ytd":efectivo(componer_acum(inicio_ytd)),
            "r1y":efectivo(componer_acum(hoy-timedelta(days=365))),"r2y":efectivo(componer_acum(hoy-timedelta(days=730))),
            "r3y":efectivo(componer_acum(hoy-timedelta(days=1095))),"backtesting":bt_puntos}


def get_banxico_dato(serie_id):
    try:
        url  = f"{BANXICO_BASE}/{serie_id}/datos/oportuno"
        hdrs = {"Bmx-Token": BANXICO_TOKEN, "Accept": "application/json"}
        resp = requests.get(url, headers=hdrs, timeout=10)
        resp.raise_for_status()
        datos = resp.json()["bmx"]["series"][0]["datos"]
        return datos[0]["dato"] if datos else None
    except Exception as e:
        print(f"[BANXICO ERROR] {serie_id}: {e}")
        return None


# ── QUILT CHART (Rendimientos por Clase de Activo) ──────────────────────
_quilt_cache = {"data": None, "ts": 0}
_quilt_ms_ticker_cache = {}  # Persistent across calls (not local to _compute_quilt)
_prewarm_done = threading.Event()  # Set when prewarm completes

def _compute_quilt():
    today = date.today()
    current_year = today.year
    years = list(range(2017, current_year + 1))

    def bx_series(serie):
        try:
            url = f"https://www.banxico.org.mx/SieAPIRest/service/v1/series/{serie}/datos/2015-12-01/{today.isoformat()}"
            r = requests.get(url, headers={"Bmx-Token": BANXICO_TOKEN, "Accept": "application/json"}, timeout=15)
            r.raise_for_status()
            datos = r.json()["bmx"]["series"][0]["datos"]
            out = {}
            for d in datos:
                try:
                    f = datetime.strptime(d["fecha"], "%d/%m/%Y")
                    out[f] = float(d["dato"].replace(",", ""))
                except: pass
            return pd.Series(out).sort_index()
        except Exception as e:
            print(f"[QUILT] Banxico {serie} error: {e}")
            return pd.Series(dtype=float)

    def fred_series(series_id):
        try:
            params = {"series_id": series_id, "api_key": FRED_API_KEY, "file_type": "json",
                      "observation_start": "2015-12-01", "observation_end": today.isoformat()}
            r = requests.get(FRED_BASE, params=params, timeout=15)
            obs = r.json().get("observations", [])
            out = {}
            for o in obs:
                try: out[datetime.strptime(o["date"], "%Y-%m-%d")] = float(o["value"])
                except: pass
            return pd.Series(out).sort_index()
        except Exception as e:
            print(f"[QUILT] FRED {series_id} error: {e}")
            return pd.Series(dtype=float)

    MS_NAV_TICKER_URL = "https://api.morningstar.com/service/mf/UnadjustedNAV/TICKER"

    def ms_series(ticker):
        """Fetch daily NAV/price from Morningstar by TICKER symbol (cached daily)."""
        cached = _quilt_ms_ticker_cache.get(ticker)
        if cached and not _cache_expired(cached["ts"]):
            return cached["data"]
        try:
            r = _ms_session.get(
                f"{MS_NAV_TICKER_URL}/{ticker}",
                params={"startdate": "2015-12-01", "enddate": today.isoformat(),
                        "accesscode": MS_ACCESS},
                timeout=15,
            )
            r.raise_for_status()
            root = ET.fromstring(r.text)
            out = {}
            for elem in root.iter("r"):
                try:
                    out[datetime.strptime(elem.get("d"), "%Y-%m-%d")] = float(elem.get("v"))
                except: pass
            result = pd.Series(out).sort_index()
            _quilt_ms_ticker_cache[ticker] = {"ts": time.time(), "data": result}
            print(f"[QUILT MS] {ticker}: {len(out)} prices")
            return result
        except Exception as e:
            print(f"[QUILT MS ERROR] {ticker}: {e}")
            return pd.Series(dtype=float)

    def ye(s, y):
        sub = s[s.index.year == y]
        if len(sub) == 0:
            return None
        val = sub.iloc[-1]
        # Handle yfinance MultiIndex columns returning Series instead of scalar
        if isinstance(val, pd.Series):
            val = val.iloc[0]
        return float(val)

    def ev(s, y):
        return ye(s, y)

    def ar(s, y):
        e = ev(s, y); st = ye(s, y - 1)
        if e is not None and st is not None and st != 0:
            return round((e / st - 1) * 100, 2)
        return None

    def ar_mxn(usd_s, fx_s, y):
        eu, su = ev(usd_s, y), ye(usd_s, y - 1)
        ef, sf = ev(fx_s, y), ye(fx_s, y - 1)
        if all(v is not None for v in [eu, su, ef, sf]) and su != 0 and sf != 0:
            return round((eu * ef / (su * sf) - 1) * 100, 2)
        return None

    # Fetch all data in parallel (Banxico, FRED, Morningstar, Yahoo Finance)
    # Fetch all data in parallel — Banxico, FRED, Morningstar only (no yfinance)
    # Gold & Oil now use FRED instead of yfinance to avoid rate limiting on Render
    _pool = ThreadPoolExecutor(max_workers=14)
    _fx_f = _pool.submit(bx_series, "SF43718")
    _cetes_f = _pool.submit(bx_series, "SF43936")
    _inpc_f = _pool.submit(bx_series, "SP1")
    _tasa_f = _pool.submit(bx_series, "SF61745")
    _bond_f = _pool.submit(fred_series, "IRLTLT01MXM156N")
    _gold_f = _pool.submit(ms_series, "IAU")                   # iShares Gold Trust (USD)
    _oil_f = _pool.submit(fred_series, "DCOILWTICO")           # WTI Crude USD/bbl
    _naftrac_f = _pool.submit(ms_series, "NAFTRAC")
    _eem_f = _pool.submit(ms_series, "EEM")
    _urth_f = _pool.submit(ms_series, "URTH")
    _bwx_f = _pool.submit(ms_series, "BWX")
    _qqq_f = _pool.submit(ms_series, "QQQ")
    fx = _fx_f.result(timeout=20)
    cetes = _cetes_f.result(timeout=20)
    inpc = _inpc_f.result(timeout=20)
    tasa = _tasa_f.result(timeout=20)
    bond10y = _bond_f.result(timeout=20)
    gold = _gold_f.result(timeout=20)
    oil = _oil_f.result(timeout=20)
    naftrac = _naftrac_f.result(timeout=20)
    eem = _eem_f.result(timeout=20)
    urth = _urth_f.result(timeout=20)
    bwx = _bwx_f.result(timeout=20)
    qqq = _qqq_f.result(timeout=20)
    _pool.shutdown(wait=False)

    rets = {}

    # 1. Dólar
    rets["Dólar"] = {str(y): ar(fx, y) for y in years}

    # 2. Bolsa Local (NAFTRAC — IPC tracker, ya en MXN, incluye dividendos)
    rets["Bolsa Local"] = {str(y): v for y in years if (v := ar(naftrac, y)) is not None}

    # 3-5. USD assets converted to MXN
    rets["Bolsa Emergentes"] = {str(y): v for y in years if (v := ar_mxn(eem, fx, y)) is not None}
    rets["Mercados Desarrollados"] = {str(y): v for y in years if (v := ar_mxn(urth, fx, y)) is not None}
    rets["Deuda Gubernamental Global"] = {str(y): v for y in years if (v := ar_mxn(bwx, fx, y)) is not None}

    # 6. Deuda Corto Plazo (avg CETES 28d yield)
    dcp = {}
    for y in years:
        sub = cetes[cetes.index.year == y]
        if len(sub) > 0:
            avg_rate = float(sub.mean())
            if y == current_year:
                months_el = sub.index[-1].month
                dcp[str(y)] = round(avg_rate * months_el / 12, 2)
            else:
                dcp[str(y)] = round(avg_rate, 2)
    rets["Deuda Corto Plazo"] = dcp

    # 7. Deuda Largo Plazo (duration model from 10Y yield)
    dlp = {}
    DUR_LP = 7.0
    for y in years:
        y0, y1 = ye(bond10y, y - 1), ev(bond10y, y)
        if y0 is not None and y1 is not None:
            carry = y0 / 100
            delta = (y1 - y0) / 100
            if y == current_year:
                sub_b = bond10y[bond10y.index.year == y]
                months_el = sub_b.index[-1].month if len(sub_b) > 0 else 1
                dlp[str(y)] = round((carry * months_el / 12 - DUR_LP * delta) * 100, 2)
            else:
                dlp[str(y)] = round((carry - DUR_LP * delta) * 100, 2)
    rets["Deuda Largo Plazo"] = dlp

    # 8. Tecnología (QQQ — Invesco QQQ Trust, USD → MXN) — already fetched in parallel above
    rets["Tecnolog\u00eda"] = {str(y): v for y in years if (v := ar_mxn(qqq, fx, y)) is not None}

    # 9. Oro (Gold futures USD/oz → MXN)
    rets["Oro"] = {str(y): v for y in years if (v := ar_mxn(gold, fx, y)) is not None}

    # 10. Diversificado (equal-weight)
    ac_names = ["D\u00f3lar", "Bolsa Local", "Bolsa Emergentes", "Mercados Desarrollados",
                "Deuda Gubernamental Global", "Deuda Corto Plazo", "Deuda Largo Plazo", "Tecnolog\u00eda", "Oro"]
    div = {}
    for y in years:
        vals = [rets[n].get(str(y)) for n in ac_names]
        valid = [v for v in vals if v is not None]
        if len(valid) >= 5: div[str(y)] = round(sum(valid) / len(valid), 2)
    rets["Diversificado"] = div

    # Cumulative & annualized
    cumulative, annualized = {}, {}
    for name, r in rets.items():
        cum, cnt = 1.0, 0
        for y in years:
            v = r.get(str(y))
            if v is not None: cum *= (1 + v / 100); cnt += 1
        if cnt > 0:
            cumulative[name] = round((cum - 1) * 100, 1)
            annualized[name] = round((cum ** (1 / cnt) - 1) * 100, 1)

    # Reference rows
    ref_tasa, ref_infl = {}, {}
    for y in years:
        # Tasa de referencia: promedio anual (prorated for current year)
        sub_tasa = tasa[tasa.index.year == y]
        if len(sub_tasa) > 0:
            avg_tasa = float(sub_tasa.mean())
            if y == current_year:
                months_el = sub_tasa.index[-1].month
                ref_tasa[str(y)] = round(avg_tasa * months_el / 12, 2)
            else:
                ref_tasa[str(y)] = round(avg_tasa, 2)
        ie, is_ = ev(inpc, y), ye(inpc, y - 1)
        if ie and is_ and is_ != 0:
            raw = ie / is_ - 1
            ref_infl[str(y)] = round(raw * 100, 2)

    colors = {
        "Bolsa Emergentes": "#00205C",          # Navy (brand)
        "Mercados Desarrollados": "#41BBC9",     # Sky (brand)
        "Bolsa Local": "#3DA5E0",               # Blue (brand)
        "Tecnolog\u00eda": "#7CC677",            # Verde (complementario)
        "Deuda Largo Plazo": "#0D3A7A",         # Navy dark shade
        "Deuda Corto Plazo": "#8FAFC4",         # Silver-Navy blend
        "Deuda Gubernamental Global": "#058B97", # Teal oscuro (paleta extendida)
        "D\u00f3lar": "#EC626E",                # Rojo (complementario)
        "Oro": "#E8A838",                       # Naranja/dorado pastel
        "Diversificado": "#A25EB5",             # P\u00farpura (paleta extendida)
    }

    asset_order = ["Bolsa Emergentes", "Deuda Corto Plazo", "Mercados Desarrollados",
                   "Tecnolog\u00eda", "Bolsa Local", "Deuda Largo Plazo",
                   "Oro", "Diversificado", "Deuda Gubernamental Global", "Dólar"]

    assets = [{"name": n, "color": colors.get(n, "#999"),
               "returns": {k: v for k, v in rets.get(n, {}).items() if v is not None}}
              for n in asset_order]

    # Cumulative & annualized for reference rows
    def _ref_cum_ann(vals):
        cum, cnt = 1.0, 0
        for y in years:
            v = vals.get(str(y))
            if v is not None: cum *= (1 + v / 100); cnt += 1
        if cnt > 0:
            return round((cum - 1) * 100, 1), round((cum ** (1 / cnt) - 1) * 100, 1)
        return None, None

    tasa_cum, tasa_ann = _ref_cum_ann(ref_tasa)
    infl_cum, infl_ann = _ref_cum_ann(ref_infl)

    return {
        "ok": True, "years": years, "assets": assets,
        "reference": [
            {"name": "Tasa de Referencia", "values": ref_tasa, "cumulative": tasa_cum, "annualized": tasa_ann},
            {"name": "Inflaci\u00f3n", "values": ref_infl, "cumulative": infl_cum, "annualized": infl_ann},
        ],
        "cumulative": cumulative, "annualized": annualized,
        "note": "Rendimientos brutos efectivos en moneda nacional. Fuentes: Morningstar (NAFTRAC, EEM, URTH, BWX, QQQ), Yahoo Finance (Oro GC=F, Petr\u00f3leo CL=F), Banxico (FX FIX, CETES, INPC, Tasa Objetivo), FRED (Bono M10 yield). Deuda LP: modelo de duraci\u00f3n (carry + delta yield \u00d7 dur). Activos en USD convertidos a MXN con tipo de cambio FIX Banxico.",
        "updated": datetime.now().strftime("%d/%m/%Y"),
    }

@app.route("/api/diag-apis")
def api_diag_apis():
    """Quick health check for all external APIs used by quilt."""
    if "usuario" not in session:
        return jsonify({"ok": False, "error": "No autenticado"}), 401
    import time as _t
    results = {}
    # 1. Banxico
    try:
        t0 = _t.time()
        r = requests.get(
            f"https://www.banxico.org.mx/SieAPIRest/service/v1/series/SF43718/datos/2025-01-01/2025-01-31",
            headers={"Bmx-Token": BANXICO_TOKEN, "Accept": "application/json"}, timeout=10)
        results["banxico"] = {"status": r.status_code, "time": round(_t.time()-t0, 2), "ok": r.ok}
    except Exception as e:
        results["banxico"] = {"error": str(e), "ok": False}
    # 2. FRED
    try:
        t0 = _t.time()
        r = requests.get(FRED_BASE, params={"series_id": "DGS10", "api_key": FRED_API_KEY,
            "file_type": "json", "observation_start": "2025-01-01", "observation_end": "2025-01-31"}, timeout=10)
        results["fred"] = {"status": r.status_code, "time": round(_t.time()-t0, 2), "ok": r.ok}
    except Exception as e:
        results["fred"] = {"error": str(e), "ok": False}
    # 3. Morningstar
    try:
        t0 = _t.time()
        r = _ms_session.get(
            "https://api.morningstar.com/service/mf/UnadjustedNAV/TICKER/NAFTRAC",
            params={"startdate": "2025-01-01", "enddate": "2025-01-31", "accesscode": MS_ACCESS}, timeout=10)
        results["morningstar"] = {"status": r.status_code, "time": round(_t.time()-t0, 2), "ok": r.ok}
    except Exception as e:
        results["morningstar"] = {"error": str(e), "ok": False}
    # 4. Morningstar IAU (Gold proxy — replaced yfinance)
    try:
        t0 = _t.time()
        r = _ms_session.get(
            "https://api.morningstar.com/service/mf/UnadjustedNAV/TICKER/IAU",
            params={"startdate": "2025-01-01", "enddate": "2025-01-31", "accesscode": MS_ACCESS}, timeout=10)
        results["ms_gold_iau"] = {"status": r.status_code, "time": round(_t.time()-t0, 2), "ok": r.ok}
    except Exception as e:
        results["ms_gold_iau"] = {"error": str(e), "ok": False}
    # Prewarm status
    results["prewarm_done"] = _prewarm_done.is_set()
    results["quilt_cached"] = _quilt_cache["data"] is not None
    results["quilt_fondos_cached"] = _quilt_fondos_cache["data"] is not None
    return jsonify(results)

@app.route("/api/quilt")
def api_quilt():
    if "usuario" not in session:
        return jsonify({"ok": False, "error": "No autenticado"}), 401
    # Return cached data immediately if available
    if _quilt_cache["data"] and not _cache_expired(_quilt_cache["ts"]):
        return jsonify(_quilt_cache["data"])
    # Wait for prewarm (up to 25s — Render proxy kills at 30s)
    if not _prewarm_done.is_set():
        _prewarm_done.wait(timeout=25)
        if _quilt_cache["data"]:
            return jsonify(_quilt_cache["data"])
        return jsonify({"ok": False, "loading": True, "error": "Datos cargando, reintenta en unos segundos"}), 202
    # Prewarm finished but cache empty (prewarm failed) — try once with timeout
    from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
    try:
        with ThreadPoolExecutor(max_workers=1) as ex:
            future = ex.submit(_compute_quilt)
            data = future.result(timeout=25)
        _quilt_cache["data"] = data
        _quilt_cache["ts"] = time.time()
        _disk_cache_save("quilt", data)
        return jsonify(data)
    except FuturesTimeout:
        print("[ERROR] api_quilt: _compute_quilt timed out (25s)")
        return jsonify({"ok": False, "error": "Timeout calculando datos históricos, reintenta"}), 504
    except Exception as e:
        print(f"[ERROR] api_quilt: {e}")
        return jsonify({"ok": False, "error": "Error al calcular datos históricos"}), 500


# ── QUILT FONDOS VALMEX (Top 10 por año) ──────────────────────────────
_quilt_fondos_cache = {"data": None, "ts": 0}

# Exact same 10 colors from the asset class quilt, assigned to funds
def _compute_quilt_fondos():
    from concurrent.futures import ThreadPoolExecutor, as_completed
    today = date.today()
    current_year = today.year
    years = list(range(2017, current_year + 1))

    # Collect all Series A ISINs
    fondos = {}
    for fondo, series in ISIN_MAP.items():
        isin = series.get("A")
        if isin:
            fondos[fondo] = isin

    # Fetch NAVs in parallel
    nav_series = {}

    def fetch_fund(fondo, isin):
        data = get_ms_nav(isin, start="2015-12-01")
        return fondo, data

    executor = ThreadPoolExecutor(max_workers=15)
    futures = {executor.submit(fetch_fund, f, i): f for f, i in fondos.items()}
    for future in as_completed(futures, timeout=45):
        try:
            fondo, data = future.result(timeout=20)
            if data:
                s = pd.Series(
                    {datetime.strptime(n["fecha"], "%Y-%m-%d"): n["nav"] for n in data}
                ).sort_index()
                nav_series[fondo] = s
                print(f"[QUILT FONDOS] {fondo}: {len(s)} NAVs")
        except Exception as e:
            fname = futures.get(future, "?")
            print(f"[QUILT FONDOS] Error {fname}: {e}")
    executor.shutdown(wait=False)

    # Calculate annual returns using operadora methodology:
    # End NAV = first business day of Y+1 (T+1 settlement), Start NAV = last day of Y-1
    # For current (incomplete) year: use last available NAV as end
    rets = {}
    for fondo, s in nav_series.items():
        fund_rets = {}
        for y in years:
            sub_start = s[s.index.year == y - 1]
            if len(sub_start) == 0:
                continue
            start_nav = float(sub_start.iloc[-1])
            # For completed years, use first NAV of next year (operadora convention)
            sub_next = s[s.index.year == y + 1]
            sub_curr = s[s.index.year == y]
            if y < current_year and len(sub_next) > 0:
                end_nav = float(sub_next.iloc[0])
            elif len(sub_curr) > 0:
                end_nav = float(sub_curr.iloc[-1])
            else:
                continue
            if start_nav > 0:
                ret = round((end_nav / start_nav - 1) * 100, 2)
                # Filter out restructurings/splits (no fund legitimately returns ±200%)
                if -200 <= ret <= 200:
                    fund_rets[str(y)] = ret
                else:
                    print(f"[QUILT FONDOS] {fondo} {y}: {ret}% skipped (likely restructuring)")
        if fund_rets:
            rets[fondo] = fund_rets

    # Determine which funds actually appear in top-10 for any year / cumulative / annualized
    visible_funds = set()
    str_years = [str(y) for y in years]
    for yr in str_years:
        yr_rets = [(f, r.get(yr)) for f, r in rets.items() if r.get(yr) is not None]
        yr_rets.sort(key=lambda x: x[1], reverse=True)
        for f, _ in yr_rets[:10]:
            visible_funds.add(f)

    _pre_cum = {}
    for name, r in rets.items():
        cum = 1.0
        for y in years:
            v = r.get(str(y))
            if v is not None: cum *= (1 + v / 100)
        _pre_cum[name] = cum - 1
    cum_sorted = sorted(_pre_cum.items(), key=lambda x: x[1], reverse=True)
    for f, _ in cum_sorted[:10]:
        visible_funds.add(f)

    # Assign colors positionally by cumulative ranking
    # Same 10-color order as asset class quilt RESUMEN, then extras
    _ASSET_QUILT_COLORS = [
        "#7CC677", "#E8A838", "#41BBC9", "#8FAFC4", "#A25EB5",
        "#0D3A7A", "#3DA5E0", "#00205C", "#EC626E", "#058B97",
    ]
    _EXTRA_COLORS = [
        "#A49E8B", "#CBC8C5", "#C7963D", "#D46B6B", "#2C9942",
        "#80BC38", "#A6D043", "#9B5A5A", "#5D8AA8", "#8B6B4A",
        "#6A5B7B", "#4A5568", "#34698A", "#3D7A70", "#6B7B8D",
    ]
    _ALL_POSITIONAL = _ASSET_QUILT_COLORS + _EXTRA_COLORS

    cum_ranked = sorted(visible_funds, key=lambda f: _pre_cum.get(f, -999), reverse=True)
    fund_color_map = {}
    for i, f in enumerate(cum_ranked):
        fund_color_map[f] = _ALL_POSITIONAL[i] if i < len(_ALL_POSITIONAL) else "#4A5568"

    # Build assets list
    assets = []
    for fondo in rets:
        assets.append({
            "name": fondo,
            "color": fund_color_map.get(fondo, "#666666"),
            "returns": rets[fondo],
        })

    # Cumulative & annualized
    cumulative, annualized, ann_years = {}, {}, {}
    for name, r in rets.items():
        cum, cnt = 1.0, 0
        for y in years:
            v = r.get(str(y))
            if v is not None:
                cum *= (1 + v / 100)
                cnt += 1
        if cnt > 0:
            cumulative[name] = round((cum - 1) * 100, 1)
            annualized[name] = round((cum ** (1 / cnt) - 1) * 100, 1)
            ann_years[name] = cnt

    # Reference rows: Tasa de Referencia e Inflación (same as asset quilt)
    def bx_series_fondos(serie):
        try:
            url = f"https://www.banxico.org.mx/SieAPIRest/service/v1/series/{serie}/datos/2015-12-01/{today.isoformat()}"
            r = requests.get(url, headers={"Bmx-Token": BANXICO_TOKEN, "Accept": "application/json"}, timeout=30)
            r.raise_for_status()
            datos = r.json()["bmx"]["series"][0]["datos"]
            out = {}
            for d in datos:
                try:
                    f = datetime.strptime(d["fecha"], "%d/%m/%Y")
                    out[f] = float(d["dato"].replace(",", ""))
                except: pass
            return pd.Series(out).sort_index()
        except Exception as e:
            print(f"[QUILT FONDOS] Banxico {serie} error: {e}")
            return pd.Series(dtype=float)

    tasa = bx_series_fondos("SF61745")
    inpc = bx_series_fondos("SP1")

    def ye_f(s, y):
        sub = s[s.index.year == y]
        return float(sub.iloc[-1]) if len(sub) > 0 else None

    def ev_f(s, y):
        return ye_f(s, y)

    ref_tasa, ref_infl = {}, {}
    for y in years:
        sub_tasa = tasa[tasa.index.year == y]
        if len(sub_tasa) > 0:
            avg_tasa = float(sub_tasa.mean())
            if y == current_year:
                months_el = sub_tasa.index[-1].month
                ref_tasa[str(y)] = round(avg_tasa * months_el / 12, 2)
            else:
                ref_tasa[str(y)] = round(avg_tasa, 2)
        ie, is_ = ev_f(inpc, y), ye_f(inpc, y - 1)
        if ie and is_ and is_ != 0:
            raw = ie / is_ - 1
            ref_infl[str(y)] = round(raw * 100, 2)

    def _ref_cum_ann(vals):
        cum, cnt = 1.0, 0
        for y in years:
            v = vals.get(str(y))
            if v is not None: cum *= (1 + v / 100); cnt += 1
        if cnt > 0:
            return round((cum - 1) * 100, 1), round((cum ** (1 / cnt) - 1) * 100, 1)
        return None, None

    tasa_cum, tasa_ann = _ref_cum_ann(ref_tasa)
    infl_cum, infl_ann = _ref_cum_ann(ref_infl)

    return {
        "ok": True, "years": years, "assets": assets,
        "reference": [
            {"name": "Tasa de Referencia", "values": ref_tasa, "cumulative": tasa_cum, "annualized": tasa_ann},
            {"name": "Inflaci\u00f3n", "values": ref_infl, "cumulative": infl_cum, "annualized": infl_ann},
        ],
        "cumulative": cumulative, "annualized": annualized, "ann_years": ann_years,
        "top_n": 10,
        "note": "Rendimientos brutos efectivos en moneda nacional. Fuente: Morningstar NAV hist\u00f3rico. Tasa de Referencia: promedio anual Banxico. Inflaci\u00f3n: INPC Banxico.",
        "updated": datetime.now().strftime("%d/%m/%Y"),
    }


@app.route("/api/quilt_fondos")
def api_quilt_fondos():
    if "usuario" not in session:
        return jsonify({"ok": False, "error": "No autenticado"}), 401
    if _quilt_fondos_cache["data"] and not _cache_expired(_quilt_fondos_cache["ts"]):
        return jsonify(_quilt_fondos_cache["data"])
    if not _prewarm_done.is_set():
        _prewarm_done.wait(timeout=25)
        if _quilt_fondos_cache["data"]:
            return jsonify(_quilt_fondos_cache["data"])
        return jsonify({"ok": False, "loading": True, "error": "Datos cargando, reintenta en unos segundos"}), 202
    from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
    try:
        with ThreadPoolExecutor(max_workers=1) as ex:
            future = ex.submit(_compute_quilt_fondos)
            data = future.result(timeout=25)
        _quilt_fondos_cache["data"] = data
        _quilt_fondos_cache["ts"] = time.time()
        _disk_cache_save("quilt_fondos", data)
        return jsonify(data)
    except FuturesTimeout:
        print("[ERROR] api_quilt_fondos: _compute_quilt_fondos timed out (25s)")
        return jsonify({"ok": False, "error": "Timeout calculando datos de fondos, reintenta"}), 504
    except Exception as e:
        print(f"[ERROR] api_quilt_fondos: {e}")
        return jsonify({"ok": False, "error": "Error al calcular datos de fondos"}), 500


_BRAND_PALETTE = {
    "VXGUBCP": "#00205C",   # Navy (brand — Pantone 281 C)
    "VXDEUDA": "#75A1DE",   # Silver-Blue (brand tint)
    "VXUDIMP": "#5B6670",   # Gray (brand — Pantone 431 C)
    "VXGUBLP": "#2E7D5B",   # Emerald Green (complementario)
    "VXTBILL": "#FECB46",   # Gold (brand accent)
    "VALMX28": "#3DA5E0",   # Blue (brand — Pantone 299 C)
    "VALMX20": "#BFDD7E",   # Lime Green (brand accent RV)
}

@app.route("/api/perfiles")
def api_perfiles():
    if "usuario" not in session:
        return jsonify({"ok": False, "error": "No autenticado"}), 401
    labels = ["Preservaci\u00f3n", "Conservador", "Balanceado", "Arriesgado", "Agresivo"]
    out = []
    for i, lbl in enumerate(labels):
        comp = PERFILES[str(i)]
        out.append({"id": i, "label": lbl, "funds": comp})
    return jsonify({"ok": True, "perfiles": out, "colors": _BRAND_PALETTE})


@app.route("/api/universo")
def api_universo():
    """Clasificacion de fondos Valmex — datos LIVE de Morningstar API.
    Refresca diario (post 4pm NYSE) o con ?force=1."""
    if "usuario" not in session:
        return jsonify({"error": "No autorizado"}), 401
    force = request.args.get("force", "0") == "1"
    universe = load_ms_universe(force=force)
    seen = {}
    for ticker, api in universe.items():
        prefix = ticker.split()[0]
        if prefix in seen:
            continue
        stock = float(api.get("AAB-StockNet", 0) or 0)
        bond  = float(api.get("AAB-BondNet", 0) or 0)
        cash  = float(api.get("AAB-CashNet", 0) or 0)
        dur   = float(api.get("PS-EffectiveDuration", 0) or 0)
        ytm   = float(api.get("PS-YieldToMaturity", 0) or 0)
        mkt   = float(api.get("PS-TotalMarketValueNet", 0) or 0)
        # Tipo
        if prefix in FONDOS_RV:
            tipo = "rv"
        elif prefix in FONDOS_DEUDA:
            tipo = "deuda"
        elif prefix in FONDOS_CICLO:
            tipo = "ciclo"
        else:
            tipo = "otro"
        # Geo
        geo = {}
        _GEO_MERGE_ETF = {"United Kingdom": "Europe - ex Euro"}
        for item in (api.get("RE-RegionalExposure") or []):
            region = item.get("Region", "")
            val = float(item.get("Value", 0) or 0)
            if val > 0.5 and region.lower() not in ("emerging market", "developing country",
                    "developed country", "developed countries", "emerging markets"):
                region = _GEO_MERGE_ETF.get(region, region)
                geo[region] = geo.get(region, 0) + round(val, 2)
        # Sectors
        sec = {}
        for k, v in api.items():
            if k.startswith("GR-") and k.endswith("Net"):
                vf = float(v or 0)
                if vf > 0.5:
                    sec[k.replace("GR-", "").replace("Net", "")] = round(vf, 2)
        # Debt supersectors
        debt_type = {}
        for k, v in api.items():
            if k.startswith("GBSR-SuperSector") and k.endswith("Net"):
                vf = float(v or 0)
                if vf > 0.5:
                    debt_type[k.replace("GBSR-SuperSector", "").replace("Net", "")] = round(vf, 2)
        # Holdings top 5
        hld = api.get("FHV2-HoldingDetail") or []
        top_hold = []
        for h in sorted(hld, key=lambda x: float(x.get("Weighting", 0) or 0), reverse=True)[:5]:
            top_hold.append({
                "ticker": h.get("Ticker", h.get("ISIN", "?")),
                "weight": round(float(h.get("Weighting", 0) or 0), 2)
            })
        seen[prefix] = {
            "ticker": prefix,
            "tipo": tipo,
            "stock_pct": round(stock, 2),
            "bond_pct": round(bond, 2),
            "cash_pct": round(cash, 2),
            "duration": round(dur, 2),
            "ytm": round(ytm, 2),
            "market_value": round(mkt),
            "geo": dict(sorted(geo.items(), key=lambda x: -x[1])[:8]),
            "sectors": dict(sorted(sec.items(), key=lambda x: -x[1])[:8]),
            "debt_type": dict(sorted(debt_type.items(), key=lambda x: -x[1])),
            "top_holdings": top_hold,
        }
    from datetime import datetime as _dt
    return jsonify({
        "ok": True,
        "n_fondos": len(seen),
        "cache_ts": _dt.fromtimestamp(_ms_cache_ts).isoformat() if _ms_cache_ts else None,
        "fondos": dict(sorted(seen.items())),
    })


@app.route("/api/diag-repo")
def diag_repo():
    if "usuario" not in session:
        return jsonify({"ok": False, "error": "No autenticado"}), 401
    user_data = USERS.get(session.get("usuario", ""))
    if not user_data or user_data.get("rol") != "admin":
        return jsonify({"ok": False, "error": "No autorizado"}), 403
    resultado = {}
    try:
        hoy = date.today(); ini = (hoy - timedelta(days=10)).isoformat(); fin = hoy.isoformat()
        raw = _banxico_serie_rango(SERIE_TIIE28, ini, fin)
        resultado["banxico"] = {"ok": len(raw) > 0, "registros": len(raw), "ultimo": raw[-1] if raw else None}
    except Exception as e:
        resultado["banxico"] = {"ok": False, "error": "Error de conexión"}
    try:
        hoy = date.today()
        params = {"series_id": "DFF", "observation_start": (hoy-timedelta(days=10)).isoformat(),
                  "observation_end": hoy.isoformat(), "api_key": FRED_API_KEY, "file_type": "json"}
        r = requests.get(FRED_BASE, params=params, timeout=10)
        obs = r.json().get("observations", [])
        resultado["fred"] = {"ok": len(obs) > 0, "registros": len(obs), "ultimo": obs[-1] if obs else None}
    except Exception as e:
        resultado["fred"] = {"ok": False, "error": "Error de conexión"}
    try:
        rend_mxn = get_repo_rendimientos(7.0, False); rend_usd = get_repo_rendimientos(4.0, True)
        resultado["rendimientos_mxn"] = rend_mxn; resultado["rendimientos_usd"] = rend_usd
    except Exception as e:
        resultado["rendimientos"] = {"error": "Error de cálculo"}
    return jsonify(resultado)


@app.route("/api/diag-nav")
def diag_nav():
    """Diagnóstico Morningstar NAV API — prueba fetch de precios históricos."""
    if "usuario" not in session:
        return jsonify({"ok": False, "error": "No autenticado"}), 401
    user_data = USERS.get(session.get("usuario", ""))
    if not user_data or user_data.get("rol") != "admin":
        return jsonify({"ok": False, "error": "No autorizado"}), 403
    try:
        isin = "MXP800501001"  # VXGUBCP A
        navs = get_ms_nav(isin, start="2026-02-01", end=date.today().isoformat())
        bt = get_fondo_backtesting("VXGUBCP", "A")
        return jsonify({
            "ok": True,
            "isin": isin,
            "nav_count": len(navs),
            "nav_sample": navs[:3] if navs else [],
            "bt_count": len(bt),
            "bt_sample": bt[-3:] if bt else [],
        })
    except Exception as e:
        print(f"[ERROR] diag-nav: {e}")
        return jsonify({"ok": False, "error": "Error en diagnóstico NAV"})


@app.route("/api/diag-yf")
def diag_yf():
    """Diagnóstico Yahoo Finance — prueba raw HTTP a Yahoo."""
    if "usuario" not in session:
        return jsonify({"ok": False, "error": "No autenticado"}), 401
    user_data = USERS.get(session.get("usuario", ""))
    if not user_data or user_data.get("rol") != "admin":
        return jsonify({"ok": False, "error": "No autorizado"}), 403
    tk = request.args.get("t", "AAPL.MX")
    if not _valid_ticker(tk):
        return jsonify({"ok": False, "error": "Ticker inválido"}), 400
    resultado = {"ticker": tk}

    # Check curl_cffi
    try:
        import curl_cffi
        resultado["curl_cffi"] = {"ok": True, "version": getattr(curl_cffi, "__version__", "?")}
    except ImportError as e:
        resultado["curl_cffi"] = {"ok": False, "error": str(e)}

    # Rate limit status (solo indica si está limitado, sin exponer timestamps internos)
    resultado["rate_limited"] = time.time() < _yf_rate_limit_until

    # Test 1: Raw HTTP to Yahoo Chart API (no yfinance, just requests)
    try:
        s = requests.Session()
        s.headers.update(_YF_DIRECT_HEADERS)
        r1 = s.get("https://fc.yahoo.com", timeout=10, allow_redirects=True)
        cookie = r1.cookies.get("B", "")
        r2 = s.get("https://query2.finance.yahoo.com/v1/test/getcrumb", timeout=10)
        crumb = r2.text.strip() if r2.status_code == 200 else f"HTTP {r2.status_code}"
        r3 = s.get(f"https://query2.finance.yahoo.com/v8/finance/chart/{tk}",
                    params={"range": "1mo", "interval": "1d", "crumb": crumb}, timeout=15)
        chart = r3.json() if r3.status_code == 200 else {}
        result_data = chart.get("chart", {}).get("result", [])
        n_points = len(result_data[0].get("timestamp", [])) if result_data else 0
        resultado["raw_http"] = {
            "cookie": bool(cookie), "crumb": crumb[:10] if crumb else "",
            "chart_status": r3.status_code, "puntos": n_points
        }
    except Exception as e:
        resultado["raw_http"] = {"error": str(e)}

    # Test 2: curl_cffi direct (if available)
    try:
        from curl_cffi import requests as cffi_req
        s2 = cffi_req.Session(impersonate="chrome")
        r4 = s2.get("https://fc.yahoo.com", timeout=10, allow_redirects=True)
        cookie2 = r4.cookies.get("B", "")
        r5 = s2.get("https://query2.finance.yahoo.com/v1/test/getcrumb", timeout=10)
        crumb2 = r5.text.strip() if r5.status_code == 200 else f"HTTP {r5.status_code}"
        r6 = s2.get(f"https://query2.finance.yahoo.com/v8/finance/chart/{tk}",
                     params={"range": "1mo", "interval": "1d", "crumb": crumb2}, timeout=15)
        chart2 = r6.json() if r6.status_code == 200 else {}
        result_data2 = chart2.get("chart", {}).get("result", [])
        n_points2 = len(result_data2[0].get("timestamp", [])) if result_data2 else 0
        resultado["curl_cffi_http"] = {
            "cookie": bool(cookie2), "crumb": crumb2[:10] if crumb2 else "",
            "chart_status": r6.status_code, "puntos": n_points2
        }
    except Exception as e:
        resultado["curl_cffi_http"] = {"error": str(e)}

    # Test 3: quoteSummary via curl_cffi
    try:
        from curl_cffi import requests as cffi_req
        s3 = cffi_req.Session(impersonate="chrome")
        s3.get("https://fc.yahoo.com", timeout=10, allow_redirects=True)
        global_tk = tk.replace(".MX", "")
        for try_tk in [tk, global_tk]:
            r7 = s3.get(f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{try_tk}",
                         params={"modules": "assetProfile,quoteType"}, timeout=15)
            if r7.status_code == 200:
                qs = r7.json().get("quoteSummary", {}).get("result", [{}])[0]
                profile = qs.get("assetProfile", {})
                qt = qs.get("quoteType", {})
                resultado[f"quoteSummary_{try_tk}"] = {
                    "status": r7.status_code,
                    "country": profile.get("country", ""),
                    "sector": profile.get("sector", ""),
                    "quoteType": qt.get("quoteType", ""),
                }
                break
            else:
                resultado[f"quoteSummary_{try_tk}"] = {"status": r7.status_code, "body": r7.text[:200]}
    except Exception as e:
        resultado["quoteSummary"] = {"error": str(e)}

    return jsonify(resultado)


@app.route("/api/emisoras/buscar")
def api_buscar_emisora():
    """Búsqueda en el catálogo en memoria — sin costo de créditos."""
    if "usuario" not in session:
        return jsonify({"ok": False, "error": "No autenticado"}), 401
    q = (request.args.get("q") or "").strip().upper()
    if not q or len(q) < 2:
        return jsonify({"ok": True, "results": []})
    if not DB_TOKEN:
        return jsonify({"ok": False, "error": "DataBursatil no configurado"}), 503

    catalogo = cargar_catalogo_emisoras()
    results  = []
    for ticker_db, info in catalogo.items():
        if q in ticker_db or q in info["nombre"].upper():
            results.append({
                "ticker": ticker_db,
                "yf_ticker": info["yf_ticker"],
                "nombre": info["nombre"],
                "bolsa":  info["bolsa"],
                "tipo":   info["tipo"],
                "mercado": info["mercado"],
            })
            if len(results) >= 30:
                break
    return jsonify({"ok": True, "results": results, "total_catalogo": len(catalogo)})


@app.route("/api/emisoras/catalogo")
def api_catalogo_emisoras():
    """Devuelve el catálogo completo (para cargar en el frontend de una vez)."""
    if "usuario" not in session:
        return jsonify({"ok": False, "error": "No autenticado"}), 401
    if not DB_TOKEN:
        return jsonify({"ok": False, "error": "DataBursatil no configurado"}), 503
    catalogo = cargar_catalogo_emisoras()
    return jsonify({"ok": True, "total": len(catalogo), "emisoras": list(catalogo.values())})


@app.route("/api/creditos/db")
def api_creditos_db():
    """Consulta créditos disponibles en DataBursatil."""
    if "usuario" not in session:
        return jsonify({"ok": False, "error": "No autenticado"}), 401
    if not DB_TOKEN:
        return jsonify({"ok": False, "error": "Token no configurado"}), 503
    try:
        r = requests.get(f"{DB_BASE}/creditos", params={"token": DB_TOKEN}, timeout=10)
        r.raise_for_status()
        return jsonify({"ok": True, "data": r.json()})
    except Exception as e:
        print(f"[ERROR] creditos: {e}")
        return jsonify({"ok": False, "error": "Error al consultar datos"}), 500


def _prewarm_quilts():
    """Pre-compute ALL caches at startup so first user request is instant."""
    import time as _t
    _t0 = _t.time()

    # Try disk cache first (survives Render restarts)
    disk_q, disk_q_ts = _disk_cache_load("quilt")
    if disk_q:
        _quilt_cache["data"] = disk_q
        _quilt_cache["ts"] = disk_q_ts
        print(f"[PREWARM] Quilt loaded from disk cache")
    else:
        for _attempt in range(2):
            try:
                print(f"[PREWARM] Computing quilt (asset classes)... attempt {_attempt+1}")
                data = _compute_quilt()
                _quilt_cache["data"] = data
                _quilt_cache["ts"] = _t.time()
                _disk_cache_save("quilt", data)
                print(f"[PREWARM] Quilt done in {_t.time()-_t0:.1f}s")
                break
            except Exception as e:
                import traceback
                print(f"[PREWARM] Quilt error (attempt {_attempt+1}): {e}")
                traceback.print_exc()
                if _attempt < 1:
                    _t.sleep(3)

    disk_qf, disk_qf_ts = _disk_cache_load("quilt_fondos")
    if disk_qf:
        _quilt_fondos_cache["data"] = disk_qf
        _quilt_fondos_cache["ts"] = disk_qf_ts
        print(f"[PREWARM] Quilt fondos loaded from disk cache")
    else:
        try:
            _t1 = _t.time()
            print("[PREWARM] Computing quilt fondos...")
            data2 = _compute_quilt_fondos()
            _quilt_fondos_cache["data"] = data2
            _quilt_fondos_cache["ts"] = _t.time()
            _disk_cache_save("quilt_fondos", data2)
            print(f"[PREWARM] Quilt fondos done in {_t.time()-_t1:.1f}s")
        except Exception as e:
            print(f"[PREWARM] Quilt fondos error: {e}")
    try:
        print("[PREWARM] Pre-warming factor series...")
        _fetch_factor_series()
    except Exception as e:
        print(f"[PREWARM] Factor series error: {e}")
    # Pre-warm NAV cache for all Serie A funds (most commonly used)
    try:
        _t2 = _t.time()
        print("[PREWARM] Pre-warming NAV cache (all Serie A)...")
        nav_items = []
        for fondo, series in ISIN_MAP.items():
            isin = series.get("A")
            if isin:
                nav_items.append((isin, fondo, "A"))
        def _fetch_nav(args):
            isin, f, s = args
            try:
                get_ms_nav(isin, expect_fund=f, expect_serie=s)
            except Exception:
                pass
        executor = ThreadPoolExecutor(max_workers=12)
        futs = [executor.submit(_fetch_nav, item) for item in nav_items]
        # Wait up to 30s for all NAV fetches, then move on
        for fut in futs:
            try:
                fut.result(timeout=30)
            except Exception:
                pass
        executor.shutdown(wait=False)
        print(f"[PREWARM] NAV cache done ({len(nav_items)} funds) in {_t.time()-_t2:.1f}s")
    except Exception as e:
        print(f"[PREWARM] NAV cache error: {e}")
    _prewarm_done.set()
    print(f"[PREWARM] All done in {_t.time()-_t0:.1f}s total")


# ── Pre-warm caches at startup (works with both gunicorn and direct run) ──
if DB_TOKEN:
    threading.Thread(target=cargar_catalogo_emisoras, daemon=True).start()
threading.Thread(target=_prewarm_quilts, daemon=True).start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
