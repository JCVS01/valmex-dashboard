#!/usr/bin/env python3
"""
Generador de Factsheet PDF para Fondos Valmex.

Uso:
    python generate_factsheet.py --fondo VLMXTEC --serie A
    python generate_factsheet.py --fondo VLMXTEC --serie A \
        --holdings '{"BROADCOM":6.5,"NVIDIA":5.8}' \
        --output VLMXTEC_A_Factsheet_FEB26.pdf
"""

import argparse
import json
import math
import os
import sys
import tempfile
import time
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
import requests
import yfinance as yf
from fpdf import FPDF

BASE = os.path.dirname(os.path.abspath(__file__))

# ─── Morningstar API ────────────────────────────────────────────────────────
MS_URL     = "https://api.morningstar.com/v2/service/mf/hlk0d0zmiy1b898b/universeid/txcm88fa8x3vxapp"
MS_ACCESS  = "hwg0cty5re7araij32k035091f43wxd0"
MS_NAV_URL = "https://api.morningstar.com/service/mf/UnadjustedNAV/ISIN"

# ─── ISIN Map (copy from app.py to avoid Flask init) ────────────────────────
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

FONDOS_DEUDA_MXN = {"VXREPO1", "VXGUBCP", "VXUDIMP", "VXDEUDA", "VXGUBLP", "VLMXETF"}
FONDOS_DEUDA_USD = {"VXTBILL", "VXCOBER", "VLMXDME"}
FONDOS_DEUDA     = FONDOS_DEUDA_MXN | FONDOS_DEUDA_USD
FONDOS_RV        = {"VALMXA", "VALMX20", "VALMX28", "VALMXVL", "VALMXES", "VLMXTEC", "VLMXESG", "VALMXHC", "VXINFRA"}
FONDOS_CICLO     = {"VLMXJUB", "VLMXP24", "VLMXP31", "VLMXP38", "VLMXP45", "VLMXP52", "VLMXP59"}

# ─── Sector mapping (Morningstar keys → Spanish) ────────────────────────────
SECTOR_MAP = {
    "GR-TechnologyNet":            "Tecnologia",
    "GR-FinancialServicesNet":     "Financiero",
    "GR-HealthcareNet":            "Salud",
    "GR-CommunicationServicesNet": "Comunicaciones",
    "GR-IndustrialsNet":           "Industriales",
    "GR-ConsumerCyclicalNet":      "Consumo Discrecional",
    "GR-ConsumerDefensiveNet":     "Consumo Basico",
    "GR-BasicMaterialsNet":        "Materiales",
    "GR-EnergyNet":                "Energia",
    "GR-RealEstateNet":            "Bienes Raices",
    "GR-UtilitiesNet":             "Utilidades",
}

GEO_EXCLUDE = {"emerging market", "developing country", "emerging markets",
               "developed countries", "developed country"}

# ─── Fund static metadata ───────────────────────────────────────────────────
FUND_INFO = {
    "VLMXTEC": {
        "nombre_completo": "Acciones Tecnologia Estados Unidos",
        "categoria": "Acciones Estados Unidos",
        "asesor": "JP Morgan Asset Management",
        "indice": "Russell 1000 Equal Weight Technology TR USD",
        "horizonte": "Largo Plazo (3+ anios)",
        "liquidez_compra": "Diaria",
        "liquidez_venta": "Diaria",
        "plazo_liquidacion": "72 horas",
        "horarios": "7:30 am - 1:00 pm",
        "descripcion_fondo": (
            "VLMXTEC invierte en las empresas de tecnologia del futuro a traves de la "
            "estrategia de JP Morgan, buscando capturar el crecimiento del sector tecnologico "
            "de Estados Unidos. El fondo se enfoca en companias de gran capitalizacion con "
            "alto potencial de innovacion y crecimiento sostenido."
        ),
        "descripcion_asesor": (
            "Operadora de Fondos Valmex realizo una alianza con J.P. Morgan Asset Management "
            "para dar acceso a sus clientes a estrategias de inversion globales. J.P. Morgan AM "
            "es uno de los administradores de activos mas grandes del mundo con mas de "
            "USD $3 billones en activos bajo administracion."
        ),
        "logo_asesor": None,
        "tipo": "rv",
    },
    "VLMXESG": {
        "nombre_completo": "Acciones ESG Global",
        "categoria": "Acciones Globales ESG",
        "asesor": "JP Morgan Asset Management",
        "indice": "MSCI World ESG Leaders Net TR USD",
        "horizonte": "Largo Plazo (3+ anios)",
        "liquidez_compra": "Diaria",
        "liquidez_venta": "Diaria",
        "plazo_liquidacion": "72 horas",
        "horarios": "7:30 am - 1:00 pm",
        "descripcion_fondo": (
            "VLMXESG invierte en companias globales que cumplen con altos estandares ambientales, "
            "sociales y de gobierno corporativo (ESG), a traves de la estrategia de JP Morgan."
        ),
        "descripcion_asesor": (
            "Operadora de Fondos Valmex realizo una alianza con J.P. Morgan Asset Management "
            "para dar acceso a estrategias de inversion globales con enfoque ESG."
        ),
        "logo_asesor": None,
        "tipo": "rv",
    },
    "VALMXHC": {
        "nombre_completo": "Acciones Salud Global",
        "categoria": "Acciones Sector Salud",
        "asesor": "JP Morgan Asset Management",
        "indice": "MSCI World Health Care Net TR USD",
        "horizonte": "Largo Plazo (3+ anios)",
        "liquidez_compra": "Diaria",
        "liquidez_venta": "Diaria",
        "plazo_liquidacion": "72 horas",
        "horarios": "7:30 am - 1:00 pm",
        "descripcion_fondo": (
            "VALMXHC invierte en companias del sector salud a nivel global, incluyendo "
            "farmaceuticas, biotecnologia y dispositivos medicos."
        ),
        "descripcion_asesor": (
            "Operadora de Fondos Valmex en alianza con J.P. Morgan Asset Management "
            "ofrece acceso al sector salud global."
        ),
        "logo_asesor": None,
        "tipo": "rv",
    },
    "VXINFRA": {
        "nombre_completo": "Acciones Infraestructura Global",
        "categoria": "Acciones Infraestructura",
        "asesor": "JP Morgan Asset Management",
        "indice": "S&P Global Infrastructure NTR USD",
        "horizonte": "Largo Plazo (3+ anios)",
        "liquidez_compra": "Diaria",
        "liquidez_venta": "Diaria",
        "plazo_liquidacion": "72 horas",
        "horarios": "7:30 am - 1:00 pm",
        "descripcion_fondo": (
            "VXINFRA invierte en companias globales de infraestructura incluyendo utilities, "
            "transporte y energia."
        ),
        "descripcion_asesor": (
            "Operadora de Fondos Valmex en alianza con J.P. Morgan Asset Management "
            "ofrece acceso a infraestructura global."
        ),
        "logo_asesor": None,
        "tipo": "rv",
    },
    "VALMXA": {
        "nombre_completo": "Acciones Mexico Activo",
        "categoria": "Acciones Mexico",
        "asesor": "Operadora Valmex",
        "indice": "S&P/BMV IPC TR MXN",
        "horizonte": "Largo Plazo (3+ anios)",
        "liquidez_compra": "Diaria",
        "liquidez_venta": "Diaria",
        "plazo_liquidacion": "72 horas",
        "horarios": "7:30 am - 1:00 pm",
        "descripcion_fondo": (
            "VALMXA invierte activamente en acciones del mercado mexicano "
            "buscando superar al indice S&P/BMV IPC."
        ),
        "descripcion_asesor": (
            "Operadora de Fondos Valmex cuenta con mas de 30 anios de experiencia "
            "administrando portafolios de renta variable en Mexico."
        ),
        "logo_asesor": None,
        "tipo": "rv",
    },
    "VALMX20": {
        "nombre_completo": "Acciones Mexico Top 20",
        "categoria": "Acciones Mexico",
        "asesor": "Operadora Valmex",
        "indice": "S&P/BMV IPC TR MXN",
        "horizonte": "Largo Plazo (3+ anios)",
        "liquidez_compra": "Diaria",
        "liquidez_venta": "Diaria",
        "plazo_liquidacion": "72 horas",
        "horarios": "7:30 am - 1:00 pm",
        "descripcion_fondo": (
            "VALMX20 invierte en las 20 emisoras mas representativas del mercado mexicano."
        ),
        "descripcion_asesor": "Operadora de Fondos Valmex.",
        "logo_asesor": None,
        "tipo": "rv",
    },
    "VALMX28": {
        "nombre_completo": "Acciones Norteamerica",
        "categoria": "Acciones Norteamerica",
        "asesor": "Operadora Valmex",
        "indice": "S&P 500 TR USD",
        "horizonte": "Largo Plazo (3+ anios)",
        "liquidez_compra": "Diaria",
        "liquidez_venta": "Diaria",
        "plazo_liquidacion": "72 horas",
        "horarios": "7:30 am - 1:00 pm",
        "descripcion_fondo": (
            "VALMX28 invierte en acciones de Norteamerica con exposicion al mercado de EUA."
        ),
        "descripcion_asesor": "Operadora de Fondos Valmex.",
        "logo_asesor": None,
        "tipo": "rv",
    },
    "VALMXVL": {
        "nombre_completo": "Acciones Global Value",
        "categoria": "Acciones Globales",
        "asesor": "Operadora Valmex",
        "indice": "MSCI World NTR USD",
        "horizonte": "Largo Plazo (3+ anios)",
        "liquidez_compra": "Diaria",
        "liquidez_venta": "Diaria",
        "plazo_liquidacion": "72 horas",
        "horarios": "7:30 am - 1:00 pm",
        "descripcion_fondo": "VALMXVL invierte en acciones globales con enfoque value.",
        "descripcion_asesor": "Operadora de Fondos Valmex.",
        "logo_asesor": None,
        "tipo": "rv",
    },
    "VALMXES": {
        "nombre_completo": "Acciones Estrategia",
        "categoria": "Acciones Globales",
        "asesor": "Operadora Valmex",
        "indice": "MSCI World NTR USD",
        "horizonte": "Largo Plazo (3+ anios)",
        "liquidez_compra": "Diaria",
        "liquidez_venta": "Diaria",
        "plazo_liquidacion": "72 horas",
        "horarios": "7:30 am - 1:00 pm",
        "descripcion_fondo": "VALMXES invierte estrategicamente en acciones globales.",
        "descripcion_asesor": "Operadora de Fondos Valmex.",
        "logo_asesor": None,
        "tipo": "rv",
    },
}

# Default for any fund not in FUND_INFO
_DEFAULT_INFO = {
    "nombre_completo": "",
    "categoria": "",
    "asesor": "Operadora Valmex",
    "indice": "",
    "horizonte": "Mediano Plazo",
    "liquidez_compra": "Diaria",
    "liquidez_venta": "Diaria",
    "plazo_liquidacion": "72 horas",
    "horarios": "7:30 am - 1:00 pm",
    "descripcion_fondo": "",
    "descripcion_asesor": "Operadora de Fondos Valmex.",
    "logo_asesor": None,
    "tipo": "deuda",
}

# ─── Color palette ───────────────────────────────────────────────────────────
NAVY       = (0, 32, 92)
SKY        = (65, 187, 201)
LIGHT_BG   = (245, 247, 250)
HDR_BG     = (220, 225, 235)
GREEN      = (0, 128, 0)
RED        = (200, 0, 0)
GRAY       = (91, 102, 112)
WHITE      = (255, 255, 255)
BLACK      = (0, 0, 0)

NAVY_HEX   = "#00205C"
SKY_HEX    = "#41BBC9"

# ─── Spanish month names ────────────────────────────────────────────────────
MESES = ["Ene", "Feb", "Mar", "Abr", "May", "Jun",
         "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]
MESES_LARGO = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
               "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]


# ═══════════════════════════════════════════════════════════════════════════
# DATA FETCHING
# ═══════════════════════════════════════════════════════════════════════════

def safe_float(val, default=0.0):
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def load_ms_universe() -> dict:
    """Load Morningstar universe data."""
    cache = {}
    try:
        resp = requests.get(MS_URL, params={"accesscode": MS_ACCESS, "format": "JSON"}, timeout=25)
        resp.raise_for_status()
        for fund in resp.json().get("data", []):
            api = fund.get("api", {})
            ticker = api.get("FSCBI-Ticker", "").strip()
            if ticker:
                cache[ticker] = api
        print(f"[MS] Universo cargado: {len(cache)} fondos")
    except Exception as e:
        print(f"[MS ERROR] {e}")
    return cache


def get_ms_nav(isin: str, start: str = "2010-01-01", end: str = None,
               expect_fund: str = None, expect_serie: str = None) -> list:
    """Fetch historical NAV from Morningstar."""
    if end is None:
        end = date.today().isoformat()
    try:
        r = requests.get(
            f"{MS_NAV_URL}/{isin}",
            params={"startdate": start, "enddate": end, "accesscode": MS_ACCESS},
            timeout=25,
        )
        r.raise_for_status()
        root = ET.fromstring(r.text)
        if expect_fund and expect_serie:
            data_elem = root.find(".//data")
            api_name = data_elem.get("fundName", "") if data_elem is not None else ""
            expected = f"{expect_fund} {expect_serie}"
            if api_name and api_name != expected:
                print(f"[MS NAV MISMATCH] {isin}: esperado '{expected}', API='{api_name}'")
                return []
        data = [{"fecha": elem.get("d"), "nav": float(elem.get("v"))}
                for elem in root.iter("r")]
        print(f"[MS NAV] {isin}: {len(data)} precios ({start} -> {end})")
        return data
    except Exception as e:
        print(f"[MS NAV ERROR] {isin}: {e}")
        return []


def get_nav_dataframe(fondo: str, serie: str) -> pd.DataFrame:
    """Get daily NAV as DataFrame with reset adjustment."""
    isin = ISIN_MAP.get(fondo, {}).get(serie)
    if not isin:
        return pd.DataFrame()
    navs = get_ms_nav(isin, start="2000-01-01", expect_fund=fondo, expect_serie=serie)
    if len(navs) < 2:
        return pd.DataFrame()
    df = pd.DataFrame(navs)
    df["fecha"] = pd.to_datetime(df["fecha"])
    df = df.set_index("fecha").sort_index()
    # Adjust NAV resets
    vals = df["nav"].values.copy()
    for i in range(1, len(vals)):
        if vals[i - 1] > 0:
            ratio = vals[i] / vals[i - 1]
            if ratio > 2.0 or ratio < 0.5:
                vals[:i] *= ratio
    df["nav"] = vals
    return df


def get_backtesting_base100k(fondo: str, serie: str) -> pd.DataFrame:
    """Build monthly base-100,000 series for backtesting chart."""
    df = get_nav_dataframe(fondo, serie)
    if df.empty:
        return pd.DataFrame()
    monthly = df["nav"].resample("MS").first().dropna()
    if len(monthly) < 2:
        return pd.DataFrame()
    base = float(monthly.iloc[0])
    result = pd.DataFrame({
        "fecha": monthly.index,
        "valor": [round(float(px) / base * 100000, 2) for px in monthly.values]
    })
    return result.set_index("fecha")


def calc_extended_returns(df_nav: pd.DataFrame) -> dict:
    """Calculate 5Y, 10Y, Inception returns from daily NAV."""
    if df_nav.empty:
        return {}
    today = df_nav.index[-1]
    nav_last = float(df_nav["nav"].iloc[-1])
    results = {}
    periods = {"5A": 5, "10A": 10}
    for label, years in periods.items():
        target = today - pd.DateOffset(years=years)
        mask = df_nav.index >= target
        if mask.any():
            nav_start = float(df_nav.loc[mask, "nav"].iloc[0])
            if nav_start > 0:
                total_ret = (nav_last / nav_start - 1) * 100
                ann_ret = ((nav_last / nav_start) ** (1 / years) - 1) * 100
                results[label] = round(ann_ret, 2)
    # Inception (annualized)
    nav_first = float(df_nav["nav"].iloc[0])
    if nav_first > 0:
        years_total = (today - df_nav.index[0]).days / 365.25
        if years_total > 0:
            ann = ((nav_last / nav_first) ** (1 / years_total) - 1) * 100
            results["Inicio"] = round(ann, 2)
    return results


def calc_monthly_returns(df_nav: pd.DataFrame, n_years: int = 5) -> dict:
    """Calculate monthly returns for the last n years.

    Returns dict: {year: {month(1-12): return%, ...}, ...}
    """
    if df_nav.empty:
        return {}
    monthly = df_nav["nav"].resample("MS").first().dropna()
    if len(monthly) < 2:
        return {}
    rets = monthly.pct_change().dropna() * 100
    current_year = date.today().year
    result = {}
    for yr in range(current_year, current_year - n_years - 1, -1):
        year_data = rets[rets.index.year == yr]
        if not year_data.empty:
            result[yr] = {}
            for dt, val in year_data.items():
                result[yr][dt.month] = round(val, 2)
    return result


def calc_annual_returns(monthly_returns: dict) -> dict:
    """Calculate annual compounded return from monthly returns."""
    annual = {}
    for yr, months in monthly_returns.items():
        if months:
            compound = 1.0
            for m in range(1, 13):
                if m in months:
                    compound *= (1 + months[m] / 100)
            annual[yr] = round((compound - 1) * 100, 2)
    return annual


def calc_std_dev(df_nav: pd.DataFrame) -> dict:
    """Calculate annualized standard deviation for 1Y, 3Y, 5Y."""
    if df_nav.empty:
        return {}
    daily_ret = df_nav["nav"].pct_change().dropna()
    today = df_nav.index[-1]
    results = {}
    for label, years in [("1A", 1), ("3A", 3), ("5A", 5)]:
        target = today - pd.DateOffset(years=years)
        subset = daily_ret[daily_ret.index >= target]
        if len(subset) > 20:
            ann_std = float(subset.std()) * (252 ** 0.5) * 100
            results[label] = round(ann_std, 2)
    return results


def calc_monthly_stats(monthly_returns: dict) -> dict:
    """Best month, worst month, % months positive."""
    all_months = []
    for yr, months in monthly_returns.items():
        for m, val in months.items():
            all_months.append((yr, m, val))
    if not all_months:
        return {}
    best = max(all_months, key=lambda x: x[2])
    worst = min(all_months, key=lambda x: x[2])
    pos = sum(1 for _, _, v in all_months if v > 0)
    return {
        "mejor_mes": f"{MESES[best[1]-1]} {best[0]}: {best[2]:+.2f}%",
        "peor_mes": f"{MESES[worst[1]-1]} {worst[0]}: {worst[2]:+.2f}%",
        "pct_positivos": f"{pos / len(all_months) * 100:.1f}%",
        "total_meses": len(all_months),
    }


def get_usdmxn_monthly() -> pd.Series:
    """Get monthly USDMXN exchange rate from yfinance."""
    try:
        ticker = yf.Ticker("USDMXN=X")
        hist = ticker.history(period="max")
        if hist.empty:
            return pd.Series(dtype=float)
        # Normalize to tz-naive for alignment with NAV data
        if hist.index.tz is not None:
            hist.index = hist.index.tz_localize(None)
        monthly = hist["Close"].resample("MS").last().dropna()
        return monthly
    except Exception as e:
        print(f"[FX ERROR] {e}")
        return pd.Series(dtype=float)


def calc_usd_returns(df_nav: pd.DataFrame, universe_data: dict) -> dict:
    """Convert MXN returns to USD using USDMXN exchange rate."""
    fx = get_usdmxn_monthly()
    if fx.empty or df_nav.empty:
        return {}

    nav_monthly = df_nav["nav"].resample("MS").first().dropna()
    # Align dates
    common = nav_monthly.index.intersection(fx.index)
    if len(common) < 2:
        return {}

    nav_usd = nav_monthly.loc[common] / fx.loc[common]
    last = float(nav_usd.iloc[-1])
    today = nav_usd.index[-1]
    results = {}

    for label, months in [("1M", 1), ("2M", 2), ("3M", 3), ("6M", 6)]:
        target = today - pd.DateOffset(months=months)
        mask = nav_usd.index <= target
        if mask.any():
            start_val = float(nav_usd.loc[mask].iloc[-1])
            if start_val > 0:
                results[label] = round((last / start_val - 1) * 100, 2)

    for label, years in [("1A", 1), ("2A", 2), ("3A", 3), ("5A", 5), ("10A", 10)]:
        target = today - pd.DateOffset(years=years)
        mask = nav_usd.index >= target
        if mask.any():
            start_val = float(nav_usd.loc[mask].iloc[0])
            if start_val > 0:
                total = (last / start_val - 1) * 100
                ann = ((last / start_val) ** (1 / int(label[:-1])) - 1) * 100
                results[label] = round(ann, 2)

    # Inception
    first = float(nav_usd.iloc[0])
    if first > 0:
        yrs = (today - nav_usd.index[0]).days / 365.25
        if yrs > 0:
            results["Inicio"] = round(((last / first) ** (1 / yrs) - 1) * 100, 2)

    return results


def fetch_fund_data(fondo: str, serie: str) -> dict:
    """Orchestrate all data fetching for a fund."""
    print(f"\n{'='*60}")
    print(f"  Obteniendo datos para {fondo} {serie}")
    print(f"{'='*60}\n")

    info = {**_DEFAULT_INFO, **FUND_INFO.get(fondo, {})}
    universe = load_ms_universe()
    ticker = f"{fondo} {serie}"
    d = universe.get(ticker, {})

    # MXN returns from universe
    mxn_returns = {}
    for label, key in [("1M", "TTR-Return1Mth"), ("3M", "TTR-Return3Mth"),
                       ("6M", "TTR-Return6Mth"), ("1A", "TTR-Return1Yr"),
                       ("2A", "TTR-Return2Yr"),   ("3A", "TTR-Return3Yr")]:
        val = safe_float(d.get(key))
        if val != 0:
            mxn_returns[label] = round(val, 2)

    # Also try to compute 2M from NAV
    df_nav = get_nav_dataframe(fondo, serie)

    # Extended returns from NAV
    extended = calc_extended_returns(df_nav)
    mxn_returns.update(extended)

    # Compute 2M from NAV if possible
    if not df_nav.empty:
        today = df_nav.index[-1]
        nav_last = float(df_nav["nav"].iloc[-1])
        target_2m = today - pd.DateOffset(months=2)
        mask = df_nav.index <= target_2m
        if mask.any():
            nav_2m = float(df_nav.loc[mask, "nav"].iloc[-1])
            if nav_2m > 0:
                mxn_returns["2M"] = round((nav_last / nav_2m - 1) * 100, 2)

    # Backtesting
    bt = get_backtesting_base100k(fondo, serie)

    # Geo exposure
    geo = {}
    geo_raw = d.get("RE-RegionalExposure", [])
    if isinstance(geo_raw, list):
        for item in geo_raw:
            region = item.get("Region", "")
            val = safe_float(item.get("Value", 0))
            if region and val > 0 and region.lower() not in GEO_EXCLUDE:
                geo[region] = round(val, 1)

    # Sectors
    sectors = {}
    for key, nombre in SECTOR_MAP.items():
        v = safe_float(d.get(key))
        if v > 0:
            sectors[nombre] = round(v, 1)

    # Monthly returns
    monthly = calc_monthly_returns(df_nav)
    annual = calc_annual_returns(monthly)
    stats = calc_monthly_stats(monthly)
    std_dev = calc_std_dev(df_nav)

    # USD returns
    usd_returns = calc_usd_returns(df_nav, d)

    # Assets (AUM)
    aum = safe_float(d.get("FSCBI-TotalNetAssets"))
    aum_str = f"${aum:,.0f} MDP" if aum > 0 else "N/D"

    # Inception date
    inception = ""
    if not df_nav.empty:
        inception = df_nav.index[0].strftime("%d/%m/%Y")

    return {
        "info": info,
        "mxn_returns": mxn_returns,
        "usd_returns": usd_returns,
        "backtesting": bt,
        "geo": geo,
        "sectors": sectors,
        "monthly": monthly,
        "annual": annual,
        "stats": stats,
        "std_dev": std_dev,
        "aum": aum_str,
        "inception": inception,
        "universe": d,
    }


# ═══════════════════════════════════════════════════════════════════════════
# CHART RENDERING
# ═══════════════════════════════════════════════════════════════════════════

def render_backtesting_chart(bt_df: pd.DataFrame, fondo: str, indice: str) -> str:
    """Render backtesting chart as PNG. Returns temp file path."""
    if bt_df.empty:
        return ""
    fig, ax = plt.subplots(figsize=(6.5, 3.0), dpi=150)
    ax.plot(bt_df.index, bt_df["valor"], color=NAVY_HEX, linewidth=1.5,
            label=fondo)
    ax.fill_between(bt_df.index, bt_df["valor"], alpha=0.08, color=NAVY_HEX)

    # Annotate final value
    last_val = bt_df["valor"].iloc[-1]
    last_date = bt_df.index[-1]
    ax.annotate(f"${last_val:,.0f}", xy=(last_date, last_val),
                fontsize=8, fontweight="bold", color=NAVY_HEX,
                xytext=(10, 5), textcoords="offset points")

    # Base line
    ax.axhline(y=100000, color="#5B6670", linestyle="--", linewidth=0.5, alpha=0.5)

    ax.set_title("Crecimiento Hipotetico de $100,000 MXN", fontsize=9,
                 fontweight="bold", color=NAVY_HEX, pad=8)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    ax.tick_params(axis="both", labelsize=7)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(axis="y", alpha=0.3, linewidth=0.5)
    if indice:
        ax.legend(fontsize=7, loc="upper left")

    fig.tight_layout()
    tmp = tempfile.NamedTemporaryFile(suffix=".png", delete=False)
    fig.savefig(tmp.name, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return tmp.name


def render_sector_bars(sectors: dict) -> str:
    """Render horizontal bar chart for sectors."""
    if not sectors:
        return ""
    sorted_sectors = sorted(sectors.items(), key=lambda x: x[1], reverse=True)
    names = [s[0] for s in sorted_sectors]
    values = [s[1] for s in sorted_sectors]

    fig, ax = plt.subplots(figsize=(3.8, max(2.0, len(names) * 0.32)), dpi=150)
    bars = ax.barh(range(len(names)), values, color=NAVY_HEX, height=0.6)
    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=7)
    ax.invert_yaxis()
    ax.set_xlabel("%", fontsize=7)
    ax.tick_params(axis="x", labelsize=7)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    for i, (bar, val) in enumerate(zip(bars, values)):
        ax.text(bar.get_width() + 0.5, bar.get_y() + bar.get_height()/2,
                f"{val:.1f}%", va="center", fontsize=6.5, color=NAVY_HEX)

    fig.tight_layout()
    tmp = tempfile.NamedTemporaryFile(suffix=".png", delete=False)
    fig.savefig(tmp.name, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return tmp.name


def render_holdings_bars(holdings: dict) -> str:
    """Render horizontal bar chart for top holdings."""
    if not holdings:
        return ""
    sorted_h = sorted(holdings.items(), key=lambda x: x[1], reverse=True)[:10]
    names = [h[0] for h in sorted_h]
    values = [h[1] for h in sorted_h]

    fig, ax = plt.subplots(figsize=(7.5, max(1.8, len(names) * 0.28)), dpi=150)
    colors = [plt.cm.Blues(0.4 + 0.5 * (i / max(len(names)-1, 1)))
              for i in range(len(names))]
    bars = ax.barh(range(len(names)), values, color=colors, height=0.55)
    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=7, fontweight="bold")
    ax.invert_yaxis()
    ax.set_xlabel("% del portafolio", fontsize=7)
    ax.tick_params(axis="x", labelsize=6.5)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    for bar, val in zip(bars, values):
        ax.text(bar.get_width() + 0.2, bar.get_y() + bar.get_height()/2,
                f"{val:.1f}%", va="center", fontsize=7.5, fontweight="bold",
                color=NAVY_HEX)

    fig.tight_layout()
    tmp = tempfile.NamedTemporaryFile(suffix=".png", delete=False)
    fig.savefig(tmp.name, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return tmp.name


def render_geo_map(geo: dict) -> str:
    """Render a simple world map with region exposure as a table/donut chart.

    Uses a donut chart instead of a geographic map to avoid heavy dependencies.
    """
    if not geo:
        return ""

    # Group into continents
    continent_map = {
        "United States": "Americas",
        "Canada": "Americas",
        "Latin America": "Americas",
        "United Kingdom": "Europe",
        "Eurozone": "Europe",
        "Europe - ex Euro": "Europe",
        "Japan": "Asia",
        "Australasia": "Asia",
        "Asia - Developed": "Asia",
        "Asia - Emerging": "Asia",
        "Africa": "Africa",
        "Middle East": "Asia",
    }
    continents = {}
    for region, val in geo.items():
        cont = continent_map.get(region, "Otros")
        continents[cont] = continents.get(cont, 0) + val

    # Sort by value
    sorted_c = sorted(continents.items(), key=lambda x: x[1], reverse=True)
    labels = [c[0] for c in sorted_c]
    sizes = [c[1] for c in sorted_c]

    colors_map = {
        "Americas": "#00205C",
        "Europe": "#41BBC9",
        "Asia": "#6B8EC7",
        "Africa": "#A0B4D0",
        "Otros": "#C8D1DB",
    }
    colors = [colors_map.get(l, "#C8D1DB") for l in labels]

    fig, ax = plt.subplots(figsize=(3.2, 2.3), dpi=150)
    wedges, texts, autotexts = ax.pie(
        sizes, labels=None, autopct="%1.1f%%", startangle=90,
        colors=colors, pctdistance=0.78,
        wedgeprops=dict(width=0.45, edgecolor="white", linewidth=1.5),
        textprops={"fontsize": 6.5},
    )
    for t in autotexts:
        t.set_fontsize(6.5)
        t.set_color("white")
        t.set_fontweight("bold")

    ax.legend(labels, loc="lower center", fontsize=6, ncol=min(3, len(labels)),
              bbox_to_anchor=(0.5, -0.08), frameon=False)
    ax.set_title("Diversificacion Geografica", fontsize=8, fontweight="bold",
                 color=NAVY_HEX, pad=2)

    fig.tight_layout()
    tmp = tempfile.NamedTemporaryFile(suffix=".png", delete=False)
    fig.savefig(tmp.name, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return tmp.name


# ═══════════════════════════════════════════════════════════════════════════
# PDF GENERATION
# ═══════════════════════════════════════════════════════════════════════════

class FactsheetPDF(FPDF):
    """Custom PDF class for Valmex factsheets."""

    def __init__(self, fondo, serie, info):
        super().__init__(orientation="P", format="letter")
        self.fondo = fondo
        self.serie = serie
        self.info = info
        self.set_auto_page_break(auto=False)
        self.alias_nb_pages()
        self._add_fonts()

    def _add_fonts(self):
        # Use built-in Helvetica
        pass

    def draw_header(self):
        """Draw navy header band with logo."""
        self.set_fill_color(*NAVY)
        self.rect(0, 0, self.w, 22, "F")

        # Logo
        logo = os.path.join(BASE, "VALMEX2.png")
        if os.path.exists(logo):
            self.image(logo, x=6, y=3, h=16)

        # Text
        self.set_xy(55, 4)
        self.set_text_color(*WHITE)
        self.set_font("Helvetica", "B", 11)
        self.cell(0, 6, "OPERADORA VALMEX DE FONDOS DE INVERSION", new_x="LMARGIN", new_y="NEXT")
        self.set_xy(55, 11)
        self.set_font("Helvetica", "", 7)
        today = date.today()
        self.cell(0, 5, f"Factsheet  |  {MESES_LARGO[today.month - 1]} {today.year}",
                  new_x="LMARGIN", new_y="NEXT")
        self.set_text_color(*BLACK)

    def draw_title(self):
        """Draw fund title section."""
        y = 25
        self.set_xy(8, y)
        self.set_font("Helvetica", "", 7)
        self.set_text_color(*GRAY)
        self.cell(0, 4, "Clave Pizarra:", new_x="LMARGIN", new_y="NEXT")

        self.set_xy(8, y + 4)
        self.set_font("Helvetica", "B", 14)
        self.set_text_color(*NAVY)
        title = f"{self.fondo} {self.serie}: {self.info.get('nombre_completo', '')}"
        self.cell(0, 7, title, new_x="LMARGIN", new_y="NEXT")

        self.set_text_color(*BLACK)
        # Thin line
        self.set_draw_color(*SKY)
        self.set_line_width(0.5)
        self.line(8, y + 13, self.w - 8, y + 13)
        return y + 15

    def draw_backtesting(self, chart_path: str, y_start: float) -> float:
        """Draw backtesting chart section."""
        if not chart_path or not os.path.exists(chart_path):
            return y_start
        self.image(chart_path, x=8, y=y_start, w=110)
        return y_start

    def draw_fund_details(self, data: dict, y_start: float) -> float:
        """Draw fund details box on the right side."""
        info = data["info"]
        x = 125
        y = y_start
        box_w = self.w - x - 8

        self.set_fill_color(*NAVY)
        self.set_xy(x, y)
        self.set_text_color(*WHITE)
        self.set_font("Helvetica", "B", 8)
        self.cell(box_w, 5, "  Detalles del Fondo", fill=True,
                  new_x="LMARGIN", new_y="NEXT")
        self.set_text_color(*BLACK)
        y += 6

        details = [
            ("Categoria", info.get("categoria", "")),
            ("Asesor", info.get("asesor", "")),
            ("Activos Netos", data.get("aum", "N/D")),
            ("Indice Ref.", info.get("indice", "")),
            ("Horizonte", info.get("horizonte", "")),
            ("Liquidez", info.get("liquidez_venta", "")),
            ("Horarios", info.get("horarios", "")),
            ("Fecha Inicio", data.get("inception", "")),
        ]

        for i, (label, value) in enumerate(details):
            bg = LIGHT_BG if i % 2 == 0 else WHITE
            self.set_fill_color(*bg)
            self.set_xy(x, y)
            self.set_font("Helvetica", "B", 6.5)
            self.cell(28, 4.5, f"  {label}", fill=True)
            self.set_font("Helvetica", "", 6.5)
            self.cell(box_w - 28, 4.5, f"  {value}", fill=True,
                      new_x="LMARGIN", new_y="NEXT")
            y += 4.5

        return y + 2

    def draw_description(self, data: dict, y_start: float) -> float:
        """Draw fund and advisor description."""
        info = data["info"]
        y = y_start

        # Fund description
        self.set_fill_color(*LIGHT_BG)
        self.set_xy(8, y)
        self.set_font("Helvetica", "B", 8)
        self.set_text_color(*NAVY)
        self.cell(95, 5, "  Que hace este Fondo?", new_x="LMARGIN", new_y="NEXT")
        self.set_text_color(*BLACK)
        y += 5

        self.set_xy(8, y)
        self.set_font("Helvetica", "", 6.5)
        desc = info.get("descripcion_fondo", "")
        if desc:
            self.multi_cell(95, 3.5, desc, new_x="LMARGIN", new_y="NEXT")
        y_after_desc = self.get_y() + 1

        # Advisor description
        self.set_xy(108, y_start)
        self.set_font("Helvetica", "B", 8)
        self.set_text_color(*NAVY)
        self.cell(self.w - 116, 5, f"  Asesor: {info.get('asesor', '')}",
                  new_x="LMARGIN", new_y="NEXT")
        self.set_text_color(*BLACK)

        self.set_xy(108, y_start + 5)
        self.set_font("Helvetica", "", 6.5)
        adesc = info.get("descripcion_asesor", "")
        if adesc:
            self.multi_cell(self.w - 116, 3.5, adesc, new_x="LMARGIN", new_y="NEXT")

        return max(y_after_desc, self.get_y()) + 2

    def draw_returns_table(self, returns: dict, currency: str, y_start: float) -> float:
        """Draw returns table (MXN or USD)."""
        y = y_start
        self.set_xy(8, y)
        self.set_fill_color(*NAVY)
        self.set_text_color(*WHITE)
        self.set_font("Helvetica", "B", 8)
        self.cell(self.w - 16, 5,
                  f"  Rendimientos en {currency} (Acumulados / Anualizados)",
                  fill=True, new_x="LMARGIN", new_y="NEXT")
        self.set_text_color(*BLACK)
        y += 6

        periods = ["1M", "2M", "3M", "6M", "1A", "2A", "3A", "5A", "10A", "Inicio"]
        col_w = (self.w - 16 - 20) / len(periods)  # 20 for label column

        # Header row
        self.set_fill_color(*HDR_BG)
        self.set_xy(8, y)
        self.set_font("Helvetica", "B", 6)
        self.cell(20, 4.5, "", fill=True, border=1)
        for p in periods:
            self.cell(col_w, 4.5, p, fill=True, border=1, align="C")
        y += 4.5

        # Fund row
        self.set_fill_color(*WHITE)
        self.set_xy(8, y)
        self.set_font("Helvetica", "B", 6)
        self.cell(20, 4.5, f"  {self.fondo}", border=1)
        self.set_font("Helvetica", "", 6)
        for p in periods:
            val = returns.get(p)
            if val is not None:
                if val > 0:
                    self.set_text_color(*GREEN)
                elif val < 0:
                    self.set_text_color(*RED)
                else:
                    self.set_text_color(*BLACK)
                self.cell(col_w, 4.5, f"{val:+.2f}%", border=1, align="C")
                self.set_text_color(*BLACK)
            else:
                self.cell(col_w, 4.5, "-", border=1, align="C")
        y += 4.5

        return y + 2

    def draw_geo_and_sectors(self, geo_img: str, sector_img: str,
                             y_start: float) -> float:
        """Draw geo map and sector bars side by side."""
        y = y_start
        half_w = (self.w - 16) / 2

        if geo_img and os.path.exists(geo_img):
            self.image(geo_img, x=8, y=y, w=half_w - 2)

        if sector_img and os.path.exists(sector_img):
            # Title for sectors
            self.set_xy(8 + half_w + 2, y)
            self.set_font("Helvetica", "B", 9)
            self.set_text_color(*NAVY)
            self.cell(half_w - 2, 5, "Diversificacion Sectorial", align="C",
                      new_x="LMARGIN", new_y="NEXT")
            self.set_text_color(*BLACK)
            self.image(sector_img, x=8 + half_w + 2, y=y + 5, w=half_w - 2)

        return y + 48

    def draw_holdings(self, holdings_img: str, y_start: float, ref_date: str,
                      n_holdings: int = 10) -> float:
        """Draw top holdings section."""
        y = y_start
        self.set_xy(8, y)
        self.set_fill_color(*NAVY)
        self.set_text_color(*WHITE)
        self.set_font("Helvetica", "B", 8)
        self.cell(self.w - 16, 5,
                  f"  Principales Emisoras (cierre de {ref_date})",
                  fill=True, new_x="LMARGIN", new_y="NEXT")
        self.set_text_color(*BLACK)
        y += 6

        if holdings_img and os.path.exists(holdings_img):
            img_w = self.w - 50
            # Calculate proportional height based on figure aspect ratio
            fig_h = max(1.8, n_holdings * 0.28)
            img_h = img_w * (fig_h / 7.5)  # match figsize ratio
            self.image(holdings_img, x=20, y=y, w=img_w)
            y += img_h + 2
        return y + 2

    def draw_monthly_table(self, monthly: dict, annual: dict,
                           y_start: float) -> float:
        """Draw monthly returns table."""
        y = y_start
        self.set_xy(8, y)
        self.set_fill_color(*NAVY)
        self.set_text_color(*WHITE)
        self.set_font("Helvetica", "B", 8)
        self.cell(self.w - 16, 5,
                  "  Informacion Estadistica - Rendimientos Mensuales (MXN)",
                  fill=True, new_x="LMARGIN", new_y="NEXT")
        self.set_text_color(*BLACK)
        y += 6

        # Header: Year | Ene | Feb | ... | Dic | Anual
        cols = ["Anio"] + MESES + ["Anual"]
        col_w = (self.w - 16) / len(cols)

        self.set_fill_color(*HDR_BG)
        self.set_xy(8, y)
        self.set_font("Helvetica", "B", 5.5)
        for c in cols:
            self.cell(col_w, 4, c, fill=True, border=1, align="C")
        y += 4

        # Data rows
        years = sorted(monthly.keys(), reverse=True)
        for i, yr in enumerate(years):
            bg = LIGHT_BG if i % 2 == 0 else WHITE
            self.set_fill_color(*bg)
            self.set_xy(8, y)
            self.set_font("Helvetica", "B", 5.5)
            self.cell(col_w, 4, str(yr), fill=True, border=1, align="C")
            self.set_font("Helvetica", "", 5.5)
            for m in range(1, 13):
                val = monthly[yr].get(m)
                if val is not None:
                    if val > 0:
                        self.set_text_color(*GREEN)
                    elif val < 0:
                        self.set_text_color(*RED)
                    else:
                        self.set_text_color(*BLACK)
                    self.cell(col_w, 4, f"{val:+.1f}", fill=True, border=1, align="C")
                    self.set_text_color(*BLACK)
                else:
                    self.cell(col_w, 4, "", fill=True, border=1, align="C")
            # Annual
            ann = annual.get(yr)
            if ann is not None:
                if ann > 0:
                    self.set_text_color(*GREEN)
                elif ann < 0:
                    self.set_text_color(*RED)
                self.set_font("Helvetica", "B", 5.5)
                self.cell(col_w, 4, f"{ann:+.1f}", fill=True, border=1, align="C")
                self.set_text_color(*BLACK)
            else:
                self.cell(col_w, 4, "", fill=True, border=1, align="C")
            y += 4

        return y + 2

    def draw_stats_box(self, stats: dict, std_dev: dict, y_start: float) -> float:
        """Draw statistics box."""
        y = y_start
        half_w = (self.w - 16) / 2

        # Stats box
        self.set_xy(8, y)
        self.set_fill_color(*NAVY)
        self.set_text_color(*WHITE)
        self.set_font("Helvetica", "B", 7)
        self.cell(half_w - 2, 4.5, "  Estadisticas", fill=True,
                  new_x="LMARGIN", new_y="NEXT")
        self.set_text_color(*BLACK)
        y_stat = y + 5

        stat_items = [
            ("Mejor Mes", stats.get("mejor_mes", "N/D")),
            ("Peor Mes", stats.get("peor_mes", "N/D")),
            ("% Meses Positivos", stats.get("pct_positivos", "N/D")),
            ("Total Meses", str(stats.get("total_meses", "N/D"))),
        ]
        for i, (label, value) in enumerate(stat_items):
            bg = LIGHT_BG if i % 2 == 0 else WHITE
            self.set_fill_color(*bg)
            self.set_xy(8, y_stat)
            self.set_font("Helvetica", "B", 6)
            self.cell(30, 4, f"  {label}", fill=True)
            self.set_font("Helvetica", "", 6)
            self.cell(half_w - 32, 4, f"  {value}", fill=True,
                      new_x="LMARGIN", new_y="NEXT")
            y_stat += 4

        # Std Dev box
        x2 = 8 + half_w + 2
        self.set_xy(x2, y)
        self.set_fill_color(*NAVY)
        self.set_text_color(*WHITE)
        self.set_font("Helvetica", "B", 7)
        self.cell(half_w - 2, 4.5, "  Desviacion Estandar (Anualizada)",
                  fill=True, new_x="LMARGIN", new_y="NEXT")
        self.set_text_color(*BLACK)
        y_std = y + 5

        for i, (label, period) in enumerate([("1 Anio", "1A"), ("3 Anios", "3A"), ("5 Anios", "5A")]):
            bg = LIGHT_BG if i % 2 == 0 else WHITE
            self.set_fill_color(*bg)
            self.set_xy(x2, y_std)
            self.set_font("Helvetica", "B", 6)
            self.cell(30, 4, f"  {label}", fill=True)
            self.set_font("Helvetica", "", 6)
            val = std_dev.get(period)
            text = f"  {val:.2f}%" if val is not None else "  N/D"
            self.cell(half_w - 32, 4, text, fill=True,
                      new_x="LMARGIN", new_y="NEXT")
            y_std += 4

        return max(y_stat, y_std) + 3

    def draw_operations(self, data: dict, y_start: float) -> float:
        """Draw fund operations section."""
        info = data["info"]
        y = y_start
        self.set_xy(8, y)
        self.set_fill_color(*NAVY)
        self.set_text_color(*WHITE)
        self.set_font("Helvetica", "B", 8)
        self.cell(self.w - 16, 5, "  Operacion del Fondo",
                  fill=True, new_x="LMARGIN", new_y="NEXT")
        self.set_text_color(*BLACK)
        y += 6

        ops = [
            ("Liquidez Compra", info.get("liquidez_compra", "Diaria")),
            ("Liquidez Venta", info.get("liquidez_venta", "Diaria")),
            ("Plazo Liquidacion", info.get("plazo_liquidacion", "72 horas")),
            ("Horario Operacion", info.get("horarios", "")),
        ]

        col_w = (self.w - 16) / 2
        for i in range(0, len(ops), 2):
            bg = LIGHT_BG if (i // 2) % 2 == 0 else WHITE
            self.set_fill_color(*bg)
            self.set_xy(8, y)
            self.set_font("Helvetica", "B", 6.5)
            self.cell(25, 4.5, f"  {ops[i][0]}", fill=True)
            self.set_font("Helvetica", "", 6.5)
            self.cell(col_w - 25, 4.5, f"  {ops[i][1]}", fill=True)
            if i + 1 < len(ops):
                self.set_font("Helvetica", "B", 6.5)
                self.cell(25, 4.5, f"  {ops[i+1][0]}", fill=True)
                self.set_font("Helvetica", "", 6.5)
                self.cell(col_w - 25, 4.5, f"  {ops[i+1][1]}", fill=True)
            y += 4.5

        return y + 3

    def draw_disclaimer(self, y_start: float) -> float:
        """Draw legal disclaimer."""
        y = y_start
        self.set_xy(8, y)
        self.set_fill_color(*NAVY)
        self.set_text_color(*WHITE)
        self.set_font("Helvetica", "B", 7)
        self.cell(self.w - 16, 4.5, "  INFORMACION IMPORTANTE",
                  fill=True, new_x="LMARGIN", new_y="NEXT")
        self.set_text_color(*GRAY)
        y += 5

        disclaimer = (
            "Los rendimientos pasados no garantizan rendimientos futuros. "
            "La informacion contenida en este documento es de caracter informativo y no constituye "
            "una oferta, invitacion o solicitud de compra o suscripcion de valores. "
            "Los rendimientos mostrados se expresan en terminos brutos antes de impuestos. "
            "Las inversiones en fondos de inversion no se encuentran garantizadas por ninguna "
            "autoridad ni por Operadora Valmex de Fondos de Inversion, S.A. de C.V. "
            "El inversionista debe estar consciente de que los rendimientos sobre su inversion "
            "pueden variar y que el capital invertido no esta garantizado. "
            "Consulte el prospecto de informacion al publico inversionista, la guia de servicios "
            "de inversion y el documento con informacion clave para la inversion del fondo antes "
            "de invertir. Estos documentos se encuentran disponibles en la pagina de internet: "
            "www.vfrisa.com.mx y en la pagina de la CNBV: www.cnbv.gob.mx. "
            "Operadora Valmex de Fondos de Inversion, S.A. de C.V. no asume responsabilidad "
            "alguna por el uso que se haga de la informacion contenida en este documento."
        )

        self.set_xy(8, y)
        self.set_font("Helvetica", "", 5)
        self.multi_cell(self.w - 16, 2.8, disclaimer, new_x="LMARGIN", new_y="NEXT")
        return self.get_y()


def generate_factsheet(fondo: str, serie: str, holdings: dict = None,
                       output: str = None):
    """Main function to generate the factsheet PDF."""
    # Fetch all data
    data = fetch_fund_data(fondo, serie)
    info = data["info"]

    today = date.today()
    ref_date = f"{MESES_LARGO[today.month - 1]} {today.year}"

    # Render charts to temp files
    print("\n[CHARTS] Generando graficas...")
    temp_files = []

    bt_img = render_backtesting_chart(data["backtesting"], fondo,
                                       info.get("indice", ""))
    if bt_img:
        temp_files.append(bt_img)

    geo_img = render_geo_map(data["geo"])
    if geo_img:
        temp_files.append(geo_img)

    sector_img = render_sector_bars(data["sectors"])
    if sector_img:
        temp_files.append(sector_img)

    holdings_img = ""
    if holdings:
        holdings_img = render_holdings_bars(holdings)
        if holdings_img:
            temp_files.append(holdings_img)

    # Build PDF
    print("[PDF] Construyendo PDF...")
    pdf = FactsheetPDF(fondo, serie, info)

    # ── PAGE 1 ──
    pdf.add_page()
    pdf.draw_header()
    y = pdf.draw_title()

    # Backtesting + Fund Details side by side
    pdf.draw_backtesting(bt_img, y)
    y_details = pdf.draw_fund_details(data, y)
    y = max(y + 50, y_details)  # chart ~50mm high

    # Description
    y = pdf.draw_description(data, y)

    # Returns MXN
    y = pdf.draw_returns_table(data["mxn_returns"], "Pesos", y)

    # Returns USD
    y = pdf.draw_returns_table(data["usd_returns"], "Dolares", y)

    # Geo + Sectors
    if geo_img or sector_img:
        y = pdf.draw_geo_and_sectors(geo_img, sector_img, y)

    # ── PAGE 2 ──
    pdf.add_page()
    pdf.draw_header()
    y = 25

    # Holdings
    if holdings_img:
        n_h = min(10, len(holdings)) if holdings else 0
        y = pdf.draw_holdings(holdings_img, y, ref_date, n_holdings=n_h)

    # Monthly returns table
    if data["monthly"]:
        y = pdf.draw_monthly_table(data["monthly"], data["annual"], y)

    # Stats + Std Dev
    if data["stats"] or data["std_dev"]:
        y = pdf.draw_stats_box(data["stats"], data["std_dev"], y)

    # Operations
    y = pdf.draw_operations(data, y)

    # Disclaimer
    pdf.draw_disclaimer(y)

    # Output
    if not output:
        output = os.path.join(BASE, f"{fondo}_{serie}_Factsheet.pdf")

    pdf.output(output)
    print(f"\n{'='*60}")
    print(f"  PDF generado: {output}")
    print(f"{'='*60}\n")

    # Cleanup temp files
    for f in temp_files:
        try:
            os.unlink(f)
        except OSError:
            pass

    return output


# ═══════════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Genera Factsheet PDF para fondos Valmex",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos:
  python generate_factsheet.py --fondo VLMXTEC --serie A
  python generate_factsheet.py --fondo VLMXTEC --serie A \\
      --holdings '{"BROADCOM":6.5,"NVIDIA":5.8,"APPLE":5.5}' \\
      --output VLMXTEC_A_Feb26.pdf
        """,
    )
    parser.add_argument("--fondo", required=True,
                        help="Clave del fondo (ej: VLMXTEC, VALMX28)")
    parser.add_argument("--serie", required=True,
                        help="Serie (ej: A, B1FI, B0CO)")
    parser.add_argument("--holdings", default=None,
                        help='JSON con top holdings: \'{"BROADCOM":6.5,...}\'')
    parser.add_argument("--output", default=None,
                        help="Ruta del PDF de salida")

    args = parser.parse_args()

    # Validate fund
    if args.fondo not in ISIN_MAP:
        print(f"ERROR: Fondo '{args.fondo}' no encontrado. Fondos disponibles:")
        for f in sorted(ISIN_MAP.keys()):
            print(f"  {f}")
        sys.exit(1)

    if args.serie not in ISIN_MAP[args.fondo]:
        print(f"ERROR: Serie '{args.serie}' no disponible para {args.fondo}.")
        print(f"Series disponibles: {', '.join(sorted(ISIN_MAP[args.fondo].keys()))}")
        sys.exit(1)

    # Parse holdings
    holdings = None
    if args.holdings:
        try:
            holdings = json.loads(args.holdings)
        except json.JSONDecodeError as e:
            print(f"ERROR: Holdings JSON invalido: {e}")
            sys.exit(1)

    generate_factsheet(args.fondo, args.serie, holdings=holdings, output=args.output)


if __name__ == "__main__":
    main()
