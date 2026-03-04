# Valmex Dashboard — Instrucciones para Claude Code

## Entorno
- Python via Anaconda: buscar `python` en PATH o usar `python` directamente
- Proyecto: repositorio git en GitHub (`JCVS01/valmex-dashboard`)
- OS: Windows (usar bash syntax en Claude Code terminal)

## Arquitectura
- `app.py` — Flask dashboard principal (~2400 lineas), APIs Morningstar, backtesting
- `generate_factsheet.py` — Script standalone para generar factsheet PDF por fondo (~750 lineas)
- `generate_series_pdf.py` — PDF de validacion de series
- Logos: `VALMEX.png`, `VALMEX2.png`
- Deps: flask, requests, pandas, yfinance, fpdf2, matplotlib (ver `requirements.txt`)

## Factsheet PDF Generator
- Clase `FactsheetPDF(FPDF)` con metodos draw_*
- `FUND_INFO` dict con metadata estatica por fondo
- `ISIN_MAP` copiado de app.py (evita import Flask)
- Charts generados con matplotlib -> PNG temporal -> embebidos en PDF
- Paleta: Navy #00205C, Teal #41BBC9, Light BG #F5F7FA, Header BG #DCE1EB
- Ejecucion: `python generate_factsheet.py --fondo VLMXTEC --serie A --holdings '{...}'`

## Fondos con metadata
VLMXTEC, VLMXESG, VALMXHC, VXINFRA, VALMXA, VALMX20, VALMX28, VALMXVL, VALMXES

## Convenciones
- No agregar emojis a menos que se pida
- Preferir editar archivos existentes sobre crear nuevos
- No hacer commit automatico — solo cuando se pida explicitamente
- Archivos sensibles en .gitignore: passwords.json, .env, *.pem, *.key
