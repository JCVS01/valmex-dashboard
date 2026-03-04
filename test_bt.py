"""Test backtesting validation logic - runs inside Docker."""
import sys, os
sys.path.insert(0, "/app")
os.chdir("/app")

from app import ISIN_MAP, SERIE_MAP, PERFILES, get_ms_nav, get_fondo_backtesting, resolve_serie

print("=" * 80)
print("TEST 1: fundName validation - ISIN correcto (VXGUBCP A)")
print("=" * 80)
navs = get_ms_nav("MXP800501001", start="2026-02-01", expect_fund="VXGUBCP", expect_serie="A")
print(f"  Resultado: {len(navs)} precios, primero={navs[0] if navs else 'N/A'}")
print()

print("=" * 80)
print("TEST 2: fundName MISMATCH - VXTBILL old B0CF ISIN (should be rejected)")
print("=" * 80)
# MX51VA1F0087 returns VXTBILL B1CF, but we pass expect B0CF
navs2 = get_ms_nav("MX51VA1F0087", start="2026-02-01", expect_fund="VXTBILL", expect_serie="B0CF")
print(f"  Resultado: {len(navs2)} precios (esperado: 0 por mismatch)")
print()

print("=" * 80)
print("TEST 3: fundName validation - VXTBILL B1CF (correct key now)")
print("=" * 80)
navs3 = get_ms_nav("MX51VA1F0087", start="2026-02-01", expect_fund="VXTBILL", expect_serie="B1CF")
print(f"  Resultado: {len(navs3)} precios (esperado: >0)")
print()

print("=" * 80)
print("TEST 4: Freshness check - VLMXP38 B1FI (serie cerrada 2016)")
print("=" * 80)
bt4 = get_fondo_backtesting("VLMXP38", "B1FI")
print(f"  Resultado: {len(bt4)} puntos (esperado: 0 por serie cerrada)")
print()

print("=" * 80)
print("TEST 5: resolve_serie fallbacks")
print("=" * 80)
# VXTBILL PPR: B0CF not in ISIN_MAP -> should fallback
s1 = resolve_serie("VXTBILL", "Plan Personal de Retiro - B1NC/B1CF")
print(f"  VXTBILL PPR: deseada=B0CF, resuelta={s1} (esperado: B0FI fallback)")

# VLMXP31 PPR: B1CF removed -> should fallback
s2 = resolve_serie("VLMXP31", "Plan Personal de Retiro - B1NC/B1CF")
print(f"  VLMXP31 PPR: deseada=B1CF, resuelta={s2} (esperado: B1FI fallback)")

# Normal case
s3 = resolve_serie("VXGUBCP", "Plan Personal de Retiro - B1NC/B1CF")
print(f"  VXGUBCP PPR: deseada=B1CF, resuelta={s3} (esperado: B1CF)")
print()

print("=" * 80)
print("TEST 6: Backtesting completo Perfil 3 PPR desde 2000")
print("=" * 80)
fondos_p3 = PERFILES.get("3", {})
print(f"  Fondos: {fondos_p3}")
for fondo, pct in fondos_p3.items():
    serie = resolve_serie(fondo, "Plan Personal de Retiro - B1NC/B1CF")
    bt = get_fondo_backtesting(fondo, serie)
    status = f"{len(bt)} pts, {bt[0]['fecha']}..{bt[-1]['fecha']}" if bt else "sin datos"
    print(f"  {fondo} {serie} ({pct}%): {status}")
print()

print("=" * 80)
print("TEST 7: Backtesting todos los fondos Serie A desde 2000")
print("=" * 80)
all_fondos = list(ISIN_MAP.keys())
for fondo in sorted(all_fondos):
    bt = get_fondo_backtesting(fondo, "A")
    if bt:
        print(f"  {fondo:10s} A: {len(bt):4d} pts | {bt[0]['fecha']} .. {bt[-1]['fecha']}")
    else:
        print(f"  {fondo:10s} A: sin datos")

print()
print("TESTS COMPLETADOS")
