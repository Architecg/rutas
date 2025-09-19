# -*- coding: utf-8 -*-
"""
Cruce CSV ('id_codigo_barras') vs TXT de rutas:
- Del TXT se toman SOLO rutas que:
    * cumplan el patrón ACTA flexible (ACTA 2 / ACTA N° 2 / ACTA Nº 2 / ACTA No. 2 / ACTA N 2 / ACTA Nro 2)
    * terminen en .pdf
- El 'código' del TXT es el nombre del PDF (solo dígitos).
- Se comparan códigos con los del CSV y se exportan faltantes/coincidencias y un resumen.
Compatible con Python 3.8/3.9
"""

import os
import re, unicodedata
from pathlib import Path
from datetime import datetime
import pandas as pd
from collections import Counter
from typing import Tuple, Set, Dict

# ── CONFIGURA AQUÍ ─────────────────────────────────────────────────────────────
CSV_PATH   = r""
TXT_PATH   = r""
ACTA_NUMERO = 1  
OUTPUT_DIR  = r""
# ───────────────────────────────────────────────────────────────────────────────

def _acta_regex(n: int) -> str:
    """
    Acepta: ACTA 2 | ACTA N° 2 | ACTA Nº 2 | ACTA No 2 | ACTA No. 2 | ACTA N 2 | ACTA Nro 2
    (insensible a mayúsculas)
    """
    num = re.escape(str(n))
    return rf"\bacta\s*(?:n(?:o|º|°|\.|ro)?\s*)?{num}\b"

def _canon(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"\s+", " ", s).strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_")

def load_csv_codes(csv_path: str) -> Tuple[Set[str], Dict[str, int]]:
    """
    Lee el CSV y devuelve (codigos_unicos, duplicados_conteo).
    1) Usa 'id_codigo_barras' (tolerante a BOM/espacios/acentos).
    2) Fallback si la cabecera está rota: concatena celdas y extrae dígitos.
    """
    df = pd.read_csv(csv_path, sep=None, engine="python", dtype="string", encoding="utf-8-sig")

    original_cols = list(df.columns)
    norm_map = {_canon(c): c for c in original_cols}
    target_norm = _canon("id_codigo_barras")

    if target_norm in norm_map:
        col = norm_map[target_norm]
        serie = (df[col].astype("string").fillna("").str.strip()
                 .str.replace(r"\D+", "", regex=True))
        codigos = [c for c in serie.tolist() if c]
    else:
        # ── Fallback robusto ──
        df_raw = pd.read_csv(csv_path, header=None, dtype="string", encoding="utf-8-sig", engine="python")
        start_idx = 1 if not df_raw.iloc[0].astype(str).str.contains(r"\d").any() else 0
        joined = df_raw.iloc[start_idx:].apply(lambda r: "".join(x for x in r.dropna().astype(str)), axis=1)
        serie = joined.str.replace(r"\D+", "", regex=True)
        codigos = [c for c in serie.tolist() if len(c) >= 3]

    dup = {k: v for k, v in Counter(codigos).items() if v > 1}
    return set(codigos), dup

def load_txt_codes(txt_path: str, acta_number: int, ext: str = ".pdf") -> Tuple[Set[str], Dict[str, int]]:
    """
    Lee rutas del TXT y devuelve códigos (nombre del PDF sin extensión -> solo dígitos),
    filtrando por patrón ACTA flexible y extensión .pdf.
    """
    codigos = []
    ext = ext.lower()
    pat = re.compile(_acta_regex(acta_number), flags=re.IGNORECASE)

    # 'errors=ignore' por si hay caracteres especiales en el TXT
    with open(txt_path, "r", encoding="utf-8", errors="ignore") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line:
                continue

            low = line.lower()
            if not low.endswith(ext):
                continue
            if not pat.search(low):
                continue

            base = os.path.splitext(os.path.basename(line))[0]
            code = re.sub(r"\D", "", base)  # solo dígitos
            if code:
                codigos.append(code)

    dup = {k: v for k, v in Counter(codigos).items() if v > 1}
    return set(codigos), dup

def main():
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    csv_codes, csv_dups = load_csv_codes(CSV_PATH)
    txt_codes, txt_dups = load_txt_codes(TXT_PATH, ACTA_NUMERO, ".pdf")

    faltan_en_txt = sorted(csv_codes - txt_codes)  # Están en CSV pero NO aparecen en rutas filtradas
    faltan_en_csv = sorted(txt_codes - csv_codes)  # Están en rutas filtradas pero NO en el CSV
    coinciden     = sorted(csv_codes & txt_codes)

    out_dir = Path(OUTPUT_DIR)
    pd.Series(faltan_en_txt, name="id_codigo_barras").to_csv(out_dir / f"faltan_en_txt_{ts}.csv", index=False)
    pd.Series(faltan_en_csv, name="id_codigo_barras").to_csv(out_dir / f"faltan_en_csv_{ts}.csv", index=False)
    pd.Series(coinciden,     name="id_codigo_barras").to_csv(out_dir / f"coinciden_{ts}.csv", index=False)

    resumen = pd.DataFrame([{
        "csv_total_unicos": len(csv_codes),
        "txt_total_unicos": len(txt_codes),
        "coinciden": len(coinciden),
        "faltan_en_txt": len(faltan_en_txt),
        "faltan_en_csv": len(faltan_en_csv),
        "csv_duplicados": sum(csv_dups.values()),
        "txt_duplicados": sum(txt_dups.values()),
        "patron_acta": _acta_regex(ACTA_NUMERO),
    }])
    resumen.to_csv(out_dir / f"resumen_{ts}.csv", index=False)

    print("✓ Validación completada")
    print(f"  CSV únicos: {len(csv_codes)} | TXT únicos (ACTA {ACTA_NUMERO}, .pdf): {len(txt_codes)}")
    print(f"  Coinciden: {len(coinciden)}")
    print(f"  Faltan en TXT (están en CSV): {len(faltan_en_txt)}  -> {out_dir / f'faltan_en_txt_{ts}.csv'}")
    print(f"  Faltan en CSV (están en rutas): {len(faltan_en_csv)} -> {out_dir / f'faltan_en_csv_{ts}.csv'}")
    if csv_dups:
        print(f"  ¡Ojo! Duplicados en CSV (muestra): {dict(list(csv_dups.items())[:5])}")
    if txt_dups:
        print(f"  ¡Ojo! Duplicados en TXT (muestra): {dict(list(txt_dups.items())[:5])}")

if __name__ == "__main__":
    main()
