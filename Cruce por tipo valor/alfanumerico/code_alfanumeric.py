# -*- coding: utf-8 -*-
"""
Cruce CSV (columna con código alfanumérico) vs TXT de rutas:
- Filtra rutas por ACTA flexible (ACTA 2 / ACTA N° 2 / ACTA Nº 2 / ACTA No. 2 / ACTA N 2 / ACTA Nro 2)
- Toma el código del TXT desde el nombre del PDF (sin extensión), pero ahora es ALFANUMÉRICO.
- Normaliza y compara: mayúsculas, sin acentos, colapsa cualquier separador no alfanumérico a '-'.
- Exporta faltan_en_txt, faltan_en_csv, coinciden y resumen.
"""

import os
import re, unicodedata  
from pathlib import Path
from datetime import datetime
import pandas as pd
from collections import Counter, defaultdict
from typing import Tuple, Set, Dict, List

# ── CONFIGURA AQUÍ ─────────────────────────────────────────────────────────────
CSV_PATH   = r""
TXT_PATH   = r""
OUTPUT_DIR  = r""
ACTA_NUMERO = 32                                         
# Si tu columna en el CSV se llama distinto, agrega sinónimos aquí:
CSV_CODE_CANDIDATES = ["codigo_digitalizacion", "id_codigo_barras", "codigo", "codigo digitalizacion"]
# ───────────────────────────────────────────────────────────────────────────────

# ====== Normalización =========================================================
def _acta_regex(n: int) -> str:
    num = re.escape(str(n))
    return rf"\bacta\s*(?:n(?:o|º|°|\.|ro)?\s*)?{num}\b"  # ACTA 2 / ACTA N° 2 / ACTA No. 2 ...

def _canon_header(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"\s+", " ", s).strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_")

def _canon_code(s: str) -> str:
    """
    Normaliza códigos alfanuméricos:
    - NFKD sin acentos
    - mayúsculas
    - cualquier secuencia NO alfanumérica -> '-'
    - quita '-' al inicio/fin
    """
    if s is None:
        return ""
    s = str(s).strip()
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.upper()
    s = re.sub(r"[^A-Z0-9]+", "-", s)   # _ . / \ espacios, etc. -> '-'
    s = s.strip("-")
    return s

# ====== CSV ===================================================================
def load_csv_codes(csv_path: str) -> Tuple[Set[str], Dict[str, int]]:
    df = pd.read_csv(csv_path, sep=None, engine="python", dtype="string", encoding="utf-8-sig")
    cols = list(df.columns)
    norm = {_canon_header(c): c for c in cols}

    # busca la primera columna candidata que exista
    col = None
    for cand in CSV_CODE_CANDIDATES:
        if _canon_header(cand) in norm:
            col = norm[_canon_header(cand)]
            break

    if col is not None:
        serie = df[col].astype("string").fillna("").map(_canon_code)
        codes = [c for c in serie.tolist() if c]
    else:
        # Fallback: une la fila completa y normaliza (por si la cabecera está rota)
        df_raw = pd.read_csv(csv_path, header=None, dtype="string", encoding="utf-8-sig", engine="python")
        start_idx = 1 if not df_raw.iloc[0].astype(str).str.contains(r"\w").any() else 0
        joined = df_raw.iloc[start_idx:].apply(lambda r: " ".join(x for x in r.dropna().astype(str)), axis=1)
        serie = joined.map(_canon_code)
        codes = [c for c in serie.tolist() if c]

    dup = {k: v for k, v in Counter(codes).items() if v > 1}
    return set(codes), dup

# ====== TXT ===================================================================
def load_txt_codes(txt_path: str, acta_number: int, ext: str = ".pdf") -> Tuple[Set[str], Dict[str, int], Dict[str, List[str]]]:
    """
    Devuelve:
      - set de códigos normalizados,
      - duplicados,
      - mapa code -> [rutas] (para depurar)
    """
    codigos: List[str] = []
    paths_by_code: Dict[str, List[str]] = defaultdict(list)
    ext = ext.lower()
    pat = re.compile(_acta_regex(acta_number), flags=re.IGNORECASE)

    with open(txt_path, "r", encoding="utf-8", errors="ignore") as f:
        for raw in f:
            line = raw.strip()
            if not line:
                continue
            low = line.lower()
            if not low.endswith(ext):
                continue
            if not pat.search(low):
                continue

            base = os.path.splitext(os.path.basename(line))[0]
            code = _canon_code(base)  # ← alfanumérico normalizado
            if code:
                codigos.append(code)
                paths_by_code[code].append(line)

    dup = {k: v for k, v in Counter(codigos).items() if v > 1}
    return set(codigos), dup, paths_by_code

# ====== Main ==================================================================
def main():
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = Path(OUTPUT_DIR)

    csv_codes, csv_dups   = load_csv_codes(CSV_PATH)
    txt_codes, txt_dups, paths_map = load_txt_codes(TXT_PATH, ACTA_NUMERO, ".pdf")

    faltan_en_txt = sorted(csv_codes - txt_codes)   # están en CSV pero no en TXT
    faltan_en_csv = sorted(txt_codes - csv_codes)   # están en TXT pero no en CSV
    coinciden     = sorted(csv_codes & txt_codes)

    pd.Series(faltan_en_txt, name="codigo").to_csv(out / f"faltan_en_txt_{ts}.csv", index=False)
    pd.Series(faltan_en_csv, name="codigo").to_csv(out / f"faltan_en_csv_{ts}.csv", index=False)
    pd.Series(coinciden,     name="codigo").to_csv(out / f"coinciden_{ts}.csv", index=False)

    # Para depurar: exporta un ejemplo de ruta por código detectado en TXT
    rows = [{"codigo": c, "ocurrencias": len(paths_map[c]), "ejemplo_ruta": paths_map[c][0]} for c in sorted(paths_map)]
    pd.DataFrame(rows).to_csv(out / f"txt_codigos_rutas_{ts}.csv", index=False)

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
    resumen.to_csv(out / f"resumen_{ts}.csv", index=False)

    print("✓ Validación alfanumérica completada")
    print(f"  CSV únicos: {len(csv_codes)} | TXT únicos (ACTA {ACTA_NUMERO}, .pdf): {len(txt_codes)}")
    print(f"  Coinciden: {len(coinciden)}")
    print(f"  Faltan en TXT: {len(faltan_en_txt)}  -> {out / f'faltan_en_txt_{ts}.csv'}")
    print(f"  Faltan en CSV: {len(faltan_en_csv)} -> {out / f'faltan_en_csv_{ts}.csv'}")

if __name__ == "__main__":
    main()
