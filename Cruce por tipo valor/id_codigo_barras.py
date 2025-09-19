# -*- coding: utf-8 -*-
"""
Cruce de validación entre un CSV (columna 'id_codigo_barras') y
un TXT con rutas de archivos PDF. Filtra solo rutas con 'ACTA N'
y extensión .pdf, luego compara códigos.
"""
# ── Librerias ──────────────────────────────────────────────────────────────
import os
import re, unicodedata
from pathlib import Path
from datetime import datetime
import pandas as pd
from collections import Counter

# ── CONFIGURA AQUÍ ─────────────────────────────────────────────────────────────
CSV_PATH   = r""
TXT_PATH   = r""
FILTRO_ACTA = "acta n° 1"                     # Subcadena a buscar (case-insensitive)
OUTPUT_DIR  = r""
# ───────────────────────────────────────────────────────────────────────────────

def _canon(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"\s+", " ", s).strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_")

def load_csv_codes(csv_path: str):
    """
    Lee el CSV y devuelve (codigos_unicos, duplicados_conteo).
    1) Intenta encontrar la columna 'id_codigo_barras' (tolerante a BOM, espacios, acentos).
    2) Si no la halla (p.ej. porque se partió en varias), hace fallback:
       lee todas las columnas, concatena cada fila, y extrae solo dígitos.
    """
    # Intento 1: lectura normal con eliminación de BOM
    df = pd.read_csv(
        csv_path,
        sep=None, engine="python",
        dtype="string",
        encoding="utf-8-sig"
    )

    # normaliza nombres
    original_cols = list(df.columns)
    norm_map = {_canon(c): c for c in original_cols}
    target_norm = _canon("id_codigo_barras")

    if target_norm in norm_map:
        col = norm_map[target_norm]
        serie = (df[col]
                 .astype("string")
                 .fillna("")
                 .str.strip()
                 .str.replace(r"\D+", "", regex=True))
        codigos = [c for c in serie.tolist() if c]
    else:
        # ── Fallback robusto ──
        # Relee sin encabezados y arma el código por fila uniendo todas las celdas
        df_raw = pd.read_csv(
            csv_path, header=None, dtype="string",
            encoding="utf-8-sig", engine="python"
        )

        # descarta la primera fila si no contiene dígitos (probable cabecera)
        start_idx = 1 if not df_raw.iloc[0].astype(str).str.contains(r"\d").any() else 0

        # concatena celdas por fila y extrae solo dígitos
        joined = df_raw.iloc[start_idx:].apply(
            lambda r: "".join(x for x in r.dropna().astype(str)), axis=1
        )
        serie = joined.str.replace(r"\D+", "", regex=True)

        # filtra longitudes razonables (ajusta si tus códigos tienen otra longitud)
        codigos = [c for c in serie.tolist() if len(c) >= 6]

    dup = {k: v for k, v in Counter(codigos).items() if v > 1}
    return set(codigos), dup

def load_txt_codes(txt_path: str, filtro_substr: str, ext: str = ".pdf") -> tuple[set[str], dict]:
    """
    Lee rutas desde un TXT y devuelve los códigos (nombre del archivo sin extensión),
    filtrando por subcadena (p.ej. 'acta 9') y extensión .pdf.
    Devuelve: (conjunto_de_codigos, duplicados_conteo)
    """
    codigos = []
    filtro = filtro_substr.lower()
    ext = ext.lower()

    # 'errors=ignore' por si hay caracteres especiales en el TXT
    with open(txt_path, "r", encoding="utf-8", errors="ignore") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line:
                continue

            low = line.lower()
            if filtro and filtro not in low:
                continue
            if not low.endswith(ext):
                continue

            # Extrae nombre de archivo sin extensión
            base = os.path.splitext(os.path.basename(line))[0]

            # Nos quedamos solo con dígitos (por si el nombre trae espacios u otros chars)
            code = re.sub(r"\D", "", base)
            if code:
                codigos.append(code)

    dup = {k: v for k, v in Counter(codigos).items() if v > 1}
    return set(codigos), dup


def main():
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    csv_codes, csv_dups = load_csv_codes(CSV_PATH)
    txt_codes, txt_dups = load_txt_codes(TXT_PATH, FILTRO_ACTA, ".pdf")

    # Diferencias
    faltan_en_txt = sorted(csv_codes - txt_codes)  # Están en CSV pero NO aparecen en rutas ACTA 9 .pdf
    faltan_en_csv = sorted(txt_codes - csv_codes)  # Están en rutas ACTA 9 .pdf pero NO están en el CSV
    coinciden     = sorted(csv_codes & txt_codes)

    # Guardar resultados
    out_dir = Path(OUTPUT_DIR)
    pd.Series(faltan_en_txt, name="id_codigo_barras").to_csv(out_dir / f"faltan_en_txt_{timestamp}.csv", index=False)
    pd.Series(faltan_en_csv, name="id_codigo_barras").to_csv(out_dir / f"faltan_en_csv_{timestamp}.csv", index=False)
    pd.Series(coinciden,     name="id_codigo_barras").to_csv(out_dir / f"coinciden_{timestamp}.csv", index=False)

    resumen = pd.DataFrame([{
        "csv_total_unicos": len(csv_codes),
        "txt_total_unicos": len(txt_codes),
        "coinciden": len(coinciden),
        "faltan_en_txt": len(faltan_en_txt),
        "faltan_en_csv": len(faltan_en_csv),
        "csv_duplicados": sum(csv_dups.values()),
        "txt_duplicados": sum(txt_dups.values()),
    }])
    resumen.to_csv(out_dir / f"resumen_{timestamp}.csv", index=False)

    # Mensaje en consola
    print("✓ Validación completada")
    print(f"  CSV únicos: {len(csv_codes)} | TXT únicos (ACTA 9, .pdf): {len(txt_codes)}")
    print(f"  Coinciden: {len(coinciden)}")
    print(f"  Faltan en TXT (están en CSV): {len(faltan_en_txt)}  -> archivo: {out_dir / f'faltan_en_txt_{timestamp}.csv'}")
    print(f"  Faltan en CSV (están en rutas): {len(faltan_en_csv)} -> archivo: {out_dir / f'faltan_en_csv_{timestamp}.csv'}")

    if csv_dups:
        print(f"  ¡Ojo! Duplicados en CSV (muestra): {dict(list(csv_dups.items())[:5])}")
    if txt_dups:
        print(f"  ¡Ojo! Duplicados en TXT (muestra): {dict(list(txt_dups.items())[:5])}")

if __name__ == "__main__":
    main()
