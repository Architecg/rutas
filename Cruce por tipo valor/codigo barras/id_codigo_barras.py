# -*- coding: utf-8 -*-
"""
Cruce de códigos entre:
- CSV/Excel (equipo dt)
- TXT de rutas de Difusión (almacenamiento general)
- TXT de rutas de dt (almacenamiento del grupo) con filtro por FASE

Fases en dt:
- "Digitalización 2"        -> DT_PHASE = "digitalizacion2"
- "Discos Duros" (Fase 1)   -> DT_PHASE = "fase1"
- "Fase 2"                   -> DT_PHASE = "fase2"

Para evitar confusiones:
- En dt exigimos que la ruta pertenezca a la FASE seleccionada y que contenga el ACTA N buscado.
"""

import os
import re
import unicodedata
from pathlib import Path
from datetime import datetime
from collections import Counter, defaultdict
from typing import Tuple, Set, Dict, List, Optional

import pandas as pd

# ── CONFIG ────────────────────────────────────────────────────────────────────
CSV_OR_XLSX_PATH   = r""
TXT_DIFUSION_PATH  = r""
TXT_DT_PATH        = r""

# Procesar una ACTA a la vez
ACTA_NUMERO = 9  # <-- ajusta aquí según el acta

# Filtro de fase solo para el TXT de dt: "fase1", "fase2", "digitalizacion2", None
DT_PHASE = "fase2"

# Aceptar número en texto ("veintidós") además del número arábigo
MATCH_TEXTO_NUMERO = True

# Normalización de códigos (NEUTRAL por tu requerimiento)
STRIP_LEADING_ZEROS = False
PAD_TO_LENGTH: Optional[int] = None  # p.ej. 12 si quisieras igualar longitudes

# Mostrar/ocultar en el resumen los parámetros de normalización
SHOW_NORMALIZATION_SETTINGS = False

# Extensión a validar
VALID_EXT = ".pdf"

# Carpeta de salida
OUTPUT_DIR = r""
# ──────────────────────────────────────────────────────────────────────────────

# ── Normalización ─────────────────────────────────────────────────────────────
def _normalize_text(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s.lower()

def _canon(s: str) -> str:
    s = _normalize_text(s)
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_")

def _normalize_code(c: str) -> str:
    c = re.sub(r"\D+", "", str(c or ""))
    if STRIP_LEADING_ZEROS:
        c = c.lstrip("0")
        if c == "":
            c = "0"
    if PAD_TO_LENGTH and c:
        c = c.zfill(PAD_TO_LENGTH)
    return c


# ── Números en texto (básico) ────────────────────────────────────────────────
def _number_to_spanish_variants(n: int) -> List[str]:
    unidades = {
        0:"cero",1:"uno",2:"dos",3:"tres",4:"cuatro",5:"cinco",
        6:"seis",7:"siete",8:"ocho",9:"nueve",10:"diez",
        11:"once",12:"doce",13:"trece",14:"catorce",15:"quince",
        16:"dieciseis",17:"diecisiete",18:"dieciocho",19:"diecinueve",
        20:"veinte",21:"veintiuno",22:"veintidos",23:"veintitres",24:"veinticuatro",
        25:"veinticinco",26:"veintiseis",27:"veintisiete",28:"veintiocho",29:"veintinueve"
    }
    decenas = {30:"treinta",40:"cuarenta",50:"cincuenta",60:"sesenta",
               70:"setenta",80:"ochenta",90:"noventa"}
    centenas = {100:"cien"}

    variants = set()
    if n in unidades:
        variants.add(unidades[n])
    elif n in decenas:
        variants.add(decenas[n])
    elif 31 <= n <= 99:
        d = (n//10)*10; u = n % 10
        if d in decenas and u in unidades:
            variants.add(f"{decenas[d]} y {unidades[u]}")
    elif n == 100:
        variants.add(centenas[100])
    elif 101 <= n <= 199:
        rest = n - 100
        for v in _number_to_spanish_variants(rest):
            variants.add(f"ciento {v}")

    more = set()
    for v in variants:
        v2 = (v.replace("dieciseis","dieciséis")
                .replace("veintidos","veintidós")
                .replace("veintitres","veintitrés")
                .replace("veintiseis","veintiséis"))
        more.add(v2)
    variants |= more
    return sorted({_normalize_text(v) for v in variants})


# ── Regex flexible de ACTA ────────────────────────────────────────────────────
def _acta_regex(n: int, include_text: bool = True) -> re.Pattern:
    num = re.escape(str(n))
    prefijos = r"(?:n(?:o|ro|\.|)|nº|n°)?"
    alt_text = []
    if include_text:
        for t in _number_to_spanish_variants(n):
            alt_text.append(fr"{re.escape(t)}\b")

    partes = [fr"\bacta\s*{prefijos}\s*0*{num}\b(?!\d)"]
    if alt_text:
        partes.append(fr"\bacta\s*{prefijos}\s*(?:{'|'.join(alt_text)})")

    return re.compile("|".join(partes), flags=re.IGNORECASE)


# ── Detección de FASE en rutas de dt ──────────────────────────────────────────
PHASE_TOKENS = {
    "fase1": ["discos duros", "discos_duros"],
    "fase2": ["fase 2", "fase_2"],
    "digitalizacion2": ["digitalizacion 2", "digitalizacion_2", "digitalización 2"]
}

def _phase_matches(line_norm: str, phase: Optional[str]) -> bool:
    if not phase:
        return True
    phase = phase.lower().strip()
    tokens = PHASE_TOKENS.get(phase, [])
    if not tokens:
        return False
    return any(tok in line_norm for tok in tokens)


# ── Lectores ──────────────────────────────────────────────────────────────────
def load_csv_or_xlsx_codes(path: str) -> Tuple[Set[str], Dict[str,int]]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"No existe: {path}")

    def _pick_codes(df: pd.DataFrame) -> List[str]:
        norm_map = {_canon(c): c for c in df.columns}
        target_norm = _canon("id_codigo_barras")
        if target_norm in norm_map:
            col = norm_map[target_norm]
            serie = df[col].astype("string").fillna("").map(_normalize_code)
            return [c for c in serie.tolist() if c]
        joined = df.apply(lambda r: "".join(x for x in r.dropna().astype(str)), axis=1)
        return [ _normalize_code(x) for x in joined.tolist() if len(_normalize_code(x)) >= 3 ]

    try:
        if p.suffix.lower() in {".xlsx", ".xls"}:
            df = pd.read_excel(path, dtype="string")
        else:
            df = pd.read_csv(path, sep=None, engine="python", dtype="string", encoding="utf-8-sig")
        codigos = _pick_codes(df)
    except Exception:
        df_raw = pd.read_csv(path, header=None, dtype="string", encoding="utf-8-sig", engine="python")
        start_idx = 1 if not df_raw.iloc[0].astype(str).str.contains(r"\d").any() else 0
        df_raw = df_raw.iloc[start_idx:]
        joined = df_raw.apply(lambda r: "".join(x for x in r.dropna().astype(str)), axis=1)
        codigos = [ _normalize_code(x) for x in joined.tolist() if len(_normalize_code(x)) >= 3 ]

    dup = {k:v for k,v in Counter(codigos).items() if v>1}
    return set(codigos), dup


def load_txt_codes_with_paths(
    txt_path: str,
    acta_number: int,
    valid_ext: str = ".pdf",
    include_text_number: bool = True,
    phase_filter: Optional[str] = None,
    require_acta_in_path: bool = True
) -> Tuple[Set[str], Dict[str,int], Dict[str,str], Dict[str,List[str]]]:
    """
    Retorna:
      codes: set de códigos normalizados
      dup:   dict código -> conteo duplicado
      sample_map: dict código -> UNA ruta (primera ocurrencia)
      routes_map: dict código -> LISTA de todas las rutas encontradas
    """
    p = Path(txt_path)
    if not p.exists():
        raise FileNotFoundError(f"No existe: {txt_path}")

    pattern = _acta_regex(acta_number, include_text=include_text_number)
    valid_ext = valid_ext.lower()

    codes: List[str] = []
    sample_map: Dict[str,str] = {}
    routes_map: Dict[str,List[str]] = defaultdict(list)

    with open(txt_path, "r", encoding="utf-8", errors="ignore") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line:
                continue
            if not line.lower().endswith(valid_ext):
                continue

            line_norm = _normalize_text(line)

            # 1) Filtro de FASE (solo para dt)
            if phase_filter is not None and not _phase_matches(line_norm, phase_filter):
                continue

            # 2) Filtro de ACTA
            if require_acta_in_path and not pattern.search(line_norm):
                continue

            base = os.path.splitext(os.path.basename(line))[0]
            code = _normalize_code(base)
            if not code:
                continue

            codes.append(code)
            if code not in sample_map:
                sample_map[code] = line
            routes_map[code].append(line)

    dup = {k:v for k,v in Counter(codes).items() if v>1}
    return set(routes_map.keys()), dup, sample_map, routes_map


# ── Helpers de salida ─────────────────────────────────────────────────────────
def _join_paths(paths: List[str]) -> str:
    # No uses coma (,) para no chocar con CSV; usa ' | '
    return " | ".join(paths) if paths else ""

def _write_codes_with_routes(
    out_dir: Path,
    fname: str,
    codes: List[str],
    routes_dif: Dict[str,List[str]] = None,
    routes_dt: Dict[str,List[str]] = None,
    add_has_dt: bool = False
) -> Path:
    routes_dif = routes_dif or {}
    routes_dt  = routes_dt or {}

    rows = []
    for c in codes:
        dif_list = routes_dif.get(c, [])
        dt_list  = routes_dt.get(c, [])
        row = {
            "id_codigo_barras": c,
            "routes_difusion": _join_paths(dif_list),
            "n_routes_difusion": len(dif_list),
            "routes_dt": _join_paths(dt_list),
            "n_routes_dt": len(dt_list),
        }
        if add_has_dt:
            row["has_dt"] = bool(dt_list)
        rows.append(row)

    df = pd.DataFrame(rows, columns=[
        "id_codigo_barras",
        "routes_difusion","n_routes_difusion",
        "routes_dt","n_routes_dt"
    ] + (["has_dt"] if add_has_dt else []))
    out_path = out_dir / fname
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    return out_path


# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(OUTPUT_DIR)

    # 1) Cargar fuentes
    csv_codes, csv_dups = load_csv_or_xlsx_codes(CSV_OR_XLSX_PATH)

    dif_codes, dif_dups, dif_samples, dif_routes = load_txt_codes_with_paths(
        TXT_DIFUSION_PATH,
        ACTA_NUMERO,
        VALID_EXT,
        MATCH_TEXTO_NUMERO,
        phase_filter=None,             # Difusión: sin filtro de fase
        require_acta_in_path=True
    )

    dt_codes, dt_dups, dt_samples, dt_routes = load_txt_codes_with_paths(
        TXT_DT_PATH,
        ACTA_NUMERO,
        VALID_EXT,
        MATCH_TEXTO_NUMERO,
        phase_filter=DT_PHASE,         # filtro por FASE en dt
        require_acta_in_path=True
    )

    # 2) Conjuntos
    coinciden_todos         = sorted(csv_codes & dif_codes & dt_codes)

    csv_dif_match           = sorted(csv_codes & dif_codes)
    csv_dt_match            = sorted(csv_codes & dt_codes)

    falta_en_dif_desde_csv  = sorted(csv_codes - dif_codes)
    falta_en_csv_desde_dif  = sorted(dif_codes - csv_codes)

    dt_no_en_difusion       = sorted(dt_codes - dif_codes)
    dt_no_en_csv            = sorted(dt_codes - csv_codes)

    # 3) Guardar resultados con RUTAS
    base = f"acta_{ACTA_NUMERO}_{DT_PHASE or 'sin_fase'}_{ts}"

    # Coinciden en las 3 fuentes
    p_all = _write_codes_with_routes(
        out_dir, f"coinciden_en_las_tres_fuentes_{base}.csv",
        coinciden_todos, dif_routes, dt_routes
    )

    # Coinciden CSV <-> Difusión (marcamos si también están en dt)
    p_cd = _write_codes_with_routes(
        out_dir, f"coinciden_csv_difusion_{base}.csv",
        csv_dif_match, dif_routes, dt_routes, add_has_dt=True
    )

    # Coinciden CSV <-> dt (marcamos si también están en Difusión)
    p_ct = _write_codes_with_routes(
        out_dir, f"coinciden_csv_dt_{base}.csv",
        csv_dt_match, dif_routes, dt_routes, add_has_dt=True
    )

    # Faltan en Difusión (desde CSV) -> mostramos si el grupo (dt) lo tiene y sus rutas
    p_fd = _write_codes_with_routes(
        out_dir, f"falta_en_difusion_desde_csv_{base}.csv",
        falta_en_dif_desde_csv, dif_routes, dt_routes, add_has_dt=True
    )

    # Faltan en CSV (desde Difusión) -> mostramos si dt lo tiene y sus rutas
    p_fc = _write_codes_with_routes(
        out_dir, f"falta_en_csv_desde_difusion_{base}.csv",
        falta_en_csv_desde_dif, dif_routes, dt_routes, add_has_dt=True
    )

    # Diagnósticos específicos de dt
    p_dn = _write_codes_with_routes(
        out_dir, f"dt_no_en_difusion_{base}.csv",
        dt_no_en_difusion, dif_routes, dt_routes
    )
    p_dx = _write_codes_with_routes(
        out_dir, f"dt_no_en_csv_{base}.csv",
        dt_no_en_csv, dif_routes, dt_routes
    )

    # 4) Resumen
    cols_base = {
        "acta_numero": ACTA_NUMERO,
        "dt_phase": DT_PHASE or "",
        "patron_acta": _acta_regex(ACTA_NUMERO, MATCH_TEXTO_NUMERO).pattern,
        "csv_total_unicos": len(csv_codes),
        "dif_total_unicos": len(dif_codes),
        "dt_total_unicos": len(dt_codes),
        "coinciden_todos": len(coinciden_todos),
        "coinciden_csv_difusion": len(csv_dif_match),
        "coinciden_csv_dt": len(csv_dt_match),
        "falta_en_difusion_desde_csv": len(falta_en_dif_desde_csv),
        "falta_en_csv_desde_difusion": len(falta_en_csv_desde_dif),
        "dt_no_en_difusion": len(dt_no_en_difusion),
        "dt_no_en_csv": len(dt_no_en_csv),
        "csv_duplicados": sum(csv_dups.values()),
        "dif_duplicados": sum(dif_dups.values()),
        "dt_duplicados": sum(dt_dups.values()),
        "ext_validada": VALID_EXT,
        "match_texto_numero": MATCH_TEXTO_NUMERO,
    }
    if SHOW_NORMALIZATION_SETTINGS:
        cols_base.update({
            "strip_leading_zeros": STRIP_LEADING_ZEROS,
            "pad_to_length": PAD_TO_LENGTH if PAD_TO_LENGTH else ""
        })

    resumen = pd.DataFrame([cols_base])
    p_res = Path(OUTPUT_DIR) / f"resumen_{base}.csv"
    resumen.to_csv(p_res, index=False, encoding="utf-8-sig")

    # 5) Logs
    print("✓ Cruce completado con rutas (normalización neutral)")
    print(f"  ACTA: {ACTA_NUMERO} | DT_PHASE: {DT_PHASE} | EXT: {VALID_EXT} | texto_numero={MATCH_TEXTO_NUMERO}")
    print(f"  CSV únicos: {len(csv_codes)} | Difusión únicos: {len(dif_codes)} | dt únicos: {len(dt_codes)}")
    print(f"  Coinciden en las 3 fuentes: {len(coinciden_todos)} -> {p_all}")
    print(f"  Coinciden CSV<->Difusión: {len(csv_dif_match)} -> {p_cd}")
    print(f"  Coinciden CSV<->dt: {len(csv_dt_match)} -> {p_ct}")
    print(f"  Falta en Difusión (desde CSV): {len(falta_en_dif_desde_csv)} -> {p_fd}")
    print(f"  Falta en CSV (desde Difusión): {len(falta_en_csv_desde_dif)} -> {p_fc}")
    print(f"  dt no en Difusión: {len(dt_no_en_difusion)} -> {p_dn}")
    print(f"  dt no en CSV: {len(dt_no_en_csv)} -> {p_dx}")
    if SHOW_NORMALIZATION_SETTINGS:
        print(f"  strip_leading_zeros={STRIP_LEADING_ZEROS} | pad_to_length={PAD_TO_LENGTH}")

if __name__ == "__main__":
    main()