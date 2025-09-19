# -*- coding: utf-8 -*-
"""
Cruce de códigos (id_codigo_barras) entre CSV/Excel, TXT Difusión y TXT dt
para **múltiples ACTAS** en una sola ejecución.

- ACTA flexible (N°, Nº, No., número en texto)
- dt con filtro por FASE (fase1/fase2/digitalizacion2)
- Lee códigos del CSV/Excel (columna 'id_codigo_barras' o fallback)
- Para cada ACTA N: compara códigos CSV ↔ Difusión (ACTA N) ↔ dt (ACTA N)
- Guarda resultados por ACTA con rutas y un resumen global
Compatible con Python 3.8+
"""

import os, re, unicodedata
from pathlib import Path
from datetime import datetime
from collections import Counter, defaultdict
from typing import Tuple, Set, Dict, List, Optional, DefaultDict

import pandas as pd

# ── CONFIG ────────────────────────────────────────────────────────────────────
CSV_OR_XLSX_PATH   = r"...\ACTA 17,22,24,26,28,29,30,31 FUID - HISTORIAS LABORALES - A-Z.csv"
TXT_DIFUSION_PATH  = r"....\Difusion.txt"
TXT_DT_PATH        = r"....\resources\dt.txt"

# Opción A: lista manual de ACTAS (si se deja None, se intentan extraer del nombre del CSV)
ACTA_LIST = [17,22,24,26,28,29,30,31]   # p.ej. [17,22,24,26,28,29,30,31]
AUTO_PARSE_ACTAS_FROM_CSV_NAME = True   # extrae números del nombre de archivo si ACTA_LIST es None

# Filtro de fase solo para el TXT de dt: "fase1", "fase2", "digitalizacion2", None
DT_PHASE = "fase2"

# Aceptar número en texto ("veintidós") además del arábigo
MATCH_TEXTO_NUMERO = True

# Normalización de códigos (neutral por tu requerimiento)
STRIP_LEADING_ZEROS = False
PAD_TO_LENGTH: Optional[int] = None  # ej. 12 si quisieras igualar longitudes

# Extensión válida
VALID_EXT = ".pdf"

# Carpeta de salida
OUTPUT_DIR = r".....\ACTAS 17-31 - FUID - HISTORIAS LABORALES"
DEBUG = True
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

def _sanitize_for_filename(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]+", "_", s).strip("_")


# ── Números en texto (básico) ────────────────────────────────────────────────
def _number_to_spanish_variants(n: int) -> List[str]:
    unidades = {0:"cero",1:"uno",2:"dos",3:"tres",4:"cuatro",5:"cinco",6:"seis",7:"siete",8:"ocho",9:"nueve",10:"diez",
                11:"once",12:"doce",13:"trece",14:"catorce",15:"quince",16:"dieciseis",17:"diecisiete",18:"dieciocho",19:"diecinueve",
                20:"veinte",21:"veintiuno",22:"veintidos",23:"veintitres",24:"veinticuatro",25:"veinticinco",26:"veintiseis",
                27:"veintisiete",28:"veintiocho",29:"veintinueve"}
    decenas  = {30:"treinta",40:"cuarenta",50:"cincuenta",60:"sesenta",70:"setenta",80:"ochenta",90:"noventa"}
    variants = set()
    if n in unidades: variants.add(unidades[n])
    elif n in decenas: variants.add(decenas[n])
    elif 31 <= n <= 99: variants.add(f"{decenas[(n//10)*10]} y {unidades[n%10]}")
    elif n == 100: variants.add("cien")
    elif 101 <= n <= 199:
        for v in _number_to_spanish_variants(n-100): variants.add(f"ciento {v}")
    norm = set()
    for v in variants:
        norm.add(_normalize_text(v.replace("dieciseis","dieciséis").replace("veintidos","veintidós")
                                   .replace("veintitr es","veintitrés".replace(" ",""))
                                   .replace("veintiseis","veintiséis")))
    return sorted(norm or {_normalize_text(str(n))})

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


# ── FASE en rutas dt ──────────────────────────────────────────────────────────
PHASE_TOKENS = {
    "fase1": ["discos duros", "discos_duros"],
    "fase2": ["fase 2", "fase_2"],
    "digitalizacion2": ["digitalizacion 2", "digitalizacion_2", "digitalización 2"]
}
def _phase_matches(line_norm: str, phase: Optional[str]) -> bool:
    if not phase:
        return True
    tokens = PHASE_TOKENS.get(phase.lower().strip(), [])
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
        return [_normalize_code(x) for x in joined.tolist() if len(_normalize_code(x)) >= 3]

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
        codigos = [_normalize_code(x) for x in joined.tolist() if len(_normalize_code(x)) >= 3]

    dup = {k:v for k,v in Counter(codigos).items() if v>1}
    return set(codigos), dup


# ── Índice de TXT por ACTA (escanea 1 vez por fuente) ────────────────────────
def index_txt_codes_by_acta_source(
    txt_path: str,
    actas: List[int],
    valid_ext: str = ".pdf",
    include_text_number: bool = True,
    phase_filter: Optional[str] = None,  # sólo dt
    require_acta_in_path: bool = True
) -> Tuple[Dict[int, Set[str]], Dict[int, Dict[str,int]], Dict[int, Dict[str, List[str]]]]:
    """
    Retorna:
      - codes_by_acta: {acta -> set(códigos)}
      - dups_by_acta:  {acta -> {codigo: conteo>1}}
      - routes_by_acta:{acta -> {codigo: [rutas...]}}
    """
    codes_by_acta: Dict[int, List[str]] = {n: [] for n in actas}
    routes_by_acta: Dict[int, DefaultDict[str, List[str]]] = {n: defaultdict(list) for n in actas}
    patterns = {n: _acta_regex(n, include_text=include_text_number) for n in actas}
    valid_ext = valid_ext.lower()

    with open(txt_path, "r", encoding="utf-8", errors="ignore") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or not line.lower().endswith(valid_ext):
                continue
            low = _normalize_text(line)
            if phase_filter is not None and not _phase_matches(low, phase_filter):
                continue

            # probar contra cada ACTA
            matched_any = False
            for n, pat in patterns.items():
                if require_acta_in_path and not pat.search(low):
                    continue
                matched_any = True
                base = os.path.splitext(os.path.basename(line))[0]
                code = _normalize_code(base)
                if not code:
                    continue
                codes_by_acta[n].append(code)
                routes_by_acta[n][code].append(line)
            # Si no coincide con ninguna ACTA, se ignora
            if require_acta_in_path and not matched_any:
                continue

    # duplicados
    dups_by_acta: Dict[int, Dict[str,int]] = {}
    codes_set_by_acta: Dict[int, Set[str]] = {}
    for n in actas:
        lst = codes_by_acta.get(n, [])
        dups_by_acta[n] = {k:v for k,v in Counter(lst).items() if v>1}
        codes_set_by_acta[n] = set(routes_by_acta[n].keys())

    return codes_set_by_acta, dups_by_acta, {n: dict(routes_by_acta[n]) for n in actas}


# ── Helpers de salida ─────────────────────────────────────────────────────────
def _join_paths(paths: List[str]) -> str:
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


# ── Detección de ACTAS en nombre del archivo ──────────────────────────────────
def _parse_actas_from_filename(p: str) -> List[int]:
    name = os.path.basename(p)
    # Busca 'ACTA ' seguido de una lista de números con separadores , ; y/o espacios
    # Ej: "ACTA 17,22,24,26,28,29,30,31 FUID ..."
    m = re.search(r"\bacta\s+([0-9,\s\-yY]+)", name, flags=re.IGNORECASE)
    if not m:
        return []
    nums = re.findall(r"\d+", m.group(1))
    return [int(x) for x in nums if x.isdigit()]


# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(OUTPUT_DIR)

    # 0) Determinar ACTAS a procesar
    actas: List[int] = []
    if ACTA_LIST:
        actas = sorted(set(int(x) for x in ACTA_LIST))
    elif AUTO_PARSE_ACTAS_FROM_CSV_NAME:
        actas = sorted(set(_parse_actas_from_filename(CSV_OR_XLSX_PATH)))
    if not actas:
        raise ValueError("No se pudo determinar la lista de ACTAS. Define ACTA_LIST o activa AUTO_PARSE_ACTAS_FROM_CSV_NAME con un nombre de archivo que contenga 'ACTA ...'.")

    # 1) Cargar códigos del CSV/Excel (universo)
    csv_codes, csv_dups = load_csv_or_xlsx_codes(CSV_OR_XLSX_PATH)

    # 2) Indexar TXT Difusión y TXT dt por ACTA (una pasada por archivo)
    dif_codes_by_acta, dif_dups_by_acta, dif_routes_by_acta = index_txt_codes_by_acta_source(
        TXT_DIFUSION_PATH, actas, VALID_EXT, MATCH_TEXTO_NUMERO, phase_filter=None, require_acta_in_path=True
    )
    dt_codes_by_acta,  dt_dups_by_acta,  dt_routes_by_acta  = index_txt_codes_by_acta_source(
        TXT_DT_PATH,       actas, VALID_EXT, MATCH_TEXTO_NUMERO, phase_filter=DT_PHASE, require_acta_in_path=True
    )

    resumen_rows = []

    # 3) Procesar cada ACTA
    for n in actas:
        dif_codes = dif_codes_by_acta.get(n, set())
        dt_codes  = dt_codes_by_acta.get(n, set())
        dif_routes = dif_routes_by_acta.get(n, {})
        dt_routes  = dt_routes_by_acta.get(n, {})

        coinciden_todos        = sorted(csv_codes & dif_codes & dt_codes)
        csv_dif_match          = sorted(csv_codes & dif_codes)
        csv_dt_match           = sorted(csv_codes & dt_codes)
        falta_en_dif_desde_csv = sorted(csv_codes - dif_codes)
        falta_en_csv_desde_dif = sorted(dif_codes - csv_codes)
        dt_no_en_difusion      = sorted(dt_codes - dif_codes)
        dt_no_en_csv           = sorted(dt_codes - csv_codes)

        base = f"acta_{n}_phase_{DT_PHASE or 'sin_fase'}_{ts}"

        p_all = _write_codes_with_routes(out_dir, f"coinciden_en_las_tres_fuentes_{base}.csv",
                                         coinciden_todos, dif_routes, dt_routes)
        p_cd  = _write_codes_with_routes(out_dir, f"coinciden_csv_difusion_{base}.csv",
                                         csv_dif_match, dif_routes, dt_routes, add_has_dt=True)
        p_ct  = _write_codes_with_routes(out_dir, f"coinciden_csv_dt_{base}.csv",
                                         csv_dt_match, dif_routes, dt_routes, add_has_dt=True)
        p_fd  = _write_codes_with_routes(out_dir, f"falta_en_difusion_desde_csv_{base}.csv",
                                         falta_en_dif_desde_csv, dif_routes, dt_routes, add_has_dt=True)
        p_fc  = _write_codes_with_routes(out_dir, f"falta_en_csv_desde_difusion_{base}.csv",
                                         falta_en_csv_desde_dif, dif_routes, dt_routes, add_has_dt=True)
        p_dn  = _write_codes_with_routes(out_dir, f"dt_no_en_difusion_{base}.csv",
                                         dt_no_en_difusion, dif_routes, dt_routes)
        p_dx  = _write_codes_with_routes(out_dir, f"dt_no_en_csv_{base}.csv",
                                         dt_no_en_csv, dif_routes, dt_routes)

        resumen_rows.append({
            "acta_numero": n,
            "dt_phase": DT_PHASE or "",
            "patron_acta": _acta_regex(n, MATCH_TEXTO_NUMERO).pattern,
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
            "dif_duplicados": sum(dif_dups_by_acta.get(n, {}).values()),
            "dt_duplicados":  sum(dt_dups_by_acta.get(n, {}).values()),
            "ext_validada": VALID_EXT,
            "match_texto_numero": MATCH_TEXTO_NUMERO,
            "paths_coinciden_tres": str(p_all),
            "paths_coinciden_csv_difusion": str(p_cd),
            "paths_coinciden_csv_dt": str(p_ct),
        })

        # Logs por ACTA
        print(f"\n✓ ACTA {n} | CSV únicos: {len(csv_codes)} | Dif únicos: {len(dif_codes)} | dt únicos: {len(dt_codes)}")
        print(f"  Coinciden 3 fuentes: {len(coinciden_todos)} -> {p_all}")
        print(f"  Coinciden CSV<->Difusión: {len(csv_dif_match)} -> {p_cd}")
        print(f"  Coinciden CSV<->dt: {len(csv_dt_match)} -> {p_ct}")
        print(f"  Falta en Difusión (desde CSV): {len(falta_en_dif_desde_csv)} -> {p_fd}")
        print(f"  Falta en CSV (desde Difusión): {len(falta_en_csv_desde_dif)} -> {p_fc}")
        print(f"  dt no en Difusión: {len(dt_no_en_difusion)} -> {p_dn}")
        print(f"  dt no en CSV: {len(dt_no_en_csv)} -> {p_dx}")

    # 4) Resumen global
    df_resumen = pd.DataFrame(resumen_rows).sort_values("acta_numero")
    tag = f"actas_{'-'.join(str(a) for a in actas)}_phase_{DT_PHASE or 'sin_fase'}_{ts}"
    p_res = out_dir / f"resumen_global_{_sanitize_for_filename(tag)}.csv"
    df_resumen.to_csv(p_res, index=False, encoding="utf-8-sig")

    print(f"\n✔ Cruce multi-ACTA finalizado. Resumen global -> {p_res}")
    if DEBUG:
        print("  ACTAS procesadas:", actas)
        print("  CSV dups (muestra):", dict(list(csv_dups.items())[:5]))

if __name__ == "__main__":
    main()
