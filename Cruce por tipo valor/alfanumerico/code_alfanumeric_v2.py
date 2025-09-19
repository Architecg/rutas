# -*- coding: utf-8 -*-
"""
Cruce CSV (CAJA alfanumérica + CARPETA=nombre PDF) ↔ Difusión ↔ dt por ACTA.
- CSV: columnas 'caja' y 'carpeta' (tolerante a encabezados similares).
- TXT: filtra por ACTA flexible y extensión .pdf, y además exige que el código
  de CAJA del CSV aparezca como segmento de directorio en la ruta.
- Compara pares (CAJA, CARPETA) normalizados.
- Exporta: coincidencias, faltantes por fuente (Dif/dt) y resumen + rutas.

Compatible con Python 3.8+
"""

import os, re, unicodedata
from pathlib import Path
from datetime import datetime
from collections import Counter, defaultdict
from typing import Tuple, Set, Dict, List, Optional, DefaultDict
import pandas as pd

# ── CONFIG ────────────────────────────────────────────────────────────────────
CSV_OR_XLSX_PATH   = r"C:\Users\juans\Downloads\cd\rta\fase 2\ACTA 19 - FUID TRD 2017 CQTA.csv"  # o .xlsx
TXT_DIFUSION_PATH  = r"C:\Users\juans\Downloads\cd\resources\Difusion.txt"
TXT_DT_PATH        = r"C:\Users\juans\Downloads\cd\resources\dt.txt"

ACTA_NUMERO        = 19
DT_PHASE           = "fase2"          # "fase1" | "fase2" | "digitalizacion2" | None
VALID_EXT          = ".pdf"
REQUIRE_ACTA_IN_PATH = True
OUTPUT_DIR         = r"C:\Users\juans\Downloads\cd\answer\fase 2\ACTA 19 - COMISIÓN QUINTA - TRD 2017 - 2021\ACTA 19 - FUID TRD 2017 CQTA"

# Aliases de columnas aceptadas
CSV_COL_CARPETA_CANDS = ["carpeta", "archivo", "nombre_pdf", "pdf", "archivo pdf", "archivo_pdf", "nombre"]
CSV_COL_CAJA_CANDS    = ["caja", "box", "codigo_caja", "codigo caja", "id_caja", "ubicacion_caja"]

DEBUG = True
# ──────────────────────────────────────────────────────────────────────────────


# ====== Normalización y patrones =============================================
def _normalize_text(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s.lower()

def _canon_header(s: str) -> str:
    s = _normalize_text(s)
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_")

def _canon_pdfname(s: str) -> str:
    """basename en minúsculas, colapsa espacios y asegura '.pdf'."""
    if s is None: return ""
    s = str(s).strip().strip('"').strip("'")
    base = os.path.basename(s)
    if not base: return ""
    base = re.sub(r"\s+", " ", base).strip().lower()
    if not base.endswith(".pdf"):
        base += ".pdf"
    return base

def _canon_box(s: str) -> str:
    """CAJA alfanumérica: mayúsculas, solo A-Z0-9, sin guiones/espacios."""
    if s is None: return ""
    s = str(s).strip().upper()
    s = re.sub(r"[^A-Z0-9]+", "", s)
    return s

def _split_path_norm(path_str: str) -> List[str]:
    """Segmentos normalizados de ruta (sin acentos, minúsculas, espacios colapsados)."""
    segs = re.split(r"[\\/]+", path_str.strip())
    out = []
    for seg in segs:
        if not seg: continue
        t = _normalize_text(seg)
        t = re.sub(r"\s+", " ", t).strip()
        if t: out.append(t)
    return out

def _path_has_box_segment(path_str: str, box_norm: str) -> bool:
    """
    ¿Algún segmento de la ruta coincide con la CAJA del CSV?
    - Comparamos en forma 'solo alfanumérico mayúsculas' para ambos.
    - Requiere igualdad total de segmento, no substring.
    """
    if not box_norm: return False
    for raw in re.split(r"[\\/]+", path_str.strip()):
        raw_clean = re.sub(r"[^A-Za-z0-9]+", "", raw).upper()
        if raw_clean == box_norm:
            return True
    return False

def _number_to_spanish_variants(n: int) -> List[str]:
    u = {0:"cero",1:"uno",2:"dos",3:"tres",4:"cuatro",5:"cinco",
         6:"seis",7:"siete",8:"ocho",9:"nueve",10:"diez",
         11:"once",12:"doce",13:"trece",14:"catorce",15:"quince",
         16:"dieciseis",17:"diecisiete",18:"dieciocho",19:"diecinueve",
         20:"veinte",21:"veintiuno",22:"veintidos",23:"veintitres",24:"veinticuatro",
         25:"veinticinco",26:"veintiseis",27:"veintisiete",28:"veintiocho",29:"veintinueve"}
    d = {30:"treinta",40:"cuarenta",50:"cincuenta",60:"sesenta",70:"setenta",80:"ochenta",90:"noventa"}
    variants = set()
    if n in u: variants.add(u[n])
    elif n in d: variants.add(d[n])
    elif 31 <= n <= 99: variants.add(f"{d[(n//10)*10]} y {u[n%10]}")
    elif n == 100: variants.add("cien")
    elif 101 <= n <= 199:
        for v in _number_to_spanish_variants(n-100): variants.add(f"ciento {v}")
    out = set()
    for v in variants:
        out.add(_normalize_text(v.replace("dieciseis","dieciséis")
                                 .replace("veintidos","veintidós")
                                 .replace("veintitres","veintitrés")
                                 .replace("veintiseis","veintiséis")))
    return sorted(out)

def _acta_regex(n: int, include_text: bool = True) -> re.Pattern:
    """Reconoce: ACTA 32 | ACTA N° 32 | ACTA Nº 32 | ACTA No 32 | ACTA No. 32 | ACTA N 32 | ACTA Nro 32 | ACTA treinta y dos"""
    num = re.escape(str(n))
    pref = r"(?:n(?:o|ro|\.|)|nº|n°)?"
    alts = []
    if include_text:
        for t in _number_to_spanish_variants(n):
            alts.append(fr"{re.escape(t)}\b")
    partes = [fr"\bacta\s*{pref}\s*0*{num}\b(?!\d)"]
    if alts:
        partes.append(fr"\bacta\s*{pref}\s*(?:{'|'.join(alts)})")
    return re.compile("|".join(partes), flags=re.IGNORECASE)

PHASE_TOKENS = {
    "fase1": ["discos duros", "discos_duros"],
    "fase2": ["fase 2", "fase_2"],
    "digitalizacion2": ["digitalizacion 2", "digitalizacion_2", "digitalización 2"]
}
def _phase_matches(line_norm: str, phase: Optional[str]) -> bool:
    if not phase: return True
    tokens = PHASE_TOKENS.get(phase.lower().strip(), [])
    return any(tok in line_norm for tok in tokens)


# ====== CSV: leer pares (CAJA, CARPETA) ======================================
def _pick_col(df: pd.DataFrame, cands: List[str]) -> Optional[str]:
    norm = {_canon_header(c): c for c in df.columns}
    for cand in cands:
        k = _canon_header(cand)
        if k in norm: return norm[k]
    return None

def load_csv_pairs(csv_or_xlsx_path: str) -> Tuple[Set[Tuple[str,str]], Dict[str,int]]:
    p = Path(csv_or_xlsx_path)
    if not p.exists():
        raise FileNotFoundError(f"No existe: {csv_or_xlsx_path}")

    # leer
    try:
        if p.suffix.lower() in {".xlsx", ".xls"}:
            df = pd.read_excel(csv_or_xlsx_path, dtype="string")
        else:
            df = pd.read_csv(csv_or_xlsx_path, sep=None, engine="python", dtype="string", encoding="utf-8-sig")
    except Exception:
        # fallback sin encabezados
        df = pd.read_csv(csv_or_xlsx_path, header=None, dtype="string", encoding="utf-8-sig", engine="python")

    # identificar columnas
    col_carpeta = _pick_col(df, CSV_COL_CARPETA_CANDS)
    col_caja    = _pick_col(df, CSV_COL_CAJA_CANDS)

    if not col_carpeta or not col_caja:
        raise ValueError(f"No se hallaron columnas requeridas. Leídas: {list(df.columns)} | "
                         f"esperaba carpeta∈{CSV_COL_CARPETA_CANDS}, caja∈{CSV_COL_CAJA_CANDS}")

    serie_pdf  = df[col_carpeta].astype("string").map(_canon_pdfname)
    serie_caja = df[col_caja].astype("string").map(_canon_box)

    pairs: List[Tuple[str,str]] = []
    for caja, pdf in zip(serie_caja.tolist(), serie_pdf.tolist()):
        if caja and pdf:
            pairs.append((caja, pdf))

    dup = Counter(pairs)
    dup = {f"{k[0]}|{k[1]}": v for k, v in dup.items() if v > 1}
    return set(pairs), dup


# ====== TXT → pares (CAJA, CARPETA) ==========================================
def load_txt_pairs_by_acta(
    txt_path: str,
    acta_number: int,
    valid_ext: str,
    require_acta_in_path: bool,
    caja_whitelist: Set[str],
    phase_filter: Optional[str] = None  # solo para dt
) -> Tuple[Set[Tuple[str,str]], Dict[str,int], Dict[str,List[str]]]:
    """
    Retorna:
      - set_pairs = {(CAJA, CARPETA)}
      - dups = {"CAJA|CARPETA": n>1}
      - routes = {"CAJA|CARPETA": [rutas]}
    Solo se generan pares si el path:
      * pasa filtros (extensión, ACTA, fase),
      * contiene alguna CAJA (de CSV) como segmento,
      * y su basename .pdf es CARPETA.
    """
    pat_acta = _acta_regex(acta_number)
    valid_ext = (valid_ext or ".pdf").lower()

    pairs: List[Tuple[str,str]] = []
    routes: DefaultDict[str, List[str]] = defaultdict(list)

    with open(txt_path, "r", encoding="utf-8", errors="ignore") as f:
        for raw in f:
            line = raw.strip()
            if not line: continue
            low = _normalize_text(line)
            if not low.endswith(valid_ext): continue
            if phase_filter is not None and not _phase_matches(low, phase_filter): continue
            if require_acta_in_path and not pat_acta.search(low): continue

            pdf_name = _canon_pdfname(line)
            if not pdf_name: continue

            # ¿Qué cajas del CSV aparecen como segmento?
            matched_boxes = []
            for seg in re.split(r"[\\/]+", line.strip()):
                seg_norm = re.sub(r"[^A-Za-z0-9]+", "", seg).upper()
                if seg_norm in caja_whitelist:
                    matched_boxes.append(seg_norm)

            if not matched_boxes:
                continue

            for caja in matched_boxes:
                key = (caja, pdf_name)
                pairs.append(key)
                routes[f"{caja}|{pdf_name}"].append(line)

    dup = Counter(pairs)
    dup = {f"{k[0]}|{k[1]}": v for k, v in dup.items() if v > 1}
    return set(pairs), dup, dict(routes)


# ====== Helpers de salida =====================================================
def _write_pairs_with_routes(
    out_dir: Path, fname: str,
    pairs: List[Tuple[str,str]],
    routes_dif: Dict[str, List[str]] = None,
    routes_dt:  Dict[str, List[str]] = None,
    add_has_dif: bool = False,
    add_has_dt:  bool = False
) -> Path:
    routes_dif = routes_dif or {}
    routes_dt  = routes_dt or {}

    def j(xs: List[str]) -> str: return " | ".join(xs) if xs else ""

    rows = []
    for caja, pdf in pairs:
        key = f"{caja}|{pdf}"
        dif_list = routes_dif.get(key, [])
        dt_list  = routes_dt.get(key, [])
        row = {
            "CAJA": caja,
            "CARPETA(pdf)": pdf,
            "routes_difusion": j(dif_list),
            "n_routes_difusion": len(dif_list),
            "routes_dt": j(dt_list),
            "n_routes_dt": len(dt_list),
        }
        if add_has_dif: row["has_difusion"] = bool(dif_list)
        if add_has_dt:  row["has_dt"] = bool(dt_list)
        rows.append(row)

    cols = ["CAJA","CARPETA(pdf)","routes_difusion","n_routes_difusion","routes_dt","n_routes_dt"]
    if add_has_dif: cols += ["has_difusion"]
    if add_has_dt:  cols += ["has_dt"]

    df = pd.DataFrame(rows, columns=cols)
    out_path = out_dir / fname
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    return out_path


def _summary_by_caja(set_pairs: Set[Tuple[str,str]]) -> Dict[str,int]:
    c = Counter(caja for (caja, _) in set_pairs)
    # retorna ordenado alfabéticamente
    return dict(sorted(c.items()))


# ====== MAIN ==================================================================
def main():
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = Path(OUTPUT_DIR)

    # 1) CSV → pares y whitelist de cajas
    csv_pairs, csv_dups = load_csv_pairs(CSV_OR_XLSX_PATH)
    caja_whitelist = {caja for (caja, _) in csv_pairs}

    # 2) TXT Difusión y TXT dt → pares detectados (solo si hay caja∈CSV en la ruta)
    dif_pairs, dif_dups, dif_routes = load_txt_pairs_by_acta(
        TXT_DIFUSION_PATH, ACTA_NUMERO, VALID_EXT, REQUIRE_ACTA_IN_PATH, caja_whitelist, phase_filter=None
    )
    dt_pairs, dt_dups, dt_routes = load_txt_pairs_by_acta(
        TXT_DT_PATH, ACTA_NUMERO, VALID_EXT, REQUIRE_ACTA_IN_PATH, caja_whitelist, phase_filter=DT_PHASE
    )

    # 3) Cruces
    coinciden_tres  = sorted(csv_pairs & dif_pairs & dt_pairs)
    csv_dif_match   = sorted(csv_pairs & dif_pairs)
    csv_dt_match    = sorted(csv_pairs & dt_pairs)

    faltan_en_dif   = sorted(csv_pairs - dif_pairs)   # Está en CSV pero NO en Difusión
    faltan_en_dt    = sorted(csv_pairs - dt_pairs)    # Está en CSV pero NO en dt
    faltan_en_csv_d = sorted(dif_pairs - csv_pairs)   # Está en Difusión pero NO en CSV
    faltan_en_csv_t = sorted(dt_pairs - csv_pairs)    # Está en dt pero NO en CSV

    # 4) Guardar con rutas
    base = f"acta_{ACTA_NUMERO}_{DT_PHASE or 'sin_fase'}_{ts}"

    p_all = _write_pairs_with_routes(out, f"coinciden_tres_fuentes_{base}.csv",
                                     coinciden_tres, dif_routes, dt_routes)
    p_cd  = _write_pairs_with_routes(out, f"coinciden_csv_difusion_{base}.csv",
                                     csv_dif_match, dif_routes, dt_routes, add_has_dt=True)
    p_ct  = _write_pairs_with_routes(out, f"coinciden_csv_dt_{base}.csv",
                                     csv_dt_match, dif_routes, dt_routes, add_has_dif=True)
    p_fd  = _write_pairs_with_routes(out, f"faltan_en_difusion_desde_csv_{base}.csv",
                                     faltan_en_dif, dif_routes, dt_routes, add_has_dt=True)
    p_ft  = _write_pairs_with_routes(out, f"faltan_en_dt_desde_csv_{base}.csv",
                                     faltan_en_dt, dif_routes, dt_routes, add_has_dif=True)
    p_fcd = _write_pairs_with_routes(out, f"faltan_en_csv_desde_difusion_{base}.csv",
                                     faltan_en_csv_d, dif_routes, dt_routes, add_has_dt=True)
    p_fct = _write_pairs_with_routes(out, f"faltan_en_csv_desde_dt_{base}.csv",
                                     faltan_en_csv_t, dif_routes, dt_routes, add_has_dif=True)

    # 5) Resumen
    resumen = pd.DataFrame([{
        "acta_numero": ACTA_NUMERO,
        "dt_phase": DT_PHASE or "",
        "csv_total_pares_unicos": len(csv_pairs),
        "dif_total_pares_unicos": len(dif_pairs),
        "dt_total_pares_unicos": len(dt_pairs),
        "coinciden_tres": len(coinciden_tres),
        "coinciden_csv_difusion": len(csv_dif_match),
        "coinciden_csv_dt": len(csv_dt_match),
        "faltan_en_difusion_desde_csv": len(faltan_en_dif),
        "faltan_en_dt_desde_csv": len(faltan_en_dt),
        "faltan_en_csv_desde_difusion": len(faltan_en_csv_d),
        "faltan_en_csv_desde_dt": len(faltan_en_csv_t),
        "csv_duplicados": sum(csv_dups.values()),
        "dif_duplicados": sum(dif_dups.values()),
        "dt_duplicados": sum(dt_dups.values()),
        "ext_validada": VALID_EXT,
        "csv_por_caja": _summary_by_caja(csv_pairs),
        "dif_por_caja": _summary_by_caja(dif_pairs),
        "dt_por_caja": _summary_by_caja(dt_pairs),
    }])
    resumen.to_csv(out / f"resumen_{base}.csv", index=False, encoding="utf-8-sig")

    # 6) Logs
    print("✓ Cruce por ACTA con validación de CAJA+CARPETA completado")
    print(f"  ACTA {ACTA_NUMERO} | CSV pares únicos: {len(csv_pairs)} | Dif: {len(dif_pairs)} | dt: {len(dt_pairs)}")
    print(f"  Coinciden 3 fuentes: {len(coinciden_tres)} -> {p_all}")
    print(f"  Faltan en Difusión (desde CSV): {len(faltan_en_dif)} -> {p_fd}")
    print(f"  Faltan en dt (desde CSV): {len(faltan_en_dt)} -> {p_ft}")
    if DEBUG:
        print(f"  CSV dups (muestra): {dict(list(csv_dups.items())[:5])}")
        print(f"  Dif dups (muestra): {dict(list(dif_dups.items())[:5])}")
        print(f"  dt  dups (muestra): {dict(list(dt_dups.items())[:5])}")

if __name__ == "__main__":
    main()
