"""Sacar fila de  un documetno de excel"""
# ── Librerias ──────────────────────────────────────────────────────────────
from pathlib import Path
from typing import Optional
import pandas as pd
from IPython.display import display

# ── Parámetros ──────────────────────────────────────────────────────────────
RUTA_XLSX = Path(r"C:.....\fase 2\ACTA 19 - COMISIÓN QUINTA - TRD 2017 - 2021\ACTA 19 - FUID TRD 2021 CQTA.xlsx")
HOJA  = 0        # o el nombre de la hoja, ej. "FUID"
FILA_INICIO  = 17       # primera fila con datos (1-indexado)
COLUMNAS     = "L,O"
ULTIMA_FILA: Optional[int] = None  # ej. 200 si quieres cortar; None = hasta el final
# ──────────────────────────────────────────────────────────────────────────
def leer_columnas(ruta, hoja=0, columnas="E,H", fila_inicio=17, ultima_fila=None):
    # Salta exactamente las filas anteriores a fila_inicio
    skip = list(range(fila_inicio - 1))   # 0..16 para empezar en la 17
    df = pd.read_excel(
        ruta,
        sheet_name=hoja,
        header=None,            # no uses encabezado del archivo
        usecols=columnas,       
        skiprows=skip,
        engine="openpyxl",
        dtype="string"          # conserva ceros a la izquierda
    )

    # Cortar por última fila opcional
    if ultima_fila is not None:
        n = max(0, ultima_fila - fila_inicio + 1)
        df = df.iloc[:n, :]

    # Limpieza mínima
    df = df.replace(r"^\s*$", pd.NA, regex=True)  # strings vacíos -> NA
    df = df.dropna(how="all")                     # quita filas totalmente vacías

    # Nombres genéricos para que luego los cambies tú
    df.columns = [f"c{i+1}" for i in range(df.shape[1])]
    return df

# Uso
df = leer_columnas(RUTA_XLSX, hoja=HOJA, columnas=COLUMNAS, fila_inicio=FILA_INICIO)
print(f"✅ Filas: {len(df)} | Columnas: {df.shape[1]} (L, M y P desde la fila {FILA_INICIO})")
display(df.head())

# ── Asignar nombre de fila ─────────────────────────────────────
df = df.rename(columns={"c1":"caja", "c2": "carpeta"})

# ── Guardado opcional ───────────────────────────────────────────────────────
df.to_csv(r"C:\Users\juans\Downloads\cd\rta\fase 2\ACTA 19 - FUID TRD 2021 CQTA.csv", index=False, encoding="utf-8-sig")
