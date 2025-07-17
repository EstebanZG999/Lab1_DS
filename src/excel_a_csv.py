#!/usr/bin/env python3
"""
Convierte el archivo Excel histórico de comercialización de hidrocarburos
en CSVs limpios (importación, consumo y combinado) para el Lab 01 de Series de Tiempo.

Autor: Edwin Ortega y Esteban Zambrano
Curso: CC3084 Data Science UVG
Semestre II 2025
"""

import argparse
import os
import pandas as pd


# ------------------------------------------------------------
# Utilidades
# ------------------------------------------------------------
def normaliza_col(col: str) -> str:
    """Normaliza nombres de columna: minúsculas, quita espacios extremos,
    reemplaza saltos de línea por espacio, colapsa espacios múltiples."""
    if not isinstance(col, str):
        return col
    col = col.replace("\n", " ")
    col = " ".join(col.strip().split())
    return col.lower()


def detecta_header(df: pd.DataFrame) -> int:
    """
    Dado un DataFrame crudo (con filas basura arriba), devuelve el índice de fila
    donde aparece la palabra 'Fecha' en alguna celda (ignorando mayúsculas/minúsculas).
    Si no la encuentra, lanza ValueError.
    """
    for i, row in df.iterrows():
        if row.astype(str).str.strip().str.lower().eq("fecha").any():
            return i
    raise ValueError("No se encontró fila de encabezados con la palabra 'Fecha'.")


def carga_y_limpia_hoja(xls: pd.ExcelFile, sheet: str) -> pd.DataFrame:
    """
    Carga una hoja del Excel, detecta encabezado, limpia nombres de columna,
    filtra columnas de interés y devuelve DataFrame con:
    fecha, gasolina regular, gasolina superior, diesel alto azufre.
    """
    # Carga cruda
    raw = xls.parse(sheet, header=None, dtype=str)

    # Detectar fila encabezado
    header_row = detecta_header(raw)

    # Leer de nuevo usando esa fila como header
    df = xls.parse(sheet, header=header_row)

    # Normalizar nombres
    df.columns = [normaliza_col(c) for c in df.columns]

    # Renombre defensivo para variaciones frecuentes
    renmap = {}
    for c in df.columns:
        c_norm = c
        if c_norm.startswith("gasolina super"):
            renmap[c] = "gasolina superior"
        elif c_norm.startswith("gasolina s") and "rior" in c_norm:
            renmap[c] = "gasolina superior"
        elif c_norm.startswith("gasolina regular"):
            renmap[c] = "gasolina regular"
        elif c_norm.startswith("diesel alto"):
            renmap[c] = "diesel alto azufre"

    if renmap:
        df = df.rename(columns=renmap)

    # Verificar que existan las columnas requeridas
    requeridas = ["fecha", "gasolina regular", "gasolina superior", "diesel alto azufre"]
    faltantes = [c for c in requeridas if c not in df.columns]
    if faltantes:
        raise KeyError(f"En hoja '{sheet}' faltan columnas requeridas: {faltantes}")

    # Filtrar solo las columnas requeridas
    df = df[requeridas].copy()

    # Convertir fecha
    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")

    # Eliminar filas sin fecha válida (notas de fuente, totales, etc.)
    df = df[df["fecha"].notna()].copy()

    # Asegurar orden cronológico
    df = df.sort_values("fecha").reset_index(drop=True)

    # Convertir valores numéricos (quitar comas, espacios, strings tipo '--')
    for col in requeridas[1:]:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(",", "", regex=False)
            .str.replace(" ", "", regex=False)
            .str.replace("--", "", regex=False)
        )
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def wide_to_long(df: pd.DataFrame, origen: str) -> pd.DataFrame:
    """
    Convierte un DataFrame ancho (fecha + 3 columnas) a formato largo,
    agregando columna 'tipo' (regular/superior/diesel) y 'origen' (importacion/consumo).
    """
    long_df = df.melt(id_vars="fecha", var_name="producto", value_name="barriles")
    long_df["producto"] = (
        long_df["producto"].str.replace("gasolina regular", "regular", regex=False)
        .str.replace("gasolina superior", "superior", regex=False)
        .str.replace("diesel alto azufre", "diesel", regex=False)
    )
    long_df["origen"] = origen
    return long_df


# ------------------------------------------------------------
# Flujo principal
# ------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Convierte Excel de combustibles a CSVs limpios.")
    parser.add_argument("--excel", required=True, help="Ruta al archivo Excel fuente.")
    parser.add_argument("--outdir", default=".", help="Directorio de salida (se crea si no existe).")
    args = parser.parse_args()

    os.makedirs(args.outdir, exist_ok=True)

    xls = pd.ExcelFile(args.excel)

    # Cargar y limpiar ambas hojas
    imp = carga_y_limpia_hoja(xls, "IMPORTACION")
    con = carga_y_limpia_hoja(xls, "CONSUMO")

    # Exportar individuales
    imp_out = os.path.join(args.outdir, "importacion_combustibles.csv")
    con_out = os.path.join(args.outdir, "consumo_combustibles.csv")
    imp.to_csv(imp_out, index=False)
    con.to_csv(con_out, index=False)

    # Merge por fecha
    combined = pd.merge(
        imp.rename(
            columns={
                "gasolina regular": "Regular_Imp",
                "gasolina superior": "Superior_Imp",
                "diesel alto azufre": "Diesel_Imp",
            }
        ),
        con.rename(
            columns={
                "gasolina regular": "Regular_Con",
                "gasolina superior": "Superior_Con",
                "diesel alto azufre": "Diesel_Con",
            }
        ),
        on="fecha",
        how="outer",
    ).sort_values("fecha")

    comb_out = os.path.join(args.outdir, "Series_de_Tiempo_Combustibles.csv")
    combined.to_csv(comb_out, index=False)


if __name__ == "__main__":
    main()
