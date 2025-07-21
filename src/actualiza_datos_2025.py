#!/usr/bin/env python3
"""
Actualiza los datos de importación de hidrocarburos con los registros de 2025.

Autor: Edwin Ortega y Esteban Zambrano
Curso: CC3084 Data Science UVG
Semestre II 2025
"""

import argparse
import os
import pandas as pd

def normaliza_col(col: str) -> str:
    """Normaliza nombres de columna."""
    if not isinstance(col, str):
        return col
    col = col.replace("\n", " ")
    col = " ".join(col.strip().split())
    return col.lower()


def detecta_header(df: pd.DataFrame) -> int:
    """Detecta la fila que contiene 'Fecha' para usar como encabezado."""
    for i, row in df.iterrows():
        if row.astype(str).str.strip().str.lower().eq("fecha").any():
            return i
    raise ValueError("No se encontró fila de encabezado con la palabra 'Fecha'.")


def carga_y_limpia_importacion(xls: pd.ExcelFile, sheet: str) -> pd.DataFrame:
    """Carga la hoja IMPORTACION, limpia nombres y columnas, transforma diesel total."""
    raw = xls.parse(sheet, header=None, dtype=str)
    header_row = detecta_header(raw)
    df = xls.parse(sheet, header=header_row)
    df.columns = [normaliza_col(c) for c in df.columns]

    requeridas = [
        "fecha", 
        "gasolina regular", 
        "gasolina superior", 
        "diesel bajo azufre", 
        "diesel ultra bajo azufre"
    ]
    faltantes = [c for c in requeridas if c not in df.columns]
    if faltantes:
        raise KeyError(f"En hoja '{sheet}' faltan columnas requeridas: {faltantes}")

    df = df[requeridas].copy()
    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
    df = df[df["fecha"].notna()].copy()
    df = df.sort_values("fecha").reset_index(drop=True)

    for col in requeridas[1:]:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(",", "", regex=False)
            .str.replace(" ", "", regex=False)
            .str.replace("--", "", regex=False)
        )
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Combinar diesel bajo + ultra bajo azufre
    df["diesel total"] = df["diesel bajo azufre"].fillna(0) + df["diesel ultra bajo azufre"].fillna(0)
    df = df[["fecha", "gasolina regular", "gasolina superior", "diesel total"]].copy()
    df = df.rename(columns={
        "gasolina regular": "Regular_Imp",
        "gasolina superior": "Superior_Imp",
        "diesel total": "Diesel_Imp"
    })
    return df


def main():
    parser = argparse.ArgumentParser(description="Actualiza CSV limpio con datos de 2025.")
    parser.add_argument("--excel", required=True, help="Ruta al archivo Excel nuevo.")
    parser.add_argument("--outdir", default=".", help="Directorio donde guardar CSV.")
    args = parser.parse_args()

    os.makedirs(args.outdir, exist_ok=True)

    xls = pd.ExcelFile(args.excel)
    df = carga_y_limpia_importacion(xls, "IMPORTACION")

    out_path = os.path.join(args.outdir, "importacion_2025_actualizado.csv")
    df.to_csv(out_path, index=False)
    print(f"Archivo exportado a: {out_path}")


if __name__ == "__main__":
    main()
