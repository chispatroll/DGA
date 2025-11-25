import os
import zipfile
import pandas as pd
from pathlib import Path
import warnings
import re

# --- CONFIGURACIÓN ---
RUTA_DATOS_RAW = Path(r"D:\12_DGA\datos\cndc")
RUTA_SALIDA = Path("data/SE_processed")
KEYWORD_HEADER = "RETIROS (MWh)"

# Ignorar advertencias de estilo de Excel
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")


def procesar_excel_individual(f_handle, fecha_carpeta):
    """
    Lee un archivo Excel abierto, encuentra la tabla, limpia y retorna un DataFrame.
    Retorna None si no encuentra el patrón.
    """
    # 1. Cargar 'sopa de datos' (limitamos a leer solo lo necesario si fuera posible, pero read_excel lee todo)
    # header=None para detectar manualmente
    df_raw = pd.read_excel(f_handle, header=None, engine="openpyxl")

    # 2. Buscar la fila del Ancla (Optimizada con generador)
    # Buscamos en las primeras 50 filas. Retorna el índice o None.
    fila_header = next(
        (
            idx
            for idx, row in df_raw.head(50).iterrows()
            if row.astype(str).str.contains(KEYWORD_HEADER, regex=False).any()
        ),
        None,
    )

    if fila_header is None:
        return None

    # 3. Cortar y Ascender Encabezados (Slicing)
    df_data = df_raw.iloc[fila_header + 1 :].copy()
    df_data.columns = df_raw.iloc[fila_header]

    # 4. Normalización de Columnas
    # Renombramos la columna clave para poder trabajarla vectorizadamente
    # Asumimos que la columna que se llama 'RETIROS (MWh)' contiene los nombres
    if KEYWORD_HEADER in df_data.columns:
        df_data.rename(columns={KEYWORD_HEADER: "Subestacion"}, inplace=True)
    else:
        # Fallback: si por alguna razón el nombre no coincide exactamente, usamos la columna 0
        df_data.rename(columns={df_data.columns[0]: "Subestacion"}, inplace=True)

    # 5. LIMPIEZA VECTORIZADA (Aquí está la velocidad) ⚡
    # a. Convertir a string y quitar espacios
    df_data["Subestacion"] = df_data["Subestacion"].astype(str).str.strip()

    # b. Filtros booleanos (Mantiene filas válidas)
    # - Longitud mayor a 2
    # - Que no contenga "TOTAL"
    # - Que no sea 'nan'
    mask = (
        (df_data["Subestacion"].str.len() > 2)
        & (~df_data["Subestacion"].str.upper().str.contains("TOTAL", na=False))
        & (df_data["Subestacion"] != "nan")
    )
    df_clean = df_data[mask].copy()

    # 6. Inyección de Metadatos
    df_clean["Fecha"] = fecha_carpeta  # Asignación masiva (broadcasting)

    return df_clean


def procesar_archivos():
    RUTA_SALIDA.mkdir(parents=True, exist_ok=True)
    print(f"🚀 Iniciando procesamiento masivo en: {RUTA_DATOS_RAW}")

    all_dataframes = []  # Lista plana para acumular todo (más rápido que dict)

    # Recorrer sistema de archivos
    for root, _, files in os.walk(RUTA_DATOS_RAW):
        # Filtramos lista de zips primero
        zips = [f for f in files if f.lower().endswith(".zip")]

        for file in zips:
            ruta_zip = Path(root) / file
            fecha_str = ruta_zip.parent.name  # Fecha desde carpeta padre

            try:
                with zipfile.ZipFile(ruta_zip, "r") as z:
                    # Filtrar excels dentro del zip
                    excels = [f for f in z.namelist() if f.endswith((".xlsx", ".xls"))]

                    for excel_name in excels:
                        with z.open(excel_name) as f:
                            df_procesado = procesar_excel_individual(f, fecha_str)

                            if df_procesado is not None and not df_procesado.empty:
                                all_dataframes.append(df_procesado)
                                # print(f"  -> Leído: {excel_name} ({len(df_procesado)} filas)")

            except Exception as e:
                print(f"❌ Error corrupto o ilegible en {file}: {e}")

    # --- CONSOLIDACIÓN Y GUARDADO ---
    if not all_dataframes:
        print("⚠️ No se encontraron datos para procesar.")
        return

    print(f"\n📦 Consolidando {len(all_dataframes)} fragmentos de datos...")

    # 1. Gran concatenación (Mucho más rápido que concat en bucle)
    df_total = pd.concat(all_dataframes, ignore_index=True)

    # 2. Conversión de fecha global
    df_total["Fecha_dt"] = pd.to_datetime(df_total["Fecha"], errors="coerce")
    df_total.sort_values("Fecha_dt", inplace=True)
    df_total.drop(columns=["Fecha_dt"], inplace=True)

    # 3. Agrupación y Exportación (La magia de GroupBy)
    # Esto reemplaza tu lógica de diccionarios manuales
    grupos = df_total.groupby("Subestacion")

    print(f"💾 Guardando archivos para {len(grupos)} subestaciones únicas...")

    for subestacion, df_sub in grupos:
        try:
            # Limpieza de nombre de archivo (Regex para seguridad)
            safe_name = re.sub(r"[^\w\s-]", "", subestacion).strip()
            if not safe_name:
                continue

            ruta_csv = RUTA_SALIDA / f"{safe_name}.csv"

            # Exportar
            df_sub.to_csv(ruta_csv, index=False, encoding="utf-8-sig")

        except Exception as e:
            print(f"❌ Error guardando {subestacion}: {e}")

    print("\n✨ ¡Proceso completado con éxito!")


if __name__ == "__main__":
    procesar_archivos()
