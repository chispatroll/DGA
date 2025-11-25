import os
import zipfile
import pandas as pd
from pathlib import Path
import warnings
import re
from datetime import timedelta

# --- CONFIGURACIÓN ---
RUTA_DATOS_RAW = Path(r"E:\13_DGA\Modelo_termico\data")
RUTA_SALIDA = Path("data/SE_processed_Tidy")  # Carpeta nueva para formato limpio
KEYWORD_HEADER = "RETIROS (MWh)"

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")


def procesar_excel_a_tidy(f_handle, fecha_carpeta):
    """
    Lee Excel, limpia y transforma DIRECTAMENTE a formato largo (Tidy).
    Integra Maxima y Hora Pico en cada fila horaria.
    """
    # 1. Lectura rápida
    df_raw = pd.read_excel(f_handle, header=None, engine="openpyxl")

    # 2. Búsqueda eficiente del header
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

    # 3. Slicing y Headers
    df_data = df_raw.iloc[fila_header + 1 :].copy()
    df_data.columns = df_raw.iloc[fila_header]

    # 4. Renombrar columnas críticas
    # Normalizamos nombres para que el código sea agnóstico a variaciones
    cols_map = {}
    for c in df_data.columns:
        c_str = str(c).strip()
        if c_str == KEYWORD_HEADER:
            cols_map[c] = "Subestacion"
        elif "MAXIMA" in c_str.upper():
            cols_map[c] = "Max_Diario_MW"
        elif c_str.upper() == "HORA":
            cols_map[c] = "Hora_Pico_Reg"

    df_data.rename(columns=cols_map, inplace=True)

    # Fallback si no encontró 'Subestacion'
    if "Subestacion" not in df_data.columns:
        df_data.rename(columns={df_data.columns[0]: "Subestacion"}, inplace=True)

    # 5. LIMPIEZA DE FILAS (El filtro de seguridad que añadimos antes)
    df_data["Subestacion"] = df_data["Subestacion"].astype(str).str.strip()
    mask = (
        (df_data["Subestacion"].str.len() > 2)
        & (df_data["Subestacion"].str.len() < 80)
        & (~df_data["Subestacion"].str.upper().str.contains("TOTAL", na=False))
        & (df_data["Subestacion"] != "nan")
    )
    df_clean = df_data[mask].copy()

    # 6. INYECCIÓN DE FECHA (Antes del Melt)
    df_clean["Fecha"] = fecha_carpeta

    # 7. TRANSFORMACIÓN A FORMATO LARGO (MELT) 🧠
    # Identificamos columnas que NO son horas (las que mantendremos fijas)
    id_vars = ["Fecha", "Subestacion", "Max_Diario_MW", "Hora_Pico_Reg"]

    # Las columnas de valor son todas las que NO están en id_vars
    # (Asumimos que el resto son las horas 01:00, 02:00...)
    value_vars = [c for c in df_clean.columns if c not in id_vars]

    df_melted = df_clean.melt(
        id_vars=id_vars,
        value_vars=value_vars,
        var_name="Hora_Str",  # La columna antigua (01:00) pasa a ser dato aquí
        value_name="MW",  # El valor de la celda pasa aquí
    )

    return df_melted


def corregir_fechas_y_tipos(df):
    """
    Función vectorizada para arreglar el problema de las '24:00' y crear Timestamp.
    """
    # 1. Limpieza de MW (Convertir a numérico, forzar errores a NaN)
    df["MW"] = pd.to_numeric(df["MW"], errors="coerce")

    # 2. Manejo de la hora "24:00"
    # Convertimos la hora string a algo manipulable
    # Creamos una bandera para las filas que son "24:00"
    es_24 = df["Hora_Str"].astype(str).str.contains("24:00")

    # Reemplazamos visualmente 24:00 por 00:00 para que Pandas no explote
    df.loc[es_24, "Hora_Str"] = "00:00"

    # 3. Creación del Timestamp Maestro
    # Convertimos Fecha (str) + Hora (str) a Datetime real
    # dayfirst=True ayuda si la fecha viene como DD/MM/YYYY, pero YYYY-MM-DD es seguro
    df["Timestamp"] = pd.to_datetime(
        df["Fecha"].astype(str) + " " + df["Hora_Str"].astype(str), errors="coerce"
    )

    # 4. CORRECCIÓN LÓGICA DE FECHA
    # Si era "24:00", significa que es el inicio del día siguiente
    df.loc[es_24, "Timestamp"] += timedelta(days=1)

    # 5. Generar columnas auxiliares limpias (Opcional, pero pediste separar)
    df["Fecha_Real"] = df["Timestamp"].dt.date
    df["Hora_Real"] = df["Timestamp"].dt.time

    # 6. Ordenar y Seleccionar
    cols_orden = [
        "Timestamp",
        "Subestacion",
        "MW",
        "Max_Diario_MW",
        "Hora_Pico_Reg",  # Datos agregados
        "Fecha_Real",
        "Hora_Real",  # Datos auxiliares
    ]
    return df[cols_orden].sort_values(["Subestacion", "Timestamp"])


def main_procesamiento():
    RUTA_SALIDA.mkdir(parents=True, exist_ok=True)
    print(f"🚀 Iniciando ETL Tidy (Largo) en: {RUTA_DATOS_RAW}")

    all_data = []

    for root, _, files in os.walk(RUTA_DATOS_RAW):
        zips = [f for f in files if f.lower().endswith(".zip")]
        for file in zips:
            ruta_zip = Path(root) / file
            fecha_str = ruta_zip.parent.name  # Ej: 2024-09-01

            try:
                with zipfile.ZipFile(ruta_zip, "r") as z:
                    excels = [f for f in z.namelist() if f.endswith((".xlsx", ".xls"))]
                    for excel_name in excels:
                        with z.open(excel_name) as f:
                            # Procesamos y obtenemos formato largo inmediatamente
                            df_part = procesar_excel_a_tidy(f, fecha_str)
                            if df_part is not None and not df_part.empty:
                                all_data.append(df_part)
            except Exception as e:
                print(f"⚠️ Error en {file}: {e}")

    if not all_data:
        print("No se encontraron datos.")
        return

    print(f"📦 Consolidando y calculando fechas para {len(all_data)} fragmentos...")

    # Concatenación Masiva
    df_total = pd.concat(all_data, ignore_index=True)

    # Aplicar correcciones de tiempo (Vectorizado = Rápido)
    df_final = corregir_fechas_y_tipos(df_total)

    # Guardar por subestación
    print("💾 Guardando CSVs optimizados...")
    for subestacion, df_sub in df_final.groupby("Subestacion"):
        try:
            safe_name = re.sub(r"[^\w\s-]", "", str(subestacion)).strip()
            if not safe_name:
                continue

            # Guardamos
            ruta_csv = RUTA_SALIDA / f"{safe_name}.csv"
            df_sub.to_csv(ruta_csv, index=False, encoding="utf-8-sig")

        except Exception as e:
            print(f"❌ Error al guardar {subestacion}: {e}")

    print("\n✨ ¡Proceso Tidy Completado! Datos listos para graficar.")


if __name__ == "__main__":
    main_procesamiento()
