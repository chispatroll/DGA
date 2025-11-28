import pandas as pd
import requests
import time
from pathlib import Path
from datetime import datetime, timedelta

# --- CONFIGURACIÓN ---
RUTA_COORDENADAS = Path(
    r"E:\13_DGA\Demo_Normas_DGA\data\subestacion_con_coordenadas.csv"
)
RUTA_CARGA_PARQUET = Path(r"E:\13_DGA\Demo_Normas_DGA\data\SE_Carga_3min_parquet")
RUTA_CLIMA_PARQUET = Path(r"E:\13_DGA\Demo_Normas_DGA\data\SE_Clima_3min_parquet")
RUTA_CLIMA_CSV = Path(r"E:\13_DGA\Demo_Normas_DGA\data\SE_Clima_3min")

# Crear directorio de salida si no existe
RUTA_CLIMA_PARQUET.mkdir(parents=True, exist_ok=True)
RUTA_CLIMA_CSV.mkdir(parents=True, exist_ok=True)

API_URL = "https://archive-api.open-meteo.com/v1/archive"


def obtener_rango_fechas(nombre_sub):
    """
    Busca el archivo de carga de la subestación para determinar fecha inicio y fin.
    Si no existe, usa un default razonable (ej. últimos 2 años).
    """
    archivo_carga = RUTA_CARGA_PARQUET / f"{nombre_sub}_3min.parquet"
    if archivo_carga.exists():
        try:
            # Leemos solo la columna Timestamp para ser rápidos
            df = pd.read_parquet(archivo_carga, columns=["Timestamp"])
            min_date = df["Timestamp"].min().date()
            max_date = df["Timestamp"].max().date()
            return min_date, max_date
        except Exception as e:
            print(f"⚠️ Error leyendo rango de {nombre_sub}: {e}")

    # Default si no hay datos de carga: Últimos 365 días
    hoy = datetime.now().date()
    return hoy - timedelta(days=365), hoy


def descargar_clima(lat, lon, start_date, end_date):
    """Descarga temperatura horaria de Open-Meteo."""
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "hourly": "temperature_2m",
        "timezone": "America/La_Paz",  # Ajustar según zona horaria de Bolivia
    }

    try:
        response = requests.get(API_URL, params=params)
        response.raise_for_status()
        data = response.json()

        # Crear DataFrame
        df = pd.DataFrame(
            {
                "Timestamp": pd.to_datetime(data["hourly"]["time"]),
                "Temperatura_C": data["hourly"]["temperature_2m"],
            }
        )
        return df
    except Exception as e:
        print(f"❌ Error API Open-Meteo: {e}")
        return None


def procesar_clima_subestacion(nombre, lat, lon):
    print(f"🌦️ Procesando clima para: {nombre}...")

    # 1. Determinar rango
    start, end = obtener_rango_fechas(nombre)
    print(f"   📅 Rango detectado: {start} a {end}")

    # 2. Descargar (Horario)
    df_horario = descargar_clima(lat, lon, start, end)
    if df_horario is None or df_horario.empty:
        print("   ⚠️ No se pudieron descargar datos.")
        return

    # 3. Resamplear a 3 min (Cubic Spline)
    # Set index
    df_horario.set_index("Timestamp", inplace=True)

    # Upsample
    # Generamos el índice completo de 3 min
    full_idx = pd.date_range(
        start=df_horario.index.min(), end=df_horario.index.max(), freq="3min"
    )

    # Reindexamos y usamos interpolación cúbica (suave para temperatura)
    df_3min = df_horario.reindex(full_idx)
    df_3min["Temperatura_C"] = df_3min["Temperatura_C"].interpolate(method="cubic")

    # Reset index
    df_3min.reset_index(inplace=True)
    df_3min.rename(columns={"index": "Timestamp"}, inplace=True)

    # Agregar columna de identificación
    df_3min["Subestacion"] = nombre

    # 4. Guardar Parquet
    archivo_salida_parquet = RUTA_CLIMA_PARQUET / f"{nombre}_clima.parquet"
    df_3min.to_parquet(archivo_salida_parquet, index=False)

    # 5. Guardar CSV
    archivo_salida_csv = RUTA_CLIMA_CSV / f"{nombre}_clima.csv"
    df_3min.to_csv(archivo_salida_csv, index=False, encoding="utf-8-sig")

    print(
        f"   ✅ Guardado: {archivo_salida_parquet.name} y CSV ({len(df_3min)} registros)"
    )


def main():
    if not RUTA_COORDENADAS.exists():
        print(f"❌ No se encontró el archivo de coordenadas: {RUTA_COORDENADAS}")
        return

    try:
        df_coords = pd.read_csv(RUTA_COORDENADAS)
    except Exception as e:
        print(f"❌ Error leyendo CSV coordenadas: {e}")
        return

    # Validar columnas
    required_cols = ["Subestacion", "Latitud", "Longitud"]
    if not all(col in df_coords.columns for col in required_cols):
        print(f"❌ El CSV debe tener las columnas: {required_cols}")
        return

    print(f"🚀 Iniciando descarga de clima para {len(df_coords)} subestaciones...")

    for _, row in df_coords.iterrows():
        sub = row["Subestacion"]
        lat = row["Latitud"]
        lon = row["Longitud"]

        procesar_clima_subestacion(sub, lat, lon)

        # Respetar límites de API (Open-Meteo pide no saturar)
        time.sleep(1.5)

    print("\n✨ ¡Proceso de clima finalizado!")


if __name__ == "__main__":
    main()
