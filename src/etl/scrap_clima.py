import pandas as pd
import requests
import time
from pathlib import Path
from datetime import datetime, timedelta

# --- CONFIGURACIÓN ---
# Definir raíz del proyecto (2 niveles arriba: src/etl -> src -> root)
PROJECT_ROOT = Path(__file__).resolve().parents[2]

RUTA_COORDENADAS = (
    PROJECT_ROOT / "data" / "SUBESTACIONES" / "subestacion_con_coordenadas.csv"
)
RUTA_CARGA_PARQUET = PROJECT_ROOT / "data" / "SE_Carga_3min_parquet"
RUTA_CLIMA_PARQUET = PROJECT_ROOT / "data" / "SE_Clima_3min_parquet"
RUTA_CLIMA_CSV = PROJECT_ROOT / "data" / "SE_Clima_3min"

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
            max_ts = df["Timestamp"].max()

            # --- CORRECCIÓN: Si es 00:00 exacto, es el cierre del día anterior ---
            if max_ts.hour == 0 and max_ts.minute == 0:
                max_ts -= timedelta(seconds=1)

            max_date = max_ts.date()
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
    print(f"🌦️ Procesando clima para: {nombre}...", end=" ")

    # 1. ¿Hasta cuándo necesitamos datos? (Meta: Fin de los datos de carga eléctrica)
    start_carga, end_carga = obtener_rango_fechas(nombre)

    # 2. ¿Desde cuándo descargamos? (Lógica Incremental)
    archivo_existente = RUTA_CLIMA_PARQUET / f"{nombre}_clima.parquet"
    df_historico = None
    fecha_inicio_descarga = start_carga  # Por defecto: descargamos todo

    if archivo_existente.exists():
        try:
            df_historico = pd.read_parquet(archivo_existente)
            if not df_historico.empty:
                # Busamos la última fecha y hora registrada
                ultimo_ts = df_historico["Timestamp"].max()

                # --- LÓGICA DE LAS 23:00 ---
                # Si el último dato es de las 23:00 (o más), tenemos el día completo.
                # Empezamos a descargar desde el DÍA SIGUIENTE.
                if ultimo_ts.hour >= 23:
                    fecha_inicio_descarga = ultimo_ts.date() + timedelta(days=1)
                else:
                    # Si el día está incompleto (ej: 15:00), lo descargamos de nuevo.
                    fecha_inicio_descarga = ultimo_ts.date()

                # Si nuestra fecha de inicio ya superó la fecha fin de carga, no hacemos nada.
                if fecha_inicio_descarga > end_carga:
                    print("✅ Ya está actualizado.")
                    return

                print(f"[Incremental: {fecha_inicio_descarga} -> {end_carga}]", end=" ")
        except Exception:
            print("[Error leyendo histórico, re-descargando todo]", end=" ")
            df_historico = None

    # 3. Descargar solo lo que falta (DELTA)
    df_horario = descargar_clima(lat, lon, fecha_inicio_descarga, end_carga)

    if df_horario is None or df_horario.empty:
        print("⚠️ No hay datos nuevos.")
        return

    # 4. Resamplear lo NUEVO a 3 min
    df_horario.set_index("Timestamp", inplace=True)

    # Creamos índice de 3 min solo para el pedazo nuevo
    full_idx = pd.date_range(
        start=df_horario.index.min(), end=df_horario.index.max(), freq="3min"
    )
    df_3min_nuevo = df_horario.reindex(full_idx)
    df_3min_nuevo["Temperatura_C"] = df_3min_nuevo["Temperatura_C"].interpolate(
        method="cubic"
    )
    df_3min_nuevo.reset_index(inplace=True)
    df_3min_nuevo.rename(columns={"index": "Timestamp"}, inplace=True)
    df_3min_nuevo["Subestacion"] = nombre

    # 5. UNIÓN (MERGE): Pegar Histórico + Nuevo
    if df_historico is not None:
        df_final = pd.concat([df_historico, df_3min_nuevo], ignore_index=True)
        # Eliminamos duplicados por si se solapó alguna hora
        df_final.drop_duplicates(subset=["Timestamp"], keep="last", inplace=True)
        df_final.sort_values("Timestamp", inplace=True)
    else:
        df_final = df_3min_nuevo

    # 6. Guardar
    archivo_salida_parquet = RUTA_CLIMA_PARQUET / f"{nombre}_clima.parquet"
    df_final.to_parquet(archivo_salida_parquet, index=False)

    # Opcional: Guardar CSV también
    archivo_salida_csv = RUTA_CLIMA_CSV / f"{nombre}_clima.csv"
    df_final.to_csv(archivo_salida_csv, index=False, encoding="utf-8-sig")

    print(f"Guardado (+{len(df_3min_nuevo)} registros nuevos)")


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
