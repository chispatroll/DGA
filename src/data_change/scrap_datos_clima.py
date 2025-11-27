import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry
import os


def get_user_input():
    """Solicita y valida la entrada del usuario."""
    print("--- Configuración de Descarga de Datos Climáticos ---")

    # Coordenadas
    while True:
        try:
            lat = float(input("Ingrese la Latitud (ej. -33.4489): "))
            lon = float(input("Ingrese la Longitud (ej. -70.6693): "))
            break
        except ValueError:
            print("Error: Por favor ingrese números válidos para las coordenadas.")

    # Fechas
    while True:
        start_date = input("Ingrese fecha de inicio (YYYY-MM-DD): ")
        end_date = input("Ingrese fecha de fin (YYYY-MM-DD): ")
        try:
            pd.to_datetime(start_date)
            pd.to_datetime(end_date)
            break
        except ValueError:
            print("Error: Formato de fecha inválido. Use YYYY-MM-DD.")

    # Archivo de salida
    output_file = input(
        "Ingrese nombre del archivo de salida (default: weather_data.csv): "
    )
    if not output_file.strip():
        output_file = "weather_data.csv"

    if not output_file.endswith(".csv"):
        output_file += ".csv"

    return lat, lon, start_date, end_date, output_file


def fetch_weather_data(lat, lon, start_date, end_date):
    """Descarga datos de Open-Meteo."""
    # Configurar cliente con caché y reintentos
    cache_session = requests_cache.CachedSession(".cache", expire_after=-1)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": "temperature_2m",
    }

    print(
        f"\nDescargando datos para Lat: {lat}, Lon: {lon} desde {start_date} hasta {end_date}..."
    )

    try:
        responses = openmeteo.weather_api(url, params=params)
        response = responses[0]  # Procesar primera ubicación
    except Exception as e:
        print(f"Error al conectar con la API: {e}")
        return None

    # Procesar datos horarios
    hourly = response.Hourly()
    hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()

    hourly_data = {
        "date": pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left",
        )
    }
    hourly_data["temperature_2m"] = hourly_temperature_2m

    dataframe = pd.DataFrame(data=hourly_data)
    return dataframe


def main():
    lat, lon, start_date, end_date, output_file = get_user_input()

    df = fetch_weather_data(lat, lon, start_date, end_date)

    if df is not None:
        try:
            df.to_csv(output_file, index=False)
            print(f"\n¡Éxito! Datos guardados en: {os.path.abspath(output_file)}")
            print(f"Total de registros: {len(df)}")
            print(df.head())
        except Exception as e:
            print(f"Error al guardar el archivo: {e}")


if __name__ == "__main__":
    main()
