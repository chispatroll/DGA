import requests
from pathlib import Path
from datetime import datetime, timedelta
import time

# --- CONFIGURACIÓN ---
RUTA_BASE = Path(r"E:\13_DGA\Modelo_termico\data\cndc")
# URL directa al ZIP. {} se reemplaza por ddmmyy (ej: 251124)
BASE_URL = "https://www.cndc.bo/media/archivos/boletindiario/deener_{}.zip"


def obtener_ultima_fecha_registrada():
    # 1.Recolectamos los nombres de todas las carpetas
    carpetas = []
    for item in RUTA_BASE.iterdir():
        if item.is_dir():
            carpetas.append(item.name)

    # 2.Si no hay carpetas, retornamos None (por seguridad)
    if not carpetas:
        return None

    # 3.Obtenemos la más reciente (orden alfabético = cronológico)
    ultima_carpeta = max(carpetas)

    # 4.Devolvemos la fecha convertida
    return datetime.strptime(ultima_carpeta, "%Y-%m-%d")


def descargar_incremental():
    print("🤖 ACTUALIZANDO CNDC (MODO DIRECTO)...")

    ultima_fecha = obtener_ultima_fecha_registrada()
    fecha_actual = ultima_fecha + timedelta(days=1)

    print(f"📅 Iniciando desde: {fecha_actual.strftime('%Y-%m-%d')}")

    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})  # Cortesía

    while fecha_actual <= datetime.now():
        fecha_str = fecha_actual.strftime("%d%m%y")
        url = BASE_URL.format(fecha_str)
        carpeta = RUTA_BASE / fecha_actual.strftime("%Y-%m-%d")
        archivo = carpeta / f"deener_{fecha_str}.zip"

        print(f" ⬇️ Probando {fecha_actual.strftime('%Y-%m-%d')}...", end=" ")

        try:
            resp = session.get(url, stream=True, timeout=10)

            if resp.status_code == 200:
                carpeta.mkdir(exist_ok=True)
                with open(archivo, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=8192):
                        f.write(chunk)
                print("✅ OK")
                fecha_actual += timedelta(days=1)
                time.sleep(0.2)
            elif resp.status_code == 404:
                print("❌ Fin (404)")
                break  # Asumimos que no hay más datos futuros
            else:
                print(f"⚠️ Error {resp.status_code}")
                break

        except Exception as e:
            print(f"❌ Error: {e}")
            break

    print("🏁 Proceso terminado.")


if __name__ == "__main__":
    descargar_incremental()
