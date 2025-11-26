import pandas as pd
from pathlib import Path
import os

# --- CONFIGURACIÓN ---
# La carpeta donde están tus CSVs limpios (Tidy)
CARPETA_DATOS = Path(r"D:\13_Proyecto\data\SE_Carga_3min")

# 1. El nombre del archivo FINAL que quieres conservar (sin .csv)
NOMBRE_CORRECTO = "EMVINTO - COMIBOL_3min"

# 2. El nombre del archivo "VIEJO" o incorrecto que quieres absorber y borrar (sin .csv)
NOMBRE_INCORRECTO = "EMVINTO_3min"


def fusionar_archivos():
    path_good = CARPETA_DATOS / f"{NOMBRE_CORRECTO}.csv"
    path_bad = CARPETA_DATOS / f"{NOMBRE_INCORRECTO}.csv"

    # Verificación de seguridad
    if not path_good.exists() or not path_bad.exists():
        print(f"❌ Error: No encuentro alguno de los archivos en:\n{CARPETA_DATOS}")
        print("Verifica los nombres en la sección CONFIGURACIÓN.")
        return

    print(
        f"🔧 Fusionando '{NOMBRE_INCORRECTO}' --> dentro de --> '{NOMBRE_CORRECTO}'..."
    )

    # 1. Cargar DataFrames
    df_good = pd.read_csv(path_good)
    df_bad = pd.read_csv(path_bad)

    print(f"   📊 Filas antes: Correcto={len(df_good)} | Incorrecto={len(df_bad)}")

    # 2. Normalizar Nombre (El paso clave)
    # Al archivo "malo" le cambiamos el nombre de la subestación para que sea igual al bueno
    nombre_real_subestacion = df_good["Subestacion"].iloc[0]
    df_bad["Subestacion"] = nombre_real_subestacion

    # 3. Concatenar (Unir verticalmente)
    df_total = pd.concat([df_good, df_bad], ignore_index=True)

    # 4. Limpieza Técnica (Ordenar y Quitar Duplicados de tiempo)
    df_total["Timestamp"] = pd.to_datetime(df_total["Timestamp"])

    # Ordenamos cronológicamente
    df_total = df_total.sort_values("Timestamp")

    # Si hay choque de horarios, nos quedamos con el último registro (keep='last')
    df_total = df_total.drop_duplicates(subset=["Timestamp"], keep="last")

    # 5. Guardar el archivo Bueno
    df_total.to_csv(path_good, index=False, encoding="utf-8-sig")
    print(f"✅ Fusión exitosa. Nuevo total de filas: {len(df_total)}")

    # 6. Borrar el archivo Malo
    try:
        os.remove(path_bad)
        print(f"🗑️ Archivo antiguo '{NOMBRE_INCORRECTO}.csv' eliminado.")
    except Exception as e:
        print(f"⚠️ No pude borrar el archivo antiguo: {e}")


if __name__ == "__main__":
    fusionar_archivos()
