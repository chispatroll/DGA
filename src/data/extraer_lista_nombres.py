import pandas as pd
from pathlib import Path

# --- CONFIGURACIÓN ---
# Tu carpeta de datos resampleados
CARPETA_DATOS = Path(r"D:\13_Proyecto\data\SE_Carga_3min")

# Nombre del archivo que generaremos
ARCHIVO_SALIDA = "lista_oficial_subestaciones.csv"


def generar_lista_maestra():
    print(f"📂 Escaneando carpeta: {CARPETA_DATOS} ...")

    archivos = list(CARPETA_DATOS.glob("*.csv"))

    if not archivos:
        print("❌ No se encontraron archivos CSV.")
        return

    lista_nombres = []

    # Iteramos archivo por archivo
    for archivo in archivos:
        try:
            # TRUCO DE VELOCIDAD 🚀:
            # 'usecols': Solo carga la columna de nombres.
            # 'nrows=1': Solo lee la primera fila. Ignora las otras 200,000.
            df = pd.read_csv(archivo, usecols=["Subestacion"], nrows=1)

            if not df.empty:
                nombre_real = df["Subestacion"].iloc[0]
                lista_nombres.append(nombre_real)

        except Exception as e:
            print(f"⚠️ No pude leer {archivo.name}: {e}")

    # Crear DataFrame final
    df_lista = pd.DataFrame(lista_nombres, columns=["Nombre_Subestacion"])

    # Ordenar alfabéticamente para que sea fácil comparar con el CNDC
    df_lista.sort_values("Nombre_Subestacion", inplace=True)

    # Guardar
    df_lista.to_csv(ARCHIVO_SALIDA, index=False, encoding="utf-8-sig")

    print("-" * 40)
    print(f"✅ ¡Éxito! Se encontraron {len(lista_nombres)} subestaciones únicas.")
    print(f"💾 Lista guardada en: {Path(ARCHIVO_SALIDA).absolute()}")
    print("-" * 40)
    # Mostramos las primeras 5 para verificar
    print(df_lista.head())


if __name__ == "__main__":
    generar_lista_maestra()
