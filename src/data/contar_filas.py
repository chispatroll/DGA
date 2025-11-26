import pandas as pd
from pathlib import Path

# --- CONFIGURACIÓN ---
# La carpeta donde tienes los CSVs de 3 minutos
CARPETA = Path(r"D:\13_Proyecto\data\SE_Carga_3min")


def contar_registros():
    print(f"📊 Analizando archivos en: {CARPETA}\n")
    print(f"{'ARCHIVO':<40} | {'FILAS':<10} | {'INICIO':<20} | {'FIN':<20}")
    print("-" * 100)

    total_global = 0
    archivos = list(CARPETA.glob("*.csv"))

    if not archivos:
        print("❌ No se encontraron archivos CSV.")
        return

    for archivo in archivos:
        try:
            # Leemos el CSV
            df = pd.read_csv(archivo)

            # Contamos
            cantidad = len(df)
            total_global += cantidad

            # Obtenemos fechas (solo si el archivo no está vacío)
            if cantidad > 0:
                inicio = df["Timestamp"].iloc[0]
                fin = df["Timestamp"].iloc[-1]
            else:
                inicio = "---"
                fin = "---"

            # Imprimimos bonito
            print(
                f"{archivo.name:<40} | {cantidad:<10,} | {str(inicio)[0:16]:<20} | {str(fin)[0:16]:<20}"
            )

        except Exception as e:
            print(f"❌ Error leyendo {archivo.name}: {e}")

    print("-" * 100)
    print(f"✨ TOTAL DE REGISTROS EN EL SISTEMA: {total_global:,}")


if __name__ == "__main__":
    contar_registros()
