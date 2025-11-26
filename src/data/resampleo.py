import pandas as pd
from pathlib import Path

# --- CONFIGURACIÓN ---
CARPETA_ENTRADA = Path(
    r"E:\13_DGA\Demo_Normas_DGA\data\SE_Carga"
)  # Donde están los CSVs horarios
CARPETA_SALIDA = Path(
    r"E:\13_DGA\Demo_Normas_DGA\data\SE_Carga_3min"
)  # Nueva carpeta para los de 3 min


def resamplear_archivo(ruta_csv):
    """
    Lee un CSV horario, crea una grilla de 3 minutos e interpola suavemente (PCHIP).
    """
    # 1. Cargar Datos
    df = pd.read_csv(ruta_csv)

    # 2. Configurar el Índice Temporal (OBLIGATORIO para resamplear)
    df["Timestamp"] = pd.to_datetime(df["Timestamp"])
    df = df.set_index("Timestamp")

    # 3. RESAMPLEO (Crear la grilla vacía)
    # '3min' genera filas a las 00:00, 00:03, 00:06...
    # .asfreq() deja los valores nuevos como NaN (Vacío)
    df_resampled = df.resample("3min").asfreq()

    # 4. INTERPOLACIÓN MATEMÁTICA (PCHIP) 🧠
    # Esta es la línea mágica que usa Scipy para las curvas suaves
    # limit_direction='both' asegura que rellene bordes si es seguro
    df_resampled["MW"] = df_resampled["MW"].interpolate(
        method="pchip", limit_direction="both"
    )

    # 5. RELLENO DE DATOS ESTÁTICOS (Forward Fill)
    # Copiamos los metadatos (Nombre, Máximos, Fecha) hacia abajo en los huecos nuevos
    cols_estaticas = ["Subestacion", "Max_Diario_MW", "Hora_Pico_Reg", "Fecha_Real"]

    # Solo rellenamos si las columnas existen en el archivo
    cols_a_usar = [c for c in cols_estaticas if c in df_resampled.columns]
    df_resampled[cols_a_usar] = df_resampled[cols_a_usar].ffill()

    # 6. LIMPIEZA Y AJUSTES FINALES
    # Eliminamos filas que hayan quedado vacías al inicio (si el archivo no empieza en hora exacta)
    df_resampled.dropna(subset=["Subestacion"], inplace=True)

    # Recalculamos la columna 'Hora_Real' para que muestre los minutos exactos (00:03, 00:06...)
    # Si hiciéramos ffill aquí, diría "01:00" a las "01:03", lo cual sería incorrecto.
    df_resampled["Hora_Real"] = df_resampled.index.time

    # Sacamos el Timestamp del índice y lo devolvemos como columna normal
    return df_resampled.reset_index()


def main():
    # Crear carpeta de salida
    CARPETA_SALIDA.mkdir(parents=True, exist_ok=True)

    print("🚀 Iniciando Upsampling (3 min) con método PCHIP...")

    # Listar archivos
    archivos = list(CARPETA_ENTRADA.glob("*.csv"))

    if not archivos:
        print("❌ No hay archivos CSV en la carpeta de entrada.")
        return

    # Loop principal
    for archivo in archivos:
        try:
            print(f"   ⚡ Procesando: {archivo.name}...", end=" ")

            # Procesar
            df_nuevo = resamplear_archivo(archivo)

            # Guardar
            nombre_salida = f"{archivo.stem}_3min.csv"
            df_nuevo.to_csv(
                CARPETA_SALIDA / nombre_salida, index=False, encoding="utf-8-sig"
            )

            print(f"✅ Ok ({len(df_nuevo)} filas)")

        except ImportError:
            print(
                "\n❌ ERROR CRÍTICO: Te falta instalar Scipy. Ejecuta 'pip install scipy'"
            )
            return
        except Exception as e:
            print(f"\n❌ Error en {archivo.name}: {e}")

    print(f"\n✨ ¡Proceso Terminado! Archivos guardados en: {CARPETA_SALIDA}")


if __name__ == "__main__":
    main()
