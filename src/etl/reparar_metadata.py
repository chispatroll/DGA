import pandas as pd
from pathlib import Path

# --- CONFIGURACIÓN ---
# Definir raíz del proyecto (2 niveles arriba: src/etl -> src -> root)
PROJECT_ROOT = Path(__file__).resolve().parents[2]

RUTA_CSVS = PROJECT_ROOT / "data" / "SE_Carga_3min"


def reparar_archivo(ruta_csv):
    try:
        df = pd.read_csv(ruta_csv)
        if df.empty:
            return False

        df["Timestamp"] = pd.to_datetime(df["Timestamp"])

        # Creamos una columna temporal de fecha real basada en el Timestamp
        df["_Fecha_Calc"] = df["Timestamp"].dt.date

        # Columnas a reparar
        cols_meta = ["Max_Diario_MW", "Hora_Pico_Reg"]

        # Verificamos que existan
        cols_existentes = [c for c in cols_meta if c in df.columns]
        if not cols_existentes:
            return False

        # Lógica de reparación:
        # Agrupamos por fecha y tomamos el valor más frecuente (moda) o el último valor del día
        # para sobreescribir todos los valores de ese día.
        # Esto corrige las filas de 00:00 - 01:00 que tenían datos del día anterior.

        for col in cols_existentes:
            # Transformamos la columna para que todos los registros del mismo día tengan el mismo valor
            # Usamos 'last' porque los datos del final del día (o 01:00 en adelante) son los correctos.
            # NOTA: Si el día solo tiene 1 registro (el 00:00 huérfano), lo dejamos vacío (None).
            # Así, cuando lleguen los datos reales de ese día, se rellenará correctamente.
            df[col] = df.groupby("_Fecha_Calc")[col].transform(
                lambda x: x.iloc[-1] if len(x) > 1 else None
            )

        # Limpieza
        df.drop(columns=["_Fecha_Calc"], inplace=True)

        # Guardar
        df.to_csv(ruta_csv, index=False, encoding="utf-8-sig")
        return True

    except Exception as e:
        print(f"[X] Error en {ruta_csv.name}: {e}")
        return False


def main():
    print("[TOOL] INICIANDO REPARACIÓN DE METADATA...\n")

    if not RUTA_CSVS.exists():
        print(f"[X] No existe la carpeta: {RUTA_CSVS}")
        return

    archivos = list(RUTA_CSVS.glob("*_3min.csv"))
    print(f"[DIR] Encontrados {len(archivos)} archivos.")

    reparados = 0
    for archivo in archivos:
        print(f"   Reparando: {archivo.name}...", end=" ")
        if reparar_archivo(archivo):
            print("[OK]")
            reparados += 1
        else:
            print("[SKIP] Saltado")

    print(f"\n[DONE] ¡Listo! Se repararon {reparados} archivos.")


if __name__ == "__main__":
    main()
