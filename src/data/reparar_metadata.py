import pandas as pd
from pathlib import Path

# --- CONFIGURACIÓN ---
RUTA_CSVS = Path(r"E:\13_DGA\Demo_Normas_DGA\data\SE_Carga_3min")


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
            # 'first' sería peligroso porque el 00:00 está mal.
            # 'mode' es más seguro pero más lento. 'last' es muy seguro dado el problema descrito.
            df[col] = df.groupby("_Fecha_Calc")[col].transform("last")

        # Limpieza
        df.drop(columns=["_Fecha_Calc"], inplace=True)

        # Guardar
        df.to_csv(ruta_csv, index=False, encoding="utf-8-sig")
        return True

    except Exception as e:
        print(f"❌ Error en {ruta_csv.name}: {e}")
        return False


def main():
    print("🔧 INICIANDO REPARACIÓN DE METADATA...\n")

    if not RUTA_CSVS.exists():
        print(f"❌ No existe la carpeta: {RUTA_CSVS}")
        return

    archivos = list(RUTA_CSVS.glob("*_3min.csv"))
    print(f"📂 Encontrados {len(archivos)} archivos.")

    reparados = 0
    for archivo in archivos:
        print(f"   Reparando: {archivo.name}...", end=" ")
        if reparar_archivo(archivo):
            print("✅ OK")
            reparados += 1
        else:
            print("⚠️ Saltado")

    print(f"\n✨ ¡Listo! Se repararon {reparados} archivos.")


if __name__ == "__main__":
    main()
