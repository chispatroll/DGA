from src.etl import scrap_cndc
from src.etl import procesar_datos
from src.etl import scrap_clima


def main():
    print("=== INICIANDO ACTUALIZACIÓN COMPLETA DEL SISTEMA ===")

    # PASO 1: Descargar nuevos datos del CNDC
    print("\n--- PASO 1: DESCARGA CNDC ---")
    scrap_cndc.descargar_incremental()

    # PASO 2: Procesar los excels descargados
    print("\n--- PASO 2: PROCESAMIENTO Y LIMPIEZA ---")
    procesar_datos.main_procesamiento()

    # PASO 3: Descargar clima para los nuevos datos
    print("\n--- PASO 3: DESCARGA DE CLIMA ---")
    scrap_clima.main()

    print("\n=== ¡TODO ACTUALIZADO EXITOSAMENTE! ===")


if __name__ == "__main__":
    main()
