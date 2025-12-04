from pathlib import Path

# --- RUTAS DE ARCHIVOS ---
# Buscamos la carpeta del proyecto (1 nivel arriba de este archivo, ya que está en src/)
PROJECT_ROOT = Path(__file__).resolve().parents[1]
RUTA_DATOS = PROJECT_ROOT / "data" / "SE_Carga_3min_parquet"
RUTA_TRANSFORMADORES = PROJECT_ROOT / "data" / "TRANSFORMADORES"
