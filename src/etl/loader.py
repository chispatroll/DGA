import streamlit as st
import pandas as pd
from src.config import RUTA_DATOS, RUTA_TRANSFORMADORES


@st.cache_data
def obtener_subestaciones():
    """Busca los archivos parquet en la carpeta de datos."""
    if not RUTA_DATOS.exists():
        return []
    archivos = list(RUTA_DATOS.glob("*_3min.parquet"))
    # Limpiamos el nombre del archivo para que se vea bonito
    nombres = [f.name.replace("_3min.parquet", "") for f in archivos]
    return sorted(nombres)


@st.cache_data
def cargar_datos_subestacion(nombre):
    """Carga los datos de una subestación específica."""
    archivo = RUTA_DATOS / f"{nombre}_3min.parquet"
    if archivo.exists():
        return pd.read_parquet(archivo)
    return None


@st.cache_data
def cargar_metadatos_trafos():
    """Carga los metadatos de los transformadores."""
    ruta_meta = RUTA_TRANSFORMADORES / "metadatos_ATRs.csv"
    if ruta_meta.exists():
        return pd.read_csv(ruta_meta)
    return None
