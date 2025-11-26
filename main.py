import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path

# --- CONFIGURACIÓN ---
# Apuntamos a la carpeta final de alta resolución
CARPETA_DATOS = Path(r"D:\13_Proyecto\data\SE_Carga_3min")

# Configuración de la página web
st.set_page_config(page_title="Visor de Curvas de Carga", layout="wide")


def main():
    st.title("⚡ Monitor de Carga Eléctrica (Alta Resolución)")
    st.markdown("Visualización de datos resampleados cada **3 minutos** (PCHIP).")

    # 1. Buscar archivos
    if not CARPETA_DATOS.exists():
        st.error(f"❌ La carpeta no existe: {CARPETA_DATOS}")
        return

    archivos = list(CARPETA_DATOS.glob("*.csv"))

    if not archivos:
        st.warning(
            "⚠️ No hay archivos CSV en la carpeta. Ejecuta el script de resampleo primero."
        )
        return

    # 2. Barra Lateral (Sidebar) para seleccionar
    nombres_archivos = [f.name for f in archivos]
    seleccion = st.sidebar.selectbox("📍 Selecciona una Subestación:", nombres_archivos)

    # Encontrar la ruta completa del archivo seleccionado
    ruta_seleccionada = CARPETA_DATOS / seleccion

    # 3. Cargar Datos
    # Usamos cache para que sea rápido si cambias de gráfico y vuelves
    @st.cache_data
    def cargar_csv(ruta):
        df = pd.read_csv(ruta)
        df["Timestamp"] = pd.to_datetime(df["Timestamp"])
        return df

    try:
        df = cargar_csv(ruta_seleccionada)

        # 4. Métricas Rápidas (Encabezado)
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Registros", f"{len(df):,}")
        col2.metric("Potencia Máx (Registrada)", f"{df['MW'].max():.2f} MW")
        # El Max Diario viene de tu columna estática, tomamos el promedio/moda para mostrar el dato nominal
        col3.metric("Max Diario (Reportado)", f"{df['Max_Diario_MW'].iloc[0]} MW")
        col4.metric("Fecha Datos", str(df["Fecha_Real"].iloc[0]))

        # 5. GRÁFICO INTERACTIVO (Plotly) 📈
        fig = px.line(
            df,
            x="Timestamp",
            y="MW",
            title=f"Curva de Carga: {seleccion.replace('.csv', '')}",
            labels={"MW": "Potencia (MW)", "Timestamp": "Hora"},
            template="plotly_dark",  # Estilo oscuro profesional
        )

        # Personalizar el gráfico (Mejoras visuales)
        fig.update_traces(line=dict(color="#00CC96", width=2))  # Línea verde neón
        fig.update_layout(
            hovermode="x unified"
        )  # Al pasar el mouse muestra todos los datos

        # Renderizar en la web
        st.plotly_chart(fig, use_container_width=True)

        # 6. Mostrar Tabla de Datos (Opcional, desplegable)
        with st.expander("🔎 Ver datos crudos (Tabla)"):
            # Creamos una copia para visualización
            df_tabla = df.copy()

            # Redondeamos a 2 decimales directamente en los datos (Mucho más rápido que .style)
            df_tabla["MW"] = df_tabla["MW"].round(2)

            # Mostramos el dataframe puro, sin estilos CSS pesados
            st.dataframe(df_tabla, use_container_width=True)

    except Exception as e:
        st.error(f"Error al leer el archivo: {e}")


if __name__ == "__main__":
    main()
