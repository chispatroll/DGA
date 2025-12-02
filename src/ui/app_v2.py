import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path

# --- 1. CONFIGURACIÓN BÁSICA ---
st.set_page_config(page_title="Mi Primer Dashboard", layout="wide")

# --- 2. RUTAS DE ARCHIVOS (Donde están tus datos) ---
# Buscamos la carpeta del proyecto (2 niveles arriba de este archivo)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
RUTA_DATOS = PROJECT_ROOT / "data" / "SE_Carga_3min_parquet"


# --- 3. FUNCIONES DE CARGA DE DATOS ---
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
    ruta_meta = PROJECT_ROOT / "data" / "metadatos_ATRs.csv"
    if ruta_meta.exists():
        return pd.read_csv(ruta_meta)
    return None


# --- 4. INTERFAZ DE USUARIO (UI) ---
def main():
    st.title("📊 Visualizador Simple de Datos")
    st.write("Esta es una versión simplificada para aprender y modificar.")

    # --- BARRA LATERAL (SIDEBAR) ---
    st.sidebar.header("Opciones")

    # Paso 1: Obtener lista de subestaciones
    lista_subs = obtener_subestaciones()

    if not lista_subs:
        st.error(
            "No encontré archivos de datos en la carpeta data/SE_Carga_3min_parquet"
        )
        return

    # Paso 2: Selector
    seleccion = st.sidebar.selectbox("Selecciona una Subestación:", lista_subs)

    # --- ÁREA PRINCIPAL ---
    df = cargar_datos_subestacion(seleccion)
    df_meta = cargar_metadatos_trafos()

    if df is not None:
        # 3. Layout: st.tabs (Pestañas para organizar)
        # En lugar de scrollear infinito, organizamos por temas
        tab_grafico, tab_datos, tab_detalles, tab_info = st.tabs(
            ["Gráfico", "Tabla de Datos", "Detalles Técnicos", "Información General"]
        )

        with tab_grafico:
            st.subheader("Tendencia de Potencia")
            # Usamos Plotly Express
            fig = px.line(
                df, x="Timestamp", y="MW", title=f"Curva de Carga - {seleccion}"
            )
            st.plotly_chart(fig, width="stretch")

            # Métricas debajo del gráfico
            col1, col2 = st.columns(2)

            # Para col1:

            # Obtener fecha de pico maximo historico
            # 1. Encontrar el índice (fila) donde está el valor máximo
            indice_max = df["Max_Diario_MW"].idxmax()
            # 2. Extraer el valor y la fecha de esa fila
            max_mw = df.loc[indice_max, "Max_Diario_MW"]
            fecha_pico = df.loc[indice_max, "Fecha_Real"]
            hora_pico = df.loc[indice_max, "Hora_Pico_Reg"]
            # 3. Mostrarlo en la métrica
            # Opción: Tarjeta personalizada con Markdown
            # Días desde el Récord.
            fecha_pico_dt = pd.to_datetime(fecha_pico)
            dias_desde_record = (pd.Timestamp.now() - fecha_pico_dt).days
            delta_valor = f"Hace {dias_desde_record} días"
            delta_color = (
                "#ff4b4b" if dias_desde_record < 7 else "#09ab3b"
            )  # Rojo si fue reciente
            col1.markdown(
                f"""
                <div style="background-color: #1E1E1E; padding: 10px; border-radius: 5px; border: 1px solid #333;" title="Máximo valor registrado en toda la historia">
                    <div style="display: flex; justify-content: space-between; align-items: center;">
                        <p style="color: #888; font-size: 14px; margin: 0;">Pico Máximo Histórico</p>
                        <span style="font-size: 12px; cursor: help;">❔</span>
                    </div>
                    <p style="color: #FFF; font-size: 24px; font-weight: bold; margin: 5px 0;">
                        {max_mw:.2f} MW
                    </p>
                    <div style="display: flex; justify-content: space-between; align-items: center;">
                        <p style="color: #AAA; font-size: 12px; margin: 0;"> {fecha_pico} &nbsp; {hora_pico}</p>
                        <span style="color: {delta_color}; font-size: 12px; font-weight: bold;">
                            {delta_valor} ▲
                        </span>
                    </div>
                </div>
            """,
                unsafe_allow_html=True,
            )

            # Para col2:
            ultimo_max_diario = df.iloc[-2]["Max_Diario_MW"]
            fecha_ultimo_pico = df.iloc[-2]["Fecha_Real"]
            hora_ultimo_pico = df.iloc[-2]["Hora_Pico_Reg"]
            # Cálculo de Capacidad Real y Porcentaje de Carga
            capacidad_total_mva = 0
            porcentaje_carga = 0

            if df_meta is not None:
                # Filtrar por subestación y solo los que están En Servicio
                trafos_sub = df_meta[
                    (df_meta["Subestacion"] == seleccion)
                    & (df_meta["En_Servicio"] == "SI")
                ]
                if not trafos_sub.empty:
                    capacidad_total_mva = trafos_sub["Potencia_Nominal_MVA"].sum()
                    if capacidad_total_mva > 0:
                        porcentaje_carga = (
                            ultimo_max_diario / capacidad_total_mva
                        ) * 100

            # Definir el delta basado en el porcentaje de carga
            if capacidad_total_mva > 0:
                delta_valor_2 = f"{porcentaje_carga:.1f}% de Capacidad"
                # Semáforo de carga: Verde < 70%, Naranja 70-90%, Rojo > 90%
                if porcentaje_carga > 90:
                    delta_color_2 = "#ff4b4b"  # Rojo
                elif porcentaje_carga > 70:
                    delta_color_2 = "#ffa421"  # Naranja
                else:
                    delta_color_2 = "#09ab3b"  # Verde
            else:
                delta_valor_2 = "Capacidad desconocida"
                delta_color_2 = "#888"
            col2.markdown(
                f"""
                <div style="background-color: #1E1E1E; padding: 10px; border-radius: 5px; border: 1px solid #333;" title="Pico máximo del último día completo registrado">
                    <div style="display: flex; justify-content: space-between; align-items: center;">
                        <p style="color: #888; font-size: 14px; margin: 0;">Último Pico Diario</p>
                        <span style="font-size: 12px; cursor: help;">❔</span>
                    </div>
                    <p style="color: #FFF; font-size: 24px; font-weight: bold; margin: 5px 0;">
                        {ultimo_max_diario:.2f} MW
                    </p>
                    <div style="display: flex; justify-content: space-between; align-items: center;">
                        <p style="color: #AAA; font-size: 12px; margin: 0;">
                        {fecha_ultimo_pico} &nbsp; {hora_ultimo_pico}
                        </p>
                        <span style="color: {delta_color_2}; font-size: 12px; font-weight: bold;">
                            {delta_valor_2}
                        </span>
                    </div>
                </div>
            """,
                unsafe_allow_html=True,
            )

        with tab_datos:
            st.subheader("Explorador de Registros")
            # st.data_editor (Tabla interactiva)
            st.data_editor(
                df,
                column_config={
                    "MW": st.column_config.NumberColumn(
                        "Potencia (MW)", format="%.2f MW"
                    ),
                    "Timestamp": st.column_config.DatetimeColumn(
                        "Fecha", format="D MMM YYYY, HH:mm"
                    ),
                },
                disabled=True,
                use_container_width=True,
                height=400,  # Altura fija para que se vea bien
            )

        with tab_detalles:
            # 4. Layout: st.expander (Acordeón para ocultar cosas)
            # Ideal para info que no siempre quieres ver
            with st.expander("Ver Metadatos del Archivo", expanded=False):
                st.json(
                    {
                        "Archivo": f"{seleccion}_3min.parquet",
                        "Ruta": str(RUTA_DATOS),
                        "Columnas": list(df.columns),
                        "Tipos de Datos": str(df.dtypes),
                    }
                )

            st.info(
                "Estos datos provienen del sistema SCADA y tienen una resolución de 3 minutos."
            )

    else:
        st.error("No se pudieron cargar los datos. Verifica que los archivos existan.")


# --- 5. PUNTO DE ENTRADA ---
if __name__ == "__main__":
    main()
