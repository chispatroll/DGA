import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import subprocess
import sys
import time
from datetime import timedelta
import numpy as np

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(
    page_title="DGA Analytics Pro",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# --- ESTILOS CSS PREMIUM (PROFESSIONAL ELECTRIC THEME) ---
st.markdown(
    """
    <style>
        /* Tipografía */
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600&display=swap');
        
        html, body, [class*="css"] {
            font-family: 'Inter', sans-serif;
        }

        /* Fondo General */
        .stApp {
            background-color: #050505; /* Ultra Dark */
            background-image: radial-gradient(circle at 50% 0%, #1a1a2e 0%, #050505 70%);
        }

        /* Sidebar */
        [data-testid="stSidebar"] {
            background-color: #0a0a0a;
            border-right: 1px solid #222;
        }

        /* Tarjetas (Metrics) */
        div[data-testid="metric-container"] {
            background-color: rgba(255, 255, 255, 0.03);
            border: 1px solid rgba(255, 255, 255, 0.08);
            padding: 15px 20px;
            border-radius: 12px;
            backdrop-filter: blur(10px);
            transition: transform 0.2s ease, border-color 0.2s ease;
        }
        div[data-testid="metric-container"]:hover {
            transform: translateY(-2px);
            border-color: #00f2ea; /* Cyan Glow */
        }

        /* Textos Métricas */
        [data-testid="stMetricLabel"] {
            font-size: 0.85rem !important;
            color: #888;
            text-transform: uppercase;
            letter-spacing: 1px;
        }
        [data-testid="stMetricValue"] {
            font-size: 2.2rem !important;
            font-weight: 600;
            color: #fff;
            text-shadow: 0 0 20px rgba(255, 255, 255, 0.1);
        }

        /* Tabs */
        .stTabs [data-baseweb="tab-list"] {
            gap: 20px;
            border-bottom: 1px solid #333;
            padding-bottom: 5px;
        }
        .stTabs [data-baseweb="tab"] {
            background-color: transparent;
            border: none;
            color: #666;
            font-weight: 500;
            padding: 10px 0;
        }
        .stTabs [data-baseweb="tab"][aria-selected="true"] {
            color: #00f2ea;
            border-bottom: 2px solid #00f2ea;
        }

        /* Botones */
        .stButton>button {
            background: linear-gradient(90deg, #00f2ea 0%, #00a8f2 100%);
            color: #000;
            font-weight: 600;
            border: none;
            border-radius: 8px;
            padding: 0.5rem 1rem;
            transition: all 0.3s ease;
        }
        .stButton>button:hover {
            box-shadow: 0 0 15px rgba(0, 242, 234, 0.4);
            transform: scale(1.02);
        }
    </style>
    """,
    unsafe_allow_html=True,
)

# --- CONSTANTES ---
RUTA_DATOS_CSV = Path(r"E:\13_DGA\Demo_Normas_DGA\data\SE_Carga_3min")
RUTA_DATOS_PARQUET = Path(r"E:\13_DGA\Demo_Normas_DGA\data\SE_Carga_3min_parquet")
RUTA_CLIMA_PARQUET = Path(r"E:\13_DGA\Demo_Normas_DGA\data\SE_Clima_3min_parquet")
SCRIPT_SCRAP = Path(r"E:\13_DGA\Demo_Normas_DGA\src\data_change\scrap_cndc.py")
SCRIPT_PROCESAR = Path(r"E:\13_DGA\Demo_Normas_DGA\src\data_change\procesar_datos.py")
SCRIPT_CLIMA = Path(r"E:\13_DGA\Demo_Normas_DGA\src\data_change\scrap_clima.py")


# --- MOTOR DE CÁLCULO (ENGINEERING CORE) ---
def calcular_energia_mwh(df_periodo):
    """Calcula la energía (integral de potencia) usando regla del trapecio o suma simple."""
    # Como son intervalos de 3 min constantes: Energía = Sum(MW) * (3/60) horas
    # Ajuste fino: Si hay huecos, esto es una aproximación, pero para 3min es muy preciso.
    if df_periodo.empty:
        return 0.0
    suma_potencia = df_periodo["MW"].sum()
    energia = suma_potencia * (3 / 60)
    return energia


def calcular_factor_carga(df_periodo):
    """Calcula Factor de Carga = Promedio / Pico."""
    if df_periodo.empty:
        return 0.0
    pico = df_periodo["MW"].max()
    promedio = df_periodo["MW"].mean()
    if pico == 0:
        return 0.0
    return (promedio / pico) * 100  # En porcentaje


def obtener_curva_duracion(df_periodo):
    """Genera datos para la Curva de Duración de Carga."""
    if df_periodo.empty:
        return pd.DataFrame()

    # Ordenar de mayor a menor
    valores_ordenados = df_periodo["MW"].sort_values(ascending=False).values
    # Eje X: Porcentaje de tiempo (0 a 100%)
    x_percent = np.linspace(0, 100, len(valores_ordenados))

    return pd.DataFrame({"Duración (%)": x_percent, "MW": valores_ordenados})


# --- CARGA DE DATOS ---
@st.cache_data(ttl=300)
def cargar_lista_subestaciones():
    if RUTA_DATOS_PARQUET.exists():
        archivos = list(RUTA_DATOS_PARQUET.glob("*_3min.parquet"))
        if archivos:
            return sorted([f.name.replace("_3min.parquet", "") for f in archivos])
    if RUTA_DATOS_CSV.exists():
        archivos = list(RUTA_DATOS_CSV.glob("*_3min.csv"))
        return sorted([f.name.replace("_3min.csv", "") for f in archivos])
    return []


@st.cache_data(ttl=300)
def cargar_datos(nombre_sub):
    # Parquet Priority
    archivo_parquet = RUTA_DATOS_PARQUET / f"{nombre_sub}_3min.parquet"
    if archivo_parquet.exists():
        try:
            return pd.read_parquet(archivo_parquet)
        except Exception:
            pass
    # CSV Fallback
    archivo_csv = RUTA_DATOS_CSV / f"{nombre_sub}_3min.csv"
    if archivo_csv.exists():
        try:
            df = pd.read_csv(archivo_csv)
            df["Timestamp"] = pd.to_datetime(df["Timestamp"])
            return df
        except Exception:
            return None
    return None


@st.cache_data(ttl=300)
def cargar_datos_clima(nombre_sub):
    """Carga datos de clima si existen."""
    archivo_clima = RUTA_CLIMA_PARQUET / f"{nombre_sub}_clima.parquet"
    if archivo_clima.exists():
        try:
            return pd.read_parquet(archivo_clima)
        except Exception:
            pass
    return None


def ejecutar_actualizacion():
    progress = st.progress(0)
    status = st.empty()
    try:
        status.info("📡 Conectando con CNDC...")
        progress.progress(10)
        subprocess.run([sys.executable, str(SCRIPT_SCRAP)], check=False)

        progress.progress(30)
        status.info("⚙️ Procesando datos de carga...")
        subprocess.run([sys.executable, str(SCRIPT_PROCESAR)], check=False)

        progress.progress(60)
        status.info("☁️ Descargando datos climáticos (Open-Meteo)...")
        subprocess.run([sys.executable, str(SCRIPT_CLIMA)], check=False)

        progress.progress(90)
        st.cache_data.clear()
        progress.progress(100)
        status.success("✅ Sistema Actualizado (Carga + Clima)")
        time.sleep(2)
        status.empty()
        progress.empty()
    except Exception as e:
        status.error(f"Error: {e}")


# --- COMPONENTES VISUALES ---


def tarjeta_kpi(titulo, valor, unidad, delta=None, color_delta="normal"):
    st.metric(
        label=titulo, value=f"{valor} {unidad}", delta=delta, delta_color=color_delta
    )


def grafico_principal(df, titulo, color_linea="#00f2ea", df_clima=None):
    # Crear figura base con Plotly Graph Objects para eje secundario
    fig = go.Figure()

    # 1. Área de Carga (Eje Izquierdo)
    fig.add_trace(
        go.Scatter(
            x=df["Timestamp"],
            y=df["MW"],
            name="Carga (MW)",
            mode="lines",
            line=dict(color=color_linea, width=2),
            fill="tozeroy",
            fillcolor=f"rgba{tuple(int(color_linea.lstrip('#')[i : i + 2], 16) for i in (0, 2, 4)) + (0.1,)}",
        )
    )

    # 2. Línea de Temperatura (Eje Derecho) - Si existe
    if df_clima is not None and not df_clima.empty:
        # Filtrar clima al mismo rango que df
        min_t = df["Timestamp"].min()
        max_t = df["Timestamp"].max()
        mask = (df_clima["Timestamp"] >= min_t) & (df_clima["Timestamp"] <= max_t)
        df_c_recorte = df_clima.loc[mask]

        if not df_c_recorte.empty:
            fig.add_trace(
                go.Scatter(
                    x=df_c_recorte["Timestamp"],
                    y=df_c_recorte["Temperatura_C"],
                    name="Temp (°C)",
                    mode="lines",
                    line=dict(
                        color="#ffeb3b", width=2, dash="dot"
                    ),  # Amarillo punteado
                    yaxis="y2",
                )
            )

    fig.update_layout(
        title=titulo,
        template="plotly_dark",
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        hovermode="x unified",
        margin=dict(l=20, r=20, t=40, b=20),
        height=400,
        yaxis=dict(title="Potencia (MW)"),
        yaxis2=dict(
            title="Temperatura (°C)", overlaying="y", side="right", showgrid=False
        ),
        legend=dict(orientation="h", y=1.1),
    )
    return fig


def grafico_heatmap(df):
    # Preparar datos para Heatmap: Hora vs Día
    df["Hora"] = df["Timestamp"].dt.hour
    df["Dia"] = df["Timestamp"].dt.date

    # Pivot table
    pivot = df.pivot_table(index="Dia", columns="Hora", values="MW", aggfunc="mean")

    fig = px.imshow(
        pivot,
        labels=dict(x="Hora del Día", y="Fecha", color="MW"),
        x=pivot.columns,
        y=pivot.index,
        color_continuous_scale="Viridis",  # Escala profesional
        aspect="auto",
        title="Mapa de Calor de Carga (Patrones Horarios)",
    )
    fig.update_layout(
        template="plotly_dark",
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
    )

    return fig


def grafico_duracion(df):
    df_dur = obtener_curva_duracion(df)
    fig = px.line(
        df_dur,
        x="Duración (%)",
        y="MW",
        title="Curva de Duración de Carga",
        template="plotly_dark",
    )
    fig.update_traces(line_color="#ff0055", line_width=3)
    fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        xaxis_title="Duración (%)",
        yaxis_title="Potencia (MW)",
    )
    # Añadir líneas guía
    pico = df_dur["MW"].max()
    fig.add_hline(
        y=pico,
        line_dash="dot",
        annotation_text="Pico",
        annotation_position="bottom right",
    )
    return fig


def vista_dashboard(seleccion):
    df = cargar_datos(seleccion)
    df_clima = cargar_datos_clima(seleccion)  # Cargar clima

    if df is None or df.empty:
        st.warning("Sin datos.")
        return

    # --- HEADER ---
    c1, c2 = st.columns([3, 1])
    with c1:
        st.title(f"{seleccion}")
        st.caption(f"Última sincronización: {df['Timestamp'].max()}")
    with c2:
        # Botón discreto de recarga
        if st.button("🔄 Refrescar"):
            st.cache_data.clear()
            st.rerun()

    st.markdown("---")

    # --- PESTAÑAS ---
    tab_hoy, tab_analisis, tab_historico = st.tabs(
        ["⚡ Operación Diaria", "� Análisis de Ingeniería", "� Histórico"]
    )

    # 1. OPERACIÓN DIARIA
    with tab_hoy:
        # Lógica de día fantasma
        fecha_max = df["Timestamp"].max().date()
        registros_hoy = df[df["Timestamp"].dt.date == fecha_max]
        if len(registros_hoy) < 2:
            fecha_visual = fecha_max - timedelta(days=1)
            aviso = (
                f"Mostrando cierre de ayer ({fecha_visual}) - Esperando datos de hoy."
            )
        else:
            fecha_visual = fecha_max
            aviso = None

        df_dia = df[df["Timestamp"].dt.date == fecha_visual]

        if not df_dia.empty:
            if aviso:
                st.info(aviso)

            # CÁLCULOS DE INGENIERÍA
            energia_dia = calcular_energia_mwh(df_dia)
            fc_dia = calcular_factor_carga(df_dia)
            pico_dia = df_dia["MW"].max()
            hora_pico = df_dia.loc[df_dia["MW"].idxmax(), "Timestamp"].strftime("%H:%M")

            # KPI ROW
            k1, k2, k3, k4 = st.columns(4)
            with k1:
                tarjeta_kpi("Pico del Día", f"{pico_dia:.2f}", "MW")
            with k2:
                tarjeta_kpi("Hora de Punta", hora_pico, "Hrs")
            with k3:
                tarjeta_kpi("Energía Total", f"{energia_dia:.2f}", "MWh")
            with k4:
                tarjeta_kpi("Factor de Carga", f"{fc_dia:.1f}", "%")

            # GRÁFICO PRINCIPAL (Con Clima)
            st.plotly_chart(
                grafico_principal(
                    df_dia, f"Curva de Carga - {fecha_visual}", df_clima=df_clima
                ),
                use_container_width=True,
            )

        else:
            st.warning("No hay datos para visualizar.")

    # 2. ANÁLISIS DE INGENIERÍA
    with tab_analisis:
        st.markdown("#### 🔬 Análisis de Comportamiento (Últimos 30 días)")

        # Filtro 30 días
        limite_30 = df["Timestamp"].max() - timedelta(days=30)
        df_30 = df[df["Timestamp"] >= limite_30]

        col_a1, col_a2 = st.columns(2)

        with col_a1:
            # Curva de Duración
            st.plotly_chart(grafico_duracion(df_30), use_container_width=True)
            st.caption(
                "**Curva de Duración:** Muestra qué porcentaje del tiempo la carga supera cierto valor. Fundamental para dimensionamiento."
            )

        with col_a2:
            # Heatmap
            st.plotly_chart(grafico_heatmap(df_30), use_container_width=True)
            st.caption(
                "**Mapa de Calor:** Identifica patrones visuales de demanda máxima (zonas amarillas/claras) a lo largo de los días."
            )

    # 3. HISTÓRICO
    with tab_historico:
        st.markdown("#### 📅 Tendencia a Largo Plazo")
        st.plotly_chart(
            grafico_principal(df, "Histórico Completo", "#00a8f2", df_clima=df_clima),
            use_container_width=True,
        )

        # Estadísticas Globales
        e_total = calcular_energia_mwh(df)
        st.metric("Energía Total Registrada en Histórico", f"{e_total:,.2f} MWh")


def vista_comparativo(lista_subs):
    st.title("🆚 Comparador de Subestaciones")
    subs = st.multiselect(
        "Selecciona hasta 3 subestaciones", lista_subs, max_selections=3
    )

    if subs:
        dfs = []
        for s in subs:
            d = cargar_datos(s)
            if d is not None:
                d["Subestacion"] = s
                dfs.append(d)

        if dfs:
            df_comp = pd.concat(dfs)

            # Gráfico Comparativo
            fig = px.line(
                df_comp,
                x="Timestamp",
                y="MW",
                color="Subestacion",
                template="plotly_dark",
                title="Comparativa de Potencia",
            )
            st.plotly_chart(fig, use_container_width=True)

            # Tabla Comparativa de Métricas
            st.markdown("### 📊 Tabla de Métricas (Promedios Históricos)")
            resumen = []
            for s in subs:
                d_sub = df_comp[df_comp["Subestacion"] == s]
                resumen.append(
                    {
                        "Subestación": s,
                        "Pico Máx (MW)": d_sub["MW"].max(),
                        "Promedio (MW)": d_sub["MW"].mean(),
                        "Factor Carga Global (%)": calcular_factor_carga(d_sub),
                    }
                )
            st.dataframe(pd.DataFrame(resumen).set_index("Subestación"))


def vista_configuracion():
    st.title("⚙️ Panel de Ingeniería")

    c1, c2 = st.columns(2)
    with c1:
        st.markdown("### 🔄 ETL Pipeline")
        st.info("Ejecutar manualmente si los datos automáticos no aparecen.")
        if st.button("Forzar Actualización de Datos"):
            ejecutar_actualizacion()

    with c2:
        st.markdown("### ℹ️ System Status")
        st.success("Sistema Operativo: Normal")
        st.text(f"Python Engine: {sys.version.split()[0]}")
        st.text("Backend: Parquet Optimized")
        st.text(f"Path: {RUTA_DATOS_PARQUET}")


# --- MAIN ---
def main():
    with st.sidebar:
        st.image(
            "https://cdn-icons-png.flaticon.com/512/2933/2933116.png", width=50
        )  # Icono genérico eléctrico
        st.title("DGA Monitor")
        st.caption("Advanced Analytics Module")
        st.markdown("---")

        menu = st.radio("MENÚ PRINCIPAL", ["Dashboard", "Comparativo", "Configuración"])

        st.markdown("---")

        lista_subs = cargar_lista_subestaciones()
        seleccion = None

        if menu == "Dashboard":
            st.markdown("### 📍 Ubicación")
            if lista_subs:
                seleccion = st.selectbox(
                    "Seleccionar SE", lista_subs, label_visibility="collapsed"
                )
            else:
                st.error("No Data Found")

    if menu == "Dashboard" and seleccion:
        vista_dashboard(seleccion)
    elif menu == "Comparativo":
        vista_comparativo(lista_subs)
    elif menu == "Configuración":
        vista_configuracion()


if __name__ == "__main__":
    main()
