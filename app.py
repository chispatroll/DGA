import streamlit as st

# Importamos módulos propios
# Al estar en la raíz, podemos importar directamente desde src
from src.config import RUTA_DATOS
from src.etl import loader
from src.logic import metrics
from src.ui import cards, charts

# --- 1. CONFIGURACIÓN BÁSICA ---
st.set_page_config(page_title="Mi Primer Dashboard", layout="wide")


# --- 2. INTERFAZ DE USUARIO (UI) ---
def main():
    st.title("📊 Visualizador Simple de Datos")
    st.write("Esta es una versión simplificada para aprender y modificar.")

    # --- BARRA LATERAL (SIDEBAR) ---
    st.sidebar.header("Opciones")

    # Paso 1: Obtener lista de subestaciones
    lista_subs = loader.obtener_subestaciones()

    if not lista_subs:
        st.error(
            "No encontré archivos de datos en la carpeta data/SE_Carga_3min_parquet"
        )
        return

    # Paso 2: Selector
    seleccion = st.sidebar.selectbox("Selecciona una Subestación:", lista_subs)

    # --- ÁREA PRINCIPAL ---
    df = loader.cargar_datos_subestacion(seleccion)
    df_meta = loader.cargar_metadatos_trafos()

    if df is not None:
        # 3. Layout: st.tabs (Pestañas para organizar)
        tab_grafico, tab_datos, tab_detalles, tab_info = st.tabs(
            ["Gráfico", "Tabla de Datos", "Detalles Técnicos", "Información General"]
        )

        with tab_grafico:
            st.subheader("Tendencia de Potencia")

            # Usamos el módulo de charts
            fig = charts.crear_grafico_potencia(df, seleccion)
            st.plotly_chart(fig, width="stretch")

            # Métricas debajo del gráfico
            col1, col2 = st.columns(2)

            # --- Métrica 1: Pico Histórico ---
            max_mw, fecha_pico, hora_pico, dias_desde_record = (
                metrics.calcular_pico_historico(df)
            )

            delta_valor = f"Hace {dias_desde_record} días"
            delta_color = (
                "#ff4b4b" if dias_desde_record < 7 else "#09ab3b"
            )  # Rojo si fue reciente

            # Usamos el módulo de cards
            html_card_1 = cards.generar_html_tarjeta(
                titulo="Pico Máximo Histórico",
                valor=max_mw,
                unidad="MW",
                subtitulo=f"{fecha_pico} {hora_pico}",
                delta_valor=f"{delta_valor} ▲",
                delta_color=delta_color,
            )
            col1.markdown(html_card_1, unsafe_allow_html=True)

            # --- Métrica 2: Último Pico Diario y Carga ---
            ultimo_max_diario, fecha_ultimo_pico, hora_ultimo_pico = (
                metrics.calcular_ultimo_pico_diario(df)
            )
            capacidad_total_mva, porcentaje_carga = metrics.calcular_estado_carga(
                df_meta, seleccion, ultimo_max_diario
            )

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

            # Usamos el módulo de cards
            html_card_2 = cards.generar_html_tarjeta(
                titulo="Último Pico Diario",
                valor=ultimo_max_diario,
                unidad="MW",
                subtitulo=f"{fecha_ultimo_pico} {hora_ultimo_pico}",
                delta_valor=delta_valor_2,
                delta_color=delta_color_2,
            )
            col2.markdown(html_card_2, unsafe_allow_html=True)

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
                width="stretch",
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


# --- 3. PUNTO DE ENTRADA ---
if __name__ == "__main__":
    main()
