import plotly.express as px


def crear_grafico_potencia(df, nombre_subestacion):
    """
    Crea un gráfico de línea para la potencia (MW) usando Plotly Express.
    """
    fig = px.line(
        df, x="Timestamp", y="MW", title=f"Curva de Carga - {nombre_subestacion}"
    )
    return fig
