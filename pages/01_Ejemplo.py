import streamlit as st

st.title("📄 Página de Ejemplo")
st.write("¡Hola! Esta es una página extra.")

st.info(
    """
    Al poner este archivo en la carpeta `pages/`, Streamlit automáticamente 
    crea un menú de navegación en la barra lateral.
    
    Es ideal para separar:
    - 🏠 Inicio (Resumen)
    - 📈 Análisis Detallado
    - ⚙️ Configuración
    """
)

st.metric(label="Temperatura Simulada", value="24 °C", delta="1.2 °C")
