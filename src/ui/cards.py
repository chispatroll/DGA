def generar_html_tarjeta(titulo, valor, unidad, subtitulo, delta_valor, delta_color):
    """
    Genera el HTML para una tarjeta de métrica con estilo oscuro.
    """
    html = f"""
    <div style="background-color: #1E1E1E; padding: 10px; border-radius: 5px; border: 1px solid #333;" title="{titulo}">
        <div style="display: flex; justify-content: space-between; align-items: center;">
            <p style="color: #888; font-size: 14px; margin: 0;">{titulo}</p>
            <span style="font-size: 12px; cursor: help;">❔</span>
        </div>
        <p style="color: #FFF; font-size: 24px; font-weight: bold; margin: 5px 0;">
            {valor:.2f} {unidad}
        </p>
        <div style="display: flex; justify-content: space-between; align-items: center;">
            <p style="color: #AAA; font-size: 12px; margin: 0;"> {subtitulo}</p>
            <span style="color: {delta_color}; font-size: 12px; font-weight: bold;">
                {delta_valor}
            </span>
        </div>
    </div>
    """
    return html
