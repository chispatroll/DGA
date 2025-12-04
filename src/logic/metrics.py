import pandas as pd


def calcular_pico_historico(df):
    """
    Calcula el pico máximo histórico de potencia.
    Retorna: (max_mw, fecha_pico, hora_pico, dias_desde_record)
    """
    # 1. Encontrar el índice (fila) donde está el valor máximo
    indice_max = df["Max_Diario_MW"].idxmax()

    # 2. Extraer el valor y la fecha de esa fila
    max_mw = df.loc[indice_max, "Max_Diario_MW"]
    fecha_pico = df.loc[indice_max, "Fecha_Real"]
    hora_pico = df.loc[indice_max, "Hora_Pico_Reg"]

    # 3. Calcular días desde el récord
    fecha_pico_dt = pd.to_datetime(fecha_pico)
    dias_desde_record = (pd.Timestamp.now() - fecha_pico_dt).days

    return max_mw, fecha_pico, hora_pico, dias_desde_record


def calcular_ultimo_pico_diario(df):
    """
    Obtiene el último pico diario registrado (penúltimo registro para asegurar día completo o lógica actual).
    Retorna: (ultimo_max_diario, fecha_ultimo_pico, hora_ultimo_pico)
    """
    # Usamos iloc[-2] como en la lógica original
    ultimo_max_diario = df.iloc[-2]["Max_Diario_MW"]
    fecha_ultimo_pico = df.iloc[-2]["Fecha_Real"]
    hora_ultimo_pico = df.iloc[-2]["Hora_Pico_Reg"]

    return ultimo_max_diario, fecha_ultimo_pico, hora_ultimo_pico


def calcular_estado_carga(df_meta, seleccion, ultimo_max_diario):
    """
    Calcula la capacidad total y el porcentaje de carga.
    Retorna: (capacidad_total_mva, porcentaje_carga)
    """
    capacidad_total_mva = 0
    porcentaje_carga = 0

    if df_meta is not None:
        # Filtrar por subestación y solo los que están En Servicio
        trafos_sub = df_meta[
            (df_meta["Subestacion"] == seleccion) & (df_meta["En_Servicio"] == "SI")
        ]
        if not trafos_sub.empty:
            capacidad_total_mva = trafos_sub["Potencia_Nominal_MVA"].sum()
            if capacidad_total_mva > 0:
                porcentaje_carga = (ultimo_max_diario / capacidad_total_mva) * 100

    return capacidad_total_mva, porcentaje_carga
