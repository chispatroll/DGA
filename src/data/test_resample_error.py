import pandas as pd
import numpy as np

# 1. Crear datos simulados (Horarios)
fechas = pd.date_range(start="2025-01-01 00:00", periods=24, freq="h")
# Una curva de carga típica (baja en la madrugada, sube en la mañana, pico en la noche)
valores = [
    50,
    48,
    47,
    46,
    47,
    50,
    55,
    60,
    62,
    63,
    62,
    61,
    60,
    61,
    62,
    64,
    66,
    68,
    70,
    69,
    67,
    64,
    60,
    55,
]

df_original = pd.DataFrame({"Timestamp": fechas, "MW": valores})
df_original.set_index("Timestamp", inplace=True)

# 2. Resamplear a 3 min usando PCHIP (Igual que tu script)
df_resampled = df_original.resample("3min").asfreq()
df_resampled["MW"] = df_resampled["MW"].interpolate(method="pchip")

# 3. Comparar
# Extraemos del resampleado SOLO los puntos que coinciden con las horas originales
df_comparacion = df_resampled.loc[df_original.index]

# Calculamos la diferencia
diferencia = df_comparacion["MW"] - df_original["MW"]
max_error = diferencia.abs().max()

print(f"🔍 ANÁLISIS DE ERROR DE RESAMPLEO (PCHIP)")
print("-" * 40)
print(f"Datos originales: {len(df_original)} puntos (cada 1 hora)")
print(f"Datos resampleados: {len(df_resampled)} puntos (cada 3 min)")
print("-" * 40)
print(f"Diferencia máxima en los puntos originales: {max_error:.20f} MW")

if max_error < 1e-9:
    print("\n✅ CONCLUSIÓN: El error es CERO (o despreciable por redondeo).")
    print("   La curva pasa EXACTAMENTE por tus datos originales.")
else:
    print("\n⚠️ OJO: Hay diferencias significativas.")
