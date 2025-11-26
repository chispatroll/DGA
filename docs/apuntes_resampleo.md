# SCIPY
1. *¿Qué es SciPy y por qué lo pide Pandas?*

SciPy (Scientific Python) es una librería de matemáticas avanzadas. Es el "cerebro matemático" pesado de Python.

    -Pandas es como un Contador: Sabe organizar tablas, sumar, restar y sacar promedios.
    -SciPy es como un Ingeniero Físico: Sabe cálculo integral, ecuaciones diferenciales y polinomios complejos.

2. *¿Qué hace exactamente aquí?*

Pandas sabe que quieres dibujar una curva entre dos puntos, pero no sabe la fórmula matemática para dibujar curvas suaves (como PCHIP o Spline). Solo sabe dibujar líneas rectas. Por eso, cuando le pides method='pchip', Pandas llama por teléfono a SciPy y le dice: "Oye, cálculame los puntos de esta curva, tú que sabes de polinomios"

## RESAMPLEO

El Resampleo (Resampling) es el proceso de cambiar la frecuencia de tus datos.  

    -Upsampling: Aumentar la frecuencia de tus datos.
    -Downsampling: Disminuir la frecuencia de tus datos.

### Paso A: La Creación de la Grilla (El Esqueleto)

Cuando ejecutas:

```df.resample("3min").asfreq()```

Pandas crea filas vacías entre los datos que ya existen. No calcula nada todavía, solo "abre huecos".

Hora,MW,Estado
01:00,100,Dato Real
01:03,NaN,Hueco Vacío
01:06,NaN,Hueco Vacío
...,...,...
01:57,NaN,Hueco Vacío
02:00,160,Dato Real

### Paso B: Interpolación

*¿De dónde saca esos datos?*
Los saca de una Función Matemática que asume cómo se comportó la electricidad en ese tiempo que no mediste.

> **Vamos a ver la matemática de las dos opciones:**

#### Opción 1: Lineal (La regla de tres simple)

Este método asume que la carga eléctrica sube o baja a una velocidad constante (línea recta).

    - Matemática detrás (Ejemplo 100 MW a 160 MW en 60 min)
        - Diferencia de Carga: 160 - 100 = 60 MW 
        - Diferencia de Tiempo: 60 minutos 
        - Pendiente (m): 60 MW / 60 min = 1 MW/min (Sube 1 MW cada minuto)

    -Cálculo para el minuto 03 (01:03):
        -MW = MW_inicial + (minutos_pasados * pendiente)
        -MW = 100 + (3 * 1) = 103 MW

    -Veredicto
        -Pros: Es fácil de calcular y "seguro" (no inventa cosas raras).
        -Contras: Es irreal para la electricidad. La carga no cambia en ángulos rectos.
        -Efecto visual: Genera picos puntiagudos (forma de "V" invertida) en las horas exactas cuando la tendencia cambia.
    
#### Opción 2: PCHIP (La curva inteligente)
Significado: Piecewise Cubic Hermite Interpolating Polynomial (Polinomio de Interpolación de Hermite Cúbico a Trozos).
A diferencia de la lineal, PCHIP mira a los vecinos (el dato anterior y el siguiente) para decidir el ángulo y la suavidad de la curva.

    **Escenario de Ejemplo**

        01:00: 100 MW (Subiendo fuerte)

        02:00: 150 MW (Llega a un pico máximo)

        03:00: 150 MW (Se mantiene estable/plano)

    **Diferencia de Comportamiento**

    1. Lo que hace la Lineal:

        Sube recto como una flecha hasta 150.

        Al llegar a las 02:00, gira bruscamente a la derecha para seguir plano.

    Resultado: Se forma un ángulo agudo (pico) en las 02:00.

    2. Lo que hace PCHIP (Matemáticamente):

        Calcula la derivada (la inclinación) en cada punto.

        Detecta que a las 01:00 la tendencia es subir.

        Detecta que a las 03:00 la tendencia es plana (pendiente 0).

        Acción: Calcula una curva suave que empieza subiendo, pero va frenando suavemente a medida que se acerca a las 02:00 para "aterrizar" horizontalmente y conectarse con el tramo plano sin golpes.

    3. Cálculo comparativo a las 01:57 (Justo antes de llegar):
        Lineal: Diría 149.5 MW. (Llega a toda velocidad y choca de golpe con el techo de 150).
        PCHIP: Diría 149.9 MW. (Dibuja una curva que ya está casi horizontal, preparando la llegada suave).