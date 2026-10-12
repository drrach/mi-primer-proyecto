# Resolución — Práctica 2: Silver, Gold y orquestación

## Datos del alumno

- Nombre: Ricardo Aníbal Cheluja
- student_id: drrach
- Escala: small
- Catálogo y esquema: workspace.bigdata_drrach

## Job y dependencias

- Nombre exacto del Job: bigdata_2<student_id>_silver_gold
- URL del Job: [•	https://dbc-4b1b6668-73d5.cloud.databricks.com/jobs/302407002750599?o=7474651579613320]

Las tareas se ejecutan en este orden:

ingest_bronze → build_silver → build_gold → validate

Cada tarea posterior se ejecuta únicamente si su dependencia
terminó correctamente.

### DAG del Job

![DAG del Job con las cuatro tareas](dag_job.png)

## Primera y segunda ejecución

La segunda ejecución se realizó sin volver a ejecutar el generador,
manteniendo los mismos parámetros y archivos de entrada.

### Primera y segunda ejecución — batch_008

| Métrica | Primera ejecución | Segunda ejecución |
|---|---:|---:|
| Cantidad de transacciones en Silver | 51146 | 51146 |
| Cantidad de registros en cuarentena | 69 | 69 |
| Cantidad de filas de gold_daily_sales | 1003 | 1003 |
| Monto total en Gold | 50942317.39 | 50942317.39 |
| idempotence_compared | False | True |
| ID de ejecución | 1117264922659891 | 1072656153727934 |
| Estado de validate | Succeeded | Succeeded |

Las métricas de negocio fueron iguales en ambas ejecuciones del Job.
Entre ambas hubo ejecuciones interactivas de validación. La segunda
ejecución del Job comparó sus métricas con el registro inmediatamente
anterior de la auditoría y superó el control de igualdad.
La auditoría agregó una fila por cada ejecución de validate.

### Cantidades aceptadas y rechazadas — batch_008

| Lote | Aceptadas | Rechazadas |
|---|---:|---:|
| batch_008 | 200 | 2 |

Según gold_batch_summary, el lote batch_008 tiene 200 transacciones
aceptadas y 2 registros rechazados.

## Reconciliación de batch_008

| Concepto | Cantidad |
|---|---:|
| Filas físicas recibidas en Bronze | 204 |
| Aceptadas según el resumen del lote | 200 |
| Rechazadas según el resumen del lote | 2 |
| Transacciones vigentes en Silver del lote | 200 |

Las 200 transacciones aceptadas del resumen coinciden con las
200 transacciones vigentes en Silver del lote.

Bronze contiene 204 filas físicas. Las aceptadas y rechazadas suman
202, por lo que hay 2 filas adicionales cuya causa debe verificarse
en Bronze; estos conteos por sí solos no permiten determinar si son
duplicados o versiones anteriores.

Consultas utilizadas:

```sql
SELECT *
FROM workspace.bigdata_drrach.gold_batch_summary
WHERE source_batch_id = 'batch_008';

SELECT
    'Filas físicas recibidas en Bronze' AS concepto,
    COUNT(*) AS cantidad
FROM workspace.bigdata_drrach.bronze_transactions_incremental
WHERE source_batch_id = 'batch_008'

UNION ALL

SELECT
    'Transacciones vigentes en Silver del lote' AS concepto,
    COUNT(*) AS cantidad
FROM workspace.bigdata_drrach.silver_transactions
WHERE source_batch_id = 'batch_008';
```
## Por qué COPY INTO y MERGE resuelven problemas diferentes

COPY INTO controla la incorporación de archivos a Bronze y evita
volver a cargar un archivo ya procesado en condiciones normales.
Su unidad de control es el archivo.

MERGE compara los registros de entrada con los existentes mediante
la clave y las condiciones definidas en el pipeline. Permite insertar
transacciones nuevas y actualizar las existentes con correcciones.

Por eso, evitar repetir archivos no alcanza para resolver duplicados
o correcciones que llegan dentro de archivos nuevos. Para mantener
una sola versión por transaction_id también se necesita deduplicar
la entrada y aplicar correctamente el MERGE.

## Visualizaciones y respuestas

### Pregunta 1

**Enunciado:** ¿Qué día tuvo el mayor monto vendido y ese día también fue el de mayor cantidad de transacciones?

![Visualización 1](Evolucion_del_monto_vendido_y_transacciones.png)

**Respuesta:** 
El 1 de marzo de 2026 (2026-03-01) tuvo el mayor monto vendido, con 7.298.974,49 unidades monetarias.
Sí, ese mismo día también registró la mayor cantidad de transacciones: 7.225 operaciones. El ticket promedio fue de 1.010,24 unidades monetarias por transacción, aunque esto no implica que haya sido el ticket promedio más alto del período.

**Consulta utilizada:**

```# Agregar con Spark antes de convertir a Pandas.
from pyspark.sql import functions as F
import matplotlib.pyplot as plt

# Agregar con Spark antes de convertir a Pandas.
daily_totals = (
    spark.table(daily)
    .groupBy("sale_date")
    .agg(
        F.sum("total_amount").alias("monto_total"),
        F.sum("transaction_count").alias("transacciones")
    )
    .withColumn(
        "ticket_promedio",
        F.when(
            F.col("transacciones") > 0,
            F.col("monto_total") / F.col("transacciones")
        )
    )
    .orderBy("sale_date")
)

assert daily_totals.count() > 0, "La tabla Gold no contiene datos."

# Sólo el resultado agregado se convierte a Pandas.
plot_data = daily_totals.toPandas()
plot_data["monto_total"] = plot_data["monto_total"].astype(float)

fig, axes = plt.subplots(2, 1, figsize=(11, 7), sharex=True)

axes[0].plot(
    plot_data["sale_date"],
    plot_data["monto_total"],
    marker="o",
    color="#2563eb"
)
axes[0].set_title("Evolución diaria del monto vendido y las transacciones")
axes[0].set_ylabel("Monto vendido\n(unidades monetarias)")
axes[0].grid(alpha=0.25)

axes[1].plot(
    plot_data["sale_date"],
    plot_data["transacciones"],
    marker="o",
    color="#16a34a"
)
axes[1].set_ylabel("Transacciones\n(cantidad)")
axes[1].set_xlabel("Fecha de venta")
axes[1].grid(alpha=0.25)

fig.autofmt_xdate()
fig.tight_layout()
plt.show()

# Identificar máximos, incluyendo posibles empates.
max_monto = plot_data["monto_total"].max()
max_transacciones = plot_data["transacciones"].max()

dias_monto = set(
    plot_data.loc[
        plot_data["monto_total"] == max_monto, "sale_date"
    ]
)
dias_transacciones = set(
    plot_data.loc[
        plot_data["transacciones"] == max_transacciones, "sale_date"
    ]
)

def fechas(dias):
    return ", ".join(str(dia) for dia in sorted(dias))

print(
    f"Mayor monto vendido: {max_monto:,.2f} unidades monetarias "
    f"el día o los días {fechas(dias_monto)}."
)
print(
    f"Mayor cantidad de transacciones: {max_transacciones:,.0f} "
    f"el día o los días {fechas(dias_transacciones)}."
)

coinciden = dias_monto & dias_transacciones

if coinciden:
    print(
        f"Los máximos coinciden en {fechas(coinciden)}. "
        "Ese día se alcanzaron tanto el mayor monto como la mayor "
        "cantidad de operaciones. Esto no implica que el ticket "
        "promedio haya sido el más alto."
    )
else:
    print(
        "Los máximos no coinciden. El día de mayor monto tuvo menos "
        "operaciones que el día de mayor cantidad de transacciones, "
        "pero un ticket promedio más alto: cada operación aportó, "
        "en promedio, más dinero."
    )

print("\nTicket promedio en los días de máximo monto o cantidad:")
display(
    daily_totals.where(
        (F.col("monto_total") == F.lit(max_monto)) |
        (F.col("transacciones") == F.lit(int(max_transacciones)))
    )
)

```

### Pregunta 2

**Enunciado:** ¿Qué canal de pago presenta la mayor tasa de fraude? ¿La conclusión se sostiene al considerar el número de transacciones de cada canal?

![Visualización 2](Canal_y_fraude.png)

**Respuesta:** 
El canal transfer presenta la mayor tasa de fraude: 13,89 %, con 2.359 transacciones fraudulentas sobre 16.986 operaciones. Le siguen wallet, con aproximadamente 12,9 %, y card, con 12,5 %.
La conclusión se sostiene al considerar el volumen: los tres canales tienen cantidades similares, cercanas a 17.000 transacciones, y transfer representa el 33,2 % del total. Por lo tanto, su mayor tasa observada no proviene de un canal con muy pocas operaciones.
Sin embargo, el gráfico permite afirmar que transfer tiene la mayor tasa observada, pero no demuestra por sí solo que la diferencia sea estadísticamente significativa; para evaluarlo harían falta intervalos de confianza o una prueba de comparación de proporciones.

**Consulta utilizada:**

```# Tu solución. Recalculá la tasa global como suma(fraud_transactions) / suma(transaction_count).
# Evitá promediar directamente fraud_rate entre grupos.

from pyspark.sql import functions as F
import matplotlib.pyplot as plt

gold = spark.table(daily)

# Identificar el nombre de la columna de canal.
channel_col = next(
    (c for c in ["payment_channel", "channel", "payment_method"]
     if c in gold.columns),
    None
)

assert channel_col is not None, (
    f"No se encontró la columna de canal. Columnas: {gold.columns}"
)

# Agregar con Spark: no promediar fraud_rate.
channel_metrics = (
    gold.groupBy(channel_col)
    .agg(
        F.sum("fraud_transactions").alias("fraudes"),
        F.sum("transaction_count").alias("transacciones")
    )
    .where(F.col("transacciones") > 0)
    .withColumn(
        "tasa_fraude_pct",
        100.0 * F.col("fraudes") / F.col("transacciones")
    )
    .orderBy(F.desc("tasa_fraude_pct"), F.desc("transacciones"))
)

display(channel_metrics)

# Convertir únicamente el resultado agregado.
plot_data = channel_metrics.toPandas()
assert not plot_data.empty, "No hay transacciones para analizar."

plot_data["tasa_fraude_pct"] = (
    plot_data["tasa_fraude_pct"].astype(float)
)
labels = plot_data[channel_col].fillna("Sin canal").astype(str)

fig, axes = plt.subplots(1, 2, figsize=(12, 5), sharey=True)
positions = list(range(len(plot_data)))

axes[0].barh(
    positions, plot_data["tasa_fraude_pct"], color="#dc2626"
)
axes[0].set_yticks(positions)
axes[0].set_yticklabels(labels)
axes[0].invert_yaxis()
axes[0].set_title("Tasa de fraude por canal")
axes[0].set_xlabel("Transacciones fraudulentas (%)")
axes[0].set_ylabel("Canal de pago")
axes[0].grid(axis="x", alpha=0.25)

axes[1].barh(
    positions, plot_data["transacciones"], color="#2563eb"
)
axes[1].set_title("Volumen por canal")
axes[1].set_xlabel("Transacciones (cantidad)")
axes[1].grid(axis="x", alpha=0.25)

fig.suptitle("Canal de pago: fraude y volumen de operaciones")
fig.tight_layout()
plt.show()

# Tasa global: suma de fraudes / suma de transacciones.
totals = channel_metrics.agg(
    F.sum("fraudes").alias("fraudes"),
    F.sum("transacciones").alias("transacciones")
).first()

global_rate = 100 * totals.fraudes / totals.transacciones

max_rate = plot_data["tasa_fraude_pct"].max()
leaders = plot_data[
    plot_data["tasa_fraude_pct"] == max_rate
]

print(f"Tasa global de fraude: {global_rate:.2f}%.")

for _, row in leaders.iterrows():
    share = 100 * row["transacciones"] / totals.transacciones
    print(
        f"El canal {row[channel_col]} presenta la mayor tasa observada: "
        f"{row['tasa_fraude_pct']:.2f}%, con "
        f"{int(row['fraudes']):,} fraudes sobre "
        f"{int(row['transacciones']):,} transacciones "
        f"({share:.1f}% del volumen total)."
    )

print(
    "\nLa tasa permite comparar canales considerando su volumen. "
    "Sin embargo, si el canal líder tiene pocas transacciones, "
    "su tasa es más sensible a unos pocos casos y la conclusión "
    "es menos sólida. Un volumen elevado aporta más evidencia, "
    "aunque estos gráficos por sí solos no demuestran que la "
    "diferencia entre canales sea estadísticamente significativa."
)
```

### Pregunta 3 

**Enunciado:** ¿Qué combinación de país y categoría genera el mayor monto? ¿Existe una categoría dominante en todos los países o cambia según el mercado?

![Visualización 3](Monto.png) 

**Respuesta:** 
La combinación que genera el mayor monto vendido es Brasil (BR) y la categoría home, con 2.388.062,02 unidades monetarias. No existe una categoría dominante en todos los países: books lidera en Argentina, Chile y México, mientras que home encabeza las ventas en Brasil y Uruguay. En Chile, la diferencia entre ambas es pequeña: aproximadamente 8.655 unidades monetarias.

**Consulta utilizada:**

```from pyspark.sql import functions as F
import pandas as pd
import matplotlib.pyplot as plt

# Agregar con Spark antes de convertir a Pandas.
country_category = (
    spark.table(daily)
    .groupBy("country", "category")
    .agg(F.sum("total_amount").alias("monto_total"))
)

plot_data = country_category.toPandas()
assert not plot_data.empty, "No hay datos para analizar."

plot_data["monto_total"] = plot_data["monto_total"].astype(float)
plot_data["country"] = plot_data["country"].fillna("Sin país")
plot_data["category"] = plot_data["category"].fillna("Sin categoría")

matrix = plot_data.pivot_table(
    index="country",
    columns="category",
    values="monto_total",
    aggfunc="sum"
).sort_index()

# Los pares sin registros quedan vacíos, no se interpretan como cero.
fig, ax = plt.subplots(
    figsize=(max(9, len(matrix.columns) * 1.5),
             max(4, len(matrix.index) * 0.7))
)

heatmap = ax.imshow(
    matrix.to_numpy(dtype=float),
    cmap="Blues",
    aspect="auto"
)

ax.set_xticks(range(len(matrix.columns)))
ax.set_xticklabels(matrix.columns, rotation=45, ha="right")
ax.set_yticks(range(len(matrix.index)))
ax.set_yticklabels(matrix.index)
ax.set_title("Monto vendido por país y categoría")
ax.set_xlabel("Categoría")
ax.set_ylabel("País")

fig.colorbar(
    heatmap, ax=ax,
    label="Monto vendido (unidades monetarias)"
)

# Anotar los montos para facilitar la comparación.
threshold = plot_data["monto_total"].max() / 2

for i in range(len(matrix.index)):
    for j in range(len(matrix.columns)):
        value = matrix.iloc[i, j]
        if pd.notna(value):
            ax.text(
                j, i, f"{value:,.0f}",
                ha="center", va="center",
                color="white" if value > threshold else "black",
                fontsize=9
            )

fig.tight_layout()
plt.show()

# Máximo global, incluyendo posibles empates.
global_max = plot_data["monto_total"].max()
global_leaders = plot_data[
    plot_data["monto_total"] == global_max
]

print("MÁXIMO GLOBAL")
for _, row in global_leaders.iterrows():
    print(
        f"{row['country']} — {row['category']}: "
        f"{row['monto_total']:,.2f} unidades monetarias."
    )

# Categorías líderes por país, incluyendo empates.
country_max = plot_data.groupby("country")["monto_total"].transform("max")
leaders = plot_data[
    plot_data["monto_total"] == country_max
].sort_values(["country", "category"])

print("\nCATEGORÍAS LÍDERES POR PAÍS")
for _, row in leaders.iterrows():
    print(
        f"{row['country']}: {row['category']} "
        f"({row['monto_total']:,.2f} unidades monetarias)."
    )

leader_sets = leaders.groupby("country")["category"].apply(set)
common_leaders = set.intersection(*leader_sets.tolist())

print("\nCONCLUSIÓN")
if common_leaders:
    categories = ", ".join(sorted(common_leaders))
    print(
        f"{categories} alcanza el mayor monto en todos los países "
        "observados, considerando posibles empates."
    )
else:
    print(
        "La categoría dominante cambia según el mercado: "
        "no existe una categoría que lidere en todos los países."
    )

print(
    "\nEl mapa de calor es adecuado porque cruza países y categorías "
    "en una misma matriz y utiliza una escala de color común para "
    "comparar montos e identificar concentraciones de ventas. "
    "Las celdas vacías representan combinaciones sin registros."
)
```

### Pregunta 4

**Enunciado:** ¿Qué proporción de cada lote fue aceptada y rechazada? ¿El lote nuevo presenta una calidad diferente del lote inicial?

![Visualización 4](Calidad.png)

**Respuesta:** 


**Consulta utilizada:**

```from pyspark.sql import functions as F
import matplotlib.pyplot as plt

# Calcular cantidades y proporciones con Spark.
batch_quality = (
    spark.table(batch)
    .groupBy("source_batch_id")
    .agg(
        F.sum("accepted_transactions").alias("aceptadas"),
        F.sum("rejected_transactions").alias("rechazadas")
    )
    .withColumn("total", F.col("aceptadas") + F.col("rechazadas"))
    .withColumn(
        "aceptadas_pct",
        F.when(F.col("total") > 0, 100.0 * F.col("aceptadas") / F.col("total"))
    )
    .withColumn(
        "rechazadas_pct",
        F.when(F.col("total") > 0, 100.0 * F.col("rechazadas") / F.col("total"))
    )
    .orderBy("source_batch_id")
)

display(batch_quality)

# Convertir sólo el resumen a Pandas.
plot_data = batch_quality.where(F.col("total") > 0).toPandas()
assert not plot_data.empty, "No hay lotes con registros para comparar."

labels = plot_data["source_batch_id"].fillna("Sin lote").astype(str)

fig, ax = plt.subplots(figsize=(10, 5))

ax.bar(
    labels, plot_data["aceptadas_pct"],
    label="Aceptadas", color="#16a34a"
)
ax.bar(
    labels, plot_data["rechazadas_pct"],
    bottom=plot_data["aceptadas_pct"],
    label="Rechazadas", color="#dc2626"
)

for i, row in plot_data.iterrows():
    accepted_pct = float(row["aceptadas_pct"])
    rejected_pct = float(row["rechazadas_pct"])

    ax.text(
        i, accepted_pct / 2,
        f"{accepted_pct:.1f}%",
        ha="center", va="center", color="white"
    )
    ax.text(
        i, 102,
        f"Rechazo: {rejected_pct:.1f}%\nn={int(row['total']):,}",
        ha="center", va="bottom", fontsize=9
    )

ax.set_title("Calidad del pipeline: aceptación y rechazo por lote")
ax.set_xlabel("Lote")
ax.set_ylabel("Proporción de registros (%)")
ax.set_ylim(0, 115)
ax.set_yticks([0, 20, 40, 60, 80, 100])
ax.legend(loc="upper left", bbox_to_anchor=(1.02, 1))
ax.grid(axis="y", alpha=0.25)

fig.tight_layout()
plt.show()

print("RESULTADOS POR LOTE")
for _, row in plot_data.iterrows():
    print(
        f"{row['source_batch_id']}: "
        f"{int(row['aceptadas']):,} aceptadas "
        f"({row['aceptadas_pct']:.2f}%) y "
        f"{int(row['rechazadas']):,} rechazadas "
        f"({row['rechazadas_pct']:.2f}%). "
        f"Total: {int(row['total']):,}."
    )

# Completar con los identificadores reales del resumen.
initial_batch_id = "batch_001"
new_batch_id = "batch_003"  # Usar "batch_002" si ése es el lote nuevo.

initial = plot_data[
    plot_data["source_batch_id"] == initial_batch_id
]
new = plot_data[
    plot_data["source_batch_id"] == new_batch_id
]

print("\nCOMPARACIÓN")
if initial.empty or new.empty:
    print(
        "No se encontraron ambos lotes seleccionados. "
        "Revisá initial_batch_id y new_batch_id con los nombres "
        "que aparecen en la tabla. Si el lote inicial no está "
        "incluido en Gold, no puede compararse con esta fuente."
    )
else:
    initial_rate = float(initial.iloc[0]["rechazadas_pct"])
    new_rate = float(new.iloc[0]["rechazadas_pct"])
    difference = new_rate - initial_rate

    print(
        f"Rechazo inicial ({initial_batch_id}): {initial_rate:.2f}%. "
        f"Rechazo nuevo ({new_batch_id}): {new_rate:.2f}%."
    )

    if abs(difference) < 1e-9:
        print("Ambos lotes presentan la misma proporción de rechazo.")
    elif difference > 0:
        print(
            f"El lote nuevo presenta {difference:.2f} puntos "
            "porcentuales más de rechazo: menor aceptación "
            "bajo las reglas de calidad del pipeline."
        )
    else:
        print(
            f"El lote nuevo presenta {abs(difference):.2f} puntos "
            "porcentuales menos de rechazo: mayor aceptación "
            "bajo las reglas de calidad del pipeline."
        )

print(
    "\nLIMITACIONES: los porcentajes permiten comparar lotes de "
    "distintos tamaños, pero en lotes pequeños unos pocos rechazos "
    "pueden cambiar mucho la tasa. La comparación presupone las "
    "mismas reglas de validación. Además, una mayor aceptación no "
    "garantiza que todos los datos sean correctos; si el generador "
    "introduce errores deliberadamente, el rechazo refleja esa prueba."
)

```
**Respuesta:**
**Fuente:** `workspace.bigdata_drrach.gold_batch_summary`.

| Lote | Aceptadas | Rechazadas | Total | % aceptadas | % rechazadas |
|---|---:|---:|---:|---:|---:|
| Inicial (`initial`) | 49.882 | 51 | 49.933 | 99,90 % | 0,10 % |
| Batch 8 (`batch_008`) | 200 | 2 | 202 | 99,01 % | 0,99 % |
| Batch 9 (`batch_009`) | 200 | 2 | 202 | 99,01 % | 0,99 % |
| Batch 10 (`batch_010`) | 201 | 2 | 203 | 99,01 % | 0,99 % |

Las proporciones se calcularon utilizando:

- **Total:** aceptadas + rechazadas.
- **Porcentaje de aceptación:** aceptadas / total × 100.
- **Porcentaje de rechazo:** rechazadas / total × 100.

**Comparación:** los lotes nuevos presentan una mayor proporción de rechazo que el inicial: aproximadamente 0,99 % frente a 0,10 %, una diferencia cercana a 0,89 puntos porcentuales. Por lo tanto, muestran una menor aceptación según las reglas de validación del pipeline. Los tres lotes nuevos tienen proporciones prácticamente iguales.

En cantidades absolutas, el lote inicial tiene más rechazos: 51 frente a 2 por lote nuevo. Sin embargo, contiene muchos más registros, por lo que su tasa de rechazo es menor. Esto demuestra la importancia de comparar porcentajes además de cantidades.

**Limitaciones:** los tamaños son muy distintos. En un lote de unos 200 registros, cada rechazo representa aproximadamente 0,5 puntos porcentuales, por lo que unos pocos casos modifican mucho la tasa. La diferencia observada no demuestra por sí sola un deterioro sistemático de la calidad. La comparación también supone las mismas reglas de validación y debe considerar diferencias en la composición de los datos o errores introducidos deliberadamente por el generador. La aceptación no garantiza que todos los datos sean correctos.

## Las 20 preguntas de análisis y comprensión

### 1. Filas físicas recibidas por lote

En `bronze_transactions_incremental`, la cantidad de **filas físicas recibidas por lote** fue:

| Lote | Filas físicas |
|---|---:|
| batch_002 | 24 |
| batch_003 | 24 |
| batch_004 | 24 |
| batch_005 | 204 |
| batch_006 | 204 |
| batch_007 | 204 |
| batch_008 | 204 |
| batch_009 | 204 |
| batch_010 | 204 |
| **Total** | **1.296** |

La consulta utilizada fue:

```sql
SELECT
    source_batch_id AS lote,
    COUNT(*) AS filas_fisicas
FROM workspace.bigdata_drrach.bronze_transactions_incremental
GROUP BY source_batch_id
ORDER BY source_batch_id;
```

Se utiliza **`COUNT(*)`** porque se cuentan todas las filas recibidas, incluidos registros inválidos y versiones repetidas de una transacción. El lote `initial` no aparece en este resultado porque la consulta corresponde a la tabla de ingesta incremental.

### 2. Aceptadas, rechazadas y reconciliación con Gold

Los resultados por lote fueron:

| Lote | Aceptadas en Silver | Aceptadas en Gold | Rechazadas en cuarentena | Rechazadas en Gold | Reconciliación |
|---|---:|---:|---:|---:|---|
| initial | 49.882 | 49.882 | 51 | 51 | Coincide |
| batch_002 | 21 | 21 | 2 | 2 | Coincide |
| batch_003 | 21 | 21 | 2 | 2 | Coincide |
| batch_004 | 21 | 21 | 2 | 2 | Coincide |
| batch_005 | 200 | 200 | 2 | 2 | Coincide |
| batch_006 | 200 | 200 | 2 | 2 | Coincide |
| batch_007 | 200 | 200 | 2 | 2 | Coincide |
| batch_008 | 200 | 200 | 2 | 2 | Coincide |
| batch_009 | 200 | 200 | 2 | 2 | Coincide |
| batch_010 | 201 | 201 | 2 | 2 | Coincide |

**La reconciliación coincide en todos los lotes:** el número de transacciones vigentes en `silver_transactions` es igual a `accepted_transactions` de `gold_batch_summary`, y el número de registros en `silver_transactions_quarantine` coincide con `rejected_transactions`.

Estos conteos corresponden al **estado actual de Silver**, que conserva una única versión vigente por transacción. Por eso, la suma de aceptadas y rechazadas no necesariamente coincide con las filas físicas de Bronze, donde también se conservan versiones repetidas.

La consulta utilizada fue:

```sql
WITH aceptadas AS (
    SELECT
        source_batch_id,
        COUNT(*) AS aceptadas
    FROM silver_transactions
    GROUP BY source_batch_id
),
rechazadas AS (
    SELECT
        source_batch_id,
        COUNT(*) AS rechazadas
    FROM silver_transactions_quarantine
    GROUP BY source_batch_id
),
observado AS (
    SELECT
        COALESCE(a.source_batch_id, r.source_batch_id)
            AS source_batch_id,
        COALESCE(a.aceptadas, 0) AS aceptadas,
        COALESCE(r.rechazadas, 0) AS rechazadas
    FROM aceptadas a
    FULL OUTER JOIN rechazadas r
        ON a.source_batch_id = r.source_batch_id
)
SELECT
    COALESCE(o.source_batch_id, g.source_batch_id) AS lote,
    COALESCE(o.aceptadas, 0) AS aceptadas_silver,
    g.accepted_transactions AS aceptadas_gold,
    COALESCE(o.rechazadas, 0) AS rechazadas_cuarentena,
    g.rejected_transactions AS rechazadas_gold,
    CASE
        WHEN g.source_batch_id IS NULL THEN 'SIN RESUMEN GOLD'
        WHEN
            COALESCE(o.aceptadas, 0) = g.accepted_transactions
            AND
            COALESCE(o.rechazadas, 0) = g.rejected_transactions
        THEN 'COINCIDE'
        ELSE 'REVISAR'
    END AS reconciliacion
FROM observado o
FULL OUTER JOIN gold_batch_summary g
    ON o.source_batch_id = g.source_batch_id
ORDER BY
    CASE
        WHEN COALESCE(o.source_batch_id, g.source_batch_id)
            = 'initial' THEN 0
        ELSE 1
    END,
    lote;
```

La columna `reconciliacion` debe mostrar **`COINCIDE`** para todos los lotes.

**Atención:** Silver conserva la versión vigente. Una corrección posterior puede cambiar el lote al que pertenece una transacción; estos conteos no necesariamente equivalen al número de registros aceptados durante cada ingesta histórica.

### 3. Motivos de rechazo por lote

En cuarentena aparecen dos motivos:

- `INVALID_AMOUNT`: importe inválido; los registros observados tienen `amount = 'N/A'`.
- `UNKNOWN_CUSTOMER`: identificador de cliente sin correspondencia en la dimensión de clientes.

| Lote | INVALID_AMOUNT | UNKNOWN_CUSTOMER | Total rechazadas |
|---|---:|---:|---:|
| initial | 51 | 0 | 51 |
| batch_002 | 1 | 1 | 2 |
| batch_003 | 1 | 1 | 2 |
| batch_004 | 1 | 1 | 2 |
| batch_005 | 1 | 1 | 2 |
| batch_006 | 1 | 1 | 2 |
| batch_007 | 1 | 1 | 2 |
| batch_008 | 1 | 1 | 2 |
| batch_009 | 1 | 1 | 2 |
| batch_010 | 1 | 1 | 2 |
| **Total** | **60** | **9** | **69** |

```sql
SELECT
    source_batch_id AS lote,
    quality_reason AS motivo_rechazo,
    COUNT(*) AS registros
FROM workspace.bigdata_drrach.silver_transactions_quarantine
GROUP BY source_batch_id, quality_reason
ORDER BY
    CASE WHEN source_batch_id = 'initial' THEN 0 ELSE 1 END,
    source_batch_id,
    quality_reason;
```

Los lotes incrementales muestran un importe `N/A` y un cliente desconocido por lote. Los clientes rechazados tienen `customer_id = 1099` en los lotes 002–004 y `5999` en los lotes 005–010.

Los resultados son compatibles con los casos inválidos introducidos por el generador, aunque confirmar la coincidencia exacta requiere revisar su código. En `initial`, los 51 rechazos corresponden exclusivamente a importes inválidos.

### 4. Versiones de la transacción 42

La transacción `42` tiene **10 versiones en Bronze**: una del lote `initial` y nueve de los lotes `batch_002` a `batch_010`.

| transaction_id | source_batch_id | amount | updated_at |
|---|---|---:|---|
| 42 | initial | 600.11 | 2026-03-06 02:07:52 |
| 42 | batch_002 | 1999.99 | 2026-03-10 02:00:00 |
| 42 | batch_003 | 1999.99 | 2026-03-11 02:00:00 |
| 42 | batch_004 | 1999.99 | 2026-03-12 02:00:00 |
| 42 | batch_005 | 1999.99 | 2026-03-13 02:00:00 |
| 42 | batch_006 | 1999.99 | 2026-03-14 02:00:00 |
| 42 | batch_007 | 1999.99 | 2026-03-15 02:00:00 |
| 42 | batch_008 | 1999.99 | 2026-03-16 02:00:00 |
| 42 | batch_009 | 1999.99 | 2026-03-17 02:00:00 |
| **42** | **batch_010** | **1999.99** | **2026-03-18 02:00:00** |

En Silver quedó vigente la versión de `batch_010`, correspondiente al mayor `updated_at`. `transaction_id` identifica la transacción, `source_batch_id` permite rastrear el origen y `amount` muestra la corrección de 600.11 a 1999.99.

```sql
-- Versiones recibidas en Bronze.
SELECT
    transaction_id,
    amount,
    source_batch_id,
    event_ts,
    updated_at,
    device_id,
    is_fraud
FROM workspace.bigdata_drrach.bronze_transactions_all
WHERE TRY_CAST(transaction_id AS BIGINT) = 42
ORDER BY updated_at DESC;

-- Cantidad de versiones.
SELECT COUNT(*) AS versiones_en_bronze
FROM workspace.bigdata_drrach.bronze_transactions_all
WHERE TRY_CAST(transaction_id AS BIGINT) = 42;

-- Versión vigente.
SELECT *
FROM workspace.bigdata_drrach.silver_transactions
WHERE transaction_id = 42;
```

Bronze conserva las diez versiones para trazabilidad; Silver mantiene la versión vigente.

### 5. Una sola fila por transacción

```sql
SELECT
    transaction_id,
    COUNT(*) AS cantidad
FROM workspace.bigdata_drrach.silver_transactions
GROUP BY transaction_id
HAVING COUNT(*) > 1
ORDER BY cantidad DESC;
```

El resultado esperado es cero filas. Si la consulta devuelve algún identificador con `cantidad > 1`, la deduplicación falló.

El control complementario obtuvo:

| Control | Resultado |
|---|---:|
| Filas en Silver | 51.146 |
| Identificadores distintos | 51.146 |
| Identificadores nulos | 0 |
| Validación | OK: IDs UNICOS Y NO NULOS |

```sql
SELECT
    COUNT(*) AS filas_silver,
    COUNT(DISTINCT transaction_id) AS ids_distintos,
    COUNT_IF(transaction_id IS NULL) AS ids_nulos,
    CASE
        WHEN COUNT(*) = COUNT(DISTINCT transaction_id)
         AND COUNT_IF(transaction_id IS NULL) = 0
        THEN 'OK: IDs UNICOS Y NO NULOS'
        ELSE 'REVISAR'
    END AS resultado
FROM workspace.bigdata_drrach.silver_transactions;
```

La igualdad entre filas e identificadores distintos, junto con la ausencia de IDs nulos, confirma que la deduplicación funcionó en el estado actual de la tabla.

### 6. Tasa de rechazo por lote

**Tasa de rechazo (%) = rechazadas / (aceptadas + rechazadas) × 100**

| Lote | Aceptadas | Rechazadas | Total | Tasa de rechazo |
|---|---:|---:|---:|---:|
| initial | 49.882 | 51 | 49.933 | 0,1021 % |
| batch_002 | 21 | 2 | 23 | 8,6957 % |
| batch_003 | 21 | 2 | 23 | 8,6957 % |
| batch_004 | 21 | 2 | 23 | 8,6957 % |
| batch_005 | 200 | 2 | 202 | 0,9901 % |
| batch_006 | 200 | 2 | 202 | 0,9901 % |
| batch_007 | 200 | 2 | 202 | 0,9901 % |
| batch_008 | 200 | 2 | 202 | 0,9901 % |
| batch_009 | 200 | 2 | 202 | 0,9901 % |
| batch_010 | 201 | 2 | 203 | 0,9852 % |

```sql
SELECT
    source_batch_id AS lote,
    accepted_transactions AS aceptadas,
    rejected_transactions AS rechazadas,
    accepted_transactions + rejected_transactions AS total,
    ROUND(
        100.0 * rejected_transactions /
        NULLIF(accepted_transactions + rejected_transactions, 0),
        4
    ) AS tasa_rechazo_pct
FROM workspace.bigdata_drrach.gold_batch_summary
ORDER BY
    CASE WHEN source_batch_id = 'initial' THEN 0 ELSE 1 END,
    source_batch_id;
```

No es correcto comparar solamente cantidades absolutas cuando los lotes tienen tamaños diferentes. El lote inicial tiene 51 rechazos, pero representan apenas el 0,1021 %. Dos rechazos representan el 8,6957 % en un lote de 23 registros y aproximadamente el 0,99 % en uno de 202.

Conviene comparar cantidades y proporciones conjuntamente. En lotes pequeños, unos pocos rechazos pueden modificar considerablemente la tasa.

### 7. Día con mayor monto y mayor cantidad de transacciones

El **1 de marzo de 2026** presenta ambos máximos:

| Fecha | Monto total | Transacciones | Ticket promedio |
|---|---:|---:|---:|
| 2026-03-01 | 7.298.974,49 | 7.225 | 1.010,24 |

```sql
WITH diario AS (
    SELECT
        sale_date,
        SUM(total_amount) AS monto,
        SUM(transaction_count) AS transacciones
    FROM workspace.bigdata_drrach.gold_daily_sales
    GROUP BY sale_date
),
ranking AS (
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY monto DESC) AS ranking_monto,
        DENSE_RANK() OVER (
            ORDER BY transacciones DESC
        ) AS ranking_transacciones
    FROM diario
)
SELECT
    sale_date AS fecha,
    monto,
    transacciones,
    ROUND(monto / NULLIF(transacciones, 0), 2) AS ticket_promedio,
    ranking_monto = 1 AS maximo_monto,
    ranking_transacciones = 1 AS maximo_transacciones
FROM ranking
WHERE ranking_monto = 1 OR ranking_transacciones = 1
ORDER BY sale_date;
```

Los máximos coinciden: `maximo_monto` y `maximo_transacciones` son ambos `true`. Esto no implica que también sea el día con mayor ticket promedio.

### 8. Canal con mayor tasa global de fraude

El canal **`transfer`** presenta la mayor tasa global de fraude.

| Canal | Fraudes | Transacciones | Tasa global de fraude |
|---|---:|---:|---:|
| transfer | 2.359 | 16.986 | **13,8879 %** |
| wallet | 2.200 | 16.988 | 12,9503 % |
| card | 2.150 | 17.172 | 12,5204 % |

```sql
SELECT
    payment_channel AS canal,
    SUM(fraud_transactions) AS fraudes,
    SUM(transaction_count) AS transacciones,
    ROUND(
        100.0 * SUM(fraud_transactions) /
        NULLIF(SUM(transaction_count), 0),
        4
    ) AS fraude_pct
FROM workspace.bigdata_drrach.gold_daily_sales
GROUP BY payment_channel
ORDER BY fraude_pct DESC;
```

No corresponde promediar directamente `fraud_rate` porque cada fila puede representar un volumen diferente. Un promedio simple daría el mismo peso a un grupo de 10 operaciones que a uno de 1.000.

La fórmula `SUM(fraud_transactions) / SUM(transaction_count)` pondera correctamente por el volumen y obtiene la proporción global de fraude de cada canal.

### 9. Máximo global y líder por país

La combinación **Brasil (`BR`) y `home`** concentra el mayor monto vendido: **2.388.062,02 unidades monetarias**.

| País | Categoría líder | Monto vendido |
|---|---|---:|
| Brasil (BR) | home | **2.388.062,02** |
| Uruguay (UY) | home | 2.317.724,80 |
| Argentina (AR) | books | 2.282.795,70 |
| México (MX) | books | 2.254.517,99 |
| Chile (CL) | books | 2.200.165,90 |

```sql
WITH ventas AS (
    SELECT
        country,
        category,
        SUM(total_amount) AS monto
    FROM workspace.bigdata_drrach.gold_daily_sales
    GROUP BY country, category
),
ranking AS (
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY monto DESC) AS ranking_global,
        DENSE_RANK() OVER (
            PARTITION BY country ORDER BY monto DESC
        ) AS ranking_pais
    FROM ventas
)
SELECT
    country AS pais,
    category AS categoria_lider,
    monto,
    ranking_global = 1 AS maximo_global
FROM ranking
WHERE ranking_pais = 1
ORDER BY monto DESC;
```

La categoría líder cambia según el mercado: `home` lidera en Brasil y Uruguay, mientras que `books` encabeza las ventas en Argentina, México y Chile.

### 10. Auditoría de la reejecución de batch_008

Se compararon las ejecuciones de `batch_008` registradas el 9 de octubre de 2026 a las 02:41 y 19:22 UTC.

| Métrica | Ejecución 02:41 UTC | Reejecución 19:22 UTC |
|---|---:|---:|
| silver_rows | 51.146 | 51.146 |
| quarantine_rows | 69 | 69 |
| gold_rows | 1.003 | 1.003 |
| gold_total_amount | 50.942.317,39 | 50.942.317,39 |
| idempotence_compared | false | **true** |

Permanecen iguales los conteos de Silver, cuarentena y Gold, y el monto total de Gold. Cambian `job_run_id` y `recorded_at`, porque corresponden a ejecuciones diferentes.

La columna **`idempotence_compared = true`** demuestra que se realizó la comparación de idempotencia. La igualdad de las métricas evidencia que la reejecución conservó esos resultados.

Estas son las dos primeras filas mostradas para ese lote en el CSV, no las dos primeras cronológicamente. Existe una ejecución anterior, a las 02:16 UTC, con métricas diferentes.

```sql
SELECT
    job_run_id,
    expected_batch_id,
    silver_rows,
    quarantine_rows,
    gold_rows,
    gold_total_amount,
    recorded_at,
    idempotence_compared
FROM workspace.bigdata_drrach.pipeline_run_audit
WHERE expected_batch_id = 'batch_008'
  AND job_run_id IN (
      '1117264922659891',
      '1072656153727934'
  )
ORDER BY recorded_at;
```

### 11. Qué problema resuelve COPY INTO

`COPY INTO` resuelve la ingesta incremental de archivos en Bronze: incorpora archivos nuevos y permite reejecutar la carga sin volver a insertar los archivos ya procesados.

Utiliza el registro de archivos previamente cargados que mantiene en la tabla Delta de destino, con los metadatos que identifican los archivos de origen.

Esta protección funciona a nivel de archivo. Si una transacción aparece en archivos diferentes, puede ingresar varias veces a Bronze. La deduplicación por `transaction_id` se realiza después en Silver.

### 12. Por qué se utiliza UNION ALL

La vista `bronze_transactions_all` utiliza `UNION ALL` para conservar todas las filas de la carga inicial y de los lotes incrementales, incluidas las distintas versiones de una transacción. Esto mantiene la trazabilidad.

Los duplicados de negocio se resuelven en Silver mediante reglas explícitas por `transaction_id`, considerando `updated_at` y los criterios de desempate.

Además, `UNION` sólo elimina filas completamente iguales: no resuelve versiones de una transacción que tienen importes o fechas diferentes.

### 13. Uso de initial y event_ts como updated_at

Las transacciones iniciales reciben `source_batch_id = 'initial'` para identificar su origen histórico y distinguirlas de los lotes incrementales.

Como la carga inicial no tiene una fecha específica de actualización, se utiliza `event_ts` como `updated_at`, estableciendo una referencia temporal para comparar versiones.

En la transacción `42`, la versión inicial tiene importe `600.11` y `updated_at = '2026-03-06 02:07:52'`. Las correcciones posteriores pueden reemplazarla en Silver. Quedó vigente `batch_010`, con importe `1999.99` y `updated_at = '2026-03-18 02:00:00'`, la fecha más reciente.

### 14. Ventaja de try_cast

`try_cast` devuelve `NULL` cuando un valor como `'N/A'` no puede convertirse a un tipo numérico. Esto permite continuar el procesamiento, clasificar el registro como `INVALID_AMOUNT` y enviarlo a cuarentena.

Un `cast` convencional puede generar un error y detener la ejecución, según la configuración del motor.

### 15. Asignación de una única quality_reason

Si un registro viola varias reglas, recibe el motivo de la primera condición que se cumple, según el orden del `CASE` o de la cadena de `when`.

Por ejemplo, si tiene importe inválido y cliente desconocido, recibe `INVALID_AMOUNT` si se evalúa primero el importe, o `UNKNOWN_CUSTOMER` si se verifica primero el cliente.

El orden establece la prioridad y afecta los conteos por motivo. Estos conteos reflejan el motivo asignado, no todas las fallas presentes en cada registro.

### 16. Construcción de _record_key y uso de row_number

`_record_key` utiliza el `transaction_id` convertido a número cuando la conversión es válida. Si no puede convertirse, utiliza un hash calculado a partir de los campos del registro.

`row_number()` particiona por `_record_key` y ordena las versiones según los criterios del pipeline, como `updated_at` descendente. Se conserva la fila con `row_number = 1`.

El hash permite distinguir registros con identificadores inválidos. Sin esa alternativa, podrían agruparse todos bajo `transaction_id = NULL`, conservando sólo uno y perdiendo otros registros que deberían estar disponibles para cuarentena.

Los campos exactos del hash y los criterios de desempate deben verificarse en el código del notebook.

### 17. Cláusulas principales del MERGE

El `MERGE` compara los registros entrantes con los existentes mediante `transaction_id`.

- **`WHEN MATCHED ... UPDATE`**: actualiza una transacción existente cuando se cumple la condición de actualización. Si se exige un `updated_at` posterior, sólo una versión más reciente reemplaza a la vigente.
- **`WHEN NOT MATCHED ... INSERT`**: inserta una transacción cuyo `transaction_id` todavía no existe en Silver.

Así se incorporan transacciones nuevas y se aplican correcciones a las existentes.

### 18. Reconstrucción de Gold y MERGE en Silver

Gold se reconstruye completamente porque contiene agregados del estado vigente de Silver. Recalcularlos simplifica la incorporación de nuevas transacciones y correcciones que modifican montos o grupos.

Silver se actualiza con `MERGE` porque conserva el detalle y necesita insertar transacciones nuevas o actualizar las existentes.

| Estrategia | Ventaja | Limitación |
|---|---|---|
| Reconstrucción completa de Gold | Recalcula los agregados de forma sencilla y consistente con Silver, incluyendo correcciones. | Reprocesa todos los datos, aumentando tiempo y costo al crecer el volumen. |
| MERGE en Silver | Permite insertar y corregir transacciones sin reemplazar toda la tabla. | Exige deduplicar la fuente y definir correctamente la clave y las condiciones de actualización. |

### 19. Función de expected_batch_id

`expected_batch_id` es un parámetro de validación; no participa en la detección del archivo nuevo.

La tarea `01_ingest_bronze_incremental`, mediante `COPY INTO`, descubre los archivos nuevos en la ubicación de entrada e incorpora `batch_009`, evitando repetir archivos ya cargados.

La tarea `04_validate_pipeline` utiliza `expected_batch_id = 'batch_009'` para comprobar que el lote esperado fue procesado y aparece en Bronze, Silver y Gold.

El parámetro indica qué lote validar, pero no determina qué archivo cargar.

### 20. Consecuencias de una falla en build_silver

Si `build_silver` falla, `build_gold` y `validate` no se ejecutan, siempre que sus dependencias estén configuradas para ejecutarse sólo cuando las tareas anteriores finalizan correctamente.

`build_gold` depende del éxito de `build_silver`, y `validate` depende del éxito de `build_gold`. Esto evita reconstruir Gold después de una actualización fallida de Silver y validar como completa una ejecución que no terminó.

Las tablas Gold de una ejecución anterior pueden seguir existiendo, pero no se actualizan ni se validan como resultado de la ejecución fallida.
````
```sql
-- Pegar consulta real.
```

[Repetir esta estructura para las preguntas 2 a 20.]
