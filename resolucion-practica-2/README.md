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

````
**1. Filas físicas recibidas por lote**

En bronze_transactions_incremental, la cantidad de **filas físicas recibidas por lote** fue:

| **Lote** | **Filas físicas** |
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

Se utiliza **COUNT(*)** porque se cuentan todas las filas recibidas, incluidos registros inválidos y versiones repetidas de una transacción. El lote initial no aparece en este resultado porque la consulta corresponde a la tabla de ingesta incremental.

**2. Aceptadas, rechazadas y reconciliación con Gold**

Los resultados por lote fueron:

| **Lote** | **Aceptadas en Silver** | **Aceptadas en Gold** | **Rechazadas en cuarentena** | **Rechazadas en Gold** | **Reconciliación** |
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

**La reconciliación coincide en todos los lotes:** el número de transacciones vigentes en silver_transactions es igual a accepted_transactions de gold_batch_summary, y el número de registros en silver_transactions_quarantine coincide con rejected_transactions.

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

La columna coincide debe ser true para todos los lotes.

**Atención:** Silver conserva la versión vigente. Una corrección posterior puede cambiar el lote al que pertenece una transacción; estos conteos no necesariamente equivalen al número de registros aceptados durante cada ingesta histórica.

**3. Motivos de rechazo por lote**

En silver_transactions_quarantine aparecen **dos motivos de rechazo**:

- **INVALID_AMOUNT**: importe inválido; los registros observados tienen amount = 'N/A'.

- **UNKNOWN_CUSTOMER**: identificador de cliente sin correspondencia en la dimensión de clientes.

| **Lote** | **INVALID_AMOUNT** | **UNKNOWN_CUSTOMER** | **Total rechazadas** |
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

**Los lotes incrementales muestran el patrón esperado de los casos inválidos del generador:** un importe N/A y un cliente desconocido por lote. Los clientes rechazados tienen customer_id = 1099 en los lotes 002–004 y 5999 en los lotes 005–010.

Los resultados son **compatibles con esos casos introducidos**, aunque confirmar la coincidencia exacta requiere revisar el código del generador. En el lote initial, los 51 rechazos corresponden exclusivamente a importes inválidos.

La consulta utilizada para resumirlos es:

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

**4. Versiones de la transacción 42**

La transacción **42 tiene 10 versiones en bronze_transactions_all**: una del lote initial y nueve de los lotes incrementales batch_002 a batch_010.

| **transaction_id** | **source_batch_id** | **amount** | **updated_at** |
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

En **silver_transactions quedó vigente únicamente la versión de batch_010**, según el resultado de Silver.

Esta elección corresponde a la versión con el **mayor updated_at**. La columna transaction_id identifica la misma transacción, source_batch_id permite rastrear su origen y amount muestra la corrección del importe original de **600.11 a 1999.99**. Bronze conserva las diez versiones para trazabilidad, mientras que Silver mantiene la versión vigente.

La consulta utilizada es:

```sql
-- Todas las versiones recibidas.

SELECT *
FROM bronze_transactions_all
WHERE TRY_CAST(transaction_id AS BIGINT) = 42
ORDER BY updated_at DESC;

-- Cantidad de filas/versiones recibidas en Bronze.

SELECT
    COUNT(*) AS versiones_en_bronze
FROM bronze_transactions_all
WHERE TRY_CAST(transaction_id AS BIGINT) = 42;

-- Versión vigente en Silver.

SELECT *
FROM silver_transactions
WHERE transaction_id = 42;
```

**5. Una sola fila por transacción**

Para comprobar que silver_transactions tiene una sola fila por transaction_id, se puede hacer:

```sql
SELECT
    transaction_id,
    COUNT(*) AS cantidad
FROM workspace.bigdata_drrach.silver_transactions
GROUP BY transaction_id
HAVING COUNT(*) > 1
ORDER BY cantidad DESC;
```

**El resultado esperado es cero filas:** significa que ningún transaction_id aparece más de una vez.

**La deduplicación habría fallado si la consulta devuelve algún registro**, porque ese identificador tiene dos o más filas en Silver. Por ejemplo, transaction_id = 42 con cantidad = 2 indicaría un duplicado.

El segundo control complementa la verificación: debería mostrar **filas_silver = ids_distintos**, **ids_nulos = 0** y **OK: IDs UNICOS Y NO NULOS**.

El control confirma que silver_transactions tiene una sola fila por transaction_id. La cantidad de filas coincide con la cantidad de identificadores distintos y no hay IDs nulos. Por lo tanto, **la deduplicación funcionó correctamente en el estado actual de la tabla**.

**6. Tasa de rechazo por lote**

La tasa de rechazo se calculó como:

**Tasa de rechazo (%) = rechazadas / (aceptadas + rechazadas) × 100**

**No es correcto comparar solamente cantidades absolutas cuando los lotes tienen tamaños diferentes.** El lote inicial tiene más rechazos —51—, pero estos representan apenas el **0,1021 %** de sus registros. En cambio, dos rechazos representan el **8,6957 %** en un lote de 23 registros y aproximadamente el **0,99 %** en uno de 202.

Por lo tanto, el lote inicial presenta la **menor proporción de rechazos**. Conviene comparar cantidades y tasas conjuntamente, considerando que en lotes pequeños unos pocos registros pueden modificar considerablemente el porcentaje.

La consulta utilizada fue:

```sql
SELECT
    source_batch_id AS lote,
    accepted_transactions AS aceptadas,
    rejected_transactions AS rechazadas,
    accepted_transactions + rejected_transactions AS total,
    ROUND(
        100.0 * accepted_transactions /
        NULLIF(
            accepted_transactions + rejected_transactions,
            0
        ),
        4
    ) AS aceptadas_pct,
    ROUND(
        100.0 * rejected_transactions /
        NULLIF(
            accepted_transactions + rejected_transactions,
            0
        ),
        4
    ) AS rechazadas_pct
FROM gold_batch_summary
ORDER BY
    CASE WHEN source_batch_id = 'initial' THEN 0 ELSE 1 END,
    source_batch_id;

-- Comparar cada lote incremental con initial.
-- Diferencia positiva: mayor rechazo que initial.
-- Diferencia negativa: menor rechazo que initial.

WITH tasas AS (
    SELECT
        source_batch_id,
        accepted_transactions + rejected_transactions AS total,
        100.0 * rejected_transactions /
        NULLIF(
            accepted_transactions + rejected_transactions,
            0
        ) AS rechazo_pct
    FROM gold_batch_summary
),
inicial AS (
    SELECT *
    FROM tasas
    WHERE source_batch_id = 'initial'
)
SELECT
    t.source_batch_id AS lote_nuevo,
    i.total AS total_initial,
    t.total AS total_lote_nuevo,
    ROUND(i.rechazo_pct, 4) AS rechazo_initial_pct,
    ROUND(t.rechazo_pct, 4) AS rechazo_nuevo_pct,
    ROUND(
        t.rechazo_pct - i.rechazo_pct,
        4
    ) AS diferencia_puntos_porcentuales
FROM tasas t
CROSS JOIN inicial i
WHERE t.source_batch_id <> 'initial'
ORDER BY t.source_batch_id;
```

**7. Día con mayor monto y mayor cantidad de transacciones**

El **1 de marzo de 2026 (2026-03-01)** presenta ambos máximos en gold_daily_sales:

| **Fecha** | **Monto total** | **Cantidad de transacciones** | **Ticket promedio** |
|---|---:|---:|---:|
| 2026-03-01 | 7.298.974,49 | 7.225 | 1.010,24 |

**Los máximos coinciden:** ese día registró tanto el mayor monto vendido como la mayor cantidad de operaciones. Esto se confirma porque las columnas maximo_monto y maximo_transacciones son ambas true.

Esta coincidencia no implica que también haya sido el día con el mayor ticket promedio.

La consulta utilizada es:

```sql
WITH diario AS (
    SELECT
        sale_date,
        SUM(total_amount) AS monto,
        SUM(transaction_count) AS transacciones
    FROM gold_daily_sales
    GROUP BY sale_date
),
ranking AS (
    SELECT
        *,
        DENSE_RANK() OVER (
            ORDER BY monto DESC
        ) AS ranking_monto,
        DENSE_RANK() OVER (
            ORDER BY transacciones DESC
        ) AS ranking_transacciones
    FROM diario
)
SELECT
    sale_date AS fecha,
    monto,
    transacciones,
    ROUND(
        monto / NULLIF(transacciones, 0),
        2
    ) AS ticket_promedio,
    ranking_monto = 1 AS maximo_monto,
    ranking_transacciones = 1 AS maximo_transacciones
FROM ranking
WHERE ranking_monto = 1
   OR ranking_transacciones = 1
ORDER BY sale_date;
```

**8. Canal con mayor tasa global de fraude**

El canal **transfer tiene la mayor tasa global de fraude: 13,8879 %**, con **2.359 fraudes sobre 16.986 transacciones**. **No corresponde promediar directamente fraud_rate** porque cada fila puede representar una cantidad diferente de transacciones. Un promedio simple daría el mismo peso a un grupo de 10 operaciones que a uno de 1.000. La división entre las sumas pondera correctamente cada grupo por su volumen y obtiene la proporción real de fraude del canal.

La consulta utilizada fue:

```sql
WITH canales AS (
    SELECT
        payment_channel,
        SUM(fraud_transactions) AS fraudes,
        SUM(transaction_count) AS transacciones
    FROM gold_daily_sales
    GROUP BY payment_channel
)
SELECT
    payment_channel AS canal,
    fraudes,
    transacciones,
    ROUND(
        100.0 * fraudes / NULLIF(transacciones, 0),
        4
    ) AS fraude_pct,
    ROUND(
        100.0 * transacciones /
        NULLIF(SUM(transacciones) OVER (), 0),
        2
    ) AS participacion_volumen_pct
FROM canales
ORDER BY fraude_pct DESC, transacciones DESC;
```

**9. Máximo global y líder por país**

La combinación **Brasil (BR) y la categoría home** concentra el mayor monto vendido: **2.388.062,02 unidades monetarias**.

Las combinaciones líderes dentro de cada país son:

| **País** | **Categoría líder** | **Monto vendido** |
|---|---|---:|
| Brasil (BR) | home | **2.388.062,02** |
| Uruguay (UY) | home | 2.317.724,80 |
| Argentina (AR) | books | 2.282.795,70 |
| México (MX) | books | 2.254.517,99 |
| Chile (CL) | books | 2.200.165,90 |

**La categoría líder cambia según el país:** home encabeza las ventas en Brasil y Uruguay, mientras que books lidera en Argentina, México y Chile.

La consulta utilizada fue:

```sql
WITH ventas AS (
    SELECT
        country,
        category,
        SUM(total_amount) AS monto
    FROM gold_daily_sales
    GROUP BY country, category
),
ranking AS (
    SELECT
        *,
        DENSE_RANK() OVER (
            ORDER BY monto DESC
        ) AS ranking_global,
        DENSE_RANK() OVER (
            PARTITION BY country
            ORDER BY monto DESC
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

**10. Auditoría de la reejecución de batch_008**

Al comparar las **dos primeras filas de batch_008 que aparecen en el CSV**, las siguientes métricas permanecen iguales:

| **Métrica** | **Ejecución 09/10 a las 02:41 UTC** | **Reejecución 09/10 a las 19:22 UTC** |
|---|---:|---:|
| silver_rows | 51.146 | 51.146 |
| quarantine_rows | 69 | 69 |
| gold_rows | 1.003 | 1.003 |
| gold_total_amount | 50.942.317,39 | 50.942.317,39 |
| idempotence_compared | false | **true** |

**Los conteos y el monto total no cambian**, lo que evidencia que la reejecución conservó esas métricas sin incrementar los datos. Permanecen iguales estas métricas:

- **silver_rows**: 51.146 filas.

- **quarantine_rows**: 69 filas.

- **gold_rows**: 1.003 filas.

- **gold_total_amount**: 50.942.317,39 unidades monetarias.

La columna **idempotence_compared**, con valor **true** en la reejecución, demuestra que se realizó la comparación de idempotencia. La igualdad de esas métricas confirma que se conservaron los conteos y el monto total.

**11. ¿Qué resuelve COPY INTO?**

COPY INTO resuelve la **ingesta incremental de archivos en Bronze**: incorpora archivos nuevos y permite reejecutar la carga sin volver a insertar los archivos ya procesados.

Para evitar cargar dos veces el mismo archivo físico, utiliza el **registro de archivos previamente cargados que mantiene en la tabla Delta de destino**, con los metadatos que identifican los archivos de origen.

Esta protección funciona **a nivel de archivo**: si una misma transacción aparece en dos archivos diferentes, puede ingresar dos veces a Bronze. La deduplicación por transaction_id se resuelve después en Silver.

**12. ¿Por qué UNION ALL?**

La vista **bronze_transactions_all usa UNION ALL para conservar todas las filas** de la carga inicial y de los lotes incrementales, incluidas las distintas versiones de una misma transacción. Esto mantiene la trazabilidad y permite auditar qué datos llegaron desde cada origen.

Los **duplicados de negocio se resuelven en Silver**, donde se aplican reglas explícitas por transaction_id para seleccionar la versión vigente, considerando updated_at y los criterios de desempate.

Eliminar duplicados en Bronze podría ocultar información del historial. Además, UNION sólo elimina filas completamente iguales: no resuelve versiones de una misma transacción con importes o fechas diferentes.

**13. ¿Por qué initial y event_ts como updated_at?**

Las transacciones iniciales reciben **source_batch_id = 'initial'** para identificar que provienen de la carga histórica y distinguirlas de los lotes incrementales.

Como esa carga no tiene una fecha específica de actualización, se utiliza **event_ts como updated_at**, estableciendo una fecha de referencia para comparar versiones.

En la transacción **42**, la versión inicial tiene importe **600.11** y updated_at = 2026-03-06 02:07:52. Las correcciones incrementales tienen fechas posteriores, por lo que pueden reemplazarla en Silver. Finalmente, queda vigente la versión de **batch_010**, con importe **1999.99** y updated_at = 2026-03-18 02:00:00, la más reciente de las diez versiones.

**14. Ventaja de try_cast**

**try_cast devuelve NULL cuando un importe como 'N/A' no puede convertirse a un tipo numérico**, permitiendo que el pipeline continúe y que la regla de calidad clasifique el registro como INVALID_AMOUNT y lo envíe a cuarentena.

En cambio, un **cast convencional puede generar un error y detener la ejecución**, según la configuración del motor.

Así, try_cast permite tratar el dato inválido como un rechazo controlado, sin interrumpir el procesamiento de los demás registros.

**15. Una única quality_reason**

Si un registro viola más de una regla, recibe **el motivo de la primera condición que se cumple**, según el orden del CASE o de la cadena de when.

Por ejemplo, si tiene un importe inválido y un cliente desconocido, recibirá INVALID_AMOUNT si esa condición se evalúa primero, o UNKNOWN_CUSTOMER si primero se verifica el cliente.

**El orden establece la prioridad de los motivos y afecta los conteos por quality_reason.** Por eso, esos conteos reflejan el motivo asignado a cada registro, no todas las fallas que puede contener.

**16. \_record_key y row_number**

**\_record_key se construye usando el transaction_id convertido a número cuando la conversión es válida.** Si no puede convertirse, se utiliza como alternativa un **hash calculado a partir de los campos del registro**.

Luego, row_number() agrupa las filas mediante PARTITION BY \_record_key y ordena sus versiones según los criterios del pipeline, como updated_at descendente. Se conserva la fila con **row_number = 1**. row_number particiona por esa clave y ordena las versiones para seleccionar una fila por grupo. **La fórmula exacta del hash y las columnas del orden deben citarse desde el código.**

El hash cubre los registros cuyo identificador es inválido o no numérico. Sin esa alternativa, todos podrían quedar agrupados bajo transaction_id = NULL, y la deduplicación conservaría sólo uno, perdiendo otros registros inválidos que deberían quedar disponibles para la cuarentena.

Los campos exactos del hash y los criterios de desempate dependen de la implementación del notebook.

**17. Las cláusulas del MERGE**

El MERGE de silver_transactions compara los registros entrantes con los existentes mediante **transaction_id**:

- **WHEN MATCHED ... UPDATE**: actualiza una fila cuando la transacción ya existe y se cumple la condición de actualización. Si se exige un updated_at posterior, sólo una versión más reciente reemplaza a la vigente; por ejemplo, una corrección de la transacción 42.

- **WHEN NOT MATCHED ... INSERT**: inserta una fila cuando el transaction_id entrante todavía no existe en Silver.

Así, el MERGE permite incorporar transacciones nuevas y actualizar las existentes con sus correcciones.

**18. Reconstrucción Gold frente a MERGE en Silver**

Las tablas **Gold se reconstruyen completamente** porque contienen agregados calculados a partir del estado vigente de Silver. Recalcularlos simplifica la incorporación de nuevas transacciones y correcciones que pueden modificar montos, fechas o grupos.

**Silver se actualiza con MERGE** porque conserva el detalle de cada transacción: permite insertar nuevos transaction_id y actualizar los existentes.

| **Estrategia** | **Ventaja** | **Limitación** |
|---|---|---|
| **Reconstrucción completa de Gold** | Recalcula los agregados de forma sencilla y consistente con Silver, incluyendo correcciones. | Reprocesa todos los datos en cada ejecución, aumentando el tiempo y el costo cuando crece el volumen. |
| **Actualización de Silver con MERGE** | Permite insertar y corregir transacciones sin reemplazar toda la tabla. | Exige deduplicar la fuente y definir correctamente la clave y las condiciones de actualización para evitar versiones incorrectas. |

**19. Función de expected_batch_id**

**expected_batch_id no participa en la detección del archivo nuevo porque es un parámetro de validación, no un filtro de ingesta.**

La tarea **01_ingest_bronze_incremental**, mediante COPY INTO, descubre los archivos nuevos en la ubicación de entrada e incorpora los registros de **batch_009**, usando el historial de archivos cargados para evitar repetir la ingesta.

La tarea **04_validate_pipeline** utiliza **expected_batch_id = 'batch_009'** para comprobar que el lote esperado fue procesado y aparece en Bronze, Silver y Gold.

Por eso, cambiar el parámetro indica **qué lote debe validar el pipeline**, pero no determina qué archivo nuevo debe cargar.

**20. Si falla build_silver**

Si **build_silver falla**, **build_gold no se ejecuta y validate tampoco**, siempre que las dependencias estén configuradas para ejecutarse sólo cuando las tareas anteriores terminan correctamente.

En el DAG, build_gold depende del éxito de build_silver, y validate depende del éxito de build_gold. Así se evita reconstruir Gold a partir de una actualización fallida de Silver y validar como completa una ejecución que no terminó.

**Las tablas Gold de una ejecución anterior pueden seguir existiendo**, pero no se actualizan ni se validan como resultado de la ejecución fallida.
````
