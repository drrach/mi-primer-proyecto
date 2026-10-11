
## Práctica 1 : Ingesta y capa Bronze

### Observaciones sobre los formatos

1. Los archivos CSV se leyeron inicialmente con todas sus columnas como texto. En transacciones, `amount` siguió siendo `string` incluso con `inferSchema=true`, por lo que no conviene confiar en la inferencia para definir su tipo.
2. El archivo Parquet conservó tipos definidos, como `product_id` de tipo `long` y `price` de tipo `decimal(12,2)`. El JSON de eventos permitió leer un campo anidado (`context`).
3. Las cuatro fuentes se guardaron como tablas Delta Bronze. Delta permite consultar metadatos e historial de la tabla con `DESCRIBE DETAIL` y `DESCRIBE HISTORY`. En Bronze se conservan los datos recibidos; la limpieza corresponde a Silver.

### Diagnóstico de calidad
https://docs.github.com/github/writing-on-github/getting-started-with-writing-and-formatting-on-github/basic-writing-and-formatting-syntax
La tabla `bronze_transactions` tiene [rows] filas, [distinct_ids] identificadores de transacción distintos y [invalid_amounts] importes que no pudieron convertirse a `DECIMAL(12,2)`.

### Las cinco V en esta práctica

- **Volumen:** con la escala `small` se generaron 200.000 eventos y 50.011 transacciones.
- **Velocidad:** los eventos y transacciones pueden incorporarse a medida que se producen; en esta práctica se hizo una carga de archivos, sin medir procesamiento en tiempo real.
- **Variedad:** se usaron CSV, Parquet y JSON, incluido un campo anidado en los eventos.
- **Veracidad:** hay identificadores potencialmente duplicados e importes que requieren validación antes de usarlos en análisis.
- **Valor:** después de controlar la calidad y preparar Silver, los datos pueden servir para analizar transacciones, productos, clientes y eventos.

## Desafio
Consigna 1:
1. Apareció el campo app_version.
2. El valor nuevo de event_type es refund.
3. El lote nuevo contiene 151 filas y 150 valores distintos de event_id. Por lo tanto, hay un identificador repetido en el lote.

Consigna 2: 
Construí bronze_events_v2 uniendo los 3.000 eventos de bronze_events con el lote nuevo de 151 filas. La unión conserva app_version y deja NULL en los registros históricos, que no tenían esa columna. Agregué _source, _source_file y _ingested_at al lote nuevo; los históricos conservan los metadatos de su primera ingesta. Luego seleccioné un solo registro por event_id, priorizando el lote nuevo cuando hubiera coincidencias. El resultado tiene 3.150 filas y 3.150 IDs distintos. Al repetir la ejecución, el total permaneció en 3.150. La tabla original bronze_events siguió con 3.000 filas.

from pyspark.sql import functions as F

v2 = spark.table("workspace.bigdata_drrach.bronze_events_v2")

print("Históricos con app_version informado:",
      v2.filter((F.col("_source") == "events_json") &
                F.col("app_version").isNotNull()).count())

print("Filas sin metadatos:",
      v2.filter(
          F.col("_source").isNull() |
          F.col("_source_file").isNull() |
          F.col("_ingested_at").isNull()
      ).count())

Consigna 3:
result = spark.table("workspace.bigdata_drrach.bronze_events_v2")

total_rows = result.count()
distinct_ids = result.select("event_id").distinct().count()

assert total_rows == distinct_ids, "Hay event_id duplicados"
assert "app_version" in result.columns, "No se preservó la evolución del esquema"
assert result.filter("event_type = 'refund'").count() > 0, "No se incorporó refund"

print(f"Verificación superada: {total_rows} filas y {distinct_ids} IDs distintos")

Reflexión final:
Detectar una evolución de esquema significa comparar el lote nuevo con los datos existentes e identificar cambios, como la aparición de app_version y del evento refund. Aceptarla técnicamente implica adaptar la integración para conservar la nueva columna, asignar NULL a los registros históricos y evitar duplicados por event_id. Decidir si es válida para el negocio requiere confirmar que refund es un tipo de evento permitido, definir el significado de app_version y verificar que sus valores cumplen las reglas del sistema. Que los datos puedan almacenarse correctamente no garantiza que deban utilizarse en los análisis.
