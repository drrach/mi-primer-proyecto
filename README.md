# mi-primer-proyecto
proyecto del curso Big Data ITBA
Soy Ricardo Anibal Cheluja
# mi objetivo
Quiero organizar mis trabajos de BigData
## Mi primer avance
Hoy cree mi repositorio y guarde mi primer comité

Este es un cambio de prueba

## Clase 1: Ingesta y capa Bronze

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
