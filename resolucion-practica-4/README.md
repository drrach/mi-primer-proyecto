# Clase práctica 4 — Streaming: logs, ventanas y garantías

- **Nombre:** Ricardo Anibal Cheluja
- **student_id:** drrach
- **Escala:** small

## Log y offsets

### 1. Cola tradicional y log

Después de leer tres mensajes, quedaron **2 mensajes en la cola** y **5 mensajes en el log**.

En la cola del ejemplo, consumir elimina los mensajes. En un log como Kafka, leer no los elimina: cada consumidor avanza su offset y puede volver a leerlos mientras sigan disponibles.

### 2. Consumidores después de la llegada 2

| Consumidor | Mensajes leídos acumulados |
|---|---:|
| Antifraude | 180 |
| Reportes | 0 |

El atraso de reportes no afectó a antifraude porque cada consumidor tiene su propio checkpoint y avanza de forma independiente sobre el mismo log.

### 3. Offset y persistencia

Un offset identifica la posición de lectura de un consumidor.

En esta práctica, cada consulta lo guarda en su propio checkpoint:

- `streaming_lab/log/checkpoints/antifraude`
- `streaming_lab/log/checkpoints/reportes`

Al finalizar, ambos indicaban **6**, la próxima versión a leer, mientras la última versión del topic era **5**.

### 4. Replay, Kappa y Lambda

El replay leyó **280 mensajes**, correspondientes a todo el contenido del topic desde el inicio.

**Kappa** propone reprocesar el log con una nueva consulta utilizando el mismo camino de streaming.

**Lambda** mantiene dos caminos, uno batch y otro streaming, con lógica que debe mantenerse en ambos.

## Ingesta y enriquecimiento

### 5. Filas nuevas y lectura incremental

| Llegada | Filas nuevas en Bronze |
|---|---:|
| stream_001 | 60 |
| stream_002 | 120 |
| stream_003 | 40 |
| stream_004 | 40 |
| stream_005 | 20 |
| Reejecución sin archivos nuevos | 0 |

Bronze procesó **280 filas en total**.

Auto Loader, mediante el estado guardado en su checkpoint, recuerda los archivos procesados y evita volver a leerlos.

### 6. Cuarentena y conservación del original

En `stream_002` aparecieron los siguientes registros en cuarentena:

| Motivo | Cantidad |
|---|---:|
| INVALID_AMOUNT: importe inválido | 20 |
| UNKNOWN_CUSTOMER: cliente inexistente | 20 |
| **Total** | **40** |

Bronze conserva el texto original para mantener la trazabilidad, investigar errores y reprocesar los datos si se corrigen las reglas o cambia el contrato.

### 7. Enriquecimiento y tipo de join

El join agregó:

- `country`, desde `silver_customers`.
- `category`, desde `silver_products`.

Es un **join stream-tabla**, porque cruza eventos del flujo con tablas estáticas de referencia mediante `customer_id` y `product_id`.

## Tiempo y ventanas

### 8. Event time y processing time

**Event time** es el momento en que ocurrió la compra.

**Processing time** es el momento en que el sistema la procesa.

Por ejemplo, `late_ok` ocurrió a las **12:03**, pero se procesó en la llegada 2, junto con compras de las **12:06** y **12:08**. Para las ventanas de event time, se agrupa según las **12:03**.

### 9. Watermark por llegada

| Llegada | Máximo event time observado | Watermark al terminar |
|---|---|---|
| stream_001 | 12:04 | 11:54 |
| stream_002 | 12:08 | 11:58 |
| stream_003 | 12:26 | 12:16 |
| stream_004 | 12:27 | 12:17 |
| stream_005 | 12:45 | 12:35 |

El notebook calcula el watermark como el **máximo event time observado menos 10 minutos**.

Todas las horas corresponden al **12/03/2026 en UTC**.

### 10. Primeras ventanas tumbling en append

Las primeras ventanas aparecieron en la **llegada 3**, cuando el watermark avanzó a **12:16** y permitió cerrar las ventanas:

- `[12:00, 12:05)`
- `[12:05, 12:10)`

Antes no se emitieron porque el watermark todavía no había alcanzado el final de esas ventanas.

En la llegada 3 se emitieron **4 filas**, correspondientes a las combinaciones de ventana y canal.

### 11. Eventos tardíos

Los **20 eventos `late_bad`**, con event time **12:02**, llegaron en la llegada 4 y fueron descartados de las agregaciones porque el watermark previo ya estaba en **12:16**.

`late_ok` fue aceptado porque llegó en la llegada 2, cuando su hora **12:03** todavía estaba dentro del margen admitido.

Los eventos `late_bad` permanecen en la entrada, pero no suman en las ventanas.

### 12. Tumbling, hopping y session

| Tipo de ventana | Cómo agrupa en la práctica | Ejemplo de negocio |
|---|---|---|
| **Tumbling** | Intervalos fijos de 5 minutos, sin superposición | Ventas por cada bloque de cinco minutos |
| **Hopping** | Intervalos de 10 minutos que avanzan cada 5 minutos y se superponen | Monitorear las ventas de los últimos diez minutos cada cinco minutos |
| **Session** | Agrupa compras por cliente hasta una pausa de 5 minutos sin actividad | Medir la cantidad y el importe de compras por sesión del cliente |

### 13. Update y append para [12:00, 12:05), canal card

| Modo | Llegada de emisión | Compras | Monto |
|---|---:|---:|---:|
| update | 1 | 40 | 6.000,00 |
| update | 2 | 60 | 8.400,00 |
| append | 3 | 60 | 8.400,00 |

En **update**, el resultado se emitió dos veces. Este modo ofrece resultados antes, pero el consumidor debe actualizar el valor anterior cuando recibe una corrección.

En **append**, se emitió una sola vez con el resultado final. Esto simplifica el consumo, pero exige esperar el cierre de la ventana.

### 14. Watermark de un minuto en lugar de diez

Las ventanas se cerrarían antes, reduciendo la demora de los resultados finales y el estado que debe conservar el sistema.

A cambio, habría menor tolerancia a eventos tardíos y podrían descartarse más compras, reduciendo la completitud de los resultados.

## Garantías y fallas

### 15. Duplicados del productor

| Tratamiento | Filas resultantes |
|---|---:|
| Sin deduplicación | 220 |
| Deduplicación por event_id | 200 |
| Duplicados eliminados | 20 |

El productor puede reenviar un evento si no recibe la confirmación del primer envío y reintenta para evitar perderlo, aunque el evento ya haya sido recibido.

### 16. Reinicio y pérdida del checkpoint

Con el **mismo checkpoint**, el destino permaneció en **200 filas**, sin nuevas escrituras. Se observa un efecto **exactly-once** en este experimento.

Al perder el checkpoint, simulado utilizando otro, se reprocesaron los datos y el destino con append aumentó a **400 filas**, mostrando un comportamiento **at-least-once** con resultados duplicados.

| Escenario | Filas en el destino |
|---|---:|
| Primera ejecución | 200 |
| Reinicio con el mismo checkpoint | 200 |
| Reprocesamiento con otro checkpoint y append | 400 |

### 17. MERGE e idempotencia

Con `MERGE`, el destino quedó en **200 filas** tanto en la primera ejecución como al utilizar otro checkpoint.

Esto ocurre porque compara por `event_id` e inserta únicamente los eventos que todavía no existen.

Una escritura es **idempotente** cuando repetirla produce el mismo resultado final que ejecutarla una sola vez.

## CDC

### 18. Eventos de cambio

| Tipo de evento | Cantidad |
|---|---:|
| insert | 6 |
| update_preimage | 3 |
| update_postimage | 3 |
| delete | 1 |
| **Total** | **13** |

Las inserciones corresponden a **5 clientes iniciales y 1 cliente nuevo**.

Cada `UPDATE` genera dos eventos porque registra la fila antes del cambio (`update_preimage`) y después del cambio (`update_postimage`).

### 19. Reconstrucción e información histórica

Sí, se reconstruyó la tabla **sin diferencias**, conservando el último cambio de cada `customer_id`, excluyendo las preimágenes y los clientes cuyo último cambio fue una eliminación.

El log conserva:

- Valores anteriores.
- Eliminaciones.
- Tipo de cambio.
- Versión.
- Fecha del cambio.

Por ejemplo, muestra que el cliente 2 pasó de **UY → CL → BR** y que existió el cliente 3. La tabla actual solamente muestra el estado final.

## Cierre

### 20. Elección del motor

| Motor | Cuándo lo elegiría | Ventaja |
|---|---|---|
| **Spark Structured Streaming** | Pipelines de datos y analítica en Databricks | Utiliza las mismas APIs de SQL y DataFrames para batch y streaming |
| **Flink** | Aplicaciones de baja latencia con estado complejo, como detección de fraude | Ofrece manejo avanzado del estado y del tiempo del evento |
| **Kafka Streams** | Aplicaciones o microservicios que consumen y producen eventos en Kafka | Se integra como una librería dentro de la aplicación, sin un clúster de procesamiento separado |
