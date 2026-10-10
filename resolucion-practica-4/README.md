Clase práctica 4 — Streaming: logs, ventanas y garantías

Nombre: Ricardo Anibal Cheluja
student_id: drrach
scala: small

Log y offsets

1. Cola tradicional y log
Después de leer tres mensajes, quedaron 2 mensajes en la cola y 5 en el log. En la cola del ejemplo, consumir elimina los mensajes; en un log como Kafka, leer no los elimina: cada consumidor avanza su offset y puede volver a leerlos mientras sigan disponibles.

2. Consumidores después de la llegada 2
Antifraude había leído 180 mensajes acumulados y reportes 0 mensajes. El atraso de reportes no afectó a antifraude porque cada consumidor tiene su propio checkpoint y avanza de forma independiente sobre el mismo log.

3. Offset y persistencia
Un offset identifica la posición de lectura de un consumidor. En esta práctica, cada consulta lo guarda en su propio checkpoint, en streaming_lab/log/checkpoints/antifraude o streaming_lab/log/checkpoints/reportes. Al finalizar, ambos indicaban 6, la próxima versión a leer, mientras la última versión del topic era 5.

4. Replay, Kappa y Lambda
El replay leyó 280 mensajes, todo el contenido del topic desde el inicio. Kappa propone reprocesar el log con una nueva consulta usando el mismo camino de streaming; Lambda mantiene dos caminos, uno batch y otro streaming, con lógica que debe mantenerse en ambos.

Ingesta y enriquecimiento

5. Filas nuevas y lectura incremental

Llegada	Filas nuevas en Bronze
stream_001	60
stream_002	120
stream_003	40
stream_004	40
stream_005	20
Reejecución sin archivos nuevos	0
Bronze procesó 280 filas en total. Auto Loader, mediante el estado guardado en su checkpoint, recuerda los archivos procesados y evita volver a leerlos.

6. Cuarentena y conservación del original
En stream_002 aparecieron 20 registros con INVALID_AMOUNT (importe inválido) y 20 con UNKNOWN_CUSTOMER (cliente inexistente), sumando 40 registros en cuarentena. Bronze conserva el texto original para mantener la trazabilidad, investigar errores y reprocesar los datos si se corrigen las reglas o cambia el contrato.

7. Enriquecimiento y tipo de join
El join agregó country, desde silver_customers, y category, desde silver_products. Es un join stream-tabla, porque cruza eventos del flujo con tablas estáticas de referencia mediante customer_id y product_id.

Tiempo y ventanas

8. Event time y processing time
Event time es el momento en que ocurrió la compra; processing time es el momento en que el sistema la procesa. Por ejemplo, late_ok ocurrió a las 12:03, pero se procesó en la llegada 2, junto con compras de las 12:06 y 12:08; se agrupa según las 12:03.

9. Watermark por llegada
Llegada	Máximo event time visto	Watermark al terminar
stream_001	12:04	11:54
stream_002	12:08	11:58
stream_003	12:26	12:16
stream_004	12:27	12:17
stream_005	12:45	12:35
El notebook calcula el watermark como máximo event time observado menos 10 minutos. Todas las horas corresponden al 12/03/2026 en UTC.

10. Primeras ventanas tumbling en append
Aparecieron en la llegada 3, cuando el watermark avanzó a 12:16 y permitió cerrar las ventanas de 12:00–12:05 y 12:05–12:10. Antes no se emitieron porque el watermark todavía no había alcanzado el final de esas ventanas. En la llegada 3 se emitieron 4 filas, correspondientes a las combinaciones de ventana y canal.

11. Eventos tardíos
Los 20 eventos late_bad, con event time 12:02, llegaron en la llegada 4 y fueron descartados de las agregaciones porque el watermark previo ya estaba en 12:16. late_ok fue aceptado porque llegó en la llegada 2, cuando su hora 12:03 todavía estaba dentro del margen admitido. late_bad permanece en la entrada, pero no suma en las ventanas.

12. Tumbling, hopping y session
Tipo	Cómo agrupa en la práctica	Ejemplo de negocio
Tumbling	Intervalos fijos de 5 minutos, sin superposición.	Ventas por cada bloque de cinco minutos.
Hopping	Intervalos de 10 minutos que avanzan cada 5 minutos y se superponen.	Monitorear las ventas de los últimos diez minutos cada cinco minutos.
Session	Agrupa compras por cliente hasta una pausa de 5 minutos sin actividad.	Medir cantidad e importe de compras por sesión del cliente.

13. Update y append para [12:00, 12:05), canal card
Modo	Llegada de emisión	Compras	Monto
update	1	40	6000,00
update	2	60	8400,00
append	3	60	8400,00
En update se emitió dos veces: ofrece resultados antes, pero el consumidor debe actualizar el valor anterior cuando recibe una corrección. En append se emitió una sola vez con el resultado final: simplifica el consumo, pero exige esperar el cierre de la ventana.

14. Watermark de 1 minuto en lugar de 10
Se cerrarían las ventanas antes, reduciendo la demora de los resultados finales y el estado que debe conservar el sistema. A cambio, habría menor tolerancia a eventos tardíos y podrían descartarse más compras, reduciendo la completitud de los resultados.

Garantías y fallas

15. Duplicados del productor
Sin deduplicar quedaron 220 filas; deduplicando por event_id quedaron 200, eliminando 20 duplicados. El productor puede reenviar un evento si no recibe la confirmación del primer envío y reintenta para evitar perderlo, aunque ya se haya recibido.

16. Reinicio y pérdida del checkpoint
Con el mismo checkpoint, el destino permaneció en 200 filas, sin nuevas escrituras: se observa un efecto exactly-once en este experimento. Al perderlo, simulado usando otro checkpoint, se reprocesaron los datos y el destino con append aumentó a 400 filas, mostrando un comportamiento at-least-once con resultados duplicados.

17. MERGE e idempotencia
Con MERGE, el destino quedó en 200 filas tanto en la primera ejecución como al usar otro checkpoint, porque compara por event_id e inserta únicamente eventos que todavía no existen. Una escritura es idempotente cuando repetirla produce el mismo resultado final que ejecutarla una sola vez.

CDC

18. Eventos de cambio
Tipo	Cantidad
insert	6
update_preimage	3
update_postimage	3
delete	1
Total	13
Las inserciones corresponden a 5 clientes iniciales y 1 nuevo. Cada UPDATE genera dos eventos porque registra la fila antes del cambio (update_preimage) y después del cambio (update_postimage).

19. Reconstrucción e información histórica
Sí, se reconstruyó la tabla sin diferencias, conservando el último cambio de cada customer_id, excluyendo las preimágenes y los clientes cuyo último cambio fue una eliminación. El log conserva valores anteriores, eliminaciones, tipo de cambio, versión y fecha. Por ejemplo, muestra que el cliente 2 pasó de UY → CL → BR y que existió el cliente 3; la tabla actual sólo muestra el estado final.Cierre

CIERRE

20. Elección del motor
Elegiría Spark Structured Streaming para pipelines de datos y analítica en Databricks: su ventaja es usar las mismas APIs de SQL y DataFrames para batch y streaming. Elegiría Flink para aplicaciones de baja latencia con estado complejo, como detección de fraude: su ventaja es el manejo avanzado del estado y del tiempo del evento. Elegiría Kafka Streams para aplicaciones o microservicios que consumen y producen eventos en Kafka: su ventaja es integrarse como una librería dentro de la aplicación, sin un clúster de procesamiento separado.
