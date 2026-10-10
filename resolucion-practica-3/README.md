
# Resolución práctica 3 — Modelos NoSQL

- **Nombre:** Ricardo Anibal Cheluja
- **student_id:** drrach
- **Escala:** small

Los resultados corresponden a las ejecuciones de los notebooks de la práctica.

## CAP y PACELC — 01_cap_pacelc

### 1. Respuesta de la réplica C durante la partición y propiedad sacrificada

En modo CP, la lectura en C respondió `ERROR: no disponible` y la escritura `saldo=50` respondió `ERROR: sin quórum`. Como C estaba aislada y no alcanzaba la mayoría, rechazó las operaciones. Este modo sacrifica disponibilidad durante la partición para preservar consistencia.

En modo AP, C devolvió `saldo=100`, aunque A y B ya tenían `saldo=80`, y aceptó la escritura `saldo=50` con una respuesta `OK`. Este modo sacrifica consistencia para mantener disponibilidad.

### 2. Saldo final en AP y escritura perdida

Después de reparar la red, las tres réplicas quedaron con `saldo=50`.

La reconciliación utilizó last-write-wins: ganó la escritura con la versión más reciente, realizada en C. Se perdió la escritura `saldo=80`, realizada en A y replicada en B. Las operaciones no se combinaron; se conservó un único valor ganador.

### 3. Latencia p99 y compromiso de PACELC

| Confirmaciones requeridas | Latencia p99 |
|---|---:|
| W=1 | 9,6 ms |
| W=3 | 55,6 ms |

La p99 indica que el 99 % de las escrituras se confirma en ese tiempo o menos.

Esperar más réplicas permite confirmar que la escritura llegó a más copias antes de responder al cliente. Con W=3, llegó a las tres réplicas. Esto reduce el riesgo de leer una copia desactualizada, a cambio de mayor latencia.

Es el compromiso entre latencia y consistencia de PACELC cuando no hay partición. En un sistema real, la garantía también depende del protocolo de lectura y escritura.

### 4. Quórum con cinco réplicas partidas en ABC | DE

El lado ABC pudo seguir escribiendo porque tenía 3 de las 5 réplicas, suficientes para alcanzar el quórum de mayoría:

`floor(5 / 2) + 1 = 3`

La escritura `saldo=80` por A respondió `OK`.

El lado DE tenía solamente dos réplicas y no alcanzó el quórum. La escritura `saldo=50` por D respondió `ERROR: sin quórum`.

## Clave-valor — 02_clave_valor

### 5. Velocidad del GET en memoria y uso de memoria en Redis

En esta ejecución, el GET en memoria fue aproximadamente **2.459.309 veces más rápido** que el GET con Spark.

| Método | Tiempo por GET |
|---|---:|
| Diccionario de Python en memoria | ≈ 0,0002 ms |
| Spark sobre Delta | ≈ 558 ms |

Redis mantiene los datos en memoria para permitir accesos de muy baja latencia, evitando leer del disco en cada consulta.

El experimento utilizó un diccionario de Python como aproximación a Redis. Un Redis real agrega comunicación por red y otros costos; por lo tanto, el factor medido corresponde a esta comparación experimental.

### 6. Consulta de clientes de AR y ventajas y desventajas

Hubo que leer y parsear los 5.000 valores porque el campo `country` estaba dentro del JSON almacenado como valor. El acceso directo era por clave y no había un índice adicional por país.

Se encontraron **992 clientes de AR**.

- **Ventaja:** acceso rápido y simple cuando se conoce la clave, como `customer:42`.
- **Desventaja:** buscar por campos internos del valor requiere recorrer los registros o mantener una estructura adicional, como un conjunto `country:AR` con las claves correspondientes.

### 7. Redistribución al pasar de cuatro a cinco nodos

En el experimento con 100 nodos virtuales por nodo físico:

| Método | Claves que cambiaron de nodo |
|---|---:|
| Hash mod N | 79,6 % |
| Hashing consistente | 21,2 % |

Con hash mod N, cambiar la cantidad de nodos modifica la asignación de la mayoría de las claves.

Con hashing consistente, el nuevo nodo ocupa posiciones en el anillo y recibe solamente una parte de las claves; las restantes conservan su ubicación.

El hashing consistente conviene porque reduce el movimiento de datos, el tráfico de red y el trabajo de redistribución al ampliar el clúster. Los nodos virtuales ayudan a equilibrar la carga.

## Documental — 03_documental

### 8. Consulta del cliente 42 y datos embebidos

| Modelo | Resultado |
|---|---|
| Relacional | 21 filas, utilizando 3 tablas y 2 joins |
| Documental | 1 documento, sin joins al consultarlo |

Dentro del documento quedaron embebidos:

- El perfil: país, segmento, fecha de alta y email.
- Las estadísticas: cantidad de transacciones, monto total y cantidad de transacciones fraudulentas.
- Las 21 transacciones: fecha, identificador, monto, canal de pago, dispositivo e indicador de fraude.
- Los datos del producto dentro de cada transacción: identificador, categoría y precio, copiados como una instantánea o snapshot.

El `product_id` se conservó también como referencia al catálogo.

### 9. Campos faltantes y esquema flexible

Spark infirió un esquema con la unión de todos los campos presentes en los documentos y completó con `null` los campos faltantes.

Por ejemplo, un documento tenía `phones` y los demás quedaron con `null` en ese campo.

Se dice que el modelo documental tiene esquema flexible porque los documentos de una misma colección pueden tener campos y estructuras diferentes.

Al convertirlos en un DataFrame, un campo ausente y un campo explícitamente escrito como `null` quedan representados igual. El JSON original permite distinguir ambos casos.

### 10. Tamaño máximo observado y crecimiento sin límite

El documento más grande pesó aproximadamente **7,2 KB**, según la medición del JSON realizada en el notebook.

Si un documento crece sin límite, aumenta el consumo de memoria y el costo de leerlo, transferirlo y actualizarlo. Además, puede alcanzar el límite de tamaño del motor; el notebook señala un límite de 16 MB por documento en MongoDB.

Una alternativa es mantener un historial reciente acotado dentro del documento y guardar las transacciones antiguas en documentos separados, vinculados por el identificador del cliente.

### 11. Consultas favorecidas por el modelo documental

El modelo documental resuelve bien consultas centradas en una entidad, como obtener un cliente con su perfil e historial o verificar si tiene alguna transacción fraudulenta. Los datos necesarios se encuentran juntos.

Las consultas transversales, que agregan información de muchos documentos, requieren más procesamiento. Un ejemplo es calcular el monto total por categoría de producto.

Para esa consulta, `explode(transactions)` convirtió cada elemento del array en una fila. Luego se agruparon las transacciones por `t.product.category` y se sumaron sus montos.

## Grafos — 04_grafos

### 12. Cantidad de nodos y aristas

| Elemento | Cantidad |
|---|---:|
| Nodos Customer | 5.000 |
| Nodos Device | 2.369 |
| Aristas USES | 6.360 |

Cada arista representa un par cliente–dispositivo único. El atributo `tx_count` indica cuántas transacciones corresponden a ese par.

### 13. Joins para dos y cuatro saltos

| Patrón | Joins en SQL |
|---|---:|
| 2 saltos | 3 |
| 4 saltos | 5 |

Se necesitó un join contra la tabla de aristas por cada salto y otro adicional para obtener el nodo cliente final.

Neo4j almacena referencias directas a las relaciones de cada nodo, mediante index-free adjacency. Esto permite recorrer vecinos sin reconstruir las conexiones mediante joins en cada salto.

Esta organización favorece las consultas que exploran caminos desde un nodo, aunque no implica que Neo4j sea más rápido para cualquier tipo de consulta.

### 14. Convergencia y componente más grande

La búsqueda de componentes conexos convergió en **43 iteraciones**.

En la iteración 43 ningún nodo cambió de etiqueta, por lo que se detuvo el algoritmo.

El componente más grande, identificado como `c:0`, contiene **3.720 nodos**, incluyendo clientes y dispositivos.

### 15. Aristas entre máquinas y dificultad de distribución

Al repartir los nodos con `hash(id) mod 4`, el **74,7 % de las aristas** quedó con sus extremos en máquinas distintas.

Distribuir un grafo exige equilibrar dos objetivos:

- Repartir la carga entre máquinas.
- Mantener cerca los nodos conectados para reducir comunicaciones por red.

El hashing distribuyó los nodos de forma pareja, pero separó muchos vecinos. El particionamiento por componente redujo las aristas entre máquinas al **0 %**, pero generó una carga muy desigual.

Por eso, equilibrar la cantidad de nodos no garantiza recorridos eficientes: también importa la ubicación de sus relaciones.

## Vectorial — 05_vectorial

### 16. Representación del cliente y similitud coseno

Cada cliente se representó mediante un vector de 10 números:

| Dimensiones | Significado |
|---|---|
| 5 | Proporción del gasto en electronics, home, books, sports y fashion |
| 3 | Proporción de pagos por card, wallet y transfer |
| 2 | Ticket promedio dividido por 2.000 y proporción de compras mayores a 1.500 |

La similitud coseno mide qué tan alineados están dos vectores, comparando su dirección.

Un valor cercano a 1 indica perfiles de comportamiento muy parecidos según estas variables. No significa necesariamente que ambos clientes tengan el mismo gasto total.

### 17. Clientes marcados entre vecinos y utilidad de la búsqueda

| Grupo | Porcentaje de clientes marcados |
|---|---:|
| Vecinos de clientes marcados | 32,9 % |
| Vecinos de clientes no marcados | 14,0 % |
| Tasa base de la población vectorizada | 15,4 % |

Los vecinos de clientes marcados tuvieron una tasa de marcados de aproximadamente 2,1 veces la tasa base.

Buscar vecinos parecidos permite identificar comportamientos similares a casos conocidos, priorizar revisiones de fraude, recomendar productos o clasificar clientes.

La similitud no demuestra fraude. En esta ejecución, el clasificador k-NN obtuvo una exactitud de **85,5 %**, inferior al **86,2 %** de predecir siempre “no marcado”. Por lo tanto, la clasificación necesita mejoras aunque exista una asociación entre similitud y marca.

### 18. IVF con nprobe=4 y compromiso de ANN

| Métrica | Resultado |
|---|---:|
| Recall@10 | 0,854 = 85,4 % |
| Vectores escaneados | 6,008 % ≈ 6,0 % |

Con `nprobe=4` se consultaron los cuatro clusters más cercanos. Se recuperó, en promedio, el 85,4 % de los diez vecinos verdaderos de la búsqueda exacta, revisando aproximadamente el 6 % de los vectores.

- **Se gana:** menos comparaciones y menor costo de búsqueda.
- **Se pierde:** la garantía de encontrar todos los vecinos más cercanos, porque algunos pueden estar en clusters no consultados.

Aumentar `nprobe` mejora el recall, pero requiere escanear más vectores.

## Columnar — 06_columnar

### 19. Tamaño, ReadSchema y archivos que se pueden saltear

Para las **511.460 transacciones** del experimento:

| Formato | Tamaño en disco |
|---|---:|
| CSV | 50,9 MB |
| JSON | 123,1 MB |
| Parquet | 5,5 MB |

Parquet ocupó aproximadamente 9,3 veces menos que CSV y 22,5 veces menos que JSON.

Para la consulta que filtró `amount > 1900` y calculó el promedio y la cantidad, Spark leyó únicamente `amount`. El plan mostró:

`ReadSchema: struct<amount:decimal(12,2)>`

| Distribución de datos | Archivos que se pueden saltear |
|---|---:|
| Al azar | 0 de 8 |
| Ordenados por amount | 7 de 8 |

Con los datos ordenados, las estadísticas min/max permiten descartar siete archivos porque su monto máximo es inferior a 1900.

### 20. Ventajas para analítica y costo de actualizar filas

El formato columnar es bueno para analítica porque permite:

- Leer solamente las columnas necesarias.
- Comprimir eficientemente valores del mismo tipo.
- Saltear archivos o bloques mediante estadísticas.
- Procesar grandes conjuntos de valores para sumas, promedios y agrupaciones.

Es menos conveniente para modificar una fila por vez porque los valores de esa fila están repartidos entre bloques de distintas columnas. En archivos como Parquet, una actualización suele requerir reescribir archivos o bloques en lugar de modificar directamente un registro.

Delta permite actualizaciones sobre Parquet, pero su costo depende del mecanismo utilizado. Agrupar cambios suele ser más eficiente que realizar muchas modificaciones individuales pequeñas.
