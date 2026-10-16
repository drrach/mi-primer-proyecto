# Entrega práctica 5 — MLOps con MLflow

- **Nombre:** Ricardo Anibal Cheluja
- **student_id:** drrach
- **Escala:** small

## Mis runs

Se encontraron 38 runs de modelos; esta es la tabla de los 15 mejores que imprime la sección 5 del notebook 03. Algunas ejecuciones tienen el mismo nombre: son runs diferentes.

| run | modelo | features | class_weight | umbral | hiperparametros | train_pr_auc | pr_auc | accuracy | precision | recall | f1 |
|---|---|---|---|---|---|---|---|---|---|---|---|
| max_depth=3 | gradient_boosting | completas | balanced | 0.3 | learning_rate=0.1, max_depth=3 | 0.602 | 0.577 | 0.687 | 0.237 | 0.904 | 0.376 |
| exp15 | gradient_boosting | completas | ninguno | 0.5 | learning_rate=0.1, max_depth=8 | 0.654 | 0.575 | 0.916 | 0.698 | 0.346 | 0.463 |
| max_depth=5 | gradient_boosting | completas | balanced | 0.3 | learning_rate=0.1, max_depth=5 | 0.635 | 0.573 | 0.703 | 0.245 | 0.889 | 0.384 |
| max_depth=8 | gradient_boosting | completas | balanced | 0.3 | learning_rate=0.1, max_depth=8 | 0.646 | 0.571 | 0.721 | 0.254 | 0.864 | 0.392 |
| exp17 | gradient_boosting | completas | balanced | 0.3 | learning_rate=0.1, max_depth=8 | 0.646 | 0.571 | 0.721 | 0.254 | 0.864 | 0.392 |
| exp16 | gradient_boosting | completas | balanced | 0.5 | learning_rate=0.1, max_depth=8 | 0.646 | 0.571 | 0.821 | 0.342 | 0.779 | 0.475 |
| max_depth=sin_limite | gradient_boosting | completas | balanced | 0.3 | learning_rate=0.1, max_depth=sin_limite | 0.621 | 0.568 | 0.709 | 0.248 | 0.879 | 0.386 |
| exp14 | random_forest | completas | ninguno | 0.5 | n_estimators=100, max_depth=8 | 0.644 | 0.556 | 0.913 | 0.774 | 0.23 | 0.355 |
| exp13 | logistic_regression | completas | ninguno | 0.5 | C=1.0 | 0.533 | 0.542 | 0.915 | 0.746 | 0.28 | 0.407 |
| exp5 | logistic_regression | completas | ninguno | 0.5 | C=1.0 | 0.533 | 0.542 | 0.915 | 0.746 | 0.28 | 0.407 |
| exp4 | logistic_regression | completas | ninguno | 0.5 | C=1.0 | 0.533 | 0.542 | 0.915 | 0.746 | 0.28 | 0.407 |
| exp3 | logistic_regression | completas | ninguno | 0.5 | C=1.0 | 0.533 | 0.542 | 0.915 | 0.746 | 0.28 | 0.407 |
| exp2 | logistic_regression | completas | ninguno | 0.5 | C=1.0 | 0.533 | 0.542 | 0.915 | 0.746 | 0.28 | 0.407 |
| exp1 | logistic_regression | completas | ninguno | 0.5 | C=1.0 | 0.533 | 0.542 | 0.915 | 0.746 | 0.28 | 0.407 |
| exp8 | random_forest | cliente | ninguno | 0.5 | n_estimators=100, max_depth=8 | 0.594 | 0.504 | 0.91 | 0.721 | 0.216 | 0.332 |

**Run registrado:** `max_depth=3`, run_id `d1b3576e3a7a447e964ad42e68af7261`; modelo `workspace.bigdata_drrach.fraud_model`, versión **1**, alias **champion**, estado **READY**.

## Datos y tracking

**1.** Train tiene **35.605 filas y 11,11% de contracargos** (1–5 de marzo de 2026), test **7.121 filas y 10,42%** (6 de marzo) y nuevas **7.156 filas y 11,12%** (7 de marzo). Se separa por fecha para simular la predicción de transacciones futuras a partir del pasado, evitando mezclar períodos al azar; además debe asegurarse que las variables sólo usen información disponible al predecir.

**2.** El modelo que siempre predice ausencia de contracargo obtiene **89,58% de accuracy** en test (6.379/7.121), pero recall cero. Se usa PR-AUC porque evalúa el ordenamiento de la clase positiva mediante precision y recall ante distintos umbrales y resulta más informativo ante este desbalance; en esta práctica se calcula con `average_precision_score`.

**3.** `01_manual_logistica` registró los parámetros `model_type=logistic_regression`, `feature_set=basicas`, `C=1.0`, `class_weight=ninguno`, `threshold=0.5`, `data_table=ml_features` y `data_version=0`; sus métricas fueron test PR-AUC **0,341**, ROC-AUC **0,793**, accuracy **0,896**, precision **0,493**, recall **0,050**, F1 **0,091**, **75 alertas** y train PR-AUC **0,343**. Guardó `matriz_confusion.png` y el modelo con su firma y dependencias, que en MLflow 3 aparece en Logged models con URI `models:/m-849f4832a8d34907bda38ef33cc49af7`; un experimento agrupa ejecuciones y un run es una ejecución individual con sus resultados.

**4.** Autolog registró **46 parámetros** y las métricas `training_accuracy_score=0.910`, `training_f1_score=0.888`, `training_log_loss=0.228`, `training_precision_score=0.901`, `training_recall_score=0.910`, `training_roc_auc=0.900` y `training_score=0.910`. En esta ejecución no registró automáticamente las métricas de test, el PR-AUC elegido para el problema, el umbral de negocio ni la versión Delta de los datos, que se registran manualmente.

**5.** Sí: la comparación imprimió `¿Métricas idénticas? True`, con train PR-AUC **0,594** y test PR-AUC **0,504** en ambos runs. Para reproducirlos hay que fijar **datos y su versión, código y configuración, semilla aleatoria (`random_state=42`) y entorno/dependencias**.

**6.** Al predecir sin `amount`, MLflow rechazó la entrada con el error `Failed to enforce schema of data`. La firma declara columnas y tipos obligatorios de entrada y la estructura de salida, permitiendo detectar entradas incompatibles antes de predecir.

## Tus experimentos

**7.** El mejor run fue `max_depth=3`: `gradient_boosting`, variables `completas`, `learning_rate=0.1`, `max_depth=3`, `class_weight=balanced` y umbral **0,3**, con test PR-AUC **0,576614** (0,577 redondeado). Las variables fueron `amount`, `payment_channel`, `hour`, `account_age_days`, `segment`, `country`, `is_night`, `day_of_week`, `category`, `price`, `amount_to_price` y `device_customers`; frente a `solo_regla` (**0,170**), mejoró aproximadamente **0,407 puntos de PR-AUC**.

**8.** Manteniendo logística con `C=1.0`, sin balanceo y umbral 0,5, el PR-AUC pasó de **0,341** con `basicas` a **0,447** con `cliente` y **0,542** con `completas`: mejoras de **0,106** y **0,095**, respectivamente. En el mejor run global, las mayores importancias por permutación fueron `amount` (**0,3409**), `payment_channel` (**0,1818**), `hour` (**0,1300**), `account_age_days` (**0,0778**) y `category` (**0,0587**).

**9.** Con `completas`, sin balanceo y umbral 0,5, boosting obtuvo **0,575**, bosque **0,556** y logística **0,542** de PR-AUC, por lo que ganó boosting en esas configuraciones. Un árbol puede hacer cortes sucesivos por horario y canal para representar interacciones; la logística utilizada combina efectos aditivos sobre el logit y necesitaría términos de interacción explícitos para representar esa combinación.

**10.** Comparando `exp15` y `exp16`, ambos boostings con completas, profundidad 8, learning rate 0,1 y umbral 0,5, `balanced` redujo precision de **0,698 a 0,342** y aumentó recall de **0,346 a 0,779**. Las alertas aumentaron de aproximadamente **368 a 1.690**, estimadas como `recall × 742 / precision` a partir de métricas redondeadas (los conteos exactos no se imprimieron en los adjuntos), mientras que PR-AUC cambió poco: **0,575 a 0,571**.

**11.** Entre `exp16` y `exp17`, bajar el umbral de **0,5 a 0,3** aumentó recall de **0,779 a 0,864**, redujo precision de **0,342 a 0,254**, aumentó las alertas de aproximadamente **1.690 a 2.527** y bajó F1 de **0,475 a 0,392**. PR-AUC permaneció en **0,571** porque se calcula a partir de los scores recorriendo los umbrales, mientras que F1 depende de las clasificaciones obtenidas con un umbral concreto.

**12.** Se barrió `max_depth`: con boosting, completas, balanced y umbral 0,3, el mejor valor fue **3**, con PR-AUC test **0,577** frente a train **0,602**. También hubo barridos de bosque con básicas: sin límite alcanzaron train PR-AUC **0,998** y test **0,208**, una diferencia grande que indica **sobreajuste**.

## Comparar y elegir

**13.** No coincidieron: el mejor por PR-AUC fue el boosting `max_depth=3` (**PR-AUC 0,577; accuracy 0,687**), y el mejor por accuracy fue `exp15` (**accuracy 0,916; PR-AUC 0,575**). Accuracy depende del umbral y cuenta todos los aciertos, dominados aquí por la clase sin contracargo, mientras que PR-AUC evalúa cómo se ordena el riesgo de la clase positiva.

**14.** Los más alejados de la diagonal fueron los dos runs de bosque con básicas llamados `max_depth=sin_limite`, con train PR-AUC **0,998** y test **0,208** (brecha **0,790**). Su desempeño mucho mayor en entrenamiento que en test indica sobreajuste y escasa generalización temporal.

**15.** `mlflow.search_runs` permite filtrar, ordenar y comparar ejecuciones desde código de forma automatizable y reproducible, sin inspeccionarlas una por una. Para traer sólo mis bosques con PR-AUC test mayor a 0,55 usaría este `filter_string`:

```python
filter_string = (
    "tags.student_id = 'drrach' "
    "AND params.model_type = 'random_forest' "
    "AND metrics.test_pr_auc > 0.55"
)
```

**16.** Un contracargo no detectado puede costar más que una alerta falsa por la pérdida económica, pero la comparación depende de los montos, costos de revisión y fricción que las alertas generen al cliente. Si el equipo puede revisar K alertas diarias, ordenaría las transacciones por score y revisaría las K de mayor riesgo, ajustando el umbral a esa capacidad y resolviendo empates para no superarla.

## Registro, uso y monitoreo

**17.** Sí: se registró `workspace.bigdata_drrach.fraud_model`, versión **1**, alias **champion**, con estado **READY**, asociado al run `d1b3576e3a7a447e964ad42e68af7261`. Cargar `models:/workspace.bigdata_drrach.fraud_model@champion` permite promover una nueva versión moviendo el alias sin modificar el código consumidor; `runs:/<run_id>/model` referencia el artefacto de un run específico cuando está guardado en esa ruta.

**18.** Se puntuaron **7.156 transacciones nuevas** y **2.808 generaron alerta (39,24%; 39,2% redondeado)** con umbral **0,3**. Las columnas de auditoría de `ml_predictions` son `transaction_id`, `score`, `alerta`, `threshold`, `model_name`, `model_version`, `run_id` y `scored_at`; el umbral se guarda fuera del modelo porque es una decisión de negocio que puede ajustarse a costos y capacidad sin reentrenar.

**19.** El PR-AUC en nuevas fue **0,575**, muy parecido al **0,577** de test; en la campaña simulada las alertas subieron de **39,2% a 57,5%** (unos **18,3 puntos porcentuales**). Drift es un cambio en las distribuciones de datos o en su relación con el resultado y puede advertirse antes de las etiquetas observando entradas y scores: aquí el importe promedio pasó aproximadamente de **989 a 1.218** y las transferencias de **33% a 60%**, aunque eso no prueba por sí solo una pérdida de desempeño.

## Cierre

**20.** El ciclo incluyó datos y features en `ml_features`, entrenamiento de pipelines, tracking en MLflow, comparación y selección, registro de la versión 1 con alias champion en Unity Catalog, inferencia batch sobre nuevas con escritura en `ml_predictions` y monitoreo con etiquetas y drift simulado. Para producción real faltan, entre otras cosas, automatizar y validar los pipelines de datos y scoring, definir alertas de monitoreo con etiquetas demoradas, establecer aprobación y rollback de versiones y verificar permisos, capacidad operativa y costos del fraude en datos futuros independientes.