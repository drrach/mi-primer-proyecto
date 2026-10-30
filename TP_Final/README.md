# Experimentación y comparación de modelos con MLflow

**Estudiante:** Ricardo Anibal Cheluja  
**Variante:** Experimentación y comparación de modelos con MLflow.

## Objetivo y datos

Comparar algoritmos para clasificar el desenlace de fallecimiento durante el seguimiento (`DEATH_EVENT`) a partir de variables basales de pacientes con insuficiencia cardíaca. Se eligió un conjunto público y pertinente a la formación cardiológica, que permite reproducir la comparación sin datos de pacientes propios.

Fuente: [UCI Heart Failure Clinical Records, dataset 519](https://archive.ics.uci.edu/dataset/519/heart+failure+clinical+records). Se descarga mediante `ucimlrepo`, id 519. Contiene 299 pacientes: 96 fallecimientos (32,11%) y 203 sin el evento registrado. No se observaron valores faltantes ni duplicados exactos. Se utilizan 11 predictores basales; se excluye `time`, que aporta información del seguimiento posterior al momento basal.

## Contenido

| Archivo | Función |
|---|---|
| `01_eda.ipynb` | Descarga, control de calidad, distribuciones, correlaciones y guardado del CSV. |
| `02_entrenamiento_mlflow.ipynb` | Ocho configuraciones de regresión logística, Random Forest y LightGBM; tracking en MLflow. |
| `03_comparacion_modelos.ipynb` | Comparación, selección por CV y exportación de tablas, gráficos e informe. |
| `04_registro_y_uso.ipynb` | Carga del modelo ganador, registro opcional, comprobación de predicciones y análisis de coeficientes. |
| `resultados/` | Informe y gráficos recuperados de las salidas ejecutadas; CSV resumido con cifras redondeadas a tres decimales. |

## Diseño y registro

Partición estratificada 80/20, semilla 42: 239 pacientes en entrenamiento y 60 en test. Se aplica validación cruzada estratificada de cinco folds sobre entrenamiento, con los mismos folds para todas las configuraciones. La imputación y el escalado de la regresión logística están dentro de los pipelines para ajustarse en cada fold.

Se comparan cinco configuraciones iniciales (logística, Random Forest, LightGBM y dos variantes de los modelos de árboles) y un barrido de C en logística con 0.01, 0.1 y 10. Se registran en MLflow parámetros, métricas de CV y test, matriz de confusión, curva ROC, predicciones de test, índices de partición y modelo con firma y ejemplo de entrada. Una etiqueta identifica cada sesión para evitar mezclar entrenamientos.

Se selecciona por ROC-AUC medio de CV. Test se informa de forma descriptiva para las ocho configuraciones, sin utilizarlo para elegir o ajustar parámetros. Las métricas adicionales incluyen average precision, sensibilidad, especificidad, precisión, F1 y accuracy; el umbral es 0.5.

## Resultados y selección del modelo ganador

El modelo seleccionado fue **regresión logística con C=10**, identificado en MLflow como `LR_barrido_C_10.0`.

| Indicador | Resultado |
|---|---:|
| ROC-AUC medio de validación cruzada | 0.804 |
| Desviación estándar entre los cinco folds | 0.048 |
| ROC-AUC en test | 0.734 |
| Sensibilidad en test, umbral 0.5 | 0.526 |
| Especificidad en test, umbral 0.5 | 0.756 |
| F1 en test, umbral 0.5 | 0.513 |

Se seleccionó por el mayor ROC-AUC medio de validación cruzada sobre entrenamiento, utilizando los valores completos registrados en MLflow. La configuración de C=1 también se redondea a 0.804: la diferencia fue mínima y no demuestra superioridad estadística de C=10. La desviación entre folds no es un intervalo de confianza.

Algunas configuraciones obtuvieron mayor AUC en test, pero se mantuvo el criterio de selección por CV. El test se presenta de forma descriptiva, sin utilizarlo para ajustar hiperparámetros o elegir al ganador. El notebook 04 confirmó que el modelo recargado reproduce las 60 predicciones guardadas.

## Cómo funciona la regresión logística seleccionada

La regresión logística combina los predictores mediante una suma ponderada y transforma ese resultado en un score entre 0 y 1 para la clase `DEATH_EVENT=1`. No es una regresión lineal de la mortalidad: modela el logaritmo de las odds del desenlace.

El pipeline contiene:

1. **Imputación por mediana:** se ajusta únicamente con los datos de entrenamiento de cada fold. Aunque no se observaron faltantes en esta base, se mantiene este paso dentro del pipeline.
2. **Estandarización:** cada variable se centra y divide por su desviación estándar calculada en entrenamiento. Incluye las variables binarias.
3. **Regresión logística:** penalización L2, `C=10`, `class_weight='balanced'`, `max_iter=2000` y `random_state=42`.

`C` controla inversamente la intensidad de la regularización: C=10 implica menor penalización que C=1. La ponderación `balanced` asigna mayor peso a los pacientes de la clase menos frecuente durante el ajuste; no agrega pacientes ni realiza sobremuestreo.

La ecuación tiene la forma:

```text
z_j = (x_j - media_j_entrenamiento) / desviacion_j_entrenamiento
eta = intercepto + suma(coeficiente_j * z_j)
score = 1 / (1 + exp(-eta))
```

Se asigna clase 1 cuando el score es mayor o igual a 0.5. El score no está validado como probabilidad clínica individual: no se evaluó su calibración y el ajuste utiliza ponderación de clases. El desenlace corresponde al seguimiento disponible, no a mortalidad a un plazo fijo.

## Variables y peso dentro del modelo

El modelo utiliza las siguientes **11 variables basales**, ordenadas por el valor absoluto de sus coeficientes estandarizados. Los valores se extrajeron del modelo ganador cargado en el notebook 04; se presentan redondeados.

| Orden | Variable del dataset | Descripción | Coeficiente estandarizado | Asociación con el score, manteniendo constantes las demás variables |
|---:|---|---|---:|---|
| 1 | `serum_creatinine` | Creatinina sérica | +1.0795 | Valores mayores aumentan el score. |
| 2 | `ejection_fraction` | Fracción de eyección | −0.7838 | Valores mayores disminuyen el score. |
| 3 | `age` | Edad | +0.4785 | Valores mayores aumentan el score. |
| 4 | `anaemia` | Indicador de anemia | +0.3491 | Pasar de ausencia a presencia aumenta el score. |
| 5 | `creatinine_phosphokinase` | Creatinfosfoquinasa (CPK) | +0.3468 | Valores mayores aumentan el score. |
| 6 | `high_blood_pressure` | Indicador de hipertensión arterial | +0.3242 | Pasar de ausencia a presencia aumenta el score. |
| 7 | `diabetes` | Indicador de diabetes | +0.2884 | Pasar de ausencia a presencia aumenta el score. |
| 8 | `smoking` | Indicador de tabaquismo | +0.2228 | Pasar de ausencia a presencia aumenta el score. |
| 9 | `serum_sodium` | Sodio sérico | −0.1824 | Valores mayores disminuyen el score. |
| 10 | `platelets` | Recuento de plaquetas | −0.1340 | Valores mayores disminuyen el score. |
| 11 | `sex` | Sexo, según codificación del dataset | −0.1148 | La categoría codificada como 1 tiene menor score que la categoría 0. |

**La creatinina sérica, la fracción de eyección y la edad presentan los mayores coeficientes absolutos.** Esto describe el peso de las variables en esta ecuación sobre la escala estandarizada. No representa porcentajes de importancia, una estimación causal ni una demostración de significación estadística. El orden puede cambiar con otra muestra, regularización o conjunto de predictores; no se evaluó su estabilidad mediante remuestreo.

Para los tres predictores principales, exponenciar el coeficiente produce las siguientes razones de odds del modelo por un aumento de una desviación estándar de entrenamiento:

| Variable | Razón de odds por +1 desviación estándar |
|---|---:|
| Creatinina sérica | 2.9432 |
| Fracción de eyección | 0.4567 |
| Edad | 1.6137 |

Estas razones corresponden a odds, no a cambios proporcionales de probabilidad. No equivalen a aumentar 1 mg/dL de creatinina, un punto porcentual de fracción de eyección o un año de edad. Para variables binarias, exponenciar el coeficiente estandarizado tampoco equivale al contraste presencia versus ausencia: ese contraste requiere dividir el coeficiente por la escala del estandarizador antes de exponenciarlo.

Se excluyó **`time`** del conjunto predictor porque describe la duración del seguimiento y no está disponible como variable basal al momento de predecir. **`DEATH_EVENT`** es la variable objetivo y no se utiliza como predictor. No se eliminaron variables por su menor coeficiente.

## Interpretación y conclusión

En este experimento, la regresión logística con C=10 obtuvo el mayor ROC-AUC medio en la validación cruzada, con una diferencia mínima respecto de la logística de C=1. Su discriminación en test fue menor que la estimada en CV. Con umbral 0.5 detectó aproximadamente el 53% de los fallecimientos del test y clasificó correctamente aproximadamente el 76% de quienes no presentaron el evento.

La función renal, la función sistólica ventricular y la edad fueron los predictores de mayor peso en la ecuación ajustada. Este resultado es descriptivo del modelo y de la muestra estudiada. No permite afirmar que las demás variables carezcan de valor ni que el modelo sea apto para decisiones clínicas.

## Reproducción en Databricks

1. Importar los cuatro notebooks en una carpeta del Workspace.
2. Usar un entorno con Python, acceso a internet para UCI y paquetes, MLflow y acceso de escritura al volumen `workspace.bigdata_drrach.landing`.
3. Si el volumen es distinto, cambiar en los cuatro notebooks la base `/Volumes/workspace/bigdata_drrach/landing/tp_final_heart_failure`. El volumen debe existir; crear subdirectorios no crea un volumen de Unity Catalog.
4. Ejecutar 01, 02, 03 y 04, en ese orden. Los dos primeros contienen las instalaciones de dependencias.
5. Consultar el experimento `/Users/<usuario>/TP_Final_Insuficiencia_Cardiaca` en MLflow. El 02 crea ocho runs por sesión. El 03 y el 04 recuperan la última sesión completa.
6. En el 04, dejar vacío `registered_model_name` para cargar desde el run. Para registrar en Unity Catalog, proporcionar `catalogo.esquema.modelo` y disponer de permisos.
7. Revisar la carpeta `resultados` del volumen y exportar los notebooks con sus salidas para conservar evidencia de una nueva ejecución.

Las salidas aportadas muestran Python 3.12, scikit-learn 1.7.2, pandas 2.3.3 y LightGBM 4.7.0. Las instalaciones no fijan todas las versiones; para reproducibilidad estricta se debe conservar el entorno de la ejecución y los requisitos que MLflow guarda con el modelo.

## Estado de la evidencia

Los notebooks contienen salidas de ejecución. Se retiraron logs de instalación y avisos técnicos para facilitar la revisión. La ejecución aportada del notebook 03 confirma el guardado correcto de los cuatro archivos de comparación en el volumen. El notebook 04 confirmó la reproducción y exportación de 60 predicciones. Los gráficos y el informe del paquete se recuperaron de las salidas ejecutadas; el CSV resumido presenta cifras redondeadas a tres decimales. La exportación de precisión completa se encuentra en el volumen de Databricks.

## Limitaciones

- Muestra pequeña y única partición de test; estimaciones sensibles a la muestra.
- Diferentes duraciones de seguimiento: no se estima riesgo a un plazo fijo ni se modela la censura.
- Sin validación externa, evaluación de calibración ni intervalos de confianza.
- Diferencias pequeñas de CV no demuestran superioridad estadística.
- El EDA utiliza todo el dataset; no se utiliza para seleccionar predictores ni ajustar hiperparámetros. Una validación más estricta reservaría test antes del EDA dirigido al desenlace.
- Umbral sin optimización clínica y ausencia de monitoreo prospectivo.
- Trabajo académico; los resultados no establecen utilidad asistencial.

## Referencias

- UCI Heart Failure Clinical Records: https://archive.ics.uci.edu/dataset/519/heart+failure+clinical+records
- MLflow: https://mlflow.org/docs/latest/
- Scikit-learn: https://scikit-learn.org/stable/
- LightGBM: https://lightgbm.readthedocs.io/
