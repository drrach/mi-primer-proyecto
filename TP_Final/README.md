# Experimentación y comparación de modelos con MLflow

**Estudiante:** Ricardo Anibal Cheluja  
**Variante:** Experimentación y comparación de modelos con MLflow.

## Objetivo y datos

Comparar algoritmos para clasificar el desenlace de fallecimiento durante el seguimiento (`DEATH_EVENT`) a partir de variables basales de pacientes con insuficiencia cardíaca. Se eligió un conjunto público, pequeño y pertinente a la formación cardiológica del estudiante, que permite reproducir la comparación sin datos de pacientes propios.

Fuente: [UCI Heart Failure Clinical Records, dataset 519](https://archive.ics.uci.edu/dataset/519/heart+failure+clinical+records). Se descarga mediante `ucimlrepo`, id 519. Contiene 299 pacientes: 96 fallecimientos (32,11%) y 203 sin el evento registrado. No se observaron valores faltantes ni duplicados exactos. Se utilizan 11 predictores basales; se excluye `time`, que aporta información del seguimiento posterior al momento basal.

## Contenido

| Archivo | Función |
|---|---|
| `01_eda.ipynb` | Descarga, control de calidad, distribuciones, correlaciones y guardado del CSV. |
| `02_entrenamiento_mlflow.ipynb` | Ocho configuraciones de regresión logística, Random Forest y LightGBM; tracking en MLflow. |
| `03_comparacion_modelos.ipynb` | Comparación, selección por CV y exportación de tablas, gráficos e informe. |
| `04_registro_y_uso.ipynb` | Carga del modelo ganador, registro opcional y comprobación de las predicciones. |
| `resultados/` | Informe y gráficos recuperados de las salidas ejecutadas; CSV resumido con cifras redondeadas a tres decimales. |

## Diseño y registro

Partición estratificada 80/20, semilla 42: 239 pacientes en entrenamiento y 60 en test. Se aplica validación cruzada estratificada de cinco folds sobre entrenamiento, con los mismos folds para todas las configuraciones. La imputación y el escalado de la regresión logística están dentro de los pipelines para ajustarse en cada fold.

Se comparan cinco configuraciones iniciales (logística, Random Forest, LightGBM y dos variantes de los modelos de árboles) y un barrido de C en logística con 0.01, 0.1 y 10. Se registran en MLflow parámetros, métricas de CV y test, matriz de confusión, curva ROC, predicciones de test, índices de partición y modelo con firma y ejemplo de entrada. Una etiqueta identifica cada sesión para evitar mezclar entrenamientos.

Se selecciona por ROC-AUC medio de CV. Test se informa de forma descriptiva para las ocho configuraciones, sin utilizarlo para elegir o ajustar parámetros. Las métricas adicionales incluyen average precision, sensibilidad, especificidad, precisión, F1 y accuracy; el umbral es 0.5.

## Resultados y selección

La configuración seleccionada fue **LR_barrido_C_10.0**, con ROC-AUC de CV **0.804 ± 0.048** (desviación estándar entre folds) y ROC-AUC de test **0.734**. En test obtuvo sensibilidad 0.526, especificidad 0.756 y F1 0.513. La logística de C=1 tiene un resultado prácticamente igual a la precisión presentada: no se demuestra superioridad estadística de C=10.

Aunque algunas configuraciones obtuvieron mayor ROC-AUC en test, se mantuvo la selección por CV definida previamente. El notebook 04 confirmó que el modelo recargado reproduce las **60 predicciones** guardadas. El registro en Unity Catalog no se ejecutó: se utilizó la URI del run; este registro no es un requisito de la consigna.

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
