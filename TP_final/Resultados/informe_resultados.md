# Resultados y discusión

Datos obtenidos de la ejecución en Databricks y redondeados a tres decimales. Los gráficos de esta carpeta se reconstruyeron a partir de esos valores; los artefactos originales de las ejecuciones permanecen en MLflow.

| Modelo | ROC-AUC CV (media ± DE) | ROC-AUC test | Sensibilidad test | Especificidad test | F1 test |
|---|---|---:|---:|---:|---:|
| Regresión logística | 0.804 ± 0.050 | 0.734 | 0.526 | 0.780 | 0.526 |
| Random Forest | 0.779 ± 0.047 | 0.791 | 0.526 | 0.829 | 0.556 |
| LightGBM | 0.754 ± 0.030 | 0.770 | 0.474 | 0.829 | 0.514 |

## Selección
Se seleccionó regresión logística por mayor ROC-AUC media de validación cruzada (0.804). Random Forest alcanzó la ROC-AUC más alta en test (0.791), pero ese conjunto se reservó para evaluación y no se utilizó para seleccionar el modelo. La diferencia entre CV y test no demuestra superioridad estadística. No se realizó búsqueda de hiperparámetros.

## Interpretación clínica
La sensibilidad en test del modelo seleccionado (0.526, umbral 0.5) es insuficiente para justificar una aplicación clínica. El ROC-AUC mide discriminación, no calibración ni utilidad clínica. El conjunto pequeño y la ausencia de validación externa impiden generalizar resultados.

## Limitaciones
299 pacientes; posible variabilidad alta de las métricas; sin intervalos de confianza ni validación externa; sin evaluación de calibración; variable `time` excluida para evitar fuga de información; el desenlace `DEATH_EVENT` se observó durante seguimientos de duración variable y no corresponde a mortalidad a plazo fijo. Los resultados son exclusivamente educativos.

Fuente: UCI Heart Failure Clinical Records, dataset 519, https://archive.ics.uci.edu/dataset/519/heart+failure+clinical+records
