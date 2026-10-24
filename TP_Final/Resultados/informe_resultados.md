# Resultados: predicción de mortalidad en insuficiencia cardíaca

## Comparación

| Modelo | CV ROC-AUC media | CV desviación estándar | Test ROC-AUC | Sensibilidad test | Especificidad test | F1 test |
|---|---:|---:|---:|---:|---:|---:|
| LR_barrido_C_10.0 | 0.804 | 0.048 | 0.734 | 0.526 | 0.756 | 0.513 |
| Regresion_logistica | 0.804 | 0.050 | 0.734 | 0.526 | 0.780 | 0.526 |
| LR_barrido_C_0.1 | 0.801 | 0.052 | 0.750 | 0.526 | 0.780 | 0.526 |
| LR_barrido_C_0.01 | 0.790 | 0.055 | 0.763 | 0.526 | 0.805 | 0.541 |
| Random_Forest | 0.779 | 0.047 | 0.791 | 0.526 | 0.829 | 0.556 |
| RF_depth3 | 0.779 | 0.043 | 0.789 | 0.579 | 0.805 | 0.579 |
| LightGBM | 0.754 | 0.030 | 0.770 | 0.474 | 0.829 | 0.514 |
| LGBM_50_arboles | 0.748 | 0.040 | 0.801 | 0.421 | 0.902 | 0.516 |

## Selección

Se selecciona **LR_barrido_C_10.0** por obtener el mayor ROC-AUC medio en la validación cruzada estratificada de cinco folds sobre entrenamiento. La evaluación en test se reporta una sola vez y no se utiliza para seleccionar modelos. Se compararon cinco configuraciones iniciales y un barrido de C de regresión logística (0.01, 0.1 y 10), usando los mismos folds. Se seleccionó por validación cruzada; el test se informa como evaluación descriptiva.

## Limitaciones

- Solo 299 pacientes: estimaciones inestables e intervalos de incertidumbre no calculados.
- Dataset observacional, sin validación externa ni evaluación de calibración.
- `time` se excluyó de los predictores para evitar información posterior al momento basal.
- `DEATH_EVENT` refleja fallecimiento durante seguimientos de distinta duración; no predice riesgo a un plazo fijo.
- El umbral 0.5 no fue optimizado clínicamente.
- La comparación no implica utilidad clínica ni autoriza decisiones asistenciales.

Fuente: UCI Heart Failure Clinical Records (dataset 519).
