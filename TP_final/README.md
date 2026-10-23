# TP Final — Predicción de mortalidad en insuficiencia cardíaca con MLflow

**Estudiante:** Ricardo Aníbal Cheluja (verificar nombre antes de entregar)  
**Variante:** Experimentación y comparación de modelos con MLflow — Databricks  
**Objetivo:** Comparar regresión logística, Random Forest y LightGBM para clasificar `DEATH_EVENT` usando 11 características clínicas basales.

## Datos
UCI Heart Failure Clinical Records, 299 pacientes, 12 variables predictoras originales y etiqueta `DEATH_EVENT`. Fuente: https://archive.ics.uci.edu/dataset/519/heart+failure+clinical+records . Se excluye `time` (duración del seguimiento) para no incorporar información posterior a la evaluación basal. El objetivo no es mortalidad a plazo fijo.

## Metodología
Partición estratificada 80% entrenamiento (239) / 20% test (60), semilla 42. Validación cruzada estratificada de cinco folds sobre entrenamiento. Imputación por mediana dentro de pipelines; escalado solo para regresión logística. Tres modelos con parámetros fijos; sin optimización de hiperparámetros. Selección por ROC-AUC media de CV; test reservado para reporte final. MLflow registra parámetros, métricas, modelo y artefactos de evaluación por ejecución.

## Archivos
- `01_eda.py`: carga y análisis exploratorio en Databricks.
- `02_entrenamiento_mlflow.py`: partición, validación, entrenamiento, evaluación y registro MLflow.
- `03_comparacion_modelos.py`: recupera las ejecuciones y exporta resultados.
- `requirements.txt`: dependencias.
- `resultados/comparacion_modelos.csv`: métricas redondeadas de la ejecución compartida.
- `resultados/comparacion_auc.png` y `comparacion_metricas.png`: gráficos reconstruidos con las métricas redondeadas.
- `resultados/informe_resultados.md`: discusión y limitaciones.

## Resultados
| Modelo | ROC-AUC CV | ROC-AUC test | Sensibilidad test | Especificidad test | F1 test |
|---|---:|---:|---:|---:|---:|
| Regresión logística | **0.804 ± 0.050** | 0.734 | 0.526 | 0.780 | 0.526 |
| Random Forest | 0.779 ± 0.047 | **0.791** | 0.526 | 0.829 | 0.556 |
| LightGBM | 0.754 ± 0.030 | 0.770 | 0.474 | 0.829 | 0.514 |

Se seleccionó regresión logística por ROC-AUC de CV. Random Forest obtuvo la mejor ROC-AUC en test; este resultado no cambió la selección predefinida. Véase `resultados/informe_resultados.md`.

## Reproducción en Databricks
1. Importar los tres `.py` como notebooks fuente de Databricks.
2. Conectar cómputo con acceso a MLflow y al volumen Unity Catalog `workspace.bigdata_drrach.landing`, o modificar las rutas de los notebooks para usar otro volumen.
3. Ejecutar `01_eda.py` para descargar/inspeccionar los datos y guardar `heart_failure.csv` en `/Volumes/workspace/bigdata_drrach/landing/tp_final_heart_failure/`.
4. Ejecutar `02_entrenamiento_mlflow.py` (instala bibliotecas y registra tres ejecuciones en `/Users/<usuario>/TP_Final_Insuficiencia_Cardiaca`).
5. Ejecutar `03_comparacion_modelos.py` para consultar las ejecuciones y comparar resultados. En entornos Serverless que restringen `dbutils.fs.cp(file:/tmp/...)`, exportar con escritura directa al volumen como se indica abajo.
6. Revisar MLflow Experiments, métricas y artefactos; exportar capturas legibles si la entrega lo requiere.

### Alternativa de exportación para Databricks Serverless
Al terminar el tercer notebook, ejecutar en una nueva celda:

```python
import os
ruta = '/Volumes/workspace/bigdata_drrach/landing/tp_final_heart_failure/resultados'
os.makedirs(ruta, exist_ok=True)
comparacion.to_csv(f'{ruta}/comparacion_modelos.csv', index=False)
fig1.savefig(f'{ruta}/comparacion_auc.png', dpi=160, bbox_inches='tight')
fig2.savefig(f'{ruta}/comparacion_metricas.png', dpi=160, bbox_inches='tight')
with open(f'{ruta}/informe_resultados.md', 'w', encoding='utf-8') as f:
    f.write(informe)
```

## Evidencias y entrega
Agregar al repositorio las exportaciones de notebooks con salidas o capturas de la pantalla de MLflow que identifiquen las tres ejecuciones. Los gráficos originales exportados desde Databricks pueden sustituir a las reconstrucciones incluidas aquí. No versionar tokens, credenciales, información de pacientes ni el directorio interno `mlruns`.

## Limitaciones
Estudio académico con 299 pacientes, seguimiento heterogéneo, sin validación externa ni evaluación de calibración; el desempeño no autoriza uso clínico. La sensibilidad del modelo seleccionado es 52.6% con umbral 0.5.
