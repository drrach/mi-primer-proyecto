# Databricks notebook source
# MAGIC %md
# MAGIC # TP Final — Etapa 3: comparación de modelos con MLflow
# MAGIC Recupera las ejecuciones existentes, compara los tres modelos y exporta resultados. **No reentrena modelos.**

# COMMAND ----------
import os
import tempfile
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import mlflow

# COMMAND ----------
# La etapa 2 creó el experimento en /Users/<usuario>/...
usuario = spark.sql('SELECT current_user() AS usuario').first()['usuario']
EXPERIMENTO = f'/Users/{usuario}/TP_Final_Insuficiencia_Cardiaca'
exp = mlflow.get_experiment_by_name(EXPERIMENTO)
if exp is None:
    raise ValueError(f'No se encontró {EXPERIMENTO}. Revisá la ruta del experimento de la etapa 2.')
print('Experimento:', exp.name, '| ID:', exp.experiment_id)

# COMMAND ----------
runs = mlflow.search_runs(experiment_ids=[exp.experiment_id],
                         filter_string="attributes.status = 'FINISHED'",
                         order_by=['attributes.start_time DESC'], max_results=1000)
if runs.empty:
    raise ValueError('El experimento no tiene ejecuciones finalizadas.')
modelos = ['Regresion_logistica', 'Random_Forest', 'LightGBM']
# Elegimos la ejecución terminada más reciente de cada modelo (sin mezclar reintentos).
runs = runs[runs['tags.mlflow.runName'].isin(modelos)].copy()
runs = runs.drop_duplicates(subset=['tags.mlflow.runName'], keep='first')
faltantes = set(modelos) - set(runs['tags.mlflow.runName'])
if faltantes:
    raise ValueError(f'Faltan ejecuciones de: {sorted(faltantes)}. Revisá la etapa 2.')
columnas = {
    'tags.mlflow.runName': 'modelo',
    'run_id': 'run_id',
    'metrics.cv_roc_auc_mean': 'cv_roc_auc_mean',
    'metrics.cv_roc_auc_std': 'cv_roc_auc_std',
    'metrics.test_roc_auc': 'test_roc_auc',
    'metrics.test_sensitivity': 'test_sensitivity',
    'metrics.test_specificity': 'test_specificity',
    'metrics.test_precision': 'test_precision',
    'metrics.test_f1': 'test_f1',
    'metrics.test_accuracy': 'test_accuracy',
    'metrics.test_average_precision': 'test_average_precision',
}
no_encontradas = [c for c in columnas if c not in runs.columns]
if no_encontradas:
    raise ValueError(f'Faltan métricas registradas: {no_encontradas}')
comparacion = runs[list(columnas)].rename(columns=columnas)
comparacion = comparacion.sort_values('cv_roc_auc_mean', ascending=False).reset_index(drop=True)
display(comparacion)

# COMMAND ----------
# Selección SOLO por validación cruzada. Test se informa, no se utiliza para elegir.
mejor = comparacion.iloc[0]
print('Modelo seleccionado por ROC-AUC media de CV:', mejor['modelo'])
print(f"ROC-AUC CV: {mejor['cv_roc_auc_mean']:.3f} ± {mejor['cv_roc_auc_std']:.3f}")
print(f"ROC-AUC test (evaluación descriptiva): {mejor['test_roc_auc']:.3f}")
print('Nota: el test no se usa para ajustar ni seleccionar modelos.')

# COMMAND ----------
# Gráfico 1: CV ROC-AUC vs test ROC-AUC
x = np.arange(len(comparacion))
fig1, ax = plt.subplots(figsize=(9, 5))
ax.bar(x - 0.18, comparacion['cv_roc_auc_mean'], width=0.36, label='CV (media)',
       yerr=comparacion['cv_roc_auc_std'], capsize=4)
ax.bar(x + 0.18, comparacion['test_roc_auc'], width=0.36, label='Test')
ax.set_xticks(x, comparacion['modelo'])
ax.set_ylabel('ROC-AUC')
ax.set_ylim(0, 1.05)
ax.set_title('Comparación de discriminación')
ax.legend()
fig1.tight_layout()
plt.show()

# COMMAND ----------
# Gráfico 2: métricas a umbral 0.5 en test
metricas = ['test_sensitivity', 'test_specificity', 'test_f1']
fig2, ax = plt.subplots(figsize=(9, 5))
for i, metrica in enumerate(metricas):
    ax.bar(x + (i - 1) * 0.24, comparacion[metrica], width=0.23,
           label=metrica.replace('test_', ''))
ax.set_xticks(x, comparacion['modelo'])
ax.set_ylabel('Valor')
ax.set_ylim(0, 1.05)
ax.set_title('Sensibilidad, especificidad y F1 en test (umbral 0.5)')
ax.legend()
fig2.tight_layout()
plt.show()

# COMMAND ----------
# Exportación local y copia al volumen Unity Catalog para GitHub.
# Se genera un informe con valores reales, sin inventar resultados.
BASE = '/Volumes/workspace/bigdata_drrach/landing/tp_final_heart_failure'
with tempfile.TemporaryDirectory() as tmp:
    archivos = {
        'comparacion_modelos.csv': os.path.join(tmp, 'comparacion_modelos.csv'),
        'comparacion_auc.png': os.path.join(tmp, 'comparacion_auc.png'),
        'comparacion_metricas.png': os.path.join(tmp, 'comparacion_metricas.png'),
        'informe_resultados.md': os.path.join(tmp, 'informe_resultados.md'),
    }
    comparacion.to_csv(archivos['comparacion_modelos.csv'], index=False)
    fig1.savefig(archivos['comparacion_auc.png'], dpi=160, bbox_inches='tight')
    fig2.savefig(archivos['comparacion_metricas.png'], dpi=160, bbox_inches='tight')
    filas = '\n'.join(
        f"| {r.modelo} | {r.cv_roc_auc_mean:.3f} | {r.cv_roc_auc_std:.3f} | "
        f"{r.test_roc_auc:.3f} | {r.test_sensitivity:.3f} | {r.test_specificity:.3f} | {r.test_f1:.3f} |"
        for r in comparacion.itertuples(index=False)
    )
    informe = f'''# Resultados: predicción de mortalidad en insuficiencia cardíaca

## Comparación

| Modelo | CV ROC-AUC media | CV desviación estándar | Test ROC-AUC | Sensibilidad test | Especificidad test | F1 test |
|---|---:|---:|---:|---:|---:|---:|
{filas}

## Selección

Se selecciona **{mejor['modelo']}** por obtener el mayor ROC-AUC medio en la validación cruzada estratificada de cinco folds sobre entrenamiento. La evaluación en test se reporta una sola vez y no se utiliza para seleccionar modelos. No se realizó búsqueda de hiperparámetros.

## Limitaciones

- Solo 299 pacientes: estimaciones inestables e intervalos de incertidumbre no calculados.
- Dataset observacional, sin validación externa ni evaluación de calibración.
- `time` se excluyó de los predictores para evitar información posterior al momento basal.
- `DEATH_EVENT` refleja fallecimiento durante seguimientos de distinta duración; no predice riesgo a un plazo fijo.
- El umbral 0.5 no fue optimizado clínicamente.
- La comparación no implica utilidad clínica ni autoriza decisiones asistenciales.

Fuente: UCI Heart Failure Clinical Records (dataset 519).
'''
    with open(archivos['informe_resultados.md'], 'w', encoding='utf-8') as f:
        f.write(informe)
    print(informe)
    try:
        dbutils.fs.mkdirs(f'{BASE}/resultados')
        for nombre, origen in archivos.items():
            dbutils.fs.cp('file:' + origen, f'{BASE}/resultados/{nombre}', True)
        print('Archivos guardados en:', f'{BASE}/resultados')
        display(dbutils.fs.ls(f'{BASE}/resultados'))
    except Exception as e:
        print('No se pudo copiar al volumen:', repr(e))
        print('Los resultados se visualizaron; revisá permisos o exportá las figuras desde el notebook.')
plt.close(fig1)
plt.close(fig2)
