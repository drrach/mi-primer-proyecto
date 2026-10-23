# Databricks notebook source
# MAGIC %md
# MAGIC # TP Final - Insuficiencia cardíaca: descarga y análisis exploratorio
# MAGIC Dataset: UCI Heart Failure Clinical Records (299 pacientes).
# MAGIC Objetivo: explorar predictores basales de DEATH_EVENT. `time` se excluye de los predictores.

# COMMAND ----------
%pip install ucimlrepo==0.0.7 pandas matplotlib seaborn scikit-learn

# COMMAND ----------
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from ucimlrepo import fetch_ucirepo
from pathlib import Path

heart = fetch_ucirepo(id=519)
X_raw = heart.data.features.copy()
y_raw = heart.data.targets.copy()
df = pd.concat([X_raw, y_raw], axis=1)
assert len(df) == 299, f'Cantidad inesperada: {len(df)}'
assert 'DEATH_EVENT' in df.columns
assert df['DEATH_EVENT'].isin([0,1]).all()
print('Dimensiones:', df.shape)
display(df.head())

# COMMAND ----------
print('Tipos de datos:')
print(df.dtypes.to_string())
print('\nValores faltantes:')
print(df.isna().sum().to_string())
print('\nDuplicados exactos:', df.duplicated().sum())
print('\nDistribución de mortalidad:')
print(df.DEATH_EVENT.value_counts().sort_index().to_string())
print('\nPorcentaje:')
print((df.DEATH_EVENT.value_counts(normalize=True).sort_index()*100).round(2).to_string())
display(df.describe(include='all').T)

# COMMAND ----------
fig, ax = plt.subplots(figsize=(5,4))
sns.countplot(data=df, x='DEATH_EVENT', ax=ax)
ax.set(title='Distribución de DEATH_EVENT', xlabel='0: No falleció | 1: Falleció', ylabel='Pacientes')
plt.tight_layout(); plt.show()

# COMMAND ----------
variables = ['age', 'ejection_fraction', 'serum_creatinine', 'serum_sodium', 'platelets']
fig, axes = plt.subplots(2,3,figsize=(13,7))
for ax, col in zip(axes.flat, variables):
    sns.histplot(data=df, x=col, hue='DEATH_EVENT', kde=True, element='step', stat='density', common_norm=False, ax=ax)
    ax.set_title(col)
for ax in axes.flat[len(variables):]: ax.axis('off')
plt.tight_layout(); plt.show()

# COMMAND ----------
fig, ax = plt.subplots(figsize=(11,8))
sns.heatmap(df.drop(columns=['time'], errors='ignore').corr(numeric_only=True), cmap='coolwarm', center=0, annot=True, fmt='.2f', ax=ax)
ax.set_title('Correlaciones entre variables basales y mortalidad')
plt.tight_layout(); plt.show()

# COMMAND ----------
# Solo se excluye `time` del conjunto predictor, no del dataframe descriptivo.
features = df.drop(columns=['DEATH_EVENT','time'], errors='ignore')
target = df['DEATH_EVENT'].astype(int)
assert features.shape[1] == 11, f'Se esperaban 11 predictores basales, hay {features.shape[1]}'
assert not features.isna().any().any(), 'Revisar valores faltantes antes del entrenamiento'
print('Predictores finales:', features.columns.tolist())
print('X:',features.shape,'y:',target.shape)

# COMMAND ----------
# Guardado reproducible en el volumen de Unity Catalog del curso.
# Si tu entorno usa otro catálogo/esquema, modificar solamente BASE_PATH.
BASE_PATH = '/Volumes/workspace/bigdata_drrach/tp_final_heart_failure'
try:
    Path(BASE_PATH).mkdir(parents=True, exist_ok=True)
    df.to_csv(f'{BASE_PATH}/heart_failure_original.csv', index=False)
    features.assign(DEATH_EVENT=target).to_csv(f'{BASE_PATH}/heart_failure_modelado.csv', index=False)
    print('Datos guardados en:',BASE_PATH)
except Exception as e:
    print('No se pudo escribir en el volumen. El análisis en memoria sí se completó:',str(e))
