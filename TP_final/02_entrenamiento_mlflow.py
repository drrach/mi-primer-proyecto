# Databricks notebook source
# MAGIC %md
# MAGIC # TP Final — Entrenamiento y seguimiento con MLflow
# MAGIC Dataset UCI Heart Failure Clinical Records. Comparación justa de regresión logística, Random Forest y LightGBM. `time` se excluye para evitar fuga de información del seguimiento.

# COMMAND ----------
# MAGIC %pip install lightgbm scikit-learn mlflow pandas matplotlib seaborn

# COMMAND ----------
import os, json, tempfile
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import mlflow
import mlflow.sklearn
from mlflow.models import infer_signature
from sklearn.model_selection import train_test_split, StratifiedKFold, cross_validate
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (roc_auc_score, average_precision_score, f1_score,
                             precision_score, recall_score, accuracy_score,
                             confusion_matrix, ConfusionMatrixDisplay, RocCurveDisplay)
from lightgbm import LGBMClassifier

# COMMAND ----------
# Leer archivo CSV guardado en la etapa 1
ruta = '/Volumes/workspace/bigdata_drrach/landing/tp_final_heart_failure/heart_failure.csv'
df = pd.read_csv(ruta)
assert len(df) == 299, f'Se esperaban 299 pacientes; se leyeron {len(df)}'
assert 'DEATH_EVENT' in df.columns
assert set(df.DEATH_EVENT.dropna().unique()) <= {0, 1}
assert not df.DEATH_EVENT.isna().any()
X = df.drop(columns=['DEATH_EVENT', 'time'], errors='ignore')
y = df['DEATH_EVENT'].astype(int)
assert X.shape[1] == 11, f'Se esperaban 11 predictores basales, hay {X.shape[1]}: {X.columns.tolist()}'
assert X.select_dtypes(exclude='number').empty, 'Revisar variables no numéricas'
print('Datos:', df.shape, '| Predictores:', X.shape[1])
print('Variables:', X.columns.tolist())
print('Distribución de clases:', y.value_counts().to_dict())

# COMMAND ----------
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.20, random_state=42, stratify=y
)
cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
print('Train:', len(X_train), 'Test:', len(X_test))

# COMMAND ----------
modelos = {
    'Regresion_logistica': Pipeline([
        ('imputacion', SimpleImputer(strategy='median')),
        ('escalado', StandardScaler()),
        ('modelo', LogisticRegression(max_iter=2000, class_weight='balanced', random_state=42))
    ]),
    'Random_Forest': Pipeline([
        ('imputacion', SimpleImputer(strategy='median')),
        ('modelo', RandomForestClassifier(n_estimators=200, min_samples_leaf=3,
                                         class_weight='balanced', random_state=42, n_jobs=-1))
    ]),
    'LightGBM': Pipeline([
        ('imputacion', SimpleImputer(strategy='median')),
        ('modelo', LGBMClassifier(n_estimators=100, learning_rate=0.05,
                                 num_leaves=7, min_child_samples=15,
                                 verbosity=-1, random_state=42, n_jobs=1))
    ])
}

# COMMAND ----------
# Databricks: experiment en el workspace; ruta única por usuario/carpeta.
# Si no hay permisos para crear el experimento, cambiá esta ruta por una ruta válida en tu workspace.
usuario = spark.sql('SELECT current_user() AS usuario').first()['usuario']
experimento = f'/Users/{usuario}/TP_Final_Insuficiencia_Cardiaca'
mlflow.set_experiment(experimento)
print('Experimento:', experimento)

# COMMAND ----------
resultados = []
for nombre, pipeline in modelos.items():
    with mlflow.start_run(run_name=nombre):
        cv_scores = cross_validate(
            pipeline, X_train, y_train, cv=cv,
            scoring={'roc_auc': 'roc_auc', 'f1': 'f1', 'recall': 'recall'},
            n_jobs=1, error_score='raise'
        )
        cv_auc = float(np.mean(cv_scores['test_roc_auc']))
        cv_std = float(np.std(cv_scores['test_roc_auc']))
        pipeline.fit(X_train, y_train)
        probas = pipeline.predict_proba(X_test)[:, 1]
        pred = (probas >= 0.5).astype(int)
        tn, fp, fn, tp = confusion_matrix(y_test, pred, labels=[0, 1]).ravel()
        metricas = {
            'cv_roc_auc_mean': cv_auc,
            'cv_roc_auc_std': cv_std,
            'cv_f1_mean': float(np.mean(cv_scores['test_f1'])),
            'cv_recall_mean': float(np.mean(cv_scores['test_recall'])),
            'test_roc_auc': float(roc_auc_score(y_test, probas)),
            'test_average_precision': float(average_precision_score(y_test, probas)),
            'test_f1': float(f1_score(y_test, pred, zero_division=0)),
            'test_precision': float(precision_score(y_test, pred, zero_division=0)),
            'test_sensitivity': float(recall_score(y_test, pred, zero_division=0)),
            'test_specificity': float(tn / (tn + fp)),
            'test_accuracy': float(accuracy_score(y_test, pred)),
        }
        mlflow.log_params({'seed': 42, 'test_size': 0.20, 'cv_folds': 5,
                           'threshold': 0.5, 'excluded_followup_time': True,
                           'n_train': len(X_train), 'n_test': len(X_test)})
        mlflow.log_params({k: str(v) for k, v in pipeline.get_params(deep=True).items()
                           if k.startswith('modelo__')})
        mlflow.log_metrics(metricas)
        mlflow.set_tags({'dataset': 'UCI Heart Failure Clinical Records (id=519)',
                         'target': 'DEATH_EVENT', 'model_family': nombre,
                         'note': 'Exploratory educational study, not clinical deployment'})
        with tempfile.TemporaryDirectory() as tmp:
            fig, ax = plt.subplots(figsize=(5, 4))
            ConfusionMatrixDisplay.from_predictions(y_test, pred, labels=[0,1], ax=ax)
            ax.set_title(f'Matriz de confusión — {nombre}')
            fig.tight_layout()
            fig.savefig(os.path.join(tmp, 'matriz_confusion.png'), dpi=150)
            plt.close(fig)
            fig, ax = plt.subplots(figsize=(5, 4))
            RocCurveDisplay.from_predictions(y_test, probas, ax=ax)
            ax.set_title(f'Curva ROC — {nombre}')
            fig.tight_layout()
            fig.savefig(os.path.join(tmp, 'curva_roc.png'), dpi=150)
            plt.close(fig)
            pd.DataFrame({'y_real': y_test.to_numpy(), 'probabilidad': probas,
                          'prediccion': pred}).to_csv(os.path.join(tmp, 'predicciones_test.csv'), index=False)
            mlflow.log_artifacts(tmp, artifact_path='evaluacion')
        firma = infer_signature(X_train, pipeline.predict(X_train))
        mlflow.sklearn.log_model(pipeline, artifact_path='modelo', signature=firma,
                                 input_example=X_train.head(3))
        resultados.append({'modelo': nombre, **metricas})
        print(nombre, '| CV AUC:', round(cv_auc, 3), '| Test AUC:', round(metricas['test_roc_auc'], 3))

# COMMAND ----------
comparacion = pd.DataFrame(resultados).sort_values('cv_roc_auc_mean', ascending=False)
display(comparacion)
print('Modelo seleccionado según CV ROC-AUC:', comparacion.iloc[0]['modelo'])
print('Nota: el conjunto test NO se usa para seleccionar el modelo.')
# Para entregar, exportar el notebook con resultados y capturar la pantalla de MLflow Experiments.
