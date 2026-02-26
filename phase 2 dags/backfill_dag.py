import pandas as pd
import os
import joblib
from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Param
from sklearn.ensemble import IsolationForest

# ==========================================
# 1. Définition des fonctions
# ==========================================
def extract_historical_data(**kwargs):
    print("Étape 1 : Extraction des données historiques depuis PostgreSQL...")
    
    start_date = kwargs['params']['start_date']
    end_date = kwargs['params']['end_date']
    
    print(f"Période demandée : du {start_date} au {end_date}")
    
    hook = PostgresHook(postgres_conn_id="postgres_default")
    
    # On nomme les paramètres avec %(nom)s pour éviter toute confusion
    sql = """
        SELECT pm2_5, pm10, no2, o3, temperature, humidity 
        FROM sensor_readings 
        WHERE event_time >= %(start)s 
          AND event_time <= %(end)s
          AND pm2_5 IS NOT NULL;
    """
    
    # On passe un DICTIONNAIRE dans l'argument parameters
    df = hook.get_pandas_df(sql, parameters={"start": start_date, "end": end_date})
    
    print(f"✅ {len(df)} lignes historiques extraites avec succès.")
    
    if len(df) == 0:
         raise ValueError("Aucune donnée trouvée pour cette période !")
         
    file_path = '/tmp/historical_sensor_data_ml.csv'
    df.to_csv(file_path, index=False)
    kwargs['ti'].xcom_push(key='ml_data_path', value=file_path)
    
def validate_historical_quality(**kwargs):
    print("Étape 2 : Validation de la qualité (Backfill)...")
    file_path = kwargs['ti'].xcom_pull(task_ids='extract_historical_data', key='ml_data_path')
    df = pd.read_csv(file_path)
    initial_len = len(df)

    df = df.dropna()
    df = df[(df['humidity'] >= 0) & (df['humidity'] <= 100)]
    final_len = len(df)

    print(f"{initial_len - final_len} lignes rejetées. {final_len} lignes valides.")

    # FIX: Vérification du minimum de lignes (comme dans le DAG quotidien)
    min_rows = kwargs['params'].get('min_rows', 50)
    if final_len < min_rows:
        raise ValueError(
            f"Pas assez de données valides ({final_len} < {min_rows} minimum). "
            "Élargissez la plage de dates ou réduisez min_rows."
        )

    val_file_path = '/tmp/validated_historical_data.csv'
    df.to_csv(val_file_path, index=False)
    kwargs['ti'].xcom_push(key='validated_data_path', value=val_file_path)

def train_historical_model(**kwargs):
    print("Étape 3 : Entraînement du modèle sur les données historiques...")
    file_path = kwargs['ti'].xcom_pull(task_ids='validate_historical_quality', key='validated_data_path')
    df = pd.read_csv(file_path)

    features = ['pm2_5', 'pm10', 'no2', 'o3', 'temperature', 'humidity']
    X = df[features]

    contamination = kwargs['params'].get('contamination', 0.05)
    model = IsolationForest(n_estimators=100, contamination=contamination, random_state=42)
    model.fit(X)

    # Read dates from params, not XCom
    start_date = kwargs['params']['start_date']
    end_date   = kwargs['params']['end_date']
    safe_start = start_date.replace(' ', 'T').replace(':', '-')
    safe_end   = end_date.replace(' ', 'T').replace(':', '-')
    model_path = f'/tmp/isolation_forest_{safe_start}_to_{safe_end}.joblib'

    joblib.dump(model, model_path)
    print(f"Modèle sauvegardé : {model_path}")

    kwargs['ti'].xcom_push(key='model_path',      value=model_path)
    kwargs['ti'].xcom_push(key='train_data_path', value=file_path)

def evaluate_historical_model(**kwargs):
    # FIX: Étape d'évaluation ajoutée (absente dans la version originale)
    print("Étape 4 : Évaluation du modèle historique...")
    model_path = kwargs['ti'].xcom_pull(task_ids='train_historical_model', key='model_path')
    data_path  = kwargs['ti'].xcom_pull(task_ids='train_historical_model', key='train_data_path')

    model = joblib.load(model_path)
    df    = pd.read_csv(data_path)

    features = ['pm2_5', 'pm10', 'no2', 'o3', 'temperature', 'humidity']
    X = df[features]

    predictions    = model.predict(X)
    anomaly_count  = list(predictions).count(-1)
    anomaly_rate   = anomaly_count / len(predictions)
    contamination  = kwargs['params'].get('contamination', 0.05)
    health_score   = 1.0 - abs(contamination - anomaly_rate)

    print(f"Taux d'anomalies détecté  : {anomaly_rate:.2%}")
    print(f"Score de santé du modèle  : {health_score:.3f}")

    # Avertissement si le modèle est pathologique
    if anomaly_rate == 0.0 or anomaly_rate == 1.0:
        raise ValueError(
            f"Modèle pathologique : taux d'anomalies = {anomaly_rate:.0%}. "
            "Vérifiez vos données ou ajustez le paramètre contamination."
        )

    kwargs['ti'].xcom_push(key='health_score',  value=health_score)
    kwargs['ti'].xcom_push(key='anomaly_rate',  value=anomaly_rate)
    print("Évaluation terminée.")


# ==========================================
# 2. Configuration du DAG
# ==========================================

default_args = {
    'owner': 'miae',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 0,
}

with DAG(
    'historical_backfill_ml_pipeline',
    default_args=default_args,
    description='DAG de Backfill pour ré-entraîner sur des dates passées (version améliorée)',
    schedule=None,
    catchup=False,
    tags=["mlops", "sensors", "backfill"],
    params={
        "start_date": Param(
            "2025-01-01 00:00:00",
            type="string",
            description="Date de début (YYYY-MM-DD HH:MM:SS)"
        ),
        "end_date": Param(
            "2025-12-31 23:59:59",
            type="string",
            description="Date de fin (YYYY-MM-DD HH:MM:SS)"
        ),
        # paramètre min_rows configurable à l'exécution
        "min_rows": Param(
            50,
            type="integer",
            description="Nombre minimum de lignes valides requis pour l'entraînement"
        ),
        # contamination configurable à l'exécution
        "contamination": Param(
            0.05,
            type="number",
            description="Taux de contamination attendu pour l'Isolation Forest (entre 0.01 et 0.5)"
        ),
    }
) as dag:

    # ==========================================
    # 3. Tâches et Exécution
    # ==========================================

    task_extract = PythonOperator(
        task_id='extract_historical_data',
        python_callable=extract_historical_data,
    )

    task_validate = PythonOperator(
        task_id='validate_historical_quality',
        python_callable=validate_historical_quality,
    )

    task_train = PythonOperator(
        task_id='train_historical_model',
        python_callable=train_historical_model,
    )

    # Tâche d'évaluation
    task_evaluate = PythonOperator(
        task_id='evaluate_historical_model',
        python_callable=evaluate_historical_model,
    )

    # Ordre d'exécution
    task_extract >> task_validate >> task_train >> task_evaluate
