import pandas as pd
import os
import joblib
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sklearn.ensemble import IsolationForest
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor

# ==========================================
# 1. Définition des fonctions Python
# ==========================================

def extract_data(**kwargs):
    print("Étape 1 : Extraction des données capteurs depuis PostgreSQL...")
    hook = PostgresHook(postgres_conn_id="postgres_default")
    
    sql = """
        SELECT pm2_5, pm10, no2, o3, temperature, humidity 
        FROM sensor_readings 
        WHERE event_time >= NOW() - INTERVAL '24 hours'
          AND pm2_5 IS NOT NULL;
    """
    df = hook.get_pandas_df(sql)
    print(f"✅ {len(df)} lignes extraites avec succès.")
    
    file_path = '/tmp/latest_sensor_data_ml.csv'
    df.to_csv(file_path, index=False)
    
    # Partage du chemin du fichier avec la tâche suivante
    kwargs['ti'].xcom_push(key='ml_data_path', value=file_path)

def validate_quality(**kwargs):
    print("Étape 2 : Validation de la qualité des données...")
    file_path = kwargs['ti'].xcom_pull(task_ids='extract_sensors_data', key='ml_data_path')
    
    if not file_path or not os.path.exists(file_path):
        raise ValueError("Erreur : Fichier de données introuvable.")
        
    df = pd.read_csv(file_path)
    initial_len = len(df)
    
    # Nettoyage de base et règles métier
    df = df.dropna()
    df = df[(df['humidity'] >= 0) & (df['humidity'] <= 100)]
    
    final_len = len(df)
    print(f"✅ Qualité validée. {initial_len - final_len} lignes rejetées.")
    
    if final_len < 50:
        raise ValueError("Pas assez de données valides pour l'entraînement (min 50 requises).")
        
    val_file_path = '/tmp/validated_sensor_data.csv'
    df.to_csv(val_file_path, index=False)
    
    kwargs['ti'].xcom_push(key='validated_data_path', value=val_file_path)

def train_model(**kwargs):
    print("Étape 3 : Entraînement du modèle (Isolation Forest)...")
    file_path = kwargs['ti'].xcom_pull(task_ids='validate_quality', key='validated_data_path')
    df = pd.read_csv(file_path)
    
    features = ['pm2_5', 'pm10', 'no2', 'o3', 'temperature', 'humidity']
    X = df[features]
    
    # Entraînement
    model = IsolationForest(n_estimators=100, contamination=0.05, random_state=42)
    model.fit(X)
    
    model_path = '/tmp/latest_isolation_forest.joblib'
    joblib.dump(model, model_path)
    print("✅ Modèle entraîné et sauvegardé.")
    
    kwargs['ti'].xcom_push(key='model_path', value=model_path)
    kwargs['ti'].xcom_push(key='train_data_path', value=file_path) # Pour l'évaluation

def evaluate_model(**kwargs):
    print("Étape 4 : Évaluation du modèle...")
    model_path = kwargs['ti'].xcom_pull(task_ids='train_model', key='model_path')
    data_path = kwargs['ti'].xcom_pull(task_ids='train_model', key='train_data_path')
    
    model = joblib.load(model_path)
    df = pd.read_csv(data_path)
    
    features = ['pm2_5', 'pm10', 'no2', 'o3', 'temperature', 'humidity']
    X = df[features]
    
    # L'Isolation forest renvoie -1 pour les anomalies et 1 pour les données normales
    predictions = model.predict(X)
    anomalies_count = list(predictions).count(-1)
    anomaly_rate = anomalies_count / len(predictions)
    
    print(f"Taux d'anomalies détecté par le modèle : {anomaly_rate:.2%}")
    
    # Pour un modèle non-supervisé, on vérifie que le modèle ne prédit pas TOUT comme anomalie ou RIEN.
    # On calcule un "score de santé" (1.0 = parfait, proche de la contamination voulue de 5%)
    health_score = 1.0 - abs(0.05 - anomaly_rate)
    
    kwargs['ti'].xcom_push(key='new_model_score', value=health_score)

def check_improvement(**kwargs):
    print("Étape 5 : Vérification de l'amélioration...")
    new_model_score = kwargs['ti'].xcom_pull(task_ids='evaluate_model', key='new_model_score')
    
    # Simulation du score du modèle actuellement en production (ex: 0.90)
    # Dans la vraie vie, on irait chercher ce score dans MLflow ou une base de données.
    current_prod_score = 0.90 
    
    if new_model_score > current_prod_score:
        print(f"✅ Nouveau modèle meilleur/plus stable ! ({new_model_score:.3f} > {current_prod_score})")
        return 'deploy_if_better' 
    else:
        print(f"❌ Pas d'amélioration suffisante. ({new_model_score:.3f} <= {current_prod_score})")
        return 'end_pipeline'

def deploy_model(**kwargs):
    print("Étape 6 : Déploiement en production...")
    model_path = kwargs['ti'].xcom_pull(task_ids='train_model', key='model_path')
    
    # Simuler un déploiement en copiant le modèle vers un dossier "production"
    prod_path = '/tmp/production_isolation_forest.joblib'
    os.system(f'cp {model_path} {prod_path}')
    
    print(f"🚀 Modèle officiellement déployé en production ici : {prod_path}")

# ==========================================
# 2. Configuration du DAG
# ==========================================

default_args = {
    'owner': 'miae',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1), # Aligné avec ton autre DAG
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
with DAG(
    'daily_ml_pipeline_sensors',
    default_args=default_args,
    description='Pipeline MLOps quotidien pour l\'entraînement (DAG 2)',
    schedule='30 1 * * *', # Tourne à 01h30 (30 min après ton DAG d'agrégation)
    catchup=False,
    tags=["mlops", "sensors", "training"]
) as dag:

    # ==========================================
    # 3. Instanciation des Capteurs (Sensors)
    # ==========================================
    
    # Capteur 1 : Vérifier la disponibilité de nouvelles données brutes
    sensor_new_data = SqlSensor(
        task_id='wait_for_new_data',
        conn_id='postgres_default',
        sql="""
            SELECT CASE WHEN COUNT(*) >= 50 THEN 1 ELSE 0 END 
            FROM sensor_readings 
            WHERE event_time >= NOW() - INTERVAL '24 hours';
        """,
        mode='poke',
        poke_interval=60, # Vérifie toutes les 60 secondes
        timeout=600       # Abandonne au bout de 10 minutes
    )

    # Capteur 2 : Attendre la fin du traitement streaming/batch
    # (On vérifie que la table de résumé du jour a bien été créée par l'autre DAG)
    sensor_streaming_batch = SqlSensor(
        task_id='wait_for_streaming_processing',
        conn_id='postgres_default',
        sql="""
            SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END
            FROM sensor_daily_summary
            WHERE summary_date = DATE(NOW());
        """,
        mode='poke',
        poke_interval=60,
        timeout=1800
    )

    # ==========================================
    # 4. Instanciation des Tâches ML
    # ==========================================

    task_extract = PythonOperator(
        task_id='extract_sensors_data',
        python_callable=extract_data,
    )

    task_validate = PythonOperator(
        task_id='validate_quality',
        python_callable=validate_quality,
    )

    task_train = PythonOperator(
        task_id='train_model',
        python_callable=train_model,
    )

    task_evaluate = PythonOperator(
        task_id='evaluate_model',
        python_callable=evaluate_model,
    )

    task_branch = BranchPythonOperator(
        task_id='model_improvement_check',
        python_callable=check_improvement,
    )

    task_deploy = PythonOperator(
        task_id='deploy_if_better',
        python_callable=deploy_model,
    )

    task_end = EmptyOperator(
        task_id='end_pipeline'
    )

    # ==========================================
    # 5. Ordre d'exécution (Le Graphe / DAG)
    # ==========================================

    # ⚠️ C'est ici que la magie opère : les capteurs bloquent l'extraction
    # tant que les données ne sont pas prêtes !
    sensor_new_data >> sensor_streaming_batch >> task_extract 
    
    # Le reste du pipeline s'exécute ensuite normalement
    task_extract >> task_validate >> task_train >> task_evaluate >> task_branch
    task_branch >> [task_deploy, task_end]
