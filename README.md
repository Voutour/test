Passer de l'environnement de développement (DevX / Notebook) à **Apache Airflow**, c'est exactement la démarche à suivre pour industrialiser ton PoC et le mettre en production.

Cependant, il y a un point architectural crucial à comprendre avant d'écrire le code : **Airflow est un chef d'orchestre, pas un ouvrier.**
Airflow ne va pas exécuter ton code Python/Spark sur sa propre machine. Il va envoyer une requête via une **API HTTP (généralement Apache Livy ou une API interne DevX)** pour dire au cluster Spark : *"Tiens, voici le chemin de mon script sur S3, lance-le et préviens-moi quand c'est fini."*

De plus, attention au piège du Streaming : Airflow est fait pour des tâches qui ont un début et une fin (Batch). Si ton script utilise `writeStream.awaitTermination()`, il tournera à l'infini, et la tâche Airflow restera bloquée au statut "Running" pour toujours. 

Voici la méthode standard pour orchestrer ce lancement via API avec Airflow.

### Étape 1 : Déposer le script sur le COS (S3)
Ton cluster Spark de production ne peut pas lire le fichier `main_cloudevent.py` qui est sur ton espace DevX personnel. Tu dois l'uploader sur ton bucket S3.
*Exemple : `s3a://bu002i004226/poc_streaming/scripts/main_cloudevent.py`*

### Étape 2 : Le DAG Airflow (Méthode API / Apache Livy)

Dans les environnements Hadoop/Spark d'entreprise (comme IBM Cloud ou les banques), l'API standard pour soumettre des jobs Spark est **Apache Livy**. 

Voici à quoi ressemble le code de ton DAG Airflow (`dag_spark_starburst.py`) qui utilise l'API REST pour lancer ton code.

```python
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from datetime import datetime, timedelta
import json

# ==========================================
# 1. CONFIGURATION DU DAG
# ==========================================
default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'poc_spark_starburst_api',
    default_args=default_args,
    description='Lance le job Spark vers Starburst via API REST',
    schedule_interval=timedelta(hours=1), # Lancement toutes les heures (Mode Batch)
    start_date=datetime(2026, 4, 21),
    catchup=False,
)

# ==========================================
# 2. LE PAYLOAD (La requête API)
# ==========================================
# C'est ici qu'on dit à l'API quel fichier lancer et avec quelle puissance
livy_payload = {
    "file": "s3a://bu002i004226/poc_streaming/scripts/main_cloudevent.py",
    "name": "PoC-Airflow-Starburst",
    "conf": {
        "spark.sql.shuffle.partitions": "4",
        "spark.hadoop.fs.s3a.endpoint": "https://s3.direct.eu-fr2.cloud-object-storage.appdomain.cloud"
    }
}

# ==========================================
# 3. LES TÂCHES AIRFLOW
# ==========================================

# Tâche 1 : Soumettre le job Spark via l'API (POST)
submit_spark_job = SimpleHttpOperator(
    task_id='submit_spark_job_via_api',
    http_conn_id='spark_livy_api_conn', # Connexion configurée dans l'interface Airflow
    endpoint='/batches', # Endpoint standard de l'API Livy
    method='POST',
    data=json.dumps(livy_payload),
    headers={"Content-Type": "application/json"},
    response_filter=lambda response: response.json()['id'], # Récupère l'ID du job
    dag=dag,
)

# Tâche 2 : Vérifier si le job est terminé (Polling API)
# Airflow va interroger l'API toutes les 60 secondes pour voir si le job est "success"
monitor_spark_job = HttpSensor(
    task_id='monitor_spark_job',
    http_conn_id='spark_livy_api_conn',
    endpoint='/batches/{{ task_instance.xcom_pull(task_ids="submit_spark_job_via_api") }}',
    method='GET',
    response_check=lambda response: response.json()['state'] == 'success',
    poke_interval=60,
    timeout=3600, # Tombe en erreur si le job dure plus d'une heure
    dag=dag,
)

# ==========================================
# 4. L'ORDRE D'EXÉCUTION
# ==========================================
submit_spark_job >> monitor_spark_job
```

### Comment l'intégrer dans ton entreprise ?

Pour que ce DAG fonctionne, il te manque une information clé : l'adresse de l'API de ton cluster de production.

Dans l'interface web de ton Airflow (menu *Admin* > *Connections*), tu devras créer une connexion nommée `spark_livy_api_conn`. C'est là que tu mettras l'URL fournie par ton équipe d'infrastructure (par exemple : `https://spark-cluster.banque.intra:8998`) et les identifiants techniques associés.

**Sais-tu si ton entreprise utilise un orchestrateur Airflow standard avec l'API Apache Livy, ou s'ils ont développé leurs propres "Custom Operators" (des opérateurs Airflow spécifiques à la BNP) pour lancer les jobs DevX ?**



C'est noté, je l'élimine définitivement. Désolé d'avoir insisté avec ça !

Voici le code hybride (Spark + API REST Python) **totalement expurgé de `dotenv`**. Tu as juste à mettre ton identifiant à la ligne 12 et c'est parti.

```python
import time
import uuid
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col
from pyspark.sql.types import StructType, StructField, StringType
import trino

# ==========================================
# 1. CONFIGURATIONS (Sans dotenv)
# ==========================================
# Mets ton identifiant ici en dur
STARBURST_USER = "ton_identifiant" 

S3_ENDPOINT = "https://s3.direct.eu-fr2.cloud-object-storage.appdomain.cloud"
S3_RAW_PATH = "s3a://bu002i004226/poc_streaming/input_cloudevent_raw/"

run_id = int(time.time())
CHECKPOINT_PATH = f"s3a://bu002i004226/poc_streaming/checkpoint_direct_{run_id}/"

TRINO_HOST = "starburst-ap26761-dev-05b792a6.data.cloud.net.intra"
TRINO_PORT = 443

# ==========================================
# 2. INITIALISATION SPARK
# ==========================================
spark = SparkSession.builder \
    .appName("PoC-Push-To-Starburst-PythonREST") \
    .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()
    
spark.sparkContext.setLogLevel("WARN")

schema_events = StructType([
    StructField("id", StringType(), True),
    StructField("time", StringType(), True),
    StructField("dh_poc_gen_timestamp", StringType(), True)
])

# ==========================================
# 3. GÉNÉRATION DES DONNÉES
# ==========================================
print("### [ÉTAPE 1] Génération d'un lot de données... ###")
mock_data = []
for i in range(20):
    now = datetime.utcnow().isoformat()[:-3] + "Z"
    mock_data.append((str(uuid.uuid4()), now, now))

df_gen = spark.createDataFrame(mock_data, schema=schema_events)
df_gen.write.mode("overwrite").json(S3_RAW_PATH)
print("### [ÉTAPE 1] 20 événements générés avec succès. ###\n")

# ==========================================
# 4. STREAMING : SPARK -> PYTHON TRINO -> STARBURST
# ==========================================
print("### [ÉTAPE 2] DÉMARRAGE DU STREAMING HYBRIDE ###")

df_stream = spark.readStream.format("json").schema(schema_events).load(S3_RAW_PATH)

df_processed = df_stream.select(
    col("id"),
    col("time"), # T1
    col("dh_poc_gen_timestamp"), # T2
    current_timestamp().cast("timestamp").alias("dh_poc_spark_read_timestamp") # T3
)

def push_to_starburst_via_python(df, batch_id):
    rows = df.collect() 
    
    if len(rows) > 0:
        print(f"### Préparation de l'envoi du batch {batch_id} ({len(rows)} lignes)... ###")
        
        values_list = []
        for row in rows:
            t3_str = row.dh_poc_spark_read_timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            values_list.append(f"('{row.id}', '{row.time}', '{row.dh_poc_gen_timestamp}', TIMESTAMP '{t3_str}')")
            
        values_string = ",\n".join(values_list)
        sql_query = f"INSERT INTO dh_poc_ice.pocspark.cloudevent_direct (id, time, dh_poc_gen_timestamp, dh_poc_spark_read_timestamp) VALUES {values_string}"
        
        try:
            conn = trino.dbapi.connect(
                host=TRINO_HOST,
                port=TRINO_PORT,
                user=STARBURST_USER,
                http_scheme='https',
                verify=False
            )
            cur = conn.cursor()
            cur.execute(sql_query)
            print(f"### -> Batch {batch_id} inséré avec succès dans Starburst via REST ! ###")
        except Exception as e:
            print(f"\n[!!!] ERREUR TRINO SUR LE BATCH {batch_id} [!!!]")
            print(str(e))
            print("--------------------------------------------------\n")

query = df_processed.writeStream \
    .foreachBatch(push_to_starburst_via_python) \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .trigger(processingTime="5 seconds") \
    .start()

query.awaitTermination()
```

Plus de fichier caché, plus d'import `os`, juste du Python pur ! Dis-moi si ce module passe enfin le pare-feu.
