C'est la phase finale et la plus gratifiante ! Tester une orchestration Airflow, c'est comme regarder les dominos tomber : on appuie sur un seul bouton et tout s'enchaîne.

Puisque tu es dans un environnement d'entreprise structuré, on ne teste pas directement en production. Voici le plan de test pas-à-pas pour valider ton pipeline de bout en bout.

### Étape 1 : Préparer le terrain (Les fichiers)
Avant de toucher à Airflow, il faut que tes fichiers soient à leur place définitive.
1. **Le Script Spark :** Prends le dernier code hybride Python/Spark qu'on a fait (sans les fausses données générées, juste le `readStream` et le `writeStream`). Uploade ce fichier `.py` sur ton Cloud Object Storage (S3), par exemple dans un dossier `s3a://bu002i004226/poc_streaming/scripts/`.
2. **Le DAG Airflow :** Prends le code Python du DAG (celui avec `SimpleHttpOperator`) et dépose-le dans le dossier `dags/` de ton environnement Airflow (souvent géré via un dépôt Git ou une interface d'upload).

### Étape 2 : Configurer la tuyauterie (Dans Airflow)
Airflow a besoin de savoir comment parler à ton cluster Spark.
1. Ouvre l'interface web de ton **Airflow**.
2. Va dans le menu en haut : **Admin -> Connections**.
3. Clique sur le bouton **"+"** pour ajouter une connexion :
   * **Conn Id :** `connexion_api_sparktacus` *(doit être exactement le nom mis dans ton code DAG)*.
   * **Conn Type :** `HTTP`.
   * **Host :** L'URL de l'API de ton cluster (ex: `https://sparktacus.cloud.intra`).
   * **Login / Password :** Tes identifiants ou le token d'API fourni par ton équipe.
4. Sauvegarde.

### Étape 3 : Le test grandeur nature (Le bouton "Play")
C'est le moment de vérité.
1. Retourne sur la page d'accueil d'Airflow (le tableau de bord des DAGs).
2. Cherche ton DAG : `deploy_sparktacus_streaming_api`.
3. Active-le (en basculant le petit interrupteur sur **"On"** ou **"Unpause"**).
4. Clique sur le bouton **"Trigger DAG"** (le bouton "Play" ▷).

### Étape 4 : Le Monitoring (Où regarder ?)
Pendant que ça tourne, voici comment suivre l'exécution :
1. **Dans Airflow :** Clique sur la tâche en cours d'exécution et ouvre l'onglet **"Logs"**. Tu y verras la réponse de l'API (ex: *"Job 142 submitted successfully"*).
2. **Dans la console Spark (Livy/DevX) :** Si tu as accès à l'interface de monitoring de ton cluster Spark, tu devrais voir ton job s'afficher au statut "Running".
3. **Le test Ultime (Dans Starburst) :** Ouvre l'interface de Starburst et lance la fameuse requête de latence :
   ```sql
   SELECT id, dh_poc_spark_read_timestamp, dh_poc_starburst_receive_timestamp 
   FROM dh_poc_ice.pocspark.cloudevent_direct 
   ORDER BY dh_poc_starburst_receive_timestamp DESC LIMIT 10;
   ```

Si des lignes s'affichent et s'actualisent, félicitations, ton architecture CTDF est opérationnelle de bout en bout !

**Une question cruciale pour ton déploiement :** Dans ton entreprise, as-tu le droit de glisser-déposer ton fichier DAG directement dans l'interface Airflow, ou dois-tu obligatoirement le *commiter* sur un dépôt Gitlab/Bitbucket pour qu'un pipeline CI/CD le déploie à ta place ?


Merci pour le schéma ! Il est extrêmement clair et nous donne la "Big Picture" de ton projet (l'architecture CTDF). 

Ce schéma valide d'ailleurs à 100 % ce que nous venons de construire ensemble. Regarde bien la flèche en pointillés qui part de la boîte **Sparktacus** vers la boîte **Refined Data storage [Starburst]** : il est explicitement écrit **"SQL insert"**. Ton script hybride Spark/Trino est donc en parfaite adéquation avec la vision des architectes !

Maintenant, décortiquons comment **Apache Airflow** et les **APIs** s'intègrent dans ce schéma précis. Il y a souvent une confusion entre l'API qui reçoit la donnée et l'API qui lance le code.

### 1. Ne pas confondre les deux APIs du schéma

Sur ton schéma, la mention **"Mediation API POST Generic Event"** tout à gauche n'a rien à voir avec Airflow. 
* C'est la porte d'entrée des données. En production, tu vas supprimer notre petite fonction Python `generate_events()`. À la place, ce sont les applications métier (Source A) qui vont faire des requêtes HTTP POST vers cette API Mediation pour y déposer les JSON.
* **Le travail d'Airflow ne se situe pas ici.**

### 2. Le vrai rôle d'Airflow (Gérer "Sparktacus")

Ton script (le Stream Spark) s'exécute dans la zone **Sparktacus** (en bas).
Puisque le schéma indique un **"TARGET : En moins d'une minute"**, ton script est un job de **Streaming continu** (il ne s'arrête jamais). 

Airflow n'est pas fait pour faire tourner du streaming en son sein. Son rôle va être d'agir comme un **Superviseur via l'API du cluster Spark**. Airflow va dire au cluster : *"Voici le script Sparktacus, lance-le. S'il plante, je le relancerai."*

### 3. Le Code Airflow (Le DAG de déploiement)

Pour lancer ton script via Airflow, on utilise l'API de ton cluster (généralement Apache Livy sur les environnements Hadoop/Cloud). Airflow va faire une requête HTTP POST (API) vers le cluster pour soumettre le script Python que tu auras déposé sur le COS.

Voici le DAG Airflow type à implémenter pour orchestrer ton script Sparktacus :

```python
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from datetime import datetime, timedelta
import json

# ==========================================
# 1. CONFIGURATION DU DAG AIRFLOW
# ==========================================
default_args = {
    'owner': 'equipe_data_ctdf',
    'depends_on_past': False,
    'email_on_failure': True, # Alerte si le stream plante
    'retries': 3, # On essaie de relancer 3 fois en cas de crash
    'retry_delay': timedelta(minutes=2),
}

# Un DAG de supervision : il vérifie l'état une fois par jour ou se déclenche manuellement
dag = DAG(
    'deploy_sparktacus_streaming_api',
    default_args=default_args,
    description='Déploie le script Sparktacus (Push vers Starburst) via API',
    schedule_interval='@daily', 
    start_date=datetime(2026, 4, 21),
    catchup=False,
)

# ==========================================
# 2. LE PAYLOAD POUR L'API SPARK (Livy)
# ==========================================
# C'est ici qu'on indique à l'API quel script exécuter
spark_livy_payload = {
    # Le chemin de ton script Python final stocké sur le COS (S3)
    "file": "s3a://bu002i004226/poc_streaming/scripts/main_cloudevent.py",
    "name": "Sparktacus-Streaming-To-Starburst",
    "conf": {
        "spark.sql.shuffle.partitions": "4",
        "spark.hadoop.fs.s3a.endpoint": "https://s3.direct.eu-fr2.cloud-object-storage.appdomain.cloud",
        "spark.hadoop.fs.s3a.path.style.access": "true"
    }
}

# ==========================================
# 3. L'APPEL API (La tâche Airflow)
# ==========================================
# Airflow fait un POST vers l'API de ton cluster pour démarrer le job
lancer_job_spark = SimpleHttpOperator(
    task_id='trigger_spark_via_api',
    http_conn_id='connexion_api_sparktacus', # La connexion définie dans l'interface Airflow (URL + Token)
    endpoint='/batches', # L'endpoint standard de soumission
    method='POST',
    data=json.dumps(spark_livy_payload),
    headers={"Content-Type": "application/json"},
    dag=dag,
)

lancer_job_spark
```

### Ce qu'il te reste à préparer pour l'industrialisation :

1. **Nettoyer ton code actuel :** Retire toute la partie de génération de données (le `mock_data`). Ton script doit uniquement faire la lecture (`readStream`) et l'écriture (`push_to_starburst_via_python`).
2. **Uploader le script :** Dépose ce fichier `.py` propre dans un dossier de ton COS (par exemple `s3a://bu002i004226/poc_streaming/scripts/`).
3. **Créer la connexion dans Airflow :** Dans l'interface web de ton Airflow d'entreprise, ajoute une connexion nommée `connexion_api_sparktacus` avec l'URL de l'API de ton cluster Spark et les identifiants techniques.



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
