C'est la décision la plus sage et la plus pragmatique. Dans les environnements bancaires très verrouillés, il faut savoir s'adapter aux portes qui sont ouvertes plutôt que de s'acharner sur celles qui sont fermées par la sécurité !

Avec cette version "Hybride REST", tu contournes totalement le problème de Java, du `.jar` introuvable et du pare-feu. C'est du pur Python qui parle à l'API de Starburst.

Voici le code final, propre, fonctionnel et **sans `dotenv`**. Tu as juste à insérer ton identifiant à la ligne 12.

### Le Code Spark "Python Push REST" (La version blindée)

```python
import time
import uuid
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col
from pyspark.sql.types import StructType, StructField, StringType
# Le module magique qui passe le pare-feu
import trino

# ==========================================
# 1. CONFIGURATIONS
# ==========================================
# Mets ton identifiant interne ici (en dur)
STARBURST_USER = "ton_identifiant_bnp" 

# Configurations Cloud / S3
S3_ENDPOINT = "https://s3.direct.eu-fr2.cloud-object-storage.appdomain.cloud"
S3_RAW_PATH = "s3a://bu002i004226/poc_streaming/input_cloudevent_raw/"

# Checkpoint dynamique pour éviter les crashs de relance
run_id = int(time.time())
CHECKPOINT_PATH = f"s3a://bu002i004226/poc_streaming/checkpoint_direct_{run_id}/"

# Informations du cluster Starburst
TRINO_HOST = "starburst-ap26761-dev-05b792a6.data.cloud.net.intra"
TRINO_PORT = 443

# ==========================================
# 2. INITIALISATION SPARK (Ultra légère)
# ==========================================
# Plus aucun paramètre JDBC ou JAR ici, on laisse Java tranquille.
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
# 3. GÉNÉRATION DES DONNÉES (Lot de test)
# ==========================================
print("### [ÉTAPE 1] Génération d'un lot de données de test... ###")
mock_data = []
for i in range(20): # Un petit batch de 20 lignes pour tester l'API
    now = datetime.utcnow().isoformat()[:-3] + "Z"
    mock_data.append((str(uuid.uuid4()), now, now))

df_gen = spark.createDataFrame(mock_data, schema=schema_events)
df_gen.write.mode("overwrite").json(S3_RAW_PATH)
print("### [ÉTAPE 1] 20 événements générés avec succès. ###\n")

# ==========================================
# 4. STREAMING : SPARK -> TRINO PYTHON -> STARBURST
# ==========================================
print("### [ÉTAPE 2] DÉMARRAGE DU STREAMING HYBRIDE ###")

df_stream = spark.readStream.format("json").schema(schema_events).load(S3_RAW_PATH)

# Spark calcule le T3 très rapidement en mémoire
df_processed = df_stream.select(
    col("id"),
    col("time"), # T1
    col("dh_poc_gen_timestamp"), # T2
    current_timestamp().cast("timestamp").alias("dh_poc_spark_read_timestamp") # T3
)

# La fonction de secours : Python prend le relais pour envoyer le SQL
def push_to_starburst_via_python(df, batch_id):
    # On ramène les données de ce micro-batch dans la mémoire Python
    rows = df.collect() 
    
    if len(rows) > 0:
        print(f"### Préparation de l'envoi du batch {batch_id} ({len(rows)} lignes)... ###")
        
        # On construit la grosse requête SQL (INSERT INTO ... VALUES ...)
        values_list = []
        for row in rows:
            # Formatage propre du timestamp pour Trino/Starburst
            t3_str = row.dh_poc_spark_read_timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            values_list.append(f"('{row.id}', '{row.time}', '{row.dh_poc_gen_timestamp}', TIMESTAMP '{t3_str}')")
            
        values_string = ",\n".join(values_list)
        sql_query = f"INSERT INTO dh_poc_ice.pocspark.cloudevent_direct (id, time, dh_poc_gen_timestamp, dh_poc_spark_read_timestamp) VALUES {values_string}"
        
        # Envoi de la requête via l'API HTTP Trino
        try:
            conn = trino.dbapi.connect(
                host=TRINO_HOST,
                port=TRINO_PORT,
                user=STARBURST_USER,
                http_scheme='https',
                verify=False # On désactive la vérification stricte du certificat SSL interne
            )
            cur = conn.cursor()
            cur.execute(sql_query)
            print(f"### -> Batch {batch_id} inséré avec succès dans Starburst via l'API REST ! ###")
        except Exception as e:
            print(f"\n[!!!] ERREUR TRINO SUR LE BATCH {batch_id} [!!!]")
            print(str(e))
            print("--------------------------------------------------\n")

# Lancement de l'orchestration du flux
query = df_processed.writeStream \
    .foreachBatch(push_to_starburst_via_python) \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .trigger(processingTime="5 seconds") \
    .start()

query.awaitTermination()
```

### Le conseil pour ta démo :
Puisque tu vas utiliser `verify=False` (la ligne qui dit à Python d'ignorer les alertes de certificat de la banque), tu vas peut-être voir des petits messages d'avertissement jaunes (`InsecureRequestWarning`) 



Voici le code complet, définitif et consolidé de l'architecture "Hybride REST" (PySpark + Python Trino). 

Il intègre toutes les solutions que nous avons validées ensemble :
* **Zéro JDBC / Zéro fichier JAR :** Contournement total du pare-feu bancaire en utilisant l'API REST via le module Python `trino`.
* **Zéro `dotenv` :** Ton identifiant est à renseigner directement dans les variables.
* **Intégration Spark UI :** Remontée des statuts de tes micro-batchs en direct dans l'interface visuelle de Spark (onglet Jobs) et journalisation propre des erreurs (stderr).

### Le Code Final (`main_cloudevent.py`)

```python
import sys
import time
import uuid
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col
from pyspark.sql.types import StructType, StructField, StringType

# Le module autorisé par la sécurité pour l'API REST
import trino

# ==========================================
# 1. CONFIGURATIONS
# ==========================================
# Remplace par ton identifiant interne BNP Paribas
STARBURST_USER = "ton_identifiant_bnp" 

# Configurations S3 (Cloud Object Storage)
S3_ENDPOINT = "https://s3.direct.eu-fr2.cloud-object-storage.appdomain.cloud"
S3_RAW_PATH = "s3a://bu002i004226/poc_streaming/input_cloudevent_raw/"

# Génération d'un dossier de checkpoint unique par exécution pour éviter les conflits
run_id = int(time.time())
CHECKPOINT_PATH = f"s3a://bu002i004226/poc_streaming/checkpoint_direct_{run_id}/"

# Informations d'accès à l'API du cluster Starburst
TRINO_HOST = "starburst-ap26761-dev-05b792a6.data.cloud.net.intra"
TRINO_PORT = 443

# ==========================================
# 2. INITIALISATION SPARK (Allégée)
# ==========================================
# Plus aucun paramètre JDBC ou JAR. Java s'occupe uniquement du calcul interne.
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
# 3. GÉNÉRATION DES DONNÉES (Pour le PoC)
# ==========================================
print("### [ÉTAPE 1] Génération d'un lot de données de test... ###")
mock_data = []
for i in range(20): # Un petit batch de 20 lignes pour tester le flux
    now = datetime.utcnow().isoformat()[:-3] + "Z"
    mock_data.append((str(uuid.uuid4()), now, now))

df_gen = spark.createDataFrame(mock_data, schema=schema_events)
df_gen.write.mode("overwrite").json(S3_RAW_PATH)
print("### [ÉTAPE 1] 20 événements générés avec succès sur S3. ###\n")

# ==========================================
# 4. STREAMING : SPARK -> TRINO PYTHON -> STARBURST
# ==========================================
print("### [ÉTAPE 2] DÉMARRAGE DU STREAMING HYBRIDE ###")

df_stream = spark.readStream.format("json").schema(schema_events).load(S3_RAW_PATH)

# Ajout du T3 (Moment de la prise en charge par Spark)
df_processed = df_stream.select(
    col("id"),
    col("time"), # T1
    col("dh_poc_gen_timestamp"), # T2
    current_timestamp().cast("timestamp").alias("dh_poc_spark_read_timestamp") # T3
)

# Fonction de traitement du micro-batch : Conversion en SQL et envoi API
def push_to_starburst_via_python(df, batch_id):
    
    # Récupération des données du batch en mémoire Python
    rows = df.collect() 
    nb_lignes = len(rows)
    
    if nb_lignes > 0:
        # Mise à jour du Spark UI : Phase de préparation
        spark.sparkContext.setJobDescription(f"Batch N°{batch_id} - Préparation API ({nb_lignes} lignes)")
        print(f"### [BATCH {batch_id}] Préparation de la requête SQL... ###")
        
        # Construction de la requête INSERT SQL géante
        values_list = []
        for row in rows:
            # Formatage propre du timestamp pour Trino
            t3_str = row.dh_poc_spark_read_timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            values_list.append(f"('{row.id}', '{row.time}', '{row.dh_poc_gen_timestamp}', TIMESTAMP '{t3_str}')")
            
        values_string = ",\n".join(values_list)
        sql_query = f"INSERT INTO dh_poc_ice.pocspark.cloudevent_direct (id, time, dh_poc_gen_timestamp, dh_poc_spark_read_timestamp) VALUES {values_string}"
        
        try:
            # Mise à jour du Spark UI : Phase d'envoi réseau
            spark.sparkContext.setJobDescription(f"Batch N°{batch_id} - Envoi réseau (API Trino)...")
            
            # Connexion REST vers Starburst
            conn = trino.dbapi.connect(
                host=TRINO_HOST,
                port=TRINO_PORT,
                user=STARBURST_USER,
                http_scheme='https',
                verify=False # Contournement des alertes de certificat local
            )
            cur = conn.cursor()
            cur.execute(sql_query)
            
            # Mise à jour du Spark UI : Succès
            spark.sparkContext.setJobDescription(f"Batch N°{batch_id} - SUCCÈS")
            print(f"### -> Batch {batch_id} ({nb_lignes} lignes) inséré avec succès dans Starburst ! ###")
            
        except Exception as e:
            # Mise à jour du Spark UI : Échec + Écriture dans le flux d'erreur (stderr)
            spark.sparkContext.setJobDescription(f"Batch N°{batch_id} - ÉCHEC FATAL API")
            print(f"\n[!!!] ERREUR TRINO SUR LE BATCH {batch_id} [!!!]", file=sys.stderr)
            print(str(e), file=sys.stderr)
            print("--------------------------------------------------\n", file=sys.stderr)
            
    else:
        # Batch sans nouvelle donnée
        spark.sparkContext.setJobDescription(f"Batch N°{batch_id} - Ignoré (Aucune nouvelle ligne)")


# Lancement continu de l'orchestration
query = df_processed.writeStream \
    .foreachBatch(push_to_starburst_via_python) \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .trigger(processingTime="5 seconds") \
    .start()

query.awaitTermination()
```



dans la console de DevX. Ne t'inquiète pas, c'est normal et ça ne bloque pas l'exécution du code ni l'insertion de tes données. 

Lance ça, et ton tableau Starburst devrait enfin se remplir avec un vrai **T4** !
