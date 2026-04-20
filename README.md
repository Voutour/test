Bien vu ! C'est la preuve ultime que fouiller dans la documentation interne (Confluence) est toujours la meilleure solution. Et c'est une excellente nouvelle : ton équipe infrastructure a bien fait son travail.

Si le driver est "déjà installé dans le cluster Spark", alors **pourquoi as-tu eu l'erreur `ClassNotFoundException` juste avant ?**

C'est très probablement à cause de la commande que je t'ai fait ajouter : `.config("spark.jars.packages", "io.trino:trino-jdbc:435")`.
En forçant cette configuration, Spark a cru qu'il *devait* aller le télécharger sur internet via son gestionnaire de paquets (Ivy). Quand le pare-feu l'a bloqué, Spark a paniqué et a crashé, sans même prendre la peine de regarder dans ses propres dossiers locaux s'il l'avait déjà !

### La Solution : Nettoyer l'initialisation

Puisque le cluster possède déjà le driver en natif, on ne demande plus rien à Spark. On le laisse démarrer normalement, et on fait simplement appel au connecteur JDBC.

Voici le code nettoyé. J'ai retiré toutes les options de `.jars` ou `.packages`.

```python
import time
import uuid
import os
from datetime import datetime
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col
from pyspark.sql.types import StructType, StructField, StringType, MapType

# ==========================================
# 1. CONFIGURATIONS
# ==========================================
load_dotenv()
STARBURST_USER = os.getenv("USER")
STARBURST_PASS = os.getenv("PASSWORD")

S3_ENDPOINT = "https://s3.direct.eu-fr2.cloud-object-storage.appdomain.cloud"
S3_RAW_PATH = "s3a://bu002i004226/poc_streaming/input_cloudevent_raw/"

run_id = int(time.time())
CHECKPOINT_PATH = f"s3a://bu002i004226/poc_streaming/checkpoint_direct_{run_id}/"

STARBURST_JDBC_URL = "jdbc:trino://starburst-ap26761-dev-05b792a6.data.cloud.net.intra:443/dh_poc_ice?SSL=true"

# ==========================================
# 2. INITIALISATION SPARK (Épurée)
# ==========================================
# On retire toute mention aux JARS. On fait confiance au cluster BNPP.
spark = SparkSession.builder \
    .appName("PoC-Push-To-Starburst-Direct") \
    .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.sql.shuffle.partitions", "4") \
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
print("### [ÉTAPE 1] Génération d'un lot de données de test... ###")
mock_data = []
for i in range(50):
    now = datetime.utcnow().isoformat() + "Z"
    mock_data.append((str(uuid.uuid4()), now, now))

df_gen = spark.createDataFrame(mock_data, schema=schema_events)
df_gen.write.mode("overwrite").json(S3_RAW_PATH)
print("### [ÉTAPE 1] 50 événements générés avec succès. ###\n")

# ==========================================
# 4. STREAMING : PUSH VERS STARBURST
# ==========================================
print("### [ÉTAPE 2] DÉMARRAGE DU STREAMING : SPARK -> JDBC -> STARBURST ###")

df_stream = spark.readStream.format("json").schema(schema_events).load(S3_RAW_PATH)

df_processed = df_stream.select(
    col("id"),
    col("time"), # T1
    col("dh_poc_gen_timestamp"), # T2
    current_timestamp().cast("timestamp").alias("dh_poc_spark_read_timestamp") # T3
)

def push_to_starburst(df, batch_id):
    if not df.isEmpty():
        print(f"### Tentative d'envoi du batch {batch_id}... ###")
        try:
            df.repartition(2).write \
                .format("jdbc") \
                .option("url", STARBURST_JDBC_URL) \
                .option("dbtable", "pocspark.cloudevent_direct") \
                .option("user", STARBURST_USER) \
                .option("password", STARBURST_PASS) \
                .option("driver", "io.trino.jdbc.TrinoDriver") \
                .mode("append") \
                .save()
            print(f"### -> Batch {batch_id} inséré avec succès dans Starburst ! ###")
        except Exception as e:
            print(f"\n[!!!] ERREUR FATALE STARBURST SUR LE BATCH {batch_id} [!!!]")
            print(str(e))
            print("--------------------------------------------------\n")

query = df_processed.writeStream \
    .foreachBatch(push_to_starburst) \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .trigger(processingTime="5 seconds") \
    .start()

query.awaitTermination()
```
