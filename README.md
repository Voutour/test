Voici le code complet et définitif. J'ai revu l'ensemble du script pour m'assurer qu'il n'y ait aucune variable manquante (j'ai notamment corrigé un oubli sur `gen_schema` dans le générateur concurrent) et j'ai structuré le faux flux de données pour qu'il envoie des "vagues" régulières, ce qui te permettra de bien voir la concurrence en action.

Ce package final contient la version **Spark (Python)** et la version **Starburst (SQL)** avec toute la traçabilité des horodatages (Observabilité).

### 1. Le script Spark complet (`main_cloudevent.py`)
Avant de lancer ce code, pense bien à vider tes dossiers sur S3 (`input_cloudevent_raw`, `checkpoint_cloudevent`, `cloudevent_landing`) pour partir sur une base totalement propre.

```python
import threading
import time
import uuid
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, MapType
from pyspark.sql.functions import current_timestamp, to_json, col

# ==========================================
# CONSTANTES & CHEMINS S3
# ==========================================
S3_ENDPOINT = "https://s3.direct.eu-fr2.cloud-object-storage.appdomain.cloud"
RAW_BUCKET_PATH = "s3a://bu002i004226/poc_streaming/input_cloudevent_raw/"
CHECKPOINT_PATH = "s3a://bu002i004226/poc_streaming/checkpoint_cloudevent/"
PARQUET_OUTPUT_PATH = "s3a://bu002i004226/poc_streaming/cloudevent_landing/"

# ==========================================
# 1. INIT SPARK (Optimisé pour la concurrence)
# ==========================================
print("### INIT SPARK - STREAMING PARQUET (CONCURRENT) ###")
spark = SparkSession.builder \
    .appName("DevX-CloudEvent-Stream-Concurrent") \
    .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.default.parallelism", "10") \
    .config("spark.sql.shuffle.partitions", "10") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ==========================================
# 2. GÉNÉRATEUR MOCK (Tempêtes de données)
# ==========================================
def generate_storm_events():
    # Configuration du test de charge : 5 vagues de 50 événements
    max_batches = 5
    events_per_batch = 50
    sleep_between_batches = 5 
    
    gen_schema = StructType([
        StructField("specversion", StringType(), True),
        StructField("type", StringType(), True),
        StructField("source", StringType(), True),
        StructField("subject", StringType(), True),
        StructField("id", StringType(), True),
        StructField("time", StringType(), True), # 1. Timestamp Origine
        StructField("datacontenttype", StringType(), True),
        StructField("data", MapType(StringType(), StringType()), True),
        StructField("dh_poc_gen_timestamp", StringType(), True) # 2. Timestamp Écriture S3
    ])

    print(f"\n[GÉNÉRATEUR] Démarrage... {max_batches} vagues prévues.")
    
    for batch in range(max_batches):
        mock_data = []
        print(f"[GÉNÉRATEUR] --- Vague {batch + 1}/{max_batches} en cours d'écriture ---")
        
        for i in range(events_per_batch):
            event_id = str(uuid.uuid4())
            time_origin = datetime.utcnow().isoformat() + "Z"
            time_written = datetime.utcnow().isoformat() + "Z"
            
            mock_data.append((
                "1.0", 
                "com.sensor", 
                f"/device-{i}", 
                "telemetry", 
                event_id, 
                time_origin, 
                "application/json", 
                {"temp": str(20 + i)}, 
                time_written
            ))
        
        df_storm = spark.createDataFrame(mock_data, schema=gen_schema)
        # .repartition(4) force Spark à écrire 4 fichiers en parallèle sur S3 par vague
        df_storm.repartition(4).write.mode("append").json(RAW_BUCKET_PATH)
        time.sleep(sleep_between_batches)

# Lancement du générateur en arrière-plan
threading.Thread(target=generate_storm_events).start()

# ==========================================
# 3. LECTURE STREAMING & TRANSFORMATION
# ==========================================
read_schema = StructType([
    StructField("specversion", StringType(), True),
    StructField("type", StringType(), True),
    StructField("source", StringType(), True),
    StructField("subject", StringType(), True),
    StructField("id", StringType(), True),
    StructField("time", StringType(), True),
    StructField("datacontenttype", StringType(), True),
    StructField("data", MapType(StringType(), StringType()), True),
    StructField("dh_poc_gen_timestamp", StringType(), True)
])

df_stream = spark.readStream \
    .format("json") \
    .schema(read_schema) \
    .load(RAW_BUCKET_PATH)

# Ajout du timestamp de lecture par Spark (3. Détection par le listener)
df_processed = df_stream \
    .withColumn("data", to_json(col("data"))) \
    .withColumn("dh_poc_spark_read_timestamp", current_timestamp())

# ==========================================
# 4. ÉCRITURE STREAMING (VERS PARQUET LANDING)
# ==========================================
print("### DÉMARRAGE DU STREAMING VERS PARQUET SUR S3 ###")
query = df_processed.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .trigger(processingTime="2 seconds") \
    .option("maxFilesPerTrigger", 200) \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .option("path", PARQUET_OUTPUT_PATH) \
    .start()

query.awaitTermination()
```

---

### 2. Les requêtes Starburst (SQL)
Pendant que ton job Spark tourne et remplit le dossier "Landing", ouvre ton éditeur Starburst et exécute ces trois requêtes à la suite.

**A. Création de la table de transit (Celle qui lit le Parquet brut)**
```sql
DROP TABLE IF EXISTS dh_poc_hive.pocspark.cloudevent_landing;

CREATE TABLE dh_poc_hive.pocspark.cloudevent_landing (
    specversion VARCHAR,
    type VARCHAR,
    source VARCHAR,
    subject VARCHAR,
    id VARCHAR,
    time VARCHAR,
    datacontenttype VARCHAR,
    data VARCHAR,
    dh_poc_gen_timestamp VARCHAR,
    dh_poc_spark_read_timestamp TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format = 'PARQUET',
    external_location = 's3a://bu002i004226/poc_streaming/cloudevent_landing/'
);
```

**B. Création de la table officielle (Iceberg)**
```sql
DROP TABLE IF EXISTS dh_poc_ice.pocspark.cloudevent_raw;

CREATE TABLE dh_poc_ice.pocspark.cloudevent_raw (
    specversion VARCHAR,
    type VARCHAR,
    source VARCHAR,
    subject VARCHAR,
    id VARCHAR,
    time VARCHAR,
    datacontenttype VARCHAR,
    data VARCHAR,
    dh_poc_gen_timestamp VARCHAR,
    dh_poc_spark_read_timestamp TIMESTAMP(6) WITH TIME ZONE,
    dh_poc_starburst_insert_timestamp TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format = 'PARQUET'
);
```

**C. L'alimentation finale (Avec la Sysdate Starburst)**
Dès que tu lances cette requête, Starburst va aspirer toutes les données générées par Spark et y apposer son tampon de date d'intégration.
```sql
INSERT INTO dh_poc_ice.pocspark.cloudevent_raw
SELECT 
    specversion,
    type,
    source,
    subject,
    id,
    time,
    datacontenttype,
    data,
    dh_poc_gen_timestamp,
    dh_poc_spark_read_timestamp,
    current_timestamp AS dh_poc_starburst_insert_timestamp
FROM dh_poc_hive.pocspark.cloudevent_landing;
```

Dès que ce dernier bloc est validé, il ne te restera plus qu'à faire un `SELECT * FROM dh_poc_ice.pocspark.cloudevent_raw;` pour contempler ton travail, tes données concurrentes et tes 4 horodatages de traçabilité !
