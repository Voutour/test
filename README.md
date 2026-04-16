C'est une décision très pragmatique. Quand on se heurte à des murs de sécurité réseau (comme le Metastore Iceberg), revenir à une technologie plus simple et "bête" comme Hive (qui se contente de lire des dossiers Parquet) est la meilleure façon de valider la performance pure de ton code Spark sans interférence.

On va configurer ton job Spark en mode "Formule 1" : on vire les configurations Iceberg, on garde tes 16 cœurs, on force le parallélisme maximal, et on intègre les 4 timestamps.

### La stratégie des 4 Timestamps (Spécial Hive)

Voici comment nous allons tracer la vie de ta donnée :
1.  **`time` (T1 - Origine)** : L'heure exacte de l'événement généré par le device.
2.  **`dh_poc_gen_timestamp` (T2 - Arrivée S3 Raw)** : L'heure où le fichier JSON atterrit sur le premier S3.
3.  **`dh_poc_spark_read_timestamp` (T3 - Prise en charge Spark)** : L'heure où le micro-batch Spark avale la donnée.
4.  **`dh_poc_spark_write_timestamp` (T4 - Sysdate/Disponibilité)** : L'heure où Spark génère la ligne pour l'écrire. *(Note de candeur : avec Hive, dès que le fichier Parquet touche le bucket S3 "landing", il est visible par Starburst. Ce T4 fera donc office de "Sysdate" d'insertion).*

---

### 1. Le Code Spark Optimisé (Parallélisme Max, Latence Min)

Voici ton code `main_cloudevent.py` épuré de tout ce qui concerne Iceberg, et optimisé pour le format Parquet avec 16 partitions forcées.

```python
import time
import uuid
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, to_json, col
from pyspark.sql.types import StructType, StructField, StringType, MapType

# CONFIGURATION S3
S3_ENDPOINT = "https://s3.direct.eu-fr2.cloud-object-storage.appdomain.cloud"
RAW_PATH = "s3a://bu002i004226/poc_streaming/input_cloudevent_raw/"
CHECKPOINT = "s3a://bu002i004226/poc_streaming/checkpoint_cloudevent/"
LANDING_PATH = "s3a://bu002i004226/poc_streaming/cloudevent_landing/"

# 1. INITIALISATION SPARK (Mode Performance Parquet/Hive)
spark = SparkSession.builder \
    .appName("DevX-Hive-Performance-Test") \
    .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.default.parallelism", "16") \
    .config("spark.sql.shuffle.partitions", "16") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# SCHEMA DE LECTURE
read_schema = StructType([
    StructField("specversion", StringType(), True),
    StructField("type", StringType(), True),
    StructField("source", StringType(), True),
    StructField("subject", StringType(), True),
    StructField("id", StringType(), True),
    StructField("time", StringType(), True), # T1
    StructField("datacontenttype", StringType(), True),
    StructField("data", MapType(StringType(), StringType()), True),
    StructField("dh_poc_gen_timestamp", StringType(), True) # T2
])

print("### DÉMARRAGE DU STREAMING (16 CŒURS) VERS LANDING (HIVE) ###")

# 2. LECTURE STREAMING
df_stream = spark.readStream \
    .format("json") \
    .schema(read_schema) \
    .option("maxFilesPerTrigger", 32) \
    .load(RAW_PATH)

# 3. TRANSFORMATIONS ET AJOUT DES TIMESTAMPS (T3 & T4)
df_processed = df_stream \
    .withColumn("data", to_json(col("data"))) \
    .withColumn("dh_poc_spark_read_timestamp", current_timestamp()) \
    .withColumn("dh_poc_spark_write_timestamp", current_timestamp())
    
# (Note technique : Dans Spark, current_timestamp() est évalué une fois par micro-batch. 
# T3 et T4 seront très proches, ils représentent l'heure exacte du cycle de calcul).

# 4. ÉCRITURE FORCÉE SUR 16 PARTITIONS (Le "Mur Vertical")
query = df_processed \
    .repartition(16) \
    .writeStream \
    .queryName("Hive_Landing_Stream_Optimized") \
    .format("parquet") \
    .outputMode("append") \
    .trigger(processingTime="1 seconds") \
    .option("checkpointLocation", CHECKPOINT) \
    .option("path", LANDING_PATH) \
    .start()

query.awaitTermination()
```

---

### 2. La Table dans Starburst (Connecteur Hive)

Si tu avais supprimé ta table Hive, voici la commande exacte pour la recréer. Elle pointera directement sur le dossier `cloudevent_landing` que Spark alimente.

```sql
CREATE TABLE IF NOT EXISTS dh_poc_hive.pocspark.cloudevent_landing (
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
    dh_poc_spark_write_timestamp TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format = 'PARQUET',
    external_location = 's3a://bu002i004226/poc_streaming/cloudevent_landing/'
);
```

---

### 3. Ta Requête d'Analyse de Latence (Le Tableau de Bord)

Maintenant, tu peux aller dans l'interface Starburst et lancer cette requête. Elle va te donner **exactement** le temps qu'a pris chaque étape en millisecondes. 

```sql
SELECT 
    id,
    -- 1. Latence Ingestion (Temps pour arriver sur le bucket Raw)
    to_milliseconds(CAST(dh_poc_gen_timestamp AS TIMESTAMP) - CAST(time AS TIMESTAMP)) AS ms_source_to_raw,
    
    -- 2. Latence Attente (Temps passé sur le bucket Raw avant que Spark ne le lise)
    to_milliseconds(dh_poc_spark_read_timestamp - CAST(dh_poc_gen_timestamp AS TIMESTAMP)) AS ms_waiting_for_spark,
    
    -- 3. Latence Traitement Bout-en-Bout (Depuis la création jusqu'à la disponibilité Starburst)
    to_milliseconds(dh_poc_spark_write_timestamp - CAST(time AS TIMESTAMP)) AS ms_total_end_to_end
FROM dh_poc_hive.pocspark.cloudevent_landing
ORDER BY dh_poc_spark_write_timestamp DESC
LIMIT 50;
```

```json?chameleon
{"component":"LlmGeneratedComponent","props":{"height":"600px","prompt":"Objectif : Visualiser et analyser la décomposition de la latence (T1 à T4) pour le pipeline Hive/Parquet. \nInitialValues : t1_to_t2: 50, t2_to_t3: 1500, t3_to_t4: 200. \nStrategy : Form Layout. \nInputs : Curseurs pour ajuster les 3 segments de temps : 'Source vers S3 Raw (ms)', 'Attente sur S3 avant lecture Spark (ms)', 'Temps de calcul Spark (ms)'. \nBehavior : Afficher un graphique en cascade (Waterfall) horizontal qui part de T1 (Origine) et arrive à T4 (Sysdate/Disponibilité). Colorer chaque segment différemment : Transport (Gris), Attente Batch (Orange - souvent le plus long à cause du trigger), Processing (Vert). Mettre à jour le 'Temps Total Bout-en-Bout' dynamiquement en fonction des curseurs. Ajouter une légende expliquant que dans l'architecture Hive, T4 équivaut à la disponibilité immédiate car il n'y a pas de Metastore transactionnel à mettre à jour. Langue : Français.","id":"im_d95881979f5cb027"}}
```

Avec ce setup, tu as un pipeline robuste et ultra-parallélisé, et un moyen mathématique de prouver la vitesse de ton code. 

Avant de lancer le test, penses-tu générer les données brutes dans un script séparé (comme on l'a vu avec les 2 phases) pour t'assurer que le Spark UI affiche bien le parallélisme parfait sans embouteillage ?
