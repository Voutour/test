C'est parti. Voici le code complet, propre et prêt à être exécuté. Il répond exactement à l'architecture cible : il ingère tes CloudEvents en streaming, convertit le payload métier en chaîne JSON sans le parser, ajoute la traçabilité temporelle, et **surtout, il prend le contrôle pour créer la table Iceberg lui-même**.

### ⚠️ Prérequis obligatoire (Avant de lancer le code)
1. Va dans Starburst et supprime la table qui posait problème : `DROP TABLE dh_poc_ice.pocspark;`
2. Assure-toi que le dossier `pocspark/` sur ton bucket S3 est complètement vide (supprime-le s'il le faut pour repartir sur une base saine).

---

### Le Code Spark (cloudevent_streaming.py)

```python
import threading
import time
import uuid
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, MapType
from pyspark.sql.functions import current_timestamp, to_json, col

# ==========================================
# 1. CONFIGURATION
# ==========================================
RAW_BUCKET_PATH = "s3a://bu002i004226/poc_streaming/input_cloudevent_raw/"
CHECKPOINT_PATH = "s3a://bu002i004226/poc_streaming/checkpoint_cloudevent/"
# Le warehouse s'arrête au dossier parent
ICEBERG_WAREHOUSE = "s3a://bu002i004226/poc_streaming/" 

# ==========================================
# 2. INITIALISATION DE LA SESSION SPARK
# ==========================================
print("### INIT SPARK - ARCHITECTURE CLOUDEVENT ###")
spark = SparkSession.builder \
    .appName("DevX-CloudEvent-Stream") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.3.1,org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg_cat", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg_cat.type", "hadoop") \
    .config("spark.sql.catalog.iceberg_cat.warehouse", ICEBERG_WAREHOUSE) \
    # --- Configurations S3A (Authentification) ---
    .config("spark.hadoop.fs.s3a.access.key", "TON_ACCESS_KEY_ICI") \
    .config("spark.hadoop.fs.s3a.secret.key", "TON_SECRET_KEY_ICI") \
    .config("spark.hadoop.fs.s3a.endpoint", "https://s3.direct.eu-fr2.cloud-object-storage.appdomain.cloud") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ==========================================
# 3. GÉNÉRATEUR MOCK (Tourne en fond)
# ==========================================
def generate_mock_events():
    max_iterations = 20
    sleep_time = 2
    
    gen_schema = StructType([
        StructField("specversion", StringType(), True),
        StructField("type", StringType(), True),
        StructField("source", StringType(), True),
        StructField("subject", StringType(), True),
        StructField("id", StringType(), True),
        StructField("time", StringType(), True),
        StructField("datacontenttype", StringType(), True),
        StructField("data", MapType(StringType(), StringType()), True)
    ])

    print(f"\n[GÉNÉRATEUR] Démarrage... {max_iterations} événements prévus.")
    for i in range(max_iterations):
        event_id = str(uuid.uuid4())
        now_iso = datetime.utcnow().isoformat() + "Z"
        
        mock_data = [(
            "1.0", "com.enterprise.sensor", f"/sensors/device-{i}", 
            "telemetry", event_id, now_iso, "application/json", 
            {"temperature": str(20 + i), "status": "OK", "battery": str(100 - i)}
        )]
        
        df_mock = spark.createDataFrame(mock_data, schema=gen_schema)
        df_mock.write.mode("append").json(RAW_BUCKET_PATH)
        
        print(f"[GÉNÉRATEUR] Événement {i+1}/{max_iterations} écrit ! (ID: {event_id[:8]})")
        time.sleep(sleep_time)
        
    print("[GÉNÉRATEUR] Terminé.\n")

generator_thread = threading.Thread(target=generate_mock_events)
generator_thread.start()

# ==========================================
# 4. LECTURE & TRANSFORMATION
# ==========================================
read_schema = StructType([
    StructField("specversion", StringType(), True),
    StructField("type", StringType(), True),
    StructField("source", StringType(), True),
    StructField("subject", StringType(), True),
    StructField("id", StringType(), True),
    StructField("time", TimestampType(), True),
    StructField("datacontenttype", StringType(), True),
    StructField("data", MapType(StringType(), StringType()), True) 
])

df_stream = spark.readStream \
    .format("json") \
    .schema(read_schema) \
    .load(RAW_BUCKET_PATH)

df_processed = df_stream \
    .withColumn("data", to_json(col("data"))) \
    .withColumn("dh_poc_insert_timestamp", current_timestamp())

# ==========================================
# 4.5. SÉCURITÉ ANTI-CRASH "TABLE NOT FOUND"
# ==========================================
# On force Spark à s'approprier la table avant de lancer le stream
print("### INITIALISATION DE LA TABLE (SÉCURITÉ) ###")
empty_df = spark.createDataFrame([], df_processed.schema)
try:
    # On utilise "cloudevent" pour correspondre au dossier exact de ta table Starburst
    empty_df.writeTo("iceberg_cat.cloudevent").createOrReplace()
    print("[SUCCÈS] Enveloppe de la table validée par Spark.")
except Exception as e:
    print(f"[INFO] Initialisation table : {e}")

# ==========================================
# 5. STREAMING VERS ICEBERG
# ==========================================
print("### DÉMARRAGE DU STREAMING VERS ICEBERG ###")
query = df_processed.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .trigger(processingTime="2 seconds") \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .toTable("iceberg_cat.cloudevent") 

query.awaitTermination()
```

Voici une explication concise du fonctionnement de ton code et de son alignement avec ton schéma d'architecture.

### 1. Comment le code fait du Spark Streaming
Le streaming dans ce script repose sur l'API **Spark Structured Streaming**. Plutôt que de lire une base de données statique, il traite les données comme un flux infini grâce à trois mécanismes clés :
* **L'écoute continue (`readStream`)** : Au lieu de charger un dossier une seule fois (mode Batch), Spark surveille activement le dossier S3 `input_cloudevent_raw`. Dès que le générateur mock (ou ta future application métier) y dépose un nouveau fichier JSON, Spark l'ingère instantanément.
* **Les Micro-batchs (`trigger`)** : Le paramètre `trigger(processingTime="2 seconds")` cadence le flux. Toutes les 2 secondes, Spark prend les nouveaux événements arrivés, les traite, et les écrit dans Iceberg. C'est du "quasi temps réel".
* **La tolérance aux pannes (`checkpointLocation`)** : Le dossier de checkpoint agit comme un marque-page. Spark y note précisément quels fichiers S3 ont déjà été traités. Si ton pod Kubernetes crashe et redémarre, Spark lit ce "marque-page" et reprend exactement là où il s'était arrêté, sans perdre de données ni créer de doublons.

### 2. Comment le code respecte ton schéma d'architecture
Ton code est la traduction technique exacte des blocs de ton schéma d'architecture CloudEvent :
* **RAW Data Storage (Zone brute)** : Tes applications métiers génèrent des CloudEvents qui atterrissent sous forme de fichiers JSON dans ton bucket S3 d'entrée (`input_cloudevent_raw`). C'est ton point de collecte.
* **Spark Processing (La transformation)** : Le moteur lit ces fichiers en streaming et applique les transformations requises. Il impose le schéma strict CloudEvent, convertit l'objet métier imbriqué `data` en une simple chaîne JSON textuelle (pour éviter les erreurs de typage dans Starburst), et ajoute la colonne de traçabilité `dh_poc_insert_timestamp`.
* **Refined Data Storage (Zone affinée / Iceberg)** : L'étape `writeStream` pousse ces données nettoyées vers le dossier S3 de destination (`cloudevent/`). En utilisant le `.format("iceberg")` et le catalogue Hadoop, Spark ne fait pas qu'écrire des fichiers Parquet : il gère le journal des transactions (métadonnées) propre à Iceberg.
* **Data Access (Starburst)** : L'architecture repose sur le découplage du stockage et du calcul. En confiant à Spark la gestion complète des fichiers et des métadonnées Iceberg sur S3, on permet à Starburst de venir simplement "se brancher" sur le dossier en lecture seule pour exécuter les requêtes SQL des utilisateurs finaux.
