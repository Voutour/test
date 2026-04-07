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
# 1. CONFIGURATION DES CHEMINS S3
# ==========================================
RAW_BUCKET_PATH = "s3a://bu002i004226/poc_streaming/input_cloudevent_raw/"
CHECKPOINT_PATH = "s3a://bu002i004226/poc_streaming/checkpoint_cloudevent/"
# Le warehouse pointe sur le dossier parent, pour que la table "cloudevent" 
# corresponde exactement à ton chemin Starburst ".../poc_streaming/cloudevent"
ICEBERG_WAREHOUSE = "s3a://bu002i004226/poc_streaming/" 

# ==========================================
# 2. INITIALISATION DE LA SESSION SPARK
# ==========================================
print("### INIT SPARK - ARCHITECTURE CLOUDEVENT ###")
spark = SparkSession.builder \
    .appName("DevX-CloudEvent-Stream") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg_cat", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg_cat.type", "hadoop") \
    .config("spark.sql.catalog.iceberg_cat.warehouse", ICEBERG_WAREHOUSE) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ==========================================
# 3. LE GÉNÉRATEUR DE DONNÉES (MOCK API)
# ==========================================
def generate_mock_events():
    """Génère un fichier JSON CloudEvent toutes les 2s avec un max d'itérations"""
    max_iterations = 20
    sleep_time = 2
    
    # Schéma pour la génération
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
        
        # Données avec contenu variable
        mock_data = [(
            "1.0", 
            "com.enterprise.sensor", 
            f"/sensors/device-{i}", 
            "telemetry", 
            event_id, 
            now_iso, 
            "application/json", 
            {"temperature": str(20 + i), "status": "OK", "battery": str(100 - i)} # Le payload métier
        )]
        
        df_mock = spark.createDataFrame(mock_data, schema=gen_schema)
        # Écriture du JSON dans le bucket RAW
        df_mock.write.mode("append").json(RAW_BUCKET_PATH)
        
        print(f"[GÉNÉRATEUR] Événement {i+1}/{max_iterations} écrit ! (ID: {event_id[:8]})")
        time.sleep(sleep_time)
        
    print("[GÉNÉRATEUR] Terminé.\n")

# Lancement du générateur dans un Thread en arrière-plan pour ne pas bloquer le Streaming
generator_thread = threading.Thread(target=generate_mock_events)
generator_thread.start()

# ==========================================
# 4. LE TRAITEMENT SPARK STREAMING
# ==========================================
# Le schéma de lecture (On lit 'time' en Timestamp et 'data' en Map)
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

print("### DÉMARRAGE DU STREAMING GET EVENT ###")

# A. Lecture du Bucket RAW
df_stream = spark.readStream \
    .format("json") \
    .schema(read_schema) \
    .load(RAW_BUCKET_PATH)

# B. Transformation (On transforme 'data' en chaîne JSON texte)
df_processed = df_stream \
    .withColumn("data", to_json(col("data"))) \
    .withColumn("dh_poc_insert_timestamp", current_timestamp())

print("### ÉCRITURE VERS REFINED DATA STORAGE (ICEBERG) ###")

# C. Écriture vers la table Iceberg configurée dans Starburst
query = df_processed.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .trigger(processingTime="2 seconds") \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .toTable("iceberg_cat.cloudevent") # Pointe vers ".../poc_streaming/cloudevent"

query.awaitTermination()
```

### Ce qui va se passer à l'exécution :
1. Spark va démarrer et se connecter au bucket d'entrée (`input_raw`).
2. S'il y a des fichiers JSON, il les prend.
3. Il va vérifier le dossier `s3a://bu002i004226/pocspark`. Comme tu as tout nettoyé, il va se dire : *"La table n'existe pas, je la crée avec mon catalogue Hadoop"*.
4. Il va créer le dossier `metadata/` et y écrire les fichiers de données Parquet.

Une fois que ce job tourne au vert et que tu vois tes fichiers Parquet apparaître dans le dossier `pocspark`, l'architecture est validée côté Spark. Il ne restera plus qu'à enregistrer cette table existante côté Starburst !
