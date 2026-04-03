C'est parti. Voici le code complet, propre et prêt à être exécuté. Il répond exactement à l'architecture cible : il ingère tes CloudEvents en streaming, convertit le payload métier en chaîne JSON sans le parser, ajoute la traçabilité temporelle, et **surtout, il prend le contrôle pour créer la table Iceberg lui-même**.

### ⚠️ Prérequis obligatoire (Avant de lancer le code)
1. Va dans Starburst et supprime la table qui posait problème : `DROP TABLE dh_poc_ice.pocspark;`
2. Assure-toi que le dossier `pocspark/` sur ton bucket S3 est complètement vide (supprime-le s'il le faut pour repartir sur une base saine).

---

### Le Code Spark (cloudevent_streaming.py)

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, MapType
from pyspark.sql.functions import current_timestamp, to_json, col

print("### INIT SPARK - ARCHITECTURE CLOUDEVENT ###")

# 1. Configuration stricte pour le catalogue Hadoop local
# C'est ce qui donne le pouvoir à Spark de créer la table Iceberg
spark = SparkSession.builder \
    .appName("Sparktacus-CloudEvent-Stream") \
    .config("spark.eventLog.enabled", "false") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "s3a://bu002i004226/") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Le schéma strict CloudEvent
# On utilise MapType pour capturer tout objet JSON contenu dans 'data' sans chercher à le typer fortement
ce_schema = StructType([
    StructField("specversion", StringType(), True),
    StructField("type", StringType(), True),
    StructField("source", StringType(), True),
    StructField("subject", StringType(), True),
    StructField("id", StringType(), True),
    StructField("time", TimestampType(), True),
    StructField("datacontenttype", StringType(), True),
    StructField("data", MapType(StringType(), StringType()), True), 
    # Extensions métiers (à adapter selon tes vrais champs)
    StructField("ext_tenant_id", StringType(), True),
    StructField("ext_correlation_id", StringType(), True)
])

# 3. GET EVENT : Lecture du flux en continu depuis le bucket RAW
df_raw = spark.readStream \
    .format("json") \
    .schema(ce_schema) \
    .load("s3a://bu002i004226/poc_streaming/input_raw/")

# 4. PROCESS : Transformation
# On transforme l'objet 'data' en une simple chaîne de caractères JSON (texte)
# On ajoute le timestamp technique d'insertion
df_processed = df_raw \
    .withColumn("data", to_json(col("data"))) \
    .withColumn("dh_poc_insert_timestamp", current_timestamp())

print("### DÉMARRAGE DU STREAMING VERS ICEBERG ###")

# 5. WRITE TO REFINED DATA STORAGE (Iceberg)
# On écrit vers "local.pocspark", ce qui crée la table à l'adresse s3a://bu002i004226/pocspark
query = df_processed.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .trigger(processingTime="5 seconds") \
    .option("checkpointLocation", "s3a://bu002i004226/poc_streaming/checkpoint_ce/") \
    .toTable("local.pocspark")

query.awaitTermination()
```

### Ce qui va se passer à l'exécution :
1. Spark va démarrer et se connecter au bucket d'entrée (`input_raw`).
2. S'il y a des fichiers JSON, il les prend.
3. Il va vérifier le dossier `s3a://bu002i004226/pocspark`. Comme tu as tout nettoyé, il va se dire : *"La table n'existe pas, je la crée avec mon catalogue Hadoop"*.
4. Il va créer le dossier `metadata/` et y écrire les fichiers de données Parquet.

Une fois que ce job tourne au vert et que tu vois tes fichiers Parquet apparaître dans le dossier `pocspark`, l'architecture est validée côté Spark. Il ne restera plus qu'à enregistrer cette table existante côté Starburst !
