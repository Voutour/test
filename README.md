C'est le plan de bataille final. Voici l'ensemble du code et des instructions, nettoyés de toutes les erreurs précédentes (notamment le problème de dossier inexistant et les conflits de types de données).

### **L'Architecture Cible**
1.  **Spark** : Dépose des fichiers Parquet bruts dans `cloudevent_landing/`.
2.  **Catalog `dh_poc_pde_s3` (Hive)** : Sert de fenêtre pour voir ces fichiers.
3.  **Catalog `dh_poc_ice` (Iceberg)** : Reçoit les données finales avec tous les calculs de temps.

---

### **Étape 1 : Le Code Spark Complet (`main_cloudevent.py`)**

Ce code inclut le **correctif de 5 secondes** pour éviter le crash au démarrage et les **3 premiers horodatages**.

```python
import threading
import time
import uuid
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, MapType
from pyspark.sql.functions import current_timestamp, to_json, col

# CONFIGURATION S3
S3_ENDPOINT = "https://s3.direct.eu-fr2.cloud-object-storage.appdomain.cloud"
RAW_PATH = "s3a://bu002i004226/poc_streaming/input_cloudevent_raw/"
CHECKPOINT = "s3a://bu002i004226/poc_streaming/checkpoint_cloudevent/"
LANDING_PATH = "s3a://bu002i004226/poc_streaming/cloudevent_landing/"

# 1. INITIALISATION SPARK
spark = SparkSession.builder \
    .appName("DevX-CloudEvent-Final") \
    .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.sql.shuffle.partitions", "10") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 2. GÉNÉRATEUR CONCURRENT (THREAD)
def generate_events():
    gen_schema = StructType([
        StructField("specversion", StringType(), True),
        StructField("type", StringType(), True),
        StructField("source", StringType(), True),
        StructField("subject", StringType(), True),
        StructField("id", StringType(), True),
        StructField("time", StringType(), True), # T1: Origine
        StructField("datacontenttype", StringType(), True),
        StructField("data", MapType(StringType(), StringType()), True),
        StructField("dh_poc_gen_timestamp", StringType(), True) # T2: Écriture S3
    ])
    
    while True: # Génération continue par vagues
        mock_data = []
        for i in range(20):
            now = datetime.utcnow().isoformat() + "Z"
            mock_data.append(("1.0", "sensor", "dev", "telemetry", str(uuid.uuid4()), now, "json", {"t": "20"}, now))
        
        df = spark.createDataFrame(mock_data, schema=gen_schema)
        df.repartition(2).write.mode("append").json(RAW_PATH)
        time.sleep(10)

threading.Thread(target=generate_events, daemon=True).start()

# CORRECTIF : On attend que le premier fichier soit écrit pour créer le dossier
print("Attente de l'initialisation du dossier S3...")
time.sleep(10)

# 3. STREAMING
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

df_stream = spark.readStream.format("json").schema(read_schema).load(RAW_PATH)

# T3: Lecture Spark
df_processed = df_stream \
    .withColumn("data", to_json(col("data"))) \
    .withColumn("dh_poc_spark_read_timestamp", current_timestamp())

# ÉCRITURE LANDING
query = df_processed.writeStream \
    .format("parquet") \
    .trigger(processingTime="5 seconds") \
    .option("checkpointLocation", CHECKPOINT) \
    .option("path", LANDING_PATH) \
    .start()

query.awaitTermination()
```

---

### **Étape 2 : Les Commandes Starburst (SQL)**

Exécute ces requêtes dans l'ordre pour configurer tes deux catalogues.

#### **1. Créer la table Passerelle (Hive / `dh_poc_pde_s3`)**
Ici, on met tout en `VARCHAR` pour éviter les erreurs de formatage Hive.
```sql
DROP TABLE IF EXISTS dh_poc_pde_s3.pocspark.cloudevent_landing;

CREATE TABLE dh_poc_pde_s3.pocspark.cloudevent_landing (
    specversion VARCHAR,
    type VARCHAR,
    source VARCHAR,
    subject VARCHAR,
    id VARCHAR,
    time VARCHAR,
    datacontenttype VARCHAR,
    data VARCHAR,
    dh_poc_gen_timestamp VARCHAR,
    dh_poc_spark_read_timestamp VARCHAR
)
WITH (
    format = 'PARQUET',
    external_location = 's3a://bu002i004226/poc_streaming/cloudevent_landing/'
);
```

#### **2. Créer la table Officielle (Iceberg / `dh_poc_ice`)**
C'est la table finale proprement typée.
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

#### **3. Alimenter Iceberg depuis Hive**
C'est ici qu'on ajoute le **T4** (Insertion Starburst).
```sql
INSERT INTO dh_poc_ice.pocspark.cloudevent_raw
SELECT 
    specversion, type, source, subject, id, time, datacontenttype, data, dh_poc_gen_timestamp,
    CAST(dh_poc_spark_read_timestamp AS TIMESTAMP(6) WITH TIME ZONE),
    current_timestamp AS dh_poc_starburst_insert_timestamp
FROM dh_poc_pde_s3.pocspark.cloudevent_landing;
```

---

### **Analyse de la Traçabilité (Observabilité)**

Pour comprendre pourquoi nous faisons tout cela, voici une visualisation de la façon dont les données voyagent et comment la latence est mesurée à chaque étape du pipeline.

[Image d'un pipeline de données streaming montrant les étapes de traitement et les points de mesure de latence]

```json?chameleon
{"component":"LlmGeneratedComponent","props":{"height":"700px","prompt":"Objectif : Simuler le flux de données et la latence entre Spark, S3, Starburst Hive et Starburst Iceberg. Valeurs initiales : intervalle_spark: 5, delai_s3: 2, frequence_starburst: 10. Stratégie : Standard Layout. Entrées : Curseur pour 'Vitesse Flux Spark (sec)', 'Délai écriture S3 (sec)', 'Délai insertion Iceberg (sec)'. Comportement : Afficher un schéma de flux horizontal (Générateur -> Spark -> S3 -> Hive Gateway -> Iceberg Final). Chaque étape affiche le timestamp généré (T1, T2, T3, T4). Calculer la latence totale cumulée. Montrer visuellement les paquets de données qui progressent. Afficher un tableau comparatif des 4 horodatages pour voir le décalage temporel. Utiliser des termes fonctionnels comme 'accent' pour mettre en évidence l'étape active. Langue : Français.","id":"im_b74644dff766607b"}}
```

Dès que tu lances le `INSERT INTO` final, ta table Iceberg `dh_poc_ice.pocspark.cloudevent_raw` contiendra l'historique complet. Tu pourras alors calculer tes KPIs de performance réelle !





C'est entendu. Oublions les passerelles, les `INSERT` manuels et les solutions de contournement. On va mettre en place la **solution native Iceberg**, telle qu'elle est censée fonctionner dans une architecture Data Lakehouse industrielle.

Pour que Spark écrive et que Starburst voie les données instantanément, ils doivent partager le **même catalogue (le Metastore)**. Puisque tu as désormais les deux connecteurs (`hive` et `iceberg`), nous allons configurer Spark pour qu'il agisse comme un membre à part entière du catalogue Iceberg.

### La Solution "Directe" : Couplage Natif Spark-Iceberg

Dans cette configuration, Spark ne dépose pas juste des fichiers ; il effectue des **transactions Iceberg**. Chaque fois que Spark finit d'écrire un micro-batch, il met à jour le catalogue. Starburst, qui surveille ce même catalogue, voit la nouvelle version de la table immédiatement.

#### 1. Configuration de la Table dans Starburst (Iceberg)
Lance cette commande une seule fois. C'est la table "Mère" que Spark va alimenter directement.

```sql
-- On crée la table DIRECTEMENT dans le catalogue Iceberg
CREATE TABLE IF NOT EXISTS dh_poc_ice.pocspark.cloudevent_raw (
    specversion VARCHAR,
    type VARCHAR,
    source VARCHAR,
    subject VARCHAR,
    id VARCHAR,
    time VARCHAR,
    datacontenttype VARCHAR,
    data VARCHAR,
    -- Timestamps pour ton analyse de latence (T1, T2, T3)
    dh_poc_gen_timestamp VARCHAR,
    dh_poc_spark_read_timestamp TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format = 'PARQUET',
    location = 's3a://bu002i004226/poc_streaming/cloudevent_direct/'
);
```

#### 2. Le Code Spark Direct (`main_cloudevent.py`)
Ici, Spark se connecte au catalogue Iceberg de la banque. **Attention** : Tu dois récupérer l'URI du Metastore (souvent `thrift://...`) auprès de ton équipe infra ou la trouver dans tes configs Hadoop.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, to_json, col
import threading
import time
import uuid
from datetime import datetime

# CONFIGURATION
S3_ENDPOINT = "https://s3.direct.eu-fr2.cloud-object-storage.appdomain.cloud"
# REMPLACE PAR LA VRAIE URI THRIFT DE TON INFRA
HMS_URI = "thrift://METASTORE_HOST:9083" 

# 1. INIT SPARK - CONNEXION NATIVE AU CATALOGUE
spark = SparkSession.builder \
    .appName("Iceberg-Direct-Streaming") \
    .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg_cat", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg_cat.type", "hive") \
    .config("spark.sql.catalog.iceberg_cat.uri", HMS_URI) \
    .getOrCreate()

# 2. GÉNÉRATEUR (T1, T2)
def generate():
    while True:
        now = datetime.utcnow().isoformat() + "Z"
        data = [("1.0", "sensor", "dev", "telemetry", str(uuid.uuid4()), now, "json", {"v": "20"}, now)]
        df = spark.createDataFrame(data, "specversion string, type string, source string, subject string, id string, time string, datacontenttype string, data map<string,string>, dh_poc_gen_timestamp string")
        df.write.mode("append").json("s3a://bu002i004226/poc_streaming/raw_input/")
        time.sleep(5)

threading.Thread(target=generate, daemon=True).start()

# 3. STREAMING DIRECT VERS LA TABLE (T3)
df_stream = spark.readStream \
    .format("json") \
    .schema(spark.table("iceberg_cat.pocspark.cloudevent_raw").schema) \
    .load("s3a://bu002i004226/poc_streaming/raw_input/")

df_final = df_stream \
    .withColumn("data", to_json(col("data"))) \
    .withColumn("dh_poc_spark_read_timestamp", current_timestamp())

# L'écriture directe : Spark met à jour le catalogue Iceberg à chaque commit
query = df_final.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .trigger(processingTime="5 seconds") \
    .option("checkpointLocation", "s3a://bu002i004226/poc_streaming/checkpoints_direct/") \
    .toTable("iceberg_cat.pocspark.cloudevent_raw")

query.awaitTermination()
```

---

### Comment obtenir le "T4" (Moment où Starburst voit la donnée) ?

C'est la condition sine qua non. Pour mesurer la latence réelle de Starburst **sans ajouter de colonne manuelle**, on utilise la puissance des **tables de métadonnées Iceberg**. 

Iceberg enregistre l'heure exacte de chaque transaction (`committed_at`). Dans Starburst, tu peux interroger l'historique pour voir quand le "cerveau" de la base a validé les données :

```sql
-- Cette requête te donne le T4 réel : le moment précis où la transaction est devenue visible
SELECT 
    h.snapshot_id,
    h.committed_at AS t4_starburst_visibility,
    s.summary['total-records'] as records_added
FROM "dh_poc_ice"."pocspark"."cloudevent_raw$history" h
JOIN "dh_poc_ice"."pocspark"."cloudevent_raw$snapshots" s ON h.snapshot_id = s.snapshot_id
ORDER BY h.committed_at DESC;
```



### Pourquoi cette solution est la "vraie" :
1.  **Atomicité** : Soit tout le batch est visible, soit rien. Pas de fichiers partiels.
2.  **Zéro Latence Humaine** : Spark prévient Starburst via le Metastore. Dès que le job Spark affiche "Batch processed", le `SELECT` dans Starburst renvoie les lignes.
3.  **Vraie Observabilité** : En comparant `time` (T1), `dh_poc_spark_read_timestamp` (T3) et le `committed_at` (T4) de la table `$history`, tu as la mesure exacte de la latence de ton infrastructure.

```json?chameleon
{"component":"LlmGeneratedComponent","props":{"height":"700px","prompt":"Objectif : Visualiser le couplage direct Spark-Iceberg via le Metastore. \nInitialValues : connection_metastore: true, spark_writing: true. \nStrategy : Standard Layout. \nInputs : Interrupteur 'Simuler Transaction Spark'. \nBehavior : Afficher 3 blocs : Spark, Hive Metastore (HMS), et Starburst. \n1. Quand Spark écrit, montrer un flux vers S3 (Fichiers Data) ET un flux vers le HMS (Mise à jour du Pointeur de Snapshot). \n2. Montrer que Starburst lit le HMS pour savoir quel fichier S3 ouvrir. \n3. Afficher les 4 points de mesure (T1 Origine, T2 S3, T3 Spark, T4 HMS Commit). \n4. Illustrer que sans la mise à jour HMS, Starburst reste aveugle aux données S3. Langue : Français.","id":"im_c804a5aa92dd18eb"}}
```

Si cette configuration échoue avec un `Access Denied`, cela confirme que la Sandbox interdit à Spark d'écrire dans le Metastore. Dans ce cas, la solution "Directe" est physiquement impossible pour des raisons de sécurité bancaire, et non pour des raisons de code. Mais techniquement, c'est **le** standard que tu dois viser.
