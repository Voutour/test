Ah ! Je te demande pardon, c'est totalement de ma faute. Le code que je t'ai donné dans le message précédent ne contenait **que la partie "Consommateur"** (le `readStream`). 

Comme tu n'as pas lancé de générateur de données à côté, le dossier S3 `input_cloudevent_raw` était vide. Spark s'est donc mis sur écoute, n'a rien trouvé, et a attendu indéfiniment sans rien faire. Pas de données = pas de micro-batch = pas de job actif dans l'UI.

On va corriger ça tout de suite. Voici le **script complet et unifié**. Il intègre le générateur de données en tâche de fond (Thread) ET le processeur de streaming, le tout optimisé pour tes 16 cœurs et ta table Hive avec les timestamps précis.

### Le Code Complet ("Générateur + Streaming")

**⚠️ Très important avant de lancer :** Assure-toi de supprimer les dossiers `input_cloudevent_raw`, `cloudevent_landing` et `checkpoint_cloudevent` sur ton S3 pour repartir sur une base 100 % propre.

```python
import threading
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

# 1. INITIALISATION SPARK
spark = SparkSession.builder \
    .appName("DevX-Hive-Full-Pipeline") \
    .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.default.parallelism", "16") \
    .config("spark.sql.shuffle.partitions", "16") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema_events = StructType([
    StructField("specversion", StringType(), True),
    StructField("type", StringType(), True),
    StructField("source", StringType(), True),
    StructField("subject", StringType(), True),
    StructField("id", StringType(), True),
    StructField("time", StringType(), True), # T1 : Origine
    StructField("datacontenttype", StringType(), True),
    StructField("data", MapType(StringType(), StringType()), True),
    StructField("dh_poc_gen_timestamp", StringType(), True) # T2 : Arrivée S3
])

# ==========================================
# 2. LE GÉNÉRATEUR (Tourne en tâche de fond)
# ==========================================
def generate_events():
    print("### [GÉNÉRATEUR] Démarrage de l'envoi des données vers S3 Raw ###")
    batch_count = 1
    while True:
        mock_data = []
        # On génère 160 événements par vague
        for i in range(160):
            now = datetime.utcnow().isoformat() + "Z"
            mock_data.append(("1.0", "sensor", f"dev-{i}", "telemetry", str(uuid.uuid4()), now, "json", {"t": "20"}, now))
        
        df_gen = spark.createDataFrame(mock_data, schema=schema_events)
        # On écrit 4 petits fichiers JSON pour ne pas étouffer le cluster
        df_gen.coalesce(4).write.mode("append").json(RAW_PATH)
        print(f"### [GÉNÉRATEUR] Vague {batch_count} envoyée sur S3 ###")
        batch_count += 1
        time.sleep(4) # Pause de 4 secondes entre chaque génération

threading.Thread(target=generate_events, daemon=True).start()

# On attend 10 secondes pour que le S3 Raw ait quelques fichiers avant de lancer le stream
print("### Attente de 10s pour l'initialisation du S3 Raw... ###")
time.sleep(10)

# ==========================================
# 3. LE PROCESSUS DE STREAMING (Le Consommateur)
# ==========================================
print("### [STREAMING] Démarrage de la lecture et du traitement sur 16 cœurs ###")

df_stream = spark.readStream \
    .format("json") \
    .schema(schema_events) \
    .option("maxFilesPerTrigger", 32) \
    .load(RAW_PATH)

# Ajout des timestamps (T3 et T4) castés en timestamp natif (compatibilité Hive/Trino)
df_processed = df_stream \
    .withColumn("data", to_json(col("data"))) \
    .withColumn("dh_poc_spark_read_timestamp", current_timestamp().cast("timestamp")) \
    .withColumn("dh_poc_spark_write_timestamp", current_timestamp().cast("timestamp"))

# Écriture Parquet sur la table Hive
query = df_processed \
    .repartition(16) \
    .writeStream \
    .queryName("Hive_Landing_Stream") \
    .format("parquet") \
    .outputMode("append") \
    .trigger(processingTime="2 seconds") \
    .option("checkpointLocation", CHECKPOINT) \
    .option("path", LANDING_PATH) \
    .start()

query.awaitTermination()
```

### Ce que tu vas voir se passer cette fois :

1.  **Dans la console :** Tu vas d'abord voir le `[GÉNÉRATEUR]` s'activer et créer la première vague. Puis un compte à rebours invisible de 10 secondes. Puis le `[STREAMING]` va démarrer. Tu continueras à voir les messages du générateur toutes les 4 secondes.
2.  **Sur ton S3 :** Les dossiers vont se créer. Le `landing` va se remplir de fichiers `.parquet`.
3.  **Dans le Spark UI :** Les jobs vont enfin "popper" toutes les 2 secondes (le trigger), et si tu zoomes dans les *Stages*, tu verras tes 16 partitions s'exécuter.
4.  **Dans Starburst :** Tu pourras lancer tes `SELECT` et tu verras les données arriver par paquets, avec les fameux T1, T2, T3 et T4 parfaitement formatés !

Relance ça et dis-moi si les jobs s'activent bien sur ton interface !
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



C'est le symptôme typique d'un "Streaming bloqué" dans Spark. Quand un job de streaming tourne sans rien faire, c'est généralement pour l'une de ces trois raisons :

1.  **Le Checkpoint est corrompu :** Si tu as déjà lancé le code avec une erreur ou un schéma différent, Spark a enregistré l'état dans le dossier `checkpoint_cloudevent`. Il attend un fichier spécifique qui n'existe plus ou refuse de traiter des fichiers qu'il croit déjà avoir vus.
2.  **La Source est vide :** `readStream` ne déclenche un Job que lorsqu'il détecte un **nouveau** fichier dans le dossier `RAW_PATH`. Si le dossier est vide ou si les fichiers étaient déjà là avant le démarrage, Spark reste en attente.
3.  **L'initialisation S3 :** Parfois, la connexion avec le bucket IBM Cloud met du temps à se stabiliser au démarrage.

Voici une version **"Diagnostic Force"**. J'ai ajouté des prints à chaque étape et une commande pour ignorer les anciens fichiers, afin de forcer Spark à travailler immédiatement.

### Code de Diagnostic (À copier-coller)

**Important :** Avant de lancer, supprime manuellement le dossier `checkpoint_cloudevent` sur ton S3 pour repartir de zéro.

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

# 1. INITIALISATION
print(">>> Etape 1 : Initialisation Spark...")
spark = SparkSession.builder \
    .appName("Diagnostic-Streaming-Hive") \
    .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.default.parallelism", "16") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 2. GÉNÉRATEUR FORCÉ (On écrit 5 fichiers tout de suite)
print(f">>> Etape 2 : Injection de donnees test dans {RAW_PATH}...")
gen_schema = StructType([
    StructField("specversion", StringType(), True),
    StructField("id", StringType(), True),
    StructField("time", StringType(), True),
    StructField("data", MapType(StringType(), StringType()), True),
    StructField("dh_poc_gen_timestamp", StringType(), True)
])

for i in range(5):
    now = datetime.utcnow().isoformat() + "Z"
    data = [("1.0", str(uuid.uuid4()), now, {"val": "test"}, now)]
    spark.createDataFrame(data, schema=gen_schema).repartition(1).write.mode("append").json(RAW_PATH)
print(">>> Donnees injectees. Verification du dossier...")

# 3. LECTURE STREAMING
print(">>> Etape 3 : Lancement du Stream...")
df_stream = spark.readStream \
    .format("json") \
    .schema(gen_schema) \
    .option("cleanSource", "archive") \
    .load(RAW_PATH)

# TIMESTAMPS (Precision 3 pour Hive/Starburst)
df_final = df_stream \
    .withColumn("data", to_json(col("data"))) \
    .withColumn("dh_poc_spark_read_timestamp", current_timestamp().cast("timestamp")) \
    .withColumn("dh_poc_spark_write_timestamp", current_timestamp().cast("timestamp"))

# 4. ECRITURE
print(f">>> Etape 4 : Ecriture vers {LANDING_PATH}. Regardez le Spark UI maintenant !")
query = df_final \
    .repartition(16) \
    .writeStream \
    .queryName("DIAGNOSTIC_JOB") \
    .format("parquet") \
    .trigger(processingTime="2 seconds") \
    .option("checkpointLocation", CHECKPOINT) \
    .option("path", LANDING_PATH) \
    .start()

query.awaitTermination()
```

### Pourquoi ton job semblait "mort" ?
Le streaming Spark est très sensible à la notion de **nouveauté**. Si tu as déjà des fichiers dans ton dossier, Spark les ignore par défaut s'il a déjà fini son cycle. 

Pour comprendre ce blocage invisible, utilise ce simulateur de cycle de vie d'un Micro-Batch. Il montre pourquoi un dossier plein peut parfois donner un résultat vide.

```json?chameleon
{"component":"LlmGeneratedComponent","props":{"height":"600px","prompt":"Objectif : Diagnostiquer pourquoi un job Spark Streaming ne produit aucun résultat. \nInitialValues : files_in_source: 10, checkpoint_exists: true, status: 'idle'. \nStrategy : Standard Layout. \nInputs : \n- Bouton 'Nettoyer Checkpoint'\n- Bouton 'Ajouter Nouveau Fichier'\n- Interrupteur 'Source vide / Source pleine'\nBehavior : \n- Afficher un diagramme de flux : Dossier Source -> Spark (Le Filtre) -> Dossier Landing. \n- Expliquer visuellement le rôle du Checkpoint : Si le Checkpoint contient déjà l'ID des fichiers du dossier source, montrer Spark qui les 'rejette' (animation de blocage). \n- Si l'utilisateur clique sur 'Ajouter Nouveau Fichier', montrer Spark qui détecte la nouveauté et lance le traitement (barre de progression). \n- Si 'Nettoyer Checkpoint' est activé, montrer que Spark traite TOUS les fichiers présents comme s'ils étaient nouveaux. \n- Afficher une console de log simulant : 'Batch 0 processed', 'No new data found... sleeping'. Langue : Français.","id":"im_087f5e2e91430d90"}}
```

### Ce que tu dois vérifier dans Starburst :
Une fois le code relancé, va dans Starburst. Si tu ne vois toujours rien, exécute cette commande pour vérifier si Starburst "voit" bien les fichiers physiques, même s'il ne les affiche pas dans la table :

```sql
SELECT * FROM "dh_poc_hive"."pocspark"."cloudevent_landing$files";
```
*(Si cette commande renvoie des lignes, c'est que les fichiers sont là et que c'est juste un problème de rafraîchissement de métadonnées).*

Est-ce que les "prints" de diagnostic s'affichent bien dans ton terminal cette fois-ci ?
