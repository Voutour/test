C'est le mur de la sécurité d'entreprise ! Et rassure-toi : **tu n'y es absolument pour rien.**

Dans les grands groupes bancaires, la commande `CALL system.register_table` est souvent bloquée par les administrateurs (via un outil comme Apache Ranger), car elle permet de manipuler directement le cerveau de Starburst. Seuls les admins ont le droit de faire des "CALL".

Mais ce n'est pas grave du tout, car nous avons un plan B imparable. Tout à l'heure, quand tu as eu l'erreur `column dh_poc_insert_timestamp cannot be resolved`, cela prouvait une chose magnifique : **Starburst arrivait parfaitement à lire les fichiers de Spark !** Son seul problème était qu'on lui avait donné un faux schéma (`dummy VARCHAR`).

Puisqu'on ne peut pas utiliser le `CALL`, on va utiliser la méthode "classique" en inversant les rôles : **C'est Starburst qui va créer la table en premier avec le schéma parfait, et Spark va juste se contenter d'écrire dedans.**

Voici le plan d'action en 4 étapes pour contourner le blocage :

### Étape 1 : Le grand ménage (Très important)
Va sur ton bucket S3 et supprime ces trois dossiers pour repartir sur une base 100% saine :
* `poc_streaming/input_cloudevent_raw/`
* `poc_streaming/checkpoint_cloudevent/`
* `poc_streaming/cloudevent/`

### Étape 2 : Créer la table dans Starburst EN PREMIER
Ouvre Starburst et lance cette requête. Elle contient le schéma exact de ce que Spark va produire. (Si la table existe encore, fais un `DROP TABLE IF EXISTS dh_poc_ice.pocspark.cloudevent_raw;` juste avant).

```sql
CREATE TABLE dh_poc_ice.pocspark.cloudevent_raw (
    specversion VARCHAR,
    type VARCHAR,
    source VARCHAR,
    subject VARCHAR,
    id VARCHAR,
    time TIMESTAMP(6) WITH TIME ZONE,
    datacontenttype VARCHAR,
    data VARCHAR,
    dh_poc_insert_timestamp TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format = 'PARQUET',
    location = 's3a://bu002i004226/poc_streaming/cloudevent'
);
```

### Étape 3 : Adapter très légèrement le code Python
Puisque Starburst a maintenant créé la table officielle, il ne faut surtout pas que Spark l'écrase. 
Ouvre ton fichier `main_cloudevent.py` sur DevX, et **supprime complètement le bloc 4.5** (celui qui s'appelait `SÉCURITÉ ANTI-CRASH` avec le `.createOrReplace()`). 

Ton code doit passer directement de la transformation à l'écriture, comme ceci :
```python
# ... (Transformation)
df_processed = df_stream \
    .withColumn("data", to_json(col("data"))) \
    .withColumn("dh_poc_insert_timestamp", current_timestamp())

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
*(N'oublie pas de sauvegarder le fichier sur ton bucket S3 dans `script/` si c'est comme ça que ton bash le lit).*

### Étape 4 : Lancement et Victoire !
1. Dans ton terminal DevX, relance le job avec `bash submit_streaming.sh`.
2. Attends une dizaine de secondes que le statut passe en "Running".
3. Va dans Starburst et lance ton SELECT :

```sql
SELECT * FROM dh_poc_ice.pocspark.cloudevent_raw ORDER BY dh_poc_insert_timestamp DESC;
```

Cette fois-ci, pas de `CALL` interdit, pas d'erreur de colonne, et pas de conflit. Starburst est le patron du schéma, et Spark est son ouvrier dévoué. Dis-moi si les données s'affichent enfin !






C'est l'évolution parfaite pour passer d'un simple "PoC" à un véritable produit prêt pour la production (Production-Ready). 

Mesurer la latence de bout en bout (Observabilité) et sécuriser les accès via un coffre-fort (HashiCorp Vault) sont les deux piliers d'une architecture Big Data d'entreprise. 

Voici comment nous allons intégrer ces trois modifications (Timestamps, SQL Latence, et Vault).

### 1. Mise à jour du code Python (Timestamps & Retrait des secrets)

Nous allons modifier le code pour faire deux choses :
1. **Nettoyer la sécurité :** Supprimer toute mention des clés (`access.key`, `secret.key`) dans le code. Spark ira les chercher dans son environnement (fourni par Vault).
2. **Ajouter les timestamps :** * `timestamp_origine` : L'heure exacte à laquelle l'événement est créé dans le JSON.
    * `sysdate` : L'heure exacte à laquelle le moteur Spark traite le fichier et l'écrit dans Iceberg (ce qui correspond à sa disponibilité pour Starburst).

Voici le nouveau fichier `main_cloudevent.py` épuré et enrichi :

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
ICEBERG_WAREHOUSE = "s3a://bu002i004226/poc_streaming/" 

# ==========================================
# 2. INITIALISATION SPARK (SÉCURISÉE)
# ==========================================
# On ne met PLUS AUCUN SECRET S3 ICI. Spark utilisera les variables 
# d'environnement injectées par Kubernetes/Vault.
spark = SparkSession.builder \
    .appName("DevX-CloudEvent-Stream") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg_cat", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg_cat.type", "hadoop") \
    .config("spark.sql.catalog.iceberg_cat.warehouse", ICEBERG_WAREHOUSE) \
    .config("spark.hadoop.fs.s3a.endpoint", "https://s3.direct.eu-fr2.cloud-object-storage.appdomain.cloud") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ==========================================
# 3. GÉNÉRATEUR AVEC TIMESTAMP ORIGINE
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
        StructField("timestamp_origine", StringType(), True), # Nouveau nom !
        StructField("datacontenttype", StringType(), True),
        StructField("data", MapType(StringType(), StringType()), True)
    ])

    print("\n[GÉNÉRATEUR] Démarrage...")
    for i in range(max_iterations):
        event_id = str(uuid.uuid4())
        # Génération du timestamp origine au format ISO 8601
        now_iso = datetime.utcnow().isoformat() + "Z"
        
        mock_data = [(
            "1.0", "com.enterprise.sensor", f"/sensors/device-{i}", 
            "telemetry", event_id, now_iso, "application/json", 
            {"temperature": str(20 + i), "status": "OK"}
        )]
        
        df_mock = spark.createDataFrame(mock_data, schema=gen_schema)
        df_mock.write.mode("append").json(RAW_BUCKET_PATH)
        time.sleep(sleep_time)

generator_thread = threading.Thread(target=generate_mock_events)
generator_thread.start()

# ==========================================
# 4. LECTURE & AJOUT DE LA SYSDATE
# ==========================================
read_schema = StructType([
    StructField("specversion", StringType(), True),
    StructField("type", StringType(), True),
    StructField("source", StringType(), True),
    StructField("subject", StringType(), True),
    StructField("id", StringType(), True),
    StructField("timestamp_origine", TimestampType(), True), # Spark le lit direct en Timestamp
    StructField("datacontenttype", StringType(), True),
    StructField("data", MapType(StringType(), StringType()), True) 
])

df_stream = spark.readStream.format("json").schema(read_schema).load(RAW_BUCKET_PATH)

# Ajout de la sysdate au moment exact où Spark traite la donnée
df_processed = df_stream \
    .withColumn("data", to_json(col("data"))) \
    .withColumn("sysdate", current_timestamp())

# ==========================================
# 5. STREAMING VERS ICEBERG
# ==========================================
query = df_processed.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .trigger(processingTime="2 seconds") \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .toTable("iceberg_cat.cloudevent") 

query.awaitTermination()
```

---

### 2. La Requête Starburst (Calcul de Latence)

Pour que Starburst prenne en compte ces nouvelles colonnes, il faudra faire un `DROP TABLE` puis recréer la table avec les bonnes colonnes (`timestamp_origine` et `sysdate` en `TIMESTAMP(6) WITH TIME ZONE`).

Une fois fait, voici la requête SQL exacte pour ton IHM Starburst. Elle utilise la fonction Trino `date_diff` pour calculer la latence en millisecondes entre la création de l'événement et sa mise à disposition par Spark :

```sql
SELECT 
    id,
    timestamp_origine,
    sysdate,
    date_diff('millisecond', timestamp_origine, sysdate) AS latence_ms
FROM 
    dh_poc_ice.pocspark.cloudevent_raw
ORDER BY 
    sysdate DESC
LIMIT 50;
```
*Note : Si la latence est très faible, tu auras des chiffres comme `1500` (1,5 seconde). Si tu préfères l'avoir en secondes, remplace `'millisecond'` par `'second'`.*

---

### 3. Intégration HashiCorp Vault

Puisque nous avons retiré les clés du code Python, il faut que Kubernetes les donne à Spark au démarrage. L'architecture BNP utilise la synchronisation Vault -> Kubernetes.

Dans ton fichier `submit_streaming.sh`, au lieu d'avoir les clés en clair, tu dois utiliser la syntaxe de référence aux secrets Kubernetes (qui sont eux-mêmes remplis automatiquement par Vault). 

Tu dois ajouter ces 4 lignes dans ta commande `spark-submit` :

```bash
spark-submit \
  --master "k8s://${KBS_API}" \
  # ... (le reste de ta config habituelle) ... \
  
  # Injection des secrets Vault via Kubernetes pour le Driver Spark
  --conf spark.kubernetes.driverEnv.AWS_ACCESS_KEY_ID="secretKeyRef:nom_du_secret_vault:access_key" \
  --conf spark.kubernetes.driverEnv.AWS_SECRET_ACCESS_KEY="secretKeyRef:nom_du_secret_vault:secret_key" \
  
  # Injection des mêmes secrets pour les Executors (ceux qui écrivent vraiment)
  --conf spark.executorEnv.AWS_ACCESS_KEY_ID="secretKeyRef:nom_du_secret_vault:access_key" \
  --conf spark.executorEnv.AWS_SECRET_ACCESS_KEY="secretKeyRef:nom_du_secret_vault:secret_key" \
  
  s3a://bu002i004226/poc_streaming/script/main_cloudevent.py
```

Sais-tu déjà quel est le `nom_du_secret_vault` créé par tes administrateurs sur ton namespace Kubernetes pour accéder à S3, ou as-tu besoin de voir avec eux comment ce secret est nommé sur ton environnement ?
