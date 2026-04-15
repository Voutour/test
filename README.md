C'est une révélation ! Cette capture d'écran est **la pièce manquante du puzzle** et elle explique absolument tout. 

Ce document Confluence interne de ton entreprise (CTDF) est une mine d'or. Il confirme officiellement que **tu n'as fait aucune erreur depuis le début**. Les problèmes de synchronisation que nous rencontrons sont en fait des limitations connues et documentées de votre environnement "Sandbox".

Voici exactement ce que dit ton document et comment nous allons adapter le code pour que ton PoC fonctionne enfin.

### L'Analyse de ton Document Interne

La documentation pointe trois choses fondamentales pour ton environnement :
1. **Pas de Hive Metastore (HMS) partagé :** Il est explicitement écrit que dans la *Sandbox*, Spark ne peut pas se connecter au catalogue HMS de Starburst. (Notre dernière idée avec l'URL `thrift://` est donc impossible ici).
2. **L'écriture Iceberg est bloquée :** Le texte dit clairement : *"Spark ne peut pas mettre à jour le catalogue HMS de Starburst en écrivant directement dans le COS [S3]"*. C'est l'explication exacte de notre syndrome du "cerveau divisé" (Split-Brain) : Spark écrit, mais Starburst reste aveugle.
3. **La Solution Officielle de Contournement :** La toute dernière phrase donne la marche à suivre exacte : *"écrire les fichiers parquet directement dans le COS qui seront consultables via le connecteur Hive dans table associée sur Starburst."*

### Le Plan d'Action : L'Architecture "Parquet Natif"

Puisque l'environnement bloque la synchronisation des métadonnées Iceberg, nous allons suivre la recommandation de ta documentation : **abandonner la surcouche Iceberg pour ce PoC, et faire du Streaming Parquet standard.** C'est beaucoup plus simple, et Starburst saura le lire sans problème !

#### Étape 1 : Le grand nettoyage (Dernière fois !)
Sur ton bucket S3, supprime tout pour repartir à zéro et éviter les conflits avec les anciens dossiers Iceberg :
* `poc_streaming/input_cloudevent_raw/`
* `poc_streaming/checkpoint_cloudevent/`
* `poc_streaming/cloudevent/`

#### Étape 2 : Le Code Spark Simplifié (Sans Iceberg)
Dans DevX, remplace le contenu de `main_cloudevent.py` par ce code. Tu verras qu'il est beaucoup plus léger car on a retiré toutes les configurations complexes d'Iceberg. On écrit simplement du Parquet.

```python
import threading
import time
import uuid
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, MapType
from pyspark.sql.functions import current_timestamp, to_json, col

# Chemins S3
RAW_BUCKET_PATH = "s3a://bu002i004226/poc_streaming/input_cloudevent_raw/"
CHECKPOINT_PATH = "s3a://bu002i004226/poc_streaming/checkpoint_cloudevent/"
# Nouveau chemin direct pour les fichiers Parquet
PARQUET_OUTPUT_PATH = "s3a://bu002i004226/poc_streaming/cloudevent_parquet/"

# 1. INIT SPARK (Ultra basique, plus d'Iceberg)
print("### INIT SPARK - STREAMING PARQUET ###")
spark = SparkSession.builder \
    .appName("DevX-CloudEvent-Parquet") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 2. GÉNÉRATEUR MOCK
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
        mock_data = [("1.0", "com.sensor", f"/device-{i}", "telemetry", event_id, now_iso, "application/json", {"temp": str(20 + i)})]
        
        df_mock = spark.createDataFrame(mock_data, schema=gen_schema)
        df_mock.write.mode("append").json(RAW_BUCKET_PATH)
        time.sleep(sleep_time)

threading.Thread(target=generate_mock_events).start()

# 3. LECTURE & TRANSFORMATION
read_schema = S









C'est une excellente capture d'écran ! Elle m'aide beaucoup car elle nous confirme exactement *où* le crash se produit.

Ce que l'on voit ici, c'est le "certificat de décès" de ton job Spark sur Kubernetes. On voit que le conteneur principal (`spark-kubernetes-driver`) s'est arrêté avec le code d'erreur **`exit code: 2`**. 

Cependant, cette partie du log est juste le résumé final envoyé par Kubernetes. Elle nous dit *que* le job a planté, mais elle ne nous dit pas *pourquoi* (l'autopsie). L'erreur `exit code: 2` dans Spark/Python signifie généralement deux choses : soit une petite erreur de syntaxe dans le fichier Python, soit un argument manquant au lancement.

### Comment trouver le vrai message d'erreur :

Tu as deux options très simples pour trouver le message exact :

**Option 1 : Le coup de molette (Le plus rapide)**
Place le curseur de ta souris dans ce même terminal et **scrolle vers le haut**. Juste avant ce bloc final "Container final statuses", tu devrais voir un gros bloc de texte avec écrit `Traceback (most recent call last):` ou `Exception in thread "main"`. C'est cette ligne précise (souvent avec le nom de ton fichier `.py` ou une erreur S3) qui nous donnera la solution.

**Option 2 : La commande Kubernetes**
Si le terminal a été effacé, tu peux demander à Kubernetes de te recracher le log complet du pod qui a planté. 
Dans ton terminal, tape ceci pour lister les pods et voir ceux en erreur :
```bash
kubectl get pods
```
Tu y verras un pod avec un nom qui ressemble à `poc-streaming-sparktacus-...-driver` avec le statut `Error`. Copie son nom exact et tape :
```bash
kubectl logs le-nom-exact-de-ton-pod
```

Est-ce que tu peux remonter un peu dans ton terminal et me dire quelle est la ligne d'erreur (le `Traceback` ou le `java.lang.Exception`) qui s'affiche juste avant ? On est à quelques centimètres du but !
tructType([
    StructField("specversion", StringType(), True),
    StructField("type", StringType(), True),
    StructField("source", StringType(), True),
    StructField("subject", StringType(), True),
    StructField("id", StringType(), True),
    StructField("time", StringType(), True), # Lu en String pour éviter les nulls
    StructField("datacontenttype", StringType(), True),
    StructField("data", MapType(StringType(), StringType()), True) 
])

df_stream = spark.readStream.format("json").schema(read_schema).load(RAW_BUCKET_PATH)

df_processed = df_stream \
    .withColumn("data", to_json(col("data"))) \
    .withColumn("dh_poc_insert_timestamp", current_timestamp())

# 4. ÉCRITURE PARQUET STANDARD
print("### DÉMARRAGE DU STREAMING VERS PARQUET ###")
query = df_processed.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .trigger(processingTime="2 seconds") \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .option("path", PARQUET_OUTPUT_PATH) \
    .start()

query.awaitTermination()
```

*(N'oublie pas de pousser ce script sur S3 et de le lancer via ton `submit_streaming.sh` comme on le fait depuis le début).*

#### Étape 3 : Création de la table dans Starburst
Une fois le job Spark lancé et les premiers fichiers Parquet générés, va dans Starburst. 
Supprime l'ancienne table Iceberg et crée une table **Hive/Parquet standard** qui pointe directement sur le dossier :

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
    dh_poc_insert_timestamp TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format = 'PARQUET',
    external_location = 's3a://bu002i004226/poc_streaming/cloudevent_parquet/'
);
```

Désormais, tout fonctionnera : Spark écrira les Parquets, et Starburst les lira directement. Tu as brillamment identifié la cause racine avec ce document !







C'est une excellente question. Pour gérer un flux massif d'événements de manière simultanée (parallélisme), il ne s'agit plus seulement du code Python, mais de la manière dont Spark distribue le travail sur ton cluster Kubernetes.

Voici comment gérer le parallélisme, optimiser les appels API et visualiser le tout.

### 1. Le parallélisme dans Spark Streaming
Dans Spark, le parallélisme repose sur trois piliers :
* **Les Partitions :** C'est l'unité de base. Si tu as 10 partitions dans tes données, Spark peut théoriquement faire travailler 10 processeurs en même temps.
* **Les Exécuteurs et Cores :** Dans ton fichier `submit_streaming.sh`, tu définis le nombre d'exécuteurs (`--num-executors`) et de cœurs par exécuteur (`--executor-cores`).
* **Le "Shuffle" :** Pour répartir les données uniformément avant un traitement lourd, on utilise souvent `.repartition(n)`.

### 2. Optimiser l'appel par API (MapPartitions)
Si ton code doit appeler une API pour chaque événement, faire un appel un par un est très lent (latence réseau). La solution est d'ouvrir **une seule connexion par partition** plutôt que par ligne.

```python
def call_api_batch(partition):
    # On crée une session HTTP une seule fois pour toute la partition (ex: 1000 lignes)
    import requests
    with requests.Session() as session:
        results = []
        for row in partition:
            # Appel API simulé
            # response = session.post(url, json=row.asDict())
            results.append(row) 
        return iter(results)

# Dans ton stream :
df_parallel = df_processed.repartition(10) # On force 10 tâches parallèles
df_enriched = df_parallel.rdd.mapPartitions(call_api_batch).toDF()
```

### 3. Visualisation dans la Spark UI
Pour "voir" le parallélisme, tu dois te connecter à la **Spark UI** (souvent sur le port 4040). 
* Dans l'onglet **Streaming**, tu verras le temps de traitement des batchs.
* Dans l'onglet **Stages**, tu verras un graphique (Timeline) montrant toutes les tâches s'exécutant horizontalement. Si elles se chevauchent, c'est que le parallélisme fonctionne.



Pour t'aider à comprendre comment la configuration de ton cluster influence la vitesse de traitement lors d'une arrivée massive d'événements, voici un simulateur de parallélisme Spark.

```json?chameleon
{"component":"LlmGeneratedComponent","props":{"height":"700px","prompt":"Simulateur de parallélisme Spark Streaming. Objectif : Visualiser comment le nombre d'exécuteurs et de cœurs gère une charge d'événements API. \n\nEntrées : \n- 'Nombre d'événements entrants' (Slider 100 à 2000).\n- 'Nombre d'exécuteurs' (Slider 1 à 10).\n- 'Cœurs par exécuteur' (Slider 1 à 8).\n- 'Latence API par événement' (ms).\n\nComportement : \n- Lorsqu'on clique sur 'Lancer le Batch', simuler la distribution des événements sur les cœurs disponibles.\n- Afficher un graphique de type 'Timeline' (Gantt chart) montrant les tâches s'exécutant en parallèle sur les différents exécuteurs.\n- Calculer et afficher le 'Temps total de traitement' vs 'Temps théorique sans parallélisme'.\n- Si le nombre d'événements dépasse la capacité immédiate, montrer la mise en file d'attente (Batch Queue).\n\nStyle : Utiliser une esthétique proche de la Spark UI (bleu/gris). Langue : Français.","id":"im_48f4d85f073df021"}}
```

### Comment tester concrètement ?
Pour tester et voir les graphes de la Spark UI saturés, tu peux modifier ton générateur pour envoyer un "burst" de données :

1.  **Augmente le nombre de partitions** juste après la lecture : `df.repartition(20)`.
2.  **Modifie le script de lancement** (`.sh`) pour demander plus de ressources :
    ```bash
    --conf spark.executor.instances=5 \
    --conf spark.executor.cores=4 \
    ```
3.  **Simule une charge lourde** dans ton code Python en augmentant le nombre d'itérations du générateur à 2000 au lieu de 20.

En ouvrant la Spark UI pendant le run, tu verras les 20 cœurs (5 instances * 4 cœurs) s'allumer simultanément dans la "Event Timeline". Est-ce que tu veux que je te donne le bloc de code spécifique pour intégrer le `repartition` et l'optimisation `mapPartitions` dans ton script actuel ?
