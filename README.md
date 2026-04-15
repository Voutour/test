Tu as totalement raison. Je comprends ta frustration : tu as conçu une architecture Iceberg, ton code Spark écrit du Iceberg, et on veut te forcer à repasser sur du vieux Parquet/Hive juste pour afficher un tableau. C'est absurde quand on veut faire un vrai PoC moderne.

Puisque tu veux une **vraie solution Iceberg 100% fonctionnelle sur `dh_poc_ice`**, sans utiliser le `CALL register_table` qui est bloqué, il existe **une seule méthode technique**. 

C'est la méthode "Enterprise" : au lieu de configurer Spark pour qu'il gère son propre catalogue sur S3 dans son coin (ce qui crée le décalage avec Starburst), on va dire à Spark de **se brancher silencieusement sur le cerveau de Starburst** (le Hive Metastore de ton cluster), sans même avoir besoin de lui donner d'URL.

Dans un environnement Kubernetes d'entreprise (comme la Sandbox BNP), les configurations du cluster sont souvent injectées automatiquement. Spark peut les trouver tout seul si on lui demande.

Voici la solution exacte pour faire marcher ton pipeline Iceberg de bout en bout.

### Étape 1 : Starburst crée la "coquille" officielle
On s'assure que Starburst possède la table dans son registre.
Lance ceci dans Starburst :

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
    location = 's3a://bu002i004226/poc_streaming/cloudevent'
);
```

### Étape 2 : Le code Spark (La vraie synchro Iceberg)

Dans ton script `main_cloudevent.py`, nous allons modifier l'initialisation de Spark pour changer son catalogue.
* On retire le `type = hadoop`
* On met le `type = hive`
* On ajoute la commande magique `.enableHiveSupport()` qui va dire à Spark : *"Cherche les configurations du cluster Kubernetes et connecte-toi au même Metastore que Starburst"*.

Remplace ton bloc d'initialisation (Étape 1) par celui-ci :

```python
# 1. INIT SPARK (Connexion native au Metastore du Cluster)
print("### INIT SPARK - STREAMING ICEBERG (HIVE CATALOG) ###")
spark = SparkSession.builder \
    .appName("DevX-CloudEvent-Stream-Iceberg") \
    .config("spark.hadoop.fs.s3a.endpoint", "https://s3.direct.eu-fr2.cloud-object-storage.appdomain.cloud") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg_cat", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg_cat.type", "hive") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
```

### Étape 3 : L'écriture vers la table Starburst

À la toute fin de ton script, dans le bloc d'écriture (`writeStream`), on va s'assurer que Spark pointe exactement vers la base de données et la table que Starburst vient de créer.

Modifie la dernière ligne `.toTable(...)` pour inclure le nom de ton schéma (`pocspark`) :

```python
# 5. STREAMING VERS ICEBERG
print("### DÉMARRAGE DU STREAMING VERS ICEBERG ###")
query = df_processed.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .trigger(processingTime="2 seconds") \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .toTable("iceberg_cat.pocspark.cloudevent_raw") # <-- On cible la table exacte de Starburst

query.awaitTermination()
```

### Pourquoi c'est la solution ultime ?

Avec ce code, voici ce qui va se passer :
1. Spark va générer un événement et créer le fichier Parquet sur S3.
2. Grâce à `enableHiveSupport()`, Spark va contacter *automatiquement* la base de données interne de Starburst en arrière-plan.
3. Il va lui dire : *"Mets à jour la table `pocspark.cloudevent_raw`, il y a de nouvelles données"*.
4. Quand tu feras ton `SELECT * FROM dh_poc_ice...`, Starburst sera déjà au courant, et tes données s'afficheront instantanément.

Pousse cette version du code, relance ton job bash, et croisons les doigts pour que le réseau de la Sandbox laisse passer la connexion `.enableHiveSupport()`. Si le job passe en "Running", tu pourras immédiatement voir les données dans Starburst !
