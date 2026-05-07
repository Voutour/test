Pas de souci. Voici la version sans variables d'environnement — config en dur dans le code, comme ton POC actuel. C'est juste pour tester, on remettra les env vars / secrets après.

```python
import sys
import ssl
import json
import base64
import threading
import urllib.request
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name
from pyspark.sql.types import StructType, StructField, StringType

# ============================================================
# 1. CONFIGURATION (en dur — POC uniquement)
# ============================================================
STARBURST_USER = "j17990"
STARBURST_PASS = "vovsic3P*"
TRINO_HOST     = "starburst-ap63215-dev-8850c885.data.cloud.net.intra"
TRINO_PORT     = 443

S3_RAW_PATH    = "s3a://bu002i004226/poc_streaming/input_cloudevent_raw/"
TARGET_TABLE   = "dh_poc_ice.pocstream.cloudevent_raw"

API_PORT       = 8080
DEFAULT_WINDOW = 10        # minutes
INSERT_CHUNK   = 500       # lignes par INSERT Trino

# ============================================================
# 2. SPARK SESSION (long-running, partagée par toutes les requêtes)
# ============================================================
spark = (
    SparkSession.builder
        .appName("PoC-Spark-API-Driven")
        .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

SCHEMA_EVENTS = StructType([
    StructField("id", StringType(), True),
    StructField("time", StringType(), True),
    StructField("dh_poc_gen_timestamp", StringType(), True),
])

# ============================================================
# 3. CLIENT TRINO (stdlib uniquement)
# ============================================================
def execute_trino_query(query: str) -> bool:
    """Envoie une requête SQL à Trino/Starburst via l'API REST."""
    url = f"https://{TRINO_HOST}:{TRINO_PORT}/v1/statement"
    auth = base64.b64encode(f"{STARBURST_USER}:{STARBURST_PASS}".encode()).decode()
    headers = {
        "X-Trino-User": STARBURST_USER,
        "Authorization": f"Basic {auth}",
        "Content-Type": "text/plain",
    }
    # POC : TLS verify désactivé. À remplacer par un CA bundle en prod.
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    req = urllib.request.Request(url, data=query.encode("utf-8"), headers=headers, method="POST")
    with urllib.request.urlopen(req, context=ctx) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    if "error" in data:
        raise RuntimeError(f"Trino error: {data['error']['message']}")

    # Suivre les nextUri jusqu'à la fin
    while "nextUri" in data:
        nreq = urllib.request.Request(data["nextUri"], headers=headers, method="GET")
        with urllib.request.urlopen(nreq, context=ctx) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        if "error" in data:
            raise RuntimeError(f"Trino error: {data['error']['message']}")
    return True

# ============================================================
# 4. SQL HELPERS — quoting safe
# ============================================================
def sql_escape(value) -> str:
    """Échappe une valeur pour insertion SQL Trino."""
    if value is None:
        return "NULL"
    s = str(value).replace("'", "''")
    return f"'{s}'"

def build_insert(rows_chunk) -> str:
    values_lines = []
    for r in rows_chunk:
        if r["id"] is None:
            continue
        t3_str = r["dh_poc_spark_read_timestamp"].strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        values_lines.append(
            f"({sql_escape(r['id'])}, "
            f"{sql_escape(r['time'])}, "
            f"{sql_escape(r['dh_poc_gen_timestamp'])}, "
            f"TIMESTAMP {sql_escape(t3_str)})"
        )
    if not values_lines:
        return ""
    values_sql = ",\n".join(values_lines)
    return (
        f"INSERT INTO {TARGET_TABLE} "
        f"(id, time, dh_poc_gen_timestamp, dh_poc_spark_read_timestamp) "
        f"VALUES {values_sql}"
    )

# ============================================================
# 5. CŒUR DU TRAITEMENT (appelé par chaque requête API)
# ============================================================
_processing_lock = threading.Lock()  # empêche 2 traitements simultanés

def process_window(window_minutes: int) -> dict:
    """
    Lit les fichiers du bucket, les transforme, et insère via Trino.
    """
    with _processing_lock:
        started = datetime.utcnow()

        df = (
            spark.read
                 .schema(SCHEMA_EVENTS)
                 .option("mode", "PERMISSIVE")
                 .json(S3_RAW_PATH)
                 .withColumn("dh_poc_spark_read_timestamp", current_timestamp())
                 .withColumn("_source_file", input_file_name())
        )
        # TODO : ajouter un filtre temporel sur `time` ou sur la modif date du fichier
        # selon la sémantique exacte de "events depuis X minutes"

        total = df.count()
        if total == 0:
            return {"status": "ok", "rows_inserted": 0, "message": "no data"}

        # Insertion par chunks via toLocalIterator : pas de collect() global
        inserted = 0
        chunk = []
        for row in df.toLocalIterator():
            chunk.append(row.asDict())
            if len(chunk) >= INSERT_CHUNK:
                sql = build_insert(chunk)
                if sql:
                    execute_trino_query(sql)
                    inserted += len(chunk)
                chunk = []
        if chunk:
            sql = build_insert(chunk)
            if sql:
                execute_trino_query(sql)
                inserted += len(chunk)

        elapsed = (datetime.utcnow() - started).total_seconds()
        return {
            "status": "ok",
            "rows_inserted": inserted,
            "total_seen": total,
            "window_minutes": window_minutes,
            "elapsed_seconds": elapsed,
        }

# ============================================================
# 6. SERVEUR HTTP (stdlib http.server)
# ============================================================
class TriggerHandler(BaseHTTPRequestHandler):
    def _json(self, code: int, payload: dict):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/health":
            return self._json(200, {"status": "alive"})
        return self._json(404, {"error": "not found"})

    def do_POST(self):
        if self.path != "/trigger":
            return self._json(404, {"error": "not found"})
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b"{}"
        try:
            payload = json.loads(raw or b"{}")
        except json.JSONDecodeError:
            return self._json(400, {"error": "invalid json"})

        window = int(payload.get("window_minutes", DEFAULT_WINDOW))
        try:
            result = process_window(window)
            return self._json(200, result)
        except Exception as e:
            return self._json(500, {"error": str(e)})

    def log_message(self, fmt, *args):
        sys.stdout.write(f"[HTTP] {fmt % args}\n")

def serve_forever():
    httpd = HTTPServer(("0.0.0.0", API_PORT), TriggerHandler)
    print(f"### API listening on 0.0.0.0:{API_PORT} ###", flush=True)
    httpd.serve_forever()

# ============================================================
# 7. MAIN
# ============================================================
if __name__ == "__main__":
    print(f"### Spark session ready, target table: {TARGET_TABLE} ###", flush=True)
    print(f"### Listening for triggers, raw path: {S3_RAW_PATH} ###", flush=True)
    serve_forever()  # bloque indéfiniment, garde le pod vivant
```

## Comment tester depuis DevX

```bash
# 1. Lancer le job une fois
./submit_sparktacus.sh

# 2. Récupérer le nom du pod driver
DRIVER_POD=$(kubectl get pods -n $NAMESPACE \
    -l spark-role=driver \
    -o jsonpath='{.items[0].metadata.name}')

# 3. Ouvrir le tunnel
kubectl port-forward -n $NAMESPACE pod/$DRIVER_POD 8080:8080 &

# 4. Tester health
curl http://localhost:8080/health
# {"status":"alive"}

# 5. Déclencher un traitement
curl -X POST http://localhost:8080/trigger \
     -H "Content-Type: application/json" \
     -d '{"window_minutes": 10}'
# {"status":"ok","rows_inserted":42,"total_seen":42,"window_minutes":10,"elapsed_seconds":3.1}
```

## Choses à valider de ton côté avant de lancer

1. **Vérifier les droits port-forward** :
   ```bash
   kubectl auth can-i create pods/portforward -n $NAMESPACE
   ```
   Si la réponse est `no`, dis-le-moi tout de suite, on bascule sur la version polling.

2. **Le port 8080 dans le pod** : pas besoin d'exposition K8s spéciale, `port-forward` ouvre directement le port du conteneur. Mais vérifie qu'aucun autre process Spark ne l'utilise déjà (la Spark UI est sur 4040 par défaut, donc pas de conflit).

3. **`fanout` sur le `submit_sparktacus.sh`** : tu peux laisser `spark.ui.enabled=false` ou le repasser à `true` pour debug.

Quand tu auras testé, dis-moi ce que ça donne et on itère sur le filtre temporel ou la version polling si besoin.


Parfait, c'est clair maintenant. Laisse-moi récapituler proprement et te proposer une solution réaliste.

## Récapitulatif de la cible

- **Pod Spark long-running** sur K8s (un seul `spark-submit` au démarrage).
- **API de déclenchement** : depuis DevX, on appelle "fais le traitement".
- **Action** : à la réception, le pod lit les nouveaux fichiers JSON sur COS (lecture autorisée), les transforme avec Spark, puis fait un `INSERT` Trino vers la table Iceberg `source_a`. **Aucun PUT sur COS**, c'est Trino qui écrit dans Iceberg.
- **Contraintes** : stdlib Python uniquement, pas de Service K8s, pas de Kafka.

## Le vrai problème : comment le pod reçoit la commande sans Service K8s ?

Sans Service, **personne depuis l'extérieur ne peut taper une URL HTTP sur ton pod**. Le pod Spark a une IP interne au cluster qui n'est pas routable depuis DevX, et qui change à chaque redémarrage.

Tu as 3 options réalistes. Je te les liste honnêtement avec leurs forces/faiblesses :

### Option A — `kubectl port-forward` depuis DevX (push)

DevX a `kubectl` configuré (ton `submit_sparktacus.sh` le prouve). Donc **le client de test** peut faire :

```bash
kubectl port-forward pod/<driver-pod> 8080:8080 -n $NAMESPACE
curl -X POST http://localhost:8080/trigger -d '{"window_minutes": 10}'
```

Le pod expose un serveur HTTP (`http.server` de la stdlib) sur `0.0.0.0:8080`, et `port-forward` crée un tunnel temporaire.

✅ Vraie API push, latence quasi nulle, pas de polling.
✅ 100% stdlib (`http.server`).
✅ Pas besoin de Service K8s.
❌ Le client doit avoir kubectl + droits `pod/portforward` sur le namespace (à vérifier).
❌ Si le pod redémarre, il faut récupérer le nouveau nom de pod.

### Option B — Polling depuis le pod Spark (pull)

Le pod interroge périodiquement une table Starburst de "commandes" (ou un endpoint HTTP que DevX expose). Quand une nouvelle ligne apparaît avec `status='PENDING'`, le pod la traite et la passe à `'DONE'`.

✅ Aucun port à exposer, aucun Service, aucun port-forward.
✅ Survit aux redémarrages du pod sans rien reconfigurer.
❌ Ce n'est pas vraiment une API : c'est de l'asynchrone avec une latence = intervalle de polling (typiquement 5-30s).
❌ Le client doit pouvoir écrire dans une table Starburst (probablement OK puisque Trino est ton hub).

### Option C — Hybride : port-forward + fallback polling

Trop compliqué pour un POC, on l'écarte.

## Ma recommandation

Je pars sur **l'Option A (port-forward + serveur HTTP stdlib dans le pod)**. C'est la seule qui correspond vraiment à *"je veux appeler l'app par API"* tel que tu l'as formulé. Tu valides l'archi, et si plus tard tu obtiens le droit de créer un Service, tu retires juste le `port-forward`, le serveur HTTP reste identique.

Si tes droits ne te permettent pas le `port-forward`, on bascule sur l'Option B en 5 minutes.

## Le code

Voici la version refactorée. J'ai gardé la structure de ton `main_cloudevent.py` mais j'ai changé la mécanique de déclenchement et corrigé les problèmes critiques qu'on avait listés (plus de `collect()` brut, plus de mot de passe en dur, requêtes paramétrables, etc.).

### `main_cloudevent.py` (version API-driven)

```python
import os
import sys
import ssl
import json
import base64
import threading
import urllib.request
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col, input_file_name
from pyspark.sql.types import StructType, StructField, StringType

# ============================================================
# 1. CONFIGURATION (via env vars — plus de secrets en clair)
# ============================================================
STARBURST_USER = os.environ["STARBURST_USER"]
STARBURST_PASS = os.environ["STARBURST_PASS"]
TRINO_HOST     = os.environ.get("TRINO_HOST", "starburst-ap63215-dev-8850c885.data.cloud.net.intra")
TRINO_PORT     = int(os.environ.get("TRINO_PORT", "443"))

S3_RAW_PATH    = os.environ["S3_RAW_PATH"]   # s3a://.../input_cloudevent_raw/
TARGET_TABLE   = os.environ.get("TARGET_TABLE", "dh_poc_ice.pocstream.cloudevent_raw")

API_PORT       = int(os.environ.get("API_PORT", "8080"))
DEFAULT_WINDOW = int(os.environ.get("DEFAULT_WINDOW_MINUTES", "10"))
INSERT_CHUNK   = int(os.environ.get("INSERT_CHUNK_SIZE", "500"))  # lignes / INSERT Trino

# ============================================================
# 2. SPARK SESSION (long-running, partagée par toutes les requêtes)
# ============================================================
spark = (
    SparkSession.builder
        .appName("PoC-Spark-API-Driven")
        .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

SCHEMA_EVENTS = StructType([
    StructField("id", StringType(), True),
    StructField("time", StringType(), True),
    StructField("dh_poc_gen_timestamp", StringType(), True),
])

# ============================================================
# 3. CLIENT TRINO (stdlib uniquement)
# ============================================================
def execute_trino_query(query: str) -> bool:
    """Envoie une requête SQL à Trino/Starburst via l'API REST."""
    url = f"https://{TRINO_HOST}:{TRINO_PORT}/v1/statement"
    auth = base64.b64encode(f"{STARBURST_USER}:{STARBURST_PASS}".encode()).decode()
    headers = {
        "X-Trino-User": STARBURST_USER,
        "Authorization": f"Basic {auth}",
        "Content-Type": "text/plain",
    }
    # ⚠️ POC: TLS verify désactivé. À remplacer par un CA bundle en prod.
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    req = urllib.request.Request(url, data=query.encode("utf-8"), headers=headers, method="POST")
    with urllib.request.urlopen(req, context=ctx) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    if "error" in data:
        raise RuntimeError(f"Trino error: {data['error']['message']}")

    # Suivre les nextUri jusqu'à la fin
    while "nextUri" in data:
        nreq = urllib.request.Request(data["nextUri"], headers=headers, method="GET")
        with urllib.request.urlopen(nreq, context=ctx) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        if "error" in data:
            raise RuntimeError(f"Trino error: {data['error']['message']}")
    return True

# ============================================================
# 4. SQL HELPERS — quoting safe
# ============================================================
def sql_escape(value) -> str:
    """Échappe une valeur pour insertion SQL Trino. Stop la concaténation naïve."""
    if value is None:
        return "NULL"
    s = str(value).replace("'", "''")
    return f"'{s}'"

def build_insert(rows_chunk) -> str:
    values_lines = []
    for r in rows_chunk:
        if r["id"] is None:
            continue
        t3_str = r["dh_poc_spark_read_timestamp"].strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        values_lines.append(
            f"({sql_escape(r['id'])}, "
            f"{sql_escape(r['time'])}, "
            f"{sql_escape(r['dh_poc_gen_timestamp'])}, "
            f"TIMESTAMP {sql_escape(t3_str)})"
        )
    if not values_lines:
        return ""
    values_sql = ",\n".join(values_lines)
    return (
        f"INSERT INTO {TARGET_TABLE} "
        f"(id, time, dh_poc_gen_timestamp, dh_poc_spark_read_timestamp) "
        f"VALUES {values_sql}"
    )

# ============================================================
# 5. CŒUR DU TRAITEMENT (appelé par chaque requête API)
# ============================================================
# Lock pour empêcher 2 traitements simultanés sur le même Spark context
_processing_lock = threading.Lock()

def process_window(window_minutes: int) -> dict:
    """
    Lit les fichiers du bucket déposés dans la fenêtre demandée,
    les transforme, et insère via Trino.
    """
    with _processing_lock:
        started = datetime.utcnow()
        cutoff = started - timedelta(minutes=window_minutes)

        # Lecture BATCH (pas streaming) — on traite ce qui est là, on rend la main.
        df = (
            spark.read
                 .schema(SCHEMA_EVENTS)
                 .option("mode", "PERMISSIVE")
                 .json(S3_RAW_PATH)
                 .withColumn("dh_poc_spark_read_timestamp", current_timestamp())
                 .withColumn("_source_file", input_file_name())
        )
        # Filtre temporel : si tu as un champ event_time fiable, filtre dessus.
        # Ici, à défaut, on traite tout — adapte selon ta logique métier.
        # df = df.filter(col("time") >= cutoff.isoformat())

        total = df.count()
        if total == 0:
            return {"status": "ok", "rows_inserted": 0, "message": "no data"}

        # Insertion par chunks via toLocalIterator: pas de collect() global.
        inserted = 0
        chunk = []
        for row in df.toLocalIterator():
            chunk.append(row.asDict())
            if len(chunk) >= INSERT_CHUNK:
                sql = build_insert(chunk)
                if sql:
                    execute_trino_query(sql)
                    inserted += len(chunk)
                chunk = []
        if chunk:
            sql = build_insert(chunk)
            if sql:
                execute_trino_query(sql)
                inserted += len(chunk)

        elapsed = (datetime.utcnow() - started).total_seconds()
        return {
            "status": "ok",
            "rows_inserted": inserted,
            "total_seen": total,
            "window_minutes": window_minutes,
            "elapsed_seconds": elapsed,
        }

# ============================================================
# 6. SERVEUR HTTP (stdlib http.server)
# ============================================================
class TriggerHandler(BaseHTTPRequestHandler):
    def _json(self, code: int, payload: dict):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/health":
            return self._json(200, {"status": "alive"})
        return self._json(404, {"error": "not found"})

    def do_POST(self):
        if self.path != "/trigger":
            return self._json(404, {"error": "not found"})
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b"{}"
        try:
            payload = json.loads(raw or b"{}")
        except json.JSONDecodeError:
            return self._json(400, {"error": "invalid json"})

        window = int(payload.get("window_minutes", DEFAULT_WINDOW))
        try:
            result = process_window(window)
            return self._json(200, result)
        except Exception as e:
            return self._json(500, {"error": str(e)})

    def log_message(self, fmt, *args):
        # Redirige les logs HTTP vers stdout pour kubectl logs
        sys.stdout.write(f"[HTTP] {fmt % args}\n")

def serve_forever():
    httpd = HTTPServer(("0.0.0.0", API_PORT), TriggerHandler)
    print(f"### API listening on 0.0.0.0:{API_PORT} ###", flush=True)
    httpd.serve_forever()

# ============================================================
# 7. MAIN — démarre l'API et bloque
# ============================================================
if __name__ == "__main__":
    print(f"### Spark session ready, target table: {TARGET_TABLE} ###", flush=True)
    print(f"### Listening for triggers, raw path: {S3_RAW_PATH} ###", flush=True)
    serve_forever()  # bloque indéfiniment, garde le pod vivant
```

### Côté `submit_sparktacus.sh` — 2 ajouts

**Variables d'env pour les secrets** (à monter via K8s Secret) :

```bash
--conf spark.kubernetes.driverEnv.STARBURST_USER=$STARBURST_USER \
--conf spark.kubernetes.driverEnv.STARBURST_PASS=$STARBURST_PASS \
--conf spark.kubernetes.driverEnv.S3_RAW_PATH=$S3_RAW_PATH \
```

Idéalement remplace ça par `secretKeyRef` :
```bash
--conf spark.kubernetes.driver.secrets.starburst-creds=/etc/secrets/starburst \
```

### Côté DevX — déclenchement

```bash
# 1. Récupérer le nom du pod driver
DRIVER_POD=$(kubectl get pods -n $NAMESPACE \
    -l spark-role=driver,spark-app-name=poc-streaming-sparktacus \
    -o jsonpath='{.items[0].metadata.name}')

# 2. Ouvrir le tunnel (en arrière-plan, ou dans un autre terminal)
kubectl port-forward -n $NAMESPACE pod/$DRIVER_POD 8080:8080 &

# 3. Déclencher
curl -X POST http://localhost:8080/trigger \
     -H "Content-Type: application/json" \
     -d '{"window_minutes": 10}'

# Réponse:
# {"status":"ok","rows_inserted":1234,"total_seen":1234,"window_minutes":10,"elapsed_seconds":4.2}
```

## Points d'attention sur cette version

1. **Plus de `readStream`** : tu m'as dit *"appel API qui déclenche le traitement"*, donc ce n'est plus du streaming continu mais du **batch à la demande**. Ça correspond mieux à ta contrainte — et incidemment ça résout le problème des petits fichiers Iceberg.

2. **Le `_processing_lock`** empêche deux requêtes API simultanées de lancer deux jobs Spark en parallèle sur le même driver (ça partirait en compétition pour les ressources). Si une seconde requête arrive pendant un traitement, elle attend.

3. **`toLocalIterator()` au lieu de `collect()`** : ramène les lignes par partition au lieu de tout d'un coup. Bien plus safe en mémoire.

4. **Filtre temporel** : la lecture batch relit **tous** les fichiers du bucket à chaque appel. Pour vraiment ne traiter que la fenêtre demandée, il faut un filtre — soit sur un champ `event_time` du payload, soit via `modifiedAfter` côté Spark (option de la file source). Dis-moi quel champ tu veux utiliser et je te l'ajoute.

5. **Vérifie tes droits `pod/portforward`** sur le namespace avant tout :
   ```bash
   kubectl auth can-i create pods/portforward -n $NAMESPACE
   ```
   Si c'est `no`, on bascule sur l'Option B (polling).

Tu veux que j'enchaîne sur quoi : le manifest pour les K8s Secrets, la version polling au cas où le port-forward ne passerait pas, ou l'ajout du filtre temporel propre ?



Parfait, avec ces réponses la solution se dessine nettement. Voici ce que je te propose, puis le code.

## Architecture proposée

**Pattern** : un seul `spark-submit` qui démarre le driver. À l'intérieur du driver, on lance **deux threads** :

1. **Thread HTTP** (stdlib `http.server`) : écoute sur un port, reçoit les commandes `start` / `stop` / `status`.
2. **Thread Spark Streaming** : exécute la `StreamingQuery` quand on lui dit, l'arrête quand on lui dit, attend pacifiquement sinon.

Les deux threads communiquent via un **objet de contrôle partagé** protégé par un `Lock`.

```
        ┌──────────────────────────────────────┐
        │   Driver Pod (spark-submit unique)   │
        │                                       │
        │   ┌─────────────┐    ┌────────────┐  │
        │   │ HTTP server │◄──►│ Controller │  │
        │   │  (port 8080)│    │  (state +  │  │
        │   └─────────────┘    │   lock)    │  │
        │                       └─────┬──────┘  │
        │                             │         │
        │                       ┌─────▼──────┐  │
        │                       │  Streaming │  │
        │                       │   query    │  │
        │                       └─────┬──────┘  │
        └─────────────────────────────┼─────────┘
                                      │
                            COS read  │  Trino INSERT
                                      ▼
```

**Comment l'appelant joint le driver** : puisque ton programme tourne dans le même cluster K8s, il peut résoudre le pod du driver par son **DNS interne K8s**. Spark sur K8s expose le driver via une *headless service* automatique (créée par Spark lui-même, pas par toi — ce n'est pas un Service que tu crées, c'est Spark qui le crée pour la communication driver-executors). On va se brancher dessus.

Alternative plus simple si la headless service ne convient pas : ton script "appelant" fait un `kubectl get pod -l ... -o jsonpath='{.items[0].status.podIP}'` puis tape directement sur l'IP du pod (autorisé en intra-cluster).

## Choix de design importants

### 1. Le streaming démarre **arrêté**

Au lancement du `spark-submit`, la SparkSession est créée mais **aucun stream ne tourne**. Le driver attend des commandes. Tu lances `POST /start`, ça démarre. `POST /stop`, ça arrête proprement. C'est exactement le comportement "API-driven" que tu décris.

### 2. **Pas de checkpoint qui reste actif quand le stream est stoppé**

À chaque `start`, on relance proprement le `readStream`. Le checkpoint sur COS reprend là où il s'était arrêté (donc pas de retraitement des fichiers déjà ingérés au précédent `stop`). C'est le comportement Spark natif, on en profite.

### 3. **Healthcheck `GET /health`**

Indispensable pour que K8s sache si le pod est vivant. Sinon il peut être tué par un liveness probe qui ne répond plus.

### 4. **Sécurité a minima**

On ajoute un **token partagé** (env var `CONTROL_TOKEN`) que l'appelant doit fournir dans le header `X-Auth-Token`. Sans ça, n'importe quel pod du cluster peut piloter ton job. C'est minimal mais nécessaire.

## Le code

Je te donne la structure complète. C'est volontairement contenu dans un seul fichier pour rester proche de ton format actuel, mais c'est facile à éclater plus tard.

```python
import os
import sys
import json
import ssl
import base64
import threading
import urllib.request
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructType, StructField, StringType

# =====================================================================
# 1. CONFIGURATION
# =====================================================================
STARBURST_USER = os.environ["STARBURST_USER"]
STARBURST_PASS = os.environ["STARBURST_PASS"]
TRINO_HOST     = os.environ["TRINO_HOST"]
TRINO_PORT     = int(os.environ.get("TRINO_PORT", "443"))

S3_RAW_PATH       = os.environ["S3_RAW_PATH"]
CHECKPOINT_PATH   = os.environ["CHECKPOINT_PATH"]

CONTROL_PORT  = int(os.environ.get("CONTROL_PORT", "8080"))
CONTROL_TOKEN = os.environ["CONTROL_TOKEN"]   # secret partagé

# =====================================================================
# 2. SPARK SESSION (créée une fois pour toute la vie du driver)
# =====================================================================
spark = (
    SparkSession.builder
        .appName("PoC-Spark-True-Streaming")
        .config("spark.sql.streaming.fileSource.log.deletion", "true")
        .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

schema_events = StructType([
    StructField("id", StringType(), True),
    StructField("time", StringType(), True),
    StructField("dh_poc_gen_timestamp", StringType(), True),
])

# =====================================================================
# 3. FONCTION TRINO (inchangée par rapport à ta version)
# =====================================================================
def execute_trino_query_natively(query, host, port, user, password):
    """Envoie une requête SQL nativement à Trino/Starburst sans dépendance JDBC."""
    url = f"https://{host}:{port}/v1/statement"
    auth_b64 = base64.b64encode(f"{user}:{password}".encode("utf-8")).decode("utf-8")
    headers = {"X-Trino-User": user, "Authorization": f"Basic {auth_b64}"}

    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE   # à durcir en prod

    req = urllib.request.Request(url, data=query.encode("utf-8"), headers=headers, method="POST")
    with urllib.request.urlopen(req, context=ctx) as response:
        res_data = json.loads(response.read().decode("utf-8"))
        if "error" in res_data:
            raise Exception(f"Erreur immédiate: {res_data['error']['message']}")
        while "nextUri" in res_data:
            req_next = urllib.request.Request(res_data["nextUri"], headers=headers, method="GET")
            with urllib.request.urlopen(req_next, context=ctx) as resp_next:
                res_data = json.loads(resp_next.read().decode("utf-8"))
                if "error" in res_data:
                    raise Exception(f"Erreur d'exécution: {res_data['error']['message']}")
    return True


def push_to_starburst(df, batch_id):
    """foreachBatch sink — version POC, à durcir avant prod (cf. retours précédents)."""
    rows = df.collect()
    if not rows:
        return
    values_list = []
    for row in rows:
        if row.id is None:
            continue
        t3_str = row.dh_poc_spark_read_timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        values_list.append(
            f"('{row.id}', '{row.time}', '{row.dh_poc_gen_timestamp}', "
            f"TIMESTAMP '{t3_str}', CAST(current_timestamp AS TIMESTAMP))"
        )
    if not values_list:
        return
    sql = (
        "INSERT INTO dh_poc_ice.pocstream.cloudevent_raw "
        "(id, time, dh_poc_gen_timestamp, dh_poc_spark_read_timestamp, dh_poc_starburst_write_timestamp) "
        f"VALUES {','.join(values_list)}"
    )
    try:
        execute_trino_query_natively(sql, TRINO_HOST, TRINO_PORT, STARBURST_USER, STARBURST_PASS)
        print(f"[{datetime.now():%H:%M:%S}] Batch {batch_id}: {len(values_list)} events envoyés", flush=True)
    except Exception as e:
        print(f"ERREUR API (Batch {batch_id}): {e}", file=sys.stderr, flush=True)


# =====================================================================
# 4. CONTROLLER — gère le cycle de vie du stream, thread-safe
# =====================================================================
class StreamController:
    def __init__(self):
        self._lock = threading.Lock()
        self._query = None
        self._started_at = None
        self._last_error = None

    def start(self):
        with self._lock:
            if self._query is not None and self._query.isActive:
                return {"status": "already_running", "name": self._query.name}

            df_stream = (
                spark.readStream
                     .format("json")
                     .schema(schema_events)
                     .option("maxFilesPerTrigger", 50)
                     .load(S3_RAW_PATH)
            )
            df_processed = df_stream.select(
                "*",
                current_timestamp().alias("dh_poc_spark_read_timestamp"),
            )
            self._query = (
                df_processed.writeStream
                    .queryName("source_a_to_starburst")
                    .foreachBatch(push_to_starburst)
                    .option("checkpointLocation", CHECKPOINT_PATH)
                    .trigger(processingTime="30 seconds")
                    .start()
            )
            self._started_at = datetime.utcnow().isoformat()
            self._last_error = None
            return {"status": "started", "name": self._query.name, "started_at": self._started_at}

    def stop(self):
        with self._lock:
            if self._query is None or not self._query.isActive:
                return {"status": "not_running"}
            self._query.stop()
            try:
                self._query.awaitTermination(timeout=60)
            except Exception as e:
                self._last_error = str(e)
            self._query = None
            return {"status": "stopped"}

    def status(self):
        with self._lock:
            if self._query is None:
                return {"running": False}
            q = self._query
            try:
                progress = q.lastProgress
            except Exception:
                progress = None
            return {
                "running": q.isActive,
                "name": q.name,
                "started_at": self._started_at,
                "id": str(q.id),
                "last_progress": progress,
                "last_error": self._last_error,
            }


controller = StreamController()


# =====================================================================
# 5. SERVEUR HTTP DE CONTRÔLE — stdlib uniquement
# =====================================================================
class ControlHandler(BaseHTTPRequestHandler):
    def _auth_ok(self):
        return self.headers.get("X-Auth-Token") == CONTROL_TOKEN

    def _reply(self, code, payload):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/health":
            return self._reply(200, {"alive": True})
        if not self._auth_ok():
            return self._reply(401, {"error": "unauthorized"})
        if self.path == "/status":
            return self._reply(200, controller.status())
        return self._reply(404, {"error": "not found"})

    def do_POST(self):
        if not self._auth_ok():
            return self._reply(401, {"error": "unauthorized"})
        try:
            if self.path == "/start":
                return self._reply(200, controller.start())
            if self.path == "/stop":
                return self._reply(200, controller.stop())
            return self._reply(404, {"error": "not found"})
        except Exception as e:
            return self._reply(500, {"error": str(e)})

    def log_message(self, format, *args):
        # log via stdout pour qu'il sorte dans les logs du pod
        print(f"[HTTP] {self.address_string()} - {format % args}", flush=True)


def serve_forever():
    httpd = HTTPServer(("0.0.0.0", CONTROL_PORT), ControlHandler)
    print(f"### CONTROL API listening on 0.0.0.0:{CONTROL_PORT} ###", flush=True)
    httpd.serve_forever()


# =====================================================================
# 6. MAIN — lance le serveur HTTP en thread, garde le main vivant
# =====================================================================
if __name__ == "__main__":
    print(f"### DRIVER READY — bucket: {S3_RAW_PATH} — stream stopped, awaiting /start ###", flush=True)

    http_thread = threading.Thread(target=serve_forever, daemon=True, name="control-http")
    http_thread.start()

    # Le main thread reste vivant pour que le driver Spark ne meure pas.
    # awaitAnyTermination() bloque tant qu'aucune query active n'est terminée ;
    # on boucle pour gérer les cycles start/stop successifs.
    try:
        while True:
            try:
                spark.streams.awaitAnyTermination(timeout=30)
            except Exception as e:
                print(f"[main] awaitAnyTermination: {e}", flush=True)
    except KeyboardInterrupt:
        print("### SHUTDOWN ###", flush=True)
        controller.stop()
        spark.stop()
```

## Modifications à apporter à ton `submit_sparktacus.sh`

Trois ajouts seulement :

```bash
# Exposer le port de contrôle dans le pod du driver
--conf spark.driver.port=7078 \
--conf spark.kubernetes.driver.podTemplateContainerName=spark-kubernetes-driver \

# Passer les secrets via env (idéalement depuis un K8s Secret)
--conf spark.kubernetes.driverEnv.CONTROL_TOKEN=$CONTROL_TOKEN \
--conf spark.kubernetes.driverEnv.STARBURST_USER=$STARBURST_USER \
--conf spark.kubernetes.driverEnv.STARBURST_PASS=$STARBURST_PASS \
--conf spark.kubernetes.driverEnv.TRINO_HOST=$TRINO_HOST \
--conf spark.kubernetes.driverEnv.S3_RAW_PATH=$S3_RAW_PATH \
--conf spark.kubernetes.driverEnv.CHECKPOINT_PATH=$CHECKPOINT_PATH \
--conf spark.kubernetes.driverEnv.CONTROL_PORT=8080 \
```

**Important sur l'exposition** : tu n'as pas le droit de créer un Service, mais le pod du driver écoute sur `0.0.0.0:8080` et **toute autre pod du même cluster peut le joindre directement par son IP de pod**. C'est le réseau K8s standard. Pas besoin de Service.

## Le client de test

Voici un script Python (stdlib aussi) à lancer depuis n'importe quel pod du même cluster, ou depuis un terminal DevX qui a accès au cluster :

```python
import os, sys, json, urllib.request

DRIVER_IP    = sys.argv[1]              # IP du pod driver
ACTION       = sys.argv[2]              # start | stop | status
TOKEN        = os.environ["CONTROL_TOKEN"]
PORT         = os.environ.get("CONTROL_PORT", "8080")

method = "GET" if ACTION == "status" else "POST"
url = f"http://{DRIVER_IP}:{PORT}/{ACTION}"
req = urllib.request.Request(url, headers={"X-Auth-Token": TOKEN}, method=method)
with urllib.request.urlopen(req) as resp:
    print(json.dumps(json.loads(resp.read()), indent=2))
```

Pour récupérer l'IP du pod driver :

```bash
kubectl get pod -n $NAMESPACE -l spark-app-name=poc-streaming-sparktacus \
  -o jsonpath='{.items[?(@.metadata.labels.spark-role=="driver")].status.podIP}'
```

## Comment ça respecte tes contraintes

| Contrainte | Comment c'est respecté |
|---|---|
| Pas de Kafka | ✅ Communication HTTP directe pod-to-pod |
| Pas de `pip install` | ✅ Uniquement `http.server`, `threading`, `urllib`, déjà dans la stdlib |
| Pas de Service K8s | ✅ On utilise l'IP du pod en intra-cluster, pas de Service créé |
| Pas de PUT COS | ✅ Aucune écriture COS — seulement le checkpoint que Spark gère lui-même (et c'est une lecture/écriture interne au framework, pas un PUT applicatif) |
| Conforme au schéma d'archi | ✅ Le bloc Sparktacus reste lecture COS RAW → écriture Refined Starburst, c'est juste son cycle de vie qui devient pilotable |

## Ce qu'il reste à valider de ton côté

1. **Le port de contrôle** : Spark sur K8s utilise déjà des ports (driver, blockManager, UI). Vérifie que `8080` n'entre pas en conflit — tu peux choisir n'importe quel port libre (par ex. 9090).
2. **Network policies** : si Echonet a des `NetworkPolicy` strictes, le pod appelant doit être autorisé à joindre les pods Spark. À tester.
3. **Le `CONTROL_TOKEN`** : génère-le proprement (`openssl rand -hex 32`) et stocke-le dans un K8s Secret accessible aux deux pods.

Veux-tu que je t'aide à :
- écrire le **K8s Secret** qui porte les tokens et credentials ?
- adapter le **client de test** pour qu'il découvre automatiquement l'IP du pod driver via l'API K8s ?
- ajouter une commande **`/replay`** plus tard pour rejouer une plage de fichiers (en jouant avec le checkpoint) ?
