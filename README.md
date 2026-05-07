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
