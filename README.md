
```
Objet : Accès API Confluence via Personal Access Token

Bonjour,

Dans le cadre du projet Agentic Data Pipeline Generator (CTDF), 
je dois accéder à l'API REST de Confluence (et JIRA) en non-interactif, 
via un Personal Access Token, depuis un outil local (MCP server).

J'ai généré un PAT depuis mon profil Confluence. Il fonctionne 
côté Confluence, mais toutes mes requêtes vers 
https://confluence.group.echonet/rest/api/... sont redirigées 
en 302 vers le SSO, qui ne reconnaît pas le header Bearer.

Questions :
1. Existe-t-il un endpoint API qui bypasse le SSO pour les PAT 
   (sous-domaine dédié, header spécifique) ?
2. Le PAT doit-il être whitelisté côté proxy SSO pour fonctionner ?
3. Quelle est la procédure standard pour automatiser des appels 
   API Confluence/JIRA en interne ?

Merci d'avance.
```

À envoyer à l'équipe **Confluence/Atlassian admin** BNP (souvent rattachée à ITG-A ou à une squad "Collaboration Tools"). L'équipe DevX mentionnée dans tes screenshots saura te rediriger.

## En attendant : plan B pour ton POC

Pour ne pas être bloqué sur la démo, tu as deux contournements possibles :

**Plan B1 — Cookie de session.** Ouvre Confluence dans ton navigateur, connecte-toi normalement, ouvre les DevTools (F12) → Application → Cookies → copie le cookie de session (`JSESSIONID` ou équivalent SSO). Teste :

```bash
curl -i -H "Cookie: JSESSIONID=<valeur>" \
  "https://confluence.group.echonet/rest/api/user/current"
```

Si ça passe en 200, tu as une auth temporaire (le cookie expire en quelques heures). Pas viable pour la prod, mais pour démontrer le POC ça suffit. Tu peux injecter ce cookie via les headers dans le MCP server (au lieu du Bearer).

**Plan B2 — Bouchon local.** Pour la démo, monte une fausse page Confluence en local (un petit serveur Python qui sert du JSON imitant l'API Confluence). Tu démontres l'agent fonctionnel sans dépendre du SSO. Moins glamour mais débloque la présentation.

## Action immédiate

Fais les 3 variantes de curl ci-dessus (étape 3) et regarde le `Location:` de la 302 initiale. Dis-moi :
1. Vers quelle URL le SSO te redirige
2. Si une des 3 variantes renvoie autre chose qu'un 302

Avec ça on saura si c'est contournable techniquement ou si tu dois passer par l'équipe Atlassian BNP.


Parfait, Python 3 dispo + erreur "auth required" = on a tout pour basculer sur la bonne solution. Le serveur `@aashari` essaie un Basic Auth qui ne passe pas sur ton Data Center → on le remplace par `mcp-atlassian` (Python) qui supporte nativement le Bearer PAT.

## Étape 1 — Installer mcp-atlassian

Dans un terminal :

```bash
pip install --user mcp-atlassian
```

Puis vérifie que ça tourne :

```bash
python -m mcp_atlassian --help
```

(si `python` ne marche pas, essaie `python3` partout)

Tu dois voir l'aide du serveur s'afficher. Si oui, tu es bon.

## Étape 2 — Remplacer ta config opencode

Remplace **complètement** ton bloc `mcp` actuel par celui-ci :

```json
{
  "$schema": "https://opencode.ai/config.json",
  "mcp": {
    "atlassian": {
      "type": "local",
      "command": ["python", "-m", "mcp_atlassian"],
      "environment": {
        "CONFLUENCE_URL": "https://confluence.group.echonet",
        "CONFLUENCE_PERSONAL_TOKEN": "<ton-PAT>",
        "JIRA_URL": "https://jira.group.echonet",
        "JIRA_PERSONAL_TOKEN": "<ton-PAT>"
      },
      "enabled": true
    }
  }
}
```

Trois points importants :

- Si `python -m mcp_atlassian` ne marche pas, mets `"python3"` au lieu de `"python"` dans le tableau `command`
- **Un seul serveur "atlassian"** qui gère Confluence + JIRA (au lieu de deux serveurs séparés comme avec @aashari)
- Le même PAT pour les deux si SSO unifié, sinon deux PAT différents

## Étape 3 — Tester

Relance opencode, puis :

```
opencode mcp list
```

Tu dois voir `atlassian` connecté. Puis dans une session opencode :

```
List the Confluence spaces I can access.
```

Si tu vois des espaces revenir → c'est bon, l'auth passe. Tu peux ensuite passer aux prompts get-page et get-issue.

## Si pip install échoue

Réseau corporate qui bloque PyPI ? Tente avec le proxy interne BNP (souvent `nexus.echonet` ou similaire) :

```bash
pip install --user --index-url https://<proxy-interne>/repository/pypi-proxy/simple mcp-atlassian
```

Demande à DevX/AIDI08 l'URL exacte du mirror PyPI interne s'il existe.

Tente l'installation et dis-moi ce que ça donne.

OK, contrainte forte mais contournable. Trois options par ordre de préférence.

## Option 1 — npx (le plus simple, si Node est dispo)

Vérifie dans un terminal :

```bash
node --version
npx --version
```

Si tu as Node, tu peux utiliser un MCP server Atlassian basé npm. Il en existe plusieurs ; le plus actif aujourd'hui est `@aashari/mcp-server-atlassian-confluence` et son équivalent JIRA :

```json
{
  "$schema": "https://opencode.ai/config.json",
  "mcp": {
    "confluence": {
      "type": "local",
      "command": ["npx", "-y", "@aashari/mcp-server-atlassian-confluence"],
      "environment": {
        "ATLASSIAN_SITE_NAME": "confluence.group.echonet",
        "ATLASSIAN_USER_EMAIL": "<ton.email@bnpparibas.com>",
        "ATLASSIAN_API_TOKEN": "<ton-PAT>"
      },
      "enabled": true
    },
    "jira": {
      "type": "local",
      "command": ["npx", "-y", "@aashari/mcp-server-atlassian-jira"],
      "environment": {
        "ATLASSIAN_SITE_NAME": "jira.group.echonet",
        "ATLASSIAN_USER_EMAIL": "<ton.email@bnpparibas.com>",
        "ATLASSIAN_API_TOKEN": "<ton-PAT>"
      },
      "enabled": true
    }
  }
}
```

**Attention** : ces serveurs npm sont surtout testés contre Atlassian Cloud. Sur du Confluence Data Center, ça **peut** marcher (les endpoints REST sont proches) mais ce n'est pas garanti. À tester.

## Option 2 — Python sans uv (`pip install --user`)

Si tu as Python mais pas uv :

```bash
pip install --user mcp-atlassian
```

(le `--user` installe dans ton home, pas besoin d'admin)

Puis trouve où c'est installé :

```bash
python -m mcp_atlassian --help
```

Et la config :

```json
{
  "mcp": {
    "atlassian": {
      "type": "local",
      "command": ["python", "-m", "mcp_atlassian"],
      "environment": {
        "CONFLUENCE_URL": "https://confluence.group.echonet",
        "CONFLUENCE_PERSONAL_TOKEN": "<ton-PAT>",
        "JIRA_URL": "https://jira.group.echonet",
        "JIRA_PERSONAL_TOKEN": "<ton-PAT>"
      },
      "enabled": true
    }
  }
}
```

`mcp-atlassian` (sooperset) supporte explicitement Data Center / on-prem avec PAT — c'est l'option la plus fiable pour ton instance echonet **si tu peux installer Python ou si tu l'as déjà**.

## Option 3 — Demander un proxy MCP corporate

Dans une banque comme BNP, il y a souvent une équipe plateforme qui prépare des connecteurs MCP standards (Confluence/JIRA inclus) déjà hébergés sur un endpoint interne. Avant de t'embêter, pose la question :

- À l'équipe **DevX** (ils sont mentionnés dans la doc CTDF)
- À l'équipe **AIDI08** (vu dans le breadcrumb GitLab de ta capture)

S'il existe un MCP server interne déjà déployé, tu n'as qu'à mettre `"type": "remote"` avec l'URL et le token internes — c'est la solution propre.

## Ordre que je te conseille

1. **Tester `node --version` et `python --version`** dans ton terminal — dis-moi ce que ça donne
2. Si Node → Option 1 (npx, 30 secondes à mettre en place)
3. Si Python → Option 2 (pip install --user)
4. En parallèle, demande à DevX/AIDI08 s'il y a déjà un MCP corporate

Dis-moi ce que tu as côté Node/Python et je t'oriente sur la bonne config.

Bonne nouvelle : ta config est lue (`opencode mcp list` montre bien le serveur `atlassian`). Mauvaise nouvelle : le container ne démarre pas, d'où le `Connection closed`.

Tu es sur `coder@cloudide-j17990` — c'est un **DevX cloud IDE** (Coder). Dans ce type d'environnement, Docker n'est généralement pas dispo (pas de Docker-in-Docker), donc le `docker run` échoue silencieusement et le MCP se ferme aussitôt.

## Diagnostic en 30 secondes

Lance ces deux commandes dans le terminal :

```bash
docker --version
docker ps
```

Trois cas possibles :

**Cas A** — `command not found` → pas de Docker dans DevX → il faut passer à la variante **uvx** (ci-dessous).

**Cas B** — Docker présent mais `permission denied` ou `Cannot connect to the Docker daemon` → pareil, passe à la variante uvx.

**Cas C** — Docker fonctionne → l'image n'a juste pas pu être pull (réseau bloqué vers `ghcr.io` ?). Teste : `docker pull ghcr.io/sooperset/mcp-atlassian:latest`. Si ça échoue, même solution : uvx.

## Solution : variante sans Docker (avec uvx)

`mcp-atlassian` est un package Python, installable directement via `uvx` (le runner d'`uv`, déjà courant dans les environnements DevX modernes). Vérifie d'abord :

```bash
uvx --version
```

Si présent, remplace ta config par ça :

```json
{
  "$schema": "https://opencode.ai/config.json",
  "mcp": {
    "atlassian": {
      "type": "local",
      "command": [
        "uvx",
        "mcp-atlassian"
      ],
      "environment": {
        "CONFLUENCE_URL": "https://confluence.group.echonet",
        "CONFLUENCE_PERSONAL_TOKEN": "TON_PAT_ICI",
        "JIRA_URL": "https://jira.group.echonet",
        "JIRA_PERSONAL_TOKEN": "TON_PAT_ICI"
      },
      "enabled": true
    }
  }
}
```

Différences avec ta version Docker :
- Plus de `docker run` ni de `-e` répétés
- Les variables d'env vont **uniquement** dans le bloc `environment` d'opencode, qui les injecte dans le process enfant
- Premier lancement = uvx télécharge le package (quelques secondes), puis met en cache

## Si uvx n'est pas dispo non plus

Variante avec pipx ou pip direct :

```bash
pip install --user mcp-atlassian
# ou
pipx install mcp-atlassian
```

Puis dans opencode.json :

```json
"command": ["mcp-atlassian"]
```

(en s'assurant que `~/.local/bin` est dans le PATH du shell que lance opencode)

## Re-tester

```bash
opencode mcp list
```

Tu devrais voir `✓ atlassian connected` au lieu de `✗ failed`.

## Vérifier les tokens en parallèle

Pendant que tu débugges, fais un test "à la main" pour valider que tes PAT marchent, indépendamment du MCP :

```bash
curl -H "Authorization: Bearer TON_PAT" \
     https://confluence.group.echonet/rest/api/space
```

Si tu reçois un JSON avec des espaces, le PAT est OK. Si tu reçois `401 Unauthorized`, le problème est sur le token (régénère-le), pas sur opencode.

---

**Plan d'action** dans l'ordre :

1. `docker --version` → me dire le résultat
2. `uvx --version` → me dire le résultat
3. `curl` de validation du PAT → me dire si ça répond 200 ou 401

Dis-moi ce qui sort et on règle ça en une itération.


Le problème vient d'une confusion classique sur où vivent les MCP. Les MCP de Continue ne sont **pas** accessibles via son API : ils s'exécutent dans le process Continue côté client, pas dans le LLM derrière. Quand opencode appelle l'API Continue, il récupère juste les complétions du modèle — sans tool use, sans MCP.

## Pourquoi ça ne marche pas

```
opencode  ──HTTP──►  Continue API  ──►  Anthropic/OpenAI
   │                     │
   │                     └── MCP servers (Confluence, JIRA)  ← attachés ici, pas exposés
   │
   └── ses propres MCP (vide)
```

Les MCP que tu as configurés dans `~/.continue/config.yaml` sont chargés par l'extension VS Code Continue et utilisés **uniquement quand tu chattes dans Continue**. L'API Continue (si elle est utilisée comme proxy LLM) ne relaie pas ces tools à opencode. C'est pour ça qu'opencode te dit qu'il n'a pas de MCP — de son point de vue, c'est vrai.

## La solution : configurer les MCP dans opencode directement

opencode a son propre système de MCP, à déclarer dans sa config (`~/.config/opencode/opencode.json` ou `opencode.json` à la racine du projet).

Exemple minimal pour Atlassian Remote MCP (Cloud) :

```json
{
  "$schema": "https://opencode.ai/config.json",
  "mcp": {
    "atlassian": {
      "type": "local",
      "command": ["npx", "-y", "mcp-remote", "https://mcp.atlassian.com/v1/sse"],
      "enabled": true
    }
  }
}
```

Au premier lancement, `mcp-remote` ouvrira une fenêtre navigateur pour l'OAuth Atlassian. Une fois fait, opencode verra les tools Confluence + JIRA.

## Si ta Confluence est on-premise (echonet)

Pas de Remote MCP officiel possible. Utilise `mcp-atlassian` de sooperset avec un PAT :

```json
{
  "mcp": {
    "atlassian": {
      "type": "local",
      "command": [
        "docker", "run", "-i", "--rm",
        "-e", "CONFLUENCE_URL",
        "-e", "CONFLUENCE_PERSONAL_TOKEN",
        "-e", "JIRA_URL",
        "-e", "JIRA_PERSONAL_TOKEN",
        "ghcr.io/sooperset/mcp-atlassian:latest"
      ],
      "environment": {
        "CONFLUENCE_URL": "https://confluence.group.echonet",
        "CONFLUENCE_PERSONAL_TOKEN": "<ton-PAT>",
        "JIRA_URL": "https://jira.group.echonet",
        "JIRA_PERSONAL_TOKEN": "<ton-PAT>"
      },
      "enabled": true
    }
  }
}
```

## Vérifier que ça marche

Dans opencode, après config :

```
List your available tools.
```

Tu dois voir apparaître des tools du type `mcp__atlassian__*` (createPage, getIssue, etc.). Si oui, tu peux relancer ton prompt Confluence/JIRA.

## À retenir

Règle générale : **les MCP doivent être configurés dans le client qui pilote la conversation** (opencode dans ton cas), pas dans un autre client qu'on utilise comme proxy LLM. Continue et opencode sont deux clients MCP indépendants ; ils ne partagent pas leurs tools.

Tu veux que je vérifie la syntaxe exacte de la config opencode pour ta version, ou tu as accès à la doc en local ?


## Confluence → HTML

```
Using the Atlassian MCP, fetch Confluence page <PAGE_ID> with body in storage format. Save the raw XHTML body to `confluence_<PAGE_ID>.html` (wrap it in <html><body>...</body></html>, keep all <ac:...> macros intact). Print the file path and page title.
```

## JIRA → JSON

```
Using the Atlassian MCP, fetch JIRA issue <ISSUE_KEY> with comments and changelog. Save it as pretty-printed JSON to `jira_<ISSUE_KEY>.json` including: key, summary, description, status, type, priority, assignee, reporter, created, updated, labels, comments, and issuelinks. Print the file path and status.
```

Remplace `<PAGE_ID>` et `<ISSUE_KEY>`.


Voici les deux prompts à utiliser dans Continue.

## Prompt 1 — Dump d'une page Confluence en HTML

```
Use the Atlassian MCP server to fetch the full content of an existing Confluence page and save it locally.

Target page:
- Page ID: <PAGE_ID>
  (or, if you only have the URL: <CONFLUENCE_PAGE_URL> — extract the pageId query param)

Steps:
1. Call the Confluence "get page" tool with the page ID, requesting the body in "storage" format (XHTML). Also include version, space, and ancestors in the expand parameters if supported.
2. From the response, extract:
   - id, title, spaceKey, version.number, lastUpdated, author
   - body.storage.value (the XHTML content)
3. Write the result to a local file named `confluence_<PAGE_ID>.html` in the current workspace using the following structure:

<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <title><!-- page title --></title>
  <!-- Metadata as HTML comments: id, spaceKey, version, lastUpdated, author, sourceUrl -->
</head>
<body>
  <!-- Raw Confluence storage-format XHTML body goes here, untouched -->
</body>
</html>

4. Do NOT clean, reformat, or strip the <ac:...> / <ri:...> macros — keep the storage format intact so the file is a faithful dump.
5. After writing the file, print: the file path, the page title, and the byte size of the body.
```

## Prompt 2 — Dump d'un ticket JIRA en JSON

```
Use the Atlassian MCP server to fetch an existing JIRA issue and save it locally as JSON.

Target issue:
- Issue key: <ISSUE_KEY>   (e.g. "AER-123")

Steps:
1. Call the JIRA "get issue" tool for <ISSUE_KEY>. Request all standard fields plus comments, attachments, issuelinks, subtasks, worklog, and changelog if available (use expand=renderedFields,names,schema,changelog if supported).
2. Build a clean JSON object with this shape:

{
  "key": "...",
  "id": "...",
  "url": "...",
  "summary": "...",
  "description": "...",            // plain text or ADF, whichever the API returns
  "status": "...",
  "issueType": "...",
  "priority": "...",
  "labels": [...],
  "components": [...],
  "fixVersions": [...],
  "assignee": { "displayName": "...", "accountId": "..." },
  "reporter": { "displayName": "...", "accountId": "..." },
  "created": "...",
  "updated": "...",
  "resolution": "...",
  "parent": { "key": "...", "summary": "..." },
  "subtasks": [ { "key": "...", "summary": "...", "status": "..." } ],
  "issuelinks": [ { "type": "...", "direction": "inward|outward", "key": "...", "summary": "..." } ],
  "comments": [
    { "author": "...", "created": "...", "body": "..." }
  ],
  "attachments": [ { "filename": "...", "size": ..., "url": "..." } ],
  "customFields": { /* any non-null custom fields, keyed by their human-readable name when available */ },
  "changelog": [ { "created": "...", "author": "...", "items": [...] } ]
}

3. Write the result to a local file named `jira_<ISSUE_KEY>.json` in the current workspace, pretty-printed with 2-space indentation and UTF-8 encoding.
4. After writing the file, print: the file path, the issue key, the summary, the current status, and the number of comments.

If any field is missing or null in the API response, omit it from the JSON rather than writing null.
```

Remplace `<PAGE_ID>` (ou `<CONFLUENCE_PAGE_URL>`) et `<ISSUE_KEY>` avant de lancer. Si Continue ne parvient pas à écrire le fichier directement, demande-lui simplement d'afficher le contenu et tu fais un copier-coller — mais avec l'outil d'édition de fichiers activé dans Continue, ça doit passer tout seul.


```
You have access to the Atlassian MCP server. Use the Confluence tools to create a new page.

Target:
- Space key: <SPACE_KEY>
- Parent page ID: <PARENT_PAGE_ID> (optional, omit if root)
- Title: "POC AiDPGen — Sales Data Pipeline Design Document"

Body must be valid Confluence storage format (XHTML with ac: macros), NOT Markdown. Include:

1. <h1>1. Context</h1> + a paragraph explaining the purpose of this pipeline.
2. <h1>2. Architecture</h1>
   - <h2>2.1 Components</h2> + a <ul> with 4 bullet points (Source, Mediation, Ingestion, Storage).
   - <h2>2.2 Data Flow</h2> + a <table> with <tbody>, header row using <th> (Step, Source, Target), and 3 data rows.
3. <h1>3. Implementation</h1>
   - <h2>3.1 Airflow DAG</h2> + a code macro:
     <ac:structured-macro ac:name="code"><ac:parameter ac:name="language">python</ac:parameter><ac:plain-text-body><![CDATA[ ... ]]></ac:plain-text-body></ac:structured-macro>
     containing a minimal DAG skeleton.
   - <h2>3.2 Configuration</h2> + a code macro with language=yaml containing a sample config.
4. <h1>4. Testing</h1>
   - <h3>4.1 Unit tests</h3> + short paragraph.
   - <h3>4.2 Integration tests</h3> + short paragraph.
5. End the page with an info macro:
   <ac:structured-macro ac:name="info"><ac:rich-text-body><p>This page was generated by the Agentic Data Pipeline Generator (AiDPGen) POC.</p></ac:rich-text-body></ac:structured-macro>

Steps:
1. First, list available Confluence spaces to confirm <SPACE_KEY> is valid.
2. Build the full storage-format XHTML body.
3. Call the Confluence createPage tool with the title and body.
4. Return the URL of the created page.

If a page with the same title already exists in that space, append " (v2)" to the title instead of failing.
```

Remplace `<SPACE_KEY>` et `<PARENT_PAGE_ID>` avant de lancer.

Parfait, tout est clair. Voici le guide complet pour ton POC.

## Architecture du POC

```
VS Code + Continue
       │
       ├── Prompt utilisateur ("Crée une page Confluence pour…")
       │
       ▼
   Continue (config MCP)
       │
       ▼
Atlassian Remote MCP Server (cloud, OAuth)
       │
       ▼
  Confluence Cloud → page créée avec format riche
```

## Étape 1 — Vérifier les prérequis Confluence

Avant tout : le MCP Remote officiel d'Atlassian **ne fonctionne qu'avec Atlassian Cloud** (pas Data Center/Server). Vérifie que ton instance est bien `*.atlassian.net`. Si ton instance est `confluence.group.echonet` (l'URL des screenshots), c'est probablement on-premise et il faudra basculer sur `mcp-atlassian` (sooperset) en self-hosted. À garder en tête.

## Étape 2 — Configurer Continue avec le MCP Atlassian

Dans VS Code, ouvre la config Continue (`Ctrl+Shift+P` → "Continue: Open config.yaml" ou `~/.continue/config.yaml`).

Ajoute le serveur MCP Atlassian :

```yaml
name: Local Assistant
version: 1.0.0
schema: v1

models:
  - name: Claude Sonnet 4.6
    provider: anthropic
    model: claude-sonnet-4-5
    apiKey: <ta-clé-anthropic>
    roles:
      - chat
      - edit

mcpServers:
  - name: atlassian
    command: npx
    args:
      - -y
      - mcp-remote
      - https://mcp.atlassian.com/v1/sse
```

Pourquoi `mcp-remote` ? Continue (et la plupart des clients MCP) parlent en stdio. Le serveur Atlassian est en SSE distant. `mcp-remote` est un proxy local qui fait le pont **et** gère le flow OAuth automatiquement.

Au premier lancement, une fenêtre navigateur s'ouvre → tu te connectes à Atlassian → tu autorises l'app → token stocké localement. Ensuite Continue voit les outils Atlassian.

## Étape 3 — Comprendre le format Confluence (le piège critique)

C'est là que la plupart des POC échouent. Confluence Cloud n'accepte **pas du Markdown brut** pour les pages. Il faut du **storage format** (XHTML enrichi avec macros `<ac:…>`).

Exemple d'éléments :

```xml
<h1>Titre principal</h1>
<h2>Section</h2>
<h3>Sous-section</h3>

<p>Paragraphe simple avec <strong>gras</strong> et <em>italique</em>.</p>

<ul>
  <li>Item de liste</li>
  <li>Autre item</li>
</ul>

<table>
  <tbody>
    <tr><th>Colonne 1</th><th>Colonne 2</th></tr>
    <tr><td>val1</td><td>val2</td></tr>
  </tbody>
</table>

<ac:structured-macro ac:name="code">
  <ac:parameter ac:name="language">python</ac:parameter>
  <ac:plain-text-body><![CDATA[
def hello():
    print("Hello CTDF")
  ]]></ac:plain-text-body>
</ac:structured-macro>

<ac:structured-macro ac:name="info">
  <ac:rich-text-body>
    <p>Bloc d'information mis en avant.</p>
  </ac:rich-text-body>
</ac:structured-macro>
```

Le MCP Atlassian officiel expose `createPage` / `updatePage` qui acceptent ce format. Tu peux aussi lui passer du markdown et il convertit, mais le résultat est moins prévisible sur les macros complexes (tableaux, code blocks avec syntaxe). **Pour un POC de démo, génère du storage format directement.**

## Étape 4 — Le prompt qui fait le job

Voici un prompt à coller dans Continue. Il est construit pour que Claude génère le bon format du premier coup.

```
Tu es un assistant qui crée des pages Confluence via le MCP Atlassian.

Crée une nouvelle page dans l'espace [SPACE_KEY] avec :
- Titre : "POC Agentic DPGen — Design Document Pipeline Ventes"
- Parent page : [PARENT_PAGE_ID ou laisse vide]

Le contenu doit être au format Confluence storage (XHTML) avec :

1. Un H1 "1. Contexte" suivi d'un paragraphe expliquant l'objectif
2. Un H1 "2. Architecture" avec :
   - Un H2 "2.1 Composants" et une liste à puces de 4 composants
   - Un H2 "2.2 Flux de données" avec un tableau (3 colonnes : Étape, Source, Cible)
3. Un H1 "3. Implémentation" avec :
   - Un H2 "3.1 Code DAG Airflow" suivi d'un bloc de code Python (macro code)
   - Un H2 "3.2 Configuration" suivi d'un bloc de code YAML
4. Un H1 "4. Tests" avec un H3 "4.1 Tests unitaires" et un H3 "4.2 Tests d'intégration"
5. Un panneau info (macro info) en fin de page rappelant que la page est générée par AiDPGen

Utilise les macros Confluence : code (avec ac:parameter language), info, et table avec <tbody>/<th>/<td>.
Génère le storage format complet, puis appelle l'outil createPage du MCP Atlassian.
```

## Étape 5 — Tester progressivement

Je te conseille trois itérations pour limiter les surprises :

**Itération 1 — Sanity check** : prompt minimal "Liste les espaces Confluence auxquels j'ai accès". Si ça répond, le MCP est OK.

**Itération 2 — Page simple** : "Crée une page intitulée Test MCP dans l'espace XXX avec un titre H1 et deux paragraphes". Valide l'écriture.

**Itération 3 — Page complète** : le prompt ci-dessus.

## Étape 6 — Améliorations pour la démo

Pour rendre le POC parlant lors de la présentation :

- **Source de vérité dans la conversation** : avant le prompt de création, demande à Continue de lire d'abord un fichier `pipeline_spec.md` local (la spec brute du dev), puis de la transformer en design doc Confluence structurée. Ça illustre vraiment le rôle "agent" qui traduit du contenu brut en doc formatée.
- **Trace dans JIRA** : ajoute aussi le MCP JIRA et fais commenter le ticket associé avec le lien vers la page créée. Boucle fermée, c'est exactement ton angle "closing the loop".
- **Diagramme Mermaid** : la doc CTDF mentionne que Confluence rend Mermaid (via macro HTML). Demande à l'agent d'inclure un diagramme Mermaid dans la page (flow source → mediation → ingestion → storage). Très visuel pour la démo.

## Points de vigilance

Quelques pièges à anticiper :
- Premier appel = OAuth navigateur ; prévois 2 minutes la première fois.
- L'espace et la page parent doivent exister ; l'agent ne les crée pas automatiquement.
- Les macros mal formées (CDATA, balises non fermées) renvoient une 400 ; si ça plante, demande à Continue de te montrer le payload XHTML avant l'appel.
- Si ta Confluence est en réalité on-premise (`*.echonet`), bascule sur `mcp-atlassian` de sooperset avec un PAT ; la config Continue change un peu (variables d'env `CONFLUENCE_URL`, `CONFLUENCE_PERSONAL_TOKEN`).

Tu veux que je te génère le fichier `pipeline_spec.md` d'exemple (la "source brute" que l'agent transformera en page Confluence) pour la démo ?

Bien reçu, j'ai analysé les 12 images. Voici la synthèse.

## Synthèse — Agentic Data Pipeline Generator (CTDF AiDPGen)

### Contexte global

**CTDF** (Cardif Trusted Data Fabric) est une plateforme de Data Fabric déjà en production chez BNP Paribas Cardif (ASS DATAFABRIC – AP63215, Code Mega AP9792). Elle héberge les Data Pipelines France et International, et tend à devenir la solution de facto pour collecter, stocker et exposer les Data Products dans une orientation **Data Mesh**.

Le projet **Agentic Data Pipeline Generator (AiDPGen)** est un outil "IT 4 IT" interne, à usage **développement uniquement**, qui va remplacer/enrichir les générateurs actuels :
- v1 (Python, il y a 2 ans)
- v2 (configurable, sans IA, basé sur Excel + génération de SQL et de tâches Airflow)
- v3 (nouvelle cible) → **agentique, avec LLM**

### Objectif

Accélérer la construction **end-to-end** des Data Pipelines CTDF, sur tous les jalons du process actuel : specs, création de tickets, développement, contrôle, tests, documentation. **Human-in-the-loop systématique** à chaque étape (BA, tech lead, dev, testeur).

### Acteurs cibles

- **Business & IT Referents** : expriment le besoin
- **Business Analyst** : analyse, rédige les specs
- **Developer** : conçoit et construit le pipeline
- **CTDF Squad** : opère la plateforme, onboarde équipes et pipelines

Tous restent en place ; l'agent les augmente, ne les remplace pas.

### Process Data Pipeline (vu d'oiseau)

Le flow tel que documenté (image 1) :
1. Express the need (Business & IT) → 2. Refine (BA) → 3. Document & inputs (BA)
2. Configure workspace / source access / consumer access (CTDF Admins)
3. Design / Develop / Unit test / Document (Developer)
4. Integration source / consumer / end-to-end / Functional tests (Devs + BA)
5. Packaging → Déploiement INT/QUAL → Recette (BA)
6. **MEP** (APS) → RUN (Business & IT)

Chaque étape est itérative et peut faire intervenir des acteurs ad'hoc.

### Architecture cible de l'AiDPGen (image 3)

L'agent au centre, avec :
- **Confluence MCP Client** ↔ **Confluence MCP Server** (read/write — sert aussi de "mémoire du Use Case")
- **JIRA MCP Client** ↔ **JIRA MCP Server** (read/write — DPS JIRA, lecture/écriture de tickets)
- **SharePoint** (read/write — point encore ouvert, marqué "???")
- **GitLab** (read/write — pour le code généré)
- **CTDF Data Pipeline Design Tools** (tool calling externe)
- Composant **DevX** intégré
- IHM : probablement web, exposée aux Developers / Business Analysts / autres acteurs
- Fichier `Agent.md` mentionné (probablement prompt/spec de l'agent)

### Écosystème technique CTDF (image 2)

Architecture cible des pipelines générés :
- **Orchestration** : Airflow (DAG + Service Account)
- **Ingestion** : API S3 PUT presigned URL, API POST generic event, CFT, ICE
- **Federation** : Starburst, multi-couches Raw / Work / Refined / Optimized Data Storage (catalog, schema, table)
- **Stockage** : Buckets OCS avec paths/subpaths
- **Accès consommateur** : SQL Starburst, API S3 GET presigned URL
- **Gouvernance** : MyAccess (Business Profile, Starburst Schema Policy/UID, access rights), Ranger (Data Access Rights), ROAD (Catalog des Data Products), Market Place
- **Sources** : Source A/B/C/D avec Service Accounts

### Classification & risques

- **Data classification : Internal** (code considéré confidentiel mais hors règles "business confidential")
- **Personal Data : No**
- **AI Act** : pas de pratique interdite, **pas de système à haut risque** (l'agent produit les mêmes artefacts qu'un humain IT, qui les revoit avant les environnements supérieurs INT > QUAL > PROD)
- **Aucune opération hors dev** : pour INT/QUAL/PROD, l'agent ne peut qu'aider à rédiger procédures et tickets

### Inputs / Outputs

- **Inputs** : specs de Data Pipelines (Excel, données test anonymisées en CSV, pages Confluence)
- **Outputs** : code (SQL, Python/Airflow), documentation (diagrammes ArchiMate, Mermaid, PlantUML, UML, BPMN)

### Documentation à générer

- **Diagrammes d'architecture et de conception** des pipelines
- Outils : **ArchiMate** (principal), **Mermaid** (rendu natif Confluence), **PlantUML** (séquences, image insérée), **UML**, **BPMN** (process métier)
- Exemple type ArchiMate fourni : CTDF avec Orchestration DAG → Mediation → Data Ingestion → Data Integration → Raw/Refined/Optimized Data Storage → Consumer

### Gouvernance projet

- **Use Case Owner / IT Integrator / Process Owner** : Arnaud Vigreux (PO Squad DFS — CTDF Product)
- **Model Developer** : N/A (utilise LLMaaS interne)
- **Pays** : France (Corporate), avec cible mondiale

### Roadmap — Step 1

**Step 1 (TODO actuellement)** : intégrer le Data Pipeline Generator v2 existant dans **DevX**, et le connecter à **Confluence, JIRA, GitLab**. C'est le point d'entrée du projet — pas encore d'IA à ce stade, on pose les rails MCP.

---

## Ce que ça implique pour ton POC

Trois choses importantes ressortent pour bien cadrer ton POC :

1. **Step 1 est le point de départ officiel** — donc ton POC devrait précisément démontrer cette intégration : v2 existant + DevX + connecteurs MCP Confluence/JIRA/GitLab. C'est aligné avec la roadmap, ça maximise ton impact "construction sur les points de focus".

2. **Tu n'as pas besoin de réinventer la génération de pipeline** — la v2 existe et marche (Excel → SQL + Airflow). L'angle agentique se met **autour** : lire la spec Confluence, créer/lire/écrire tickets JIRA, déclencher la v2, commiter sur GitLab, documenter en Mermaid/ArchiMate.

3. **Mémoire du Use Case via Confluence** : c'est explicite dans le diagramme — Confluence sert de mémoire persistante de l'agent. À designer comme tel (pas juste lecture, mais écriture structurée de l'état du use case).

Tu veux qu'on cadre maintenant le périmètre précis du POC sur le Step 1, ou tu as encore de la doc à me partager d'abord ?
