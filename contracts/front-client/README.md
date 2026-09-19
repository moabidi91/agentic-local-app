# Client HTTP pour le front

Le client `ApiClient` réel du front de bureau (`agentic-front`), branché sur l'API locale de cette application, plus une vérification de bout en bout exécutable.

Le contrat que ce dossier met en œuvre est [`docs/contracts/front-backend-v1.md`](../../docs/contracts/front-backend-v1.md) : les numéros de section cités dans les commentaires du code sont ceux de ce document, et les marqueurs « G-n » sont ses écarts numérotés (§9).

## Contenu

| Fichier | Rôle |
|---|---|
| `HttpApiClient.ts` | **le livrable** — `ApiClient` sur `fetch` + `EventSource`, sans aucune dépendance |
| `smoke.mjs` | vérification du contrat contre un serveur qui tourne, Node seul |
| `tsconfig.json` | de quoi type-checker ce dossier tout seul (`npx tsc -p tsconfig.json`) |
| `ApiClient.ts`, `types.ts` | **copies** des fichiers du front, présentes uniquement pour que le dossier compile isolément — ne jamais les recopier vers `agentic-front`, c'est lui qui en est propriétaire |

## Installer dans `agentic-front`

1. Copier **`HttpApiClient.ts`** dans le dossier qui contient déjà `ApiClient.ts`, `types.ts` et `mock.ts` (`src/api/`). Les imports sont relatifs (`./ApiClient`, `./types`) : il n'y a rien à ajuster.
2. Ne rien copier d'autre. `ApiClient.ts` et `types.ts` de ce dossier sont des copies de référence ; `smoke.mjs` et `tsconfig.json` restent dans le dépôt de l'application.
3. Aucune dépendance à installer : le client n'utilise que `fetch`, `EventSource`, `URLSearchParams` et `setTimeout`.

Prérequis TypeScript : `strict` activé et la bibliothèque `DOM` dans `lib` (c'est la configuration par défaut d'un projet Vite + React). Le fichier compile sous `strict`, `noUncheckedIndexedAccess` et `verbatimModuleSyntax`, et ne contient aucun `any`.

## Ce qu'il faut changer dans `ApiProvider`

Le provider ne connaît qu'une chose : *quelle* implémentation d'`ApiClient` il met dans le contexte. Il y a donc exactement une ligne à changer — celle qui construit le client.

```ts
// src/api/ApiProvider.tsx
import { MockApiClient } from './mock';
import { HttpApiClient } from './HttpApiClient';
import type { ApiClient } from './ApiClient';

// ─── la bascule : une ligne ───────────────────────────────────────────────
const client: ApiClient = new HttpApiClient();          // application réelle
// const client: ApiClient = new MockApiClient();       // maquette en mémoire
// ──────────────────────────────────────────────────────────────────────────
```

Aucun écran ne bouge : ils parlent tous à `ApiClient`, jamais à une implémentation.

Pour ne pas avoir à éditer le fichier à chaque fois, la même bascule pilotée par une variable d'environnement Vite (`VITE_API_BASE_URL` vide ⇒ maquette) :

```ts
const baseUrl = import.meta.env.VITE_API_BASE_URL as string | undefined;
const client: ApiClient = baseUrl
  ? new HttpApiClient(baseUrl)
  : new MockApiClient();
```

`new HttpApiClient()` sans argument vise `http://127.0.0.1:8765/api/v1`, la valeur par défaut de `api.host` / `api.port`.

### Options du constructeur

```ts
new HttpApiClient('http://127.0.0.1:8765/api/v1', {
  fetch: myFetch,                  // injection pour les tests
  eventSource: (url) => new FakeEventSource(url),
  reconnect: { initialDelayMs: 500, maxDelayMs: 15_000, factor: 2, jitter: 0.2, maxAttempts: Infinity },
  pageSize: 200,                   // taille de page demandée aux routes paginées
  openingGoal: 'Console de bureau',        // voir l'écart G-4 ci-dessous
  openingMessage: 'Session ouverte depuis la console de bureau.',
  onStreamGaveUp: (sessionId, error) => { /* le flux live a renoncé */ },
});
```

`fetch` et `eventSource` sont là pour les tests : le client n'a besoin d'aucun serveur pour être testé unitairement.

## Ce que le front devra tout de même ajuster

Le client applique le contrat tel qu'il est ; il ne peut pas inventer ce qui manque. Les points qui demandent une modification **côté front**, chacun détaillé au §9 du contrat :

| Écart | À faire dans `agentic-front` | En attendant |
|---|---|---|
| **G-6** | *fait* : l'union `ConversationStatus` de `types.ts` porte `PAUSED`, `RUNNING` et `INTERRUPTING` | plus rien à élargir : la table de composition du §6.2 est l'identité |
| **G-1** | ajouter `source` et `host` à `WhoAmI` | `client.whoAmIVerbose()` rend les trois champs |
| **G-2** | ajouter `active: boolean` à `ModelOption` | `client.activeModelName()` rend le nom du profil actif |
| **G-4** | **arrêter d'envoyer un message d'ouverture** : `POST /sessions` n'en exige plus (ADR-028), donc `signIn` doit poster `user_id`, `working_space`, `skills` et `effort` **sans** `goal` ni `user_message`, et laisser `sendMessage` porter la première phrase de l'utilisateur | le client envoie encore `openingGoal` / `openingMessage` — **le modèle reçoit donc un message que l'utilisateur n'a pas tapé**. Attention en le retirant : les deux champs sont une paire, en envoyer un seul est refusé en `400 GOAL_REQUIRED` / `USER_MESSAGE_REQUIRED` |
| **G-5** | un budget de requête avec ses trois limites obligatoires | un budget partiel est simplement omis, les valeurs par défaut du poste s'appliquent |
| **G-8** | retirer `ChatMessage.queued` | jamais rempli : l'application refuse en `409 SESSION_BUSY` au lieu de mettre en file |
| **G-11** | ajouter une lecture du fil et un champ pour le type de message | `subscribeMessages` rejoue les tours déjà stockés au moment de l'abonnement |

Trois méthodes en plus de l'interface, utiles aux écrans et sans équivalent dans `ApiClient` : `whoAmIVerbose()`, `activeModelName()`, `setCredentials(token)`, `resume(id)` et `pauseReason(id)` — les deux dernières pour le bandeau de pause (§7.2 du contrat).

## Vérifier le contrat contre un vrai serveur

`smoke.mjs` ne démarre rien : il faut un serveur en marche. Node ≥ 20, aucune dépendance.

**Terminal 1 — le modèle simulé** (la configuration livrée pointe le transport sur `127.0.0.1:9000`) :

```bash
uv run agentic-app mock-server --host 127.0.0.1 --port 9000
```

**Terminal 2 — l'API locale :**

```bash
uv run agentic-app serve
```

**Terminal 3 — la vérification :**

```bash
node contracts/front-client/smoke.mjs
# ou, si l'API écoute ailleurs :
node contracts/front-client/smoke.mjs http://127.0.0.1:9100/api/v1
```

Une ligne par vérification, et un code de sortie non nul dès qu'une seule tombe :

```
ok   01  GET /health — status=200
ok   02  GET /whoami → {user_id, source, host} — user_id=alice source=env:USER
...
ok   12  POST /sessions/{sid}/messages while RUNNING → 409 SESSION_BUSY — 409 SESSION_BUSY
...
30 passed, 0 failed — http://127.0.0.1:8765/api/v1
```

Ce qu'il parcourt : identité, catalogue de modèles et absence de secret, les deux refus de `POST /credentials`, l'enveloppe d'erreur, création de session, `working_space` invalide, `409 SESSION_BUSY` pendant que la boucle tourne, le flux SSE et ses identifiants, la route de pause, l'instantané et ses compteurs de budget, la conversation avec et sans les tours système, un message de suivi accepté, l'interruption et le retour à `READY`, la chaîne d'audit et sa vérification, les trois vues d'administration, et le garde-fou de `POST /admin/reset-database`.

**Il n'envoie et n'affiche jamais de jeton.** La route d'identifiants est éprouvée par ses deux refus (jeton blanc, champ mal orthographié), qui n'écrivent rien côté serveur.

**Le vidage de la base est destructif.** Il n'est appelé que si le serveur déclare `api.allow_destructive_admin = false` — l'appel est alors un `403` inoffensif qui vérifie le garde-fou. Si le réglage est ouvert, la vérification est **sautée** et le dit ; `--allow-reset` la force et vide alors réellement la base.

Options : `--allow-reset`, `--timeout-ms=<ms>` (attente maximale que la session quitte `RUNNING`, 90 s par défaut).

## Type-checker ce dossier

```bash
npx tsc -p contracts/front-client/tsconfig.json
```

Vérifie `HttpApiClient.ts` contre les copies de `ApiClient.ts` et `types.ts`, en mode strict. C'est le garde-fou contre une dérive silencieuse de l'interface : si le front change `types.ts`, remplacer la copie ici et relancer la commande dit immédiatement ce qui casse.
