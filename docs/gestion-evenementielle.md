# Comprendre la gestion évènementielle de ce projet

> Document pédagogique. Objectif : comprendre **ce que fait** ce module, **pourquoi** il est
> construit ainsi, et **quel problème** chaque pièce résout. Aucun prérequis sur les patterns
> distribués — on part du problème concret.

---

## 1. Le problème de départ

Imaginons un service métier banal : créer une commande.

```java
@Transactional
public Commande creer(String client, BigDecimal montant) {
    Commande commande = commandeRepository.save(new Commande(client, montant));
    // ... et maintenant ? Il faut prévenir le reste du système :
    //     « une commande a été créée »
    kafka.send("commandes", evenement); // <-- piège
    return commande;
}
```

Le réflexe naïf : enregistrer la commande en base, puis envoyer un message
(Kafka, RabbitMQ, webhook…). **Ce code a un bug invisible.** Il y a deux systèmes distincts
— la base de données et le broker de messages — et **rien ne garantit qu'ils restent
cohérents**. Trois scénarios qui tournent mal :

1. **Crash entre les deux.** La commande est enregistrée (`commit` SQL réussi), puis le
   service tombe avant `kafka.send`. → La commande existe, mais **personne n'est prévenu**.
   Évènement perdu pour toujours.

2. **Le broker est indisponible.** `kafka.send` lève une exception. Que fait-on ? Si on
   laisse remonter l'exception, la transaction SQL est annulée (rollback) : on **refuse une
   commande valide** juste parce que Kafka est en panne. Si on l'avale, retour au cas 1.

3. **Double envoi.** On envoie le message, mais la réponse réseau se perd. On ne sait pas si
   c'est passé. On renvoie → **doublon**.

Le fond du problème : **on essaie de coordonner deux systèmes qui ne partagent pas la même
transaction.** Une base de données sait être atomique *en son sein* (tout ou rien). Un broker
externe est hors de cette transaction. Le « dual write » (écrire dans deux endroits) n'est
jamais atomique.

---

## 2. L'idée centrale : le pattern *Transactional Outbox*

L'astuce est simple et élégante : **ne pas sortir de la base au moment du métier.**

Au lieu d'envoyer le message tout de suite, on **écrit l'évènement dans une table de la même
base de données**, `outbox_event`, **dans la même transaction** que la donnée métier.

```java
@Transactional
public Commande creer(String client, BigDecimal montant) {
    Commande commande = commandeRepository.save(new Commande(client, montant));
    outboxEventService.record("Commande", commande.getId().toString(),
                              "CommandeCreee", payload);   // même transaction
    return commande;
}
```

Voir [`CommandeService.java`](../src/main/java/misterbil/eventing/demo/CommandeService.java).

Maintenant **les deux écritures sont dans la même transaction SQL.** Donc :
- soit la commande **et** l'évènement sont enregistrés ensemble,
- soit aucun des deux ne l'est (rollback).

Le scénario « commande sans évènement » devient **impossible**. On a remplacé un problème de
coordination entre deux systèmes par une simple garantie d'atomicité interne à la base — ce
que toute base SQL sait faire parfaitement.

Reste une question : *qui envoie réellement le message vers Kafka ?* → un processus séparé et
asynchrone : le **relay**.

```
métier ──tx──> outbox_event (payload, statut, retryCount, nextAttemptAt)
                       │
                 OutboxRelay        (scrute la table périodiquement)
                       │
                 OutboxEventProcessor (1 transaction par évènement)
                       │
                 ResilientPublisher (retry + circuit breaker)
                       │
                 EventPublisher     (interface — Kafka, log, webhook…)
```

---

## 3. Le cycle de vie d'un évènement

Un évènement n'est pas « envoyé ou pas » : il a un **statut** qui raconte son histoire.
Voir [`EventStatus.java`](../src/main/java/misterbil/eventing/outbox/EventStatus.java).

```
   PENDING ──> PUBLISHING ──> PUBLISHED          (chemin heureux)
       ^            │
       │            └──> RETRYING ──> PUBLISHING  (échec, on réessaie plus tard)
       │                    │
   (replay)                 └──> DEAD             (trop d'échecs, on abandonne)
```

| Statut | Sens |
|---|---|
| `PENDING` | Créé, attend d'être pris par le relay. |
| `PUBLISHING` | Le relay l'a pris en charge, envoi en cours. |
| `PUBLISHED` | Envoyé avec succès. Terminé. |
| `RETRYING` | Échec, une nouvelle tentative est **planifiée** (`nextAttemptAt`). |
| `DEAD` | Trop d'échecs (`retryCount >= maxRetries`). Abandon — *dead-letter* logique. |

Ce statut, persisté en base, est ce qui rend le système **observable** (on peut lister les
`DEAD`) et **reprenable** (un `RETRYING` sera retenté, un crash laisse l'évènement dans un
état connu). Les transitions sont portées par l'entité elle-même —
[`OutboxEvent.java`](../src/main/java/misterbil/eventing/outbox/OutboxEvent.java) :
`markPublishing()`, `markPublished()`, `markFailure()`, `resetForReplay()`.

---

## 4. Le relay : qui envoie, et comment il survit aux pannes

### 4.1 La scrutation (polling)

[`OutboxRelay`](../src/main/java/misterbil/eventing/outbox/OutboxRelay.java) tourne en boucle
(`@Scheduled`, toutes les 2 s par défaut). À chaque tick il demande à la base :
*« donne-moi les évènements `PENDING` ou `RETRYING` dont l'échéance (`nextAttemptAt`) est
passée, les plus vieux d'abord, par lots de N. »*

```java
repository.findByStatusInAndNextAttemptAtBeforeOrderByCreatedAtAsc(
        List.of(PENDING, RETRYING), Instant.now(), PageRequest.of(0, batchSize));
```

Puis il délègue **chaque** évènement au processor. Le relay lui-même ne fait aucune logique
métier ni transaction longue : il ne fait que *trouver le travail à faire*.

### 4.2 Une transaction par évènement

Point clé de conception :
[`OutboxEventProcessor.process()`](../src/main/java/misterbil/eventing/outbox/OutboxEventProcessor.java)
est `@Transactional` et traite **un seul** évènement. Pourquoi pas tout le lot dans une
transaction ?

> Parce qu'un échec sur l'évènement n°3 ne doit **pas** annuler la publication des
> évènements 1 et 2. Transactions **courtes et indépendantes** = isolation des pannes.

Le processor :
1. recharge l'évènement, vérifie qu'il n'est pas déjà `PUBLISHED`/`DEAD` (garde-fou),
2. passe en `PUBLISHING`,
3. appelle le publisher,
4. selon le résultat : `markPublished()` ou `markFailure(...)`,
5. sauvegarde le nouveau statut.

### 4.3 Deux niveaux de résilience (et pourquoi deux)

L'envoi réel passe par
[`ResilientPublisher`](../src/main/java/misterbil/eventing/publisher/ResilientPublisher.java),
qui empile **deux** protections Resilience4j — et un **backoff** géré par le relay. Trois
échelles de temps différentes, c'est voulu :

| Mécanisme | Échelle | Rôle |
|---|---|---|
| `@Retry` | millisecondes | Rejoue les **ratés ponctuels** (timeout réseau bref) tout de suite, 3 fois (500 ms, 1 s…). |
| `@CircuitBreaker` | secondes | Si le fournisseur est **durablement HS** (>50 % d'échecs), **ouvre le circuit** : on arrête de le marteler pendant 10 s. Protège le broker *et* nous. |
| Backoff outbox | secondes → minutes | Si tout échoue, l'évènement passe `RETRYING` avec `nextAttemptAt = now + base·2^retryCount`. Le relay le **reprendra plus tard**, espacé. |

L'idée : on essaie vite (retry), on évite de s'acharner sur un système mort (circuit breaker),
et on **persiste l'intention de réessayer** au cas où même ça échoue (backoff + statut en
base). Le retry et le circuit breaker vivent en mémoire ; le backoff survit à un redémarrage
car il est écrit en base.

Le backoff exponentiel (`base · 2^retryCount`) : avec `base=5s` → 5 s, 10 s, 20 s, 40 s, 80 s…
On laisse au fournisseur de plus en plus de temps pour se rétablir, sans abandonner.

> Détail technique utile : `ResilientPublisher` est une **classe distincte** du relay. Les
> annotations Resilience4j passent par un proxy AOP, qui est inopérant sur un appel interne
> à la même classe. D'où la séparation.

---

## 5. Le découplage du fournisseur : changer Kafka pour autre chose sans rien casser

Le relay ne connaît pas Kafka. Il ne connaît qu'une **interface**,
[`EventPublisher`](../src/main/java/misterbil/eventing/publisher/EventPublisher.java) :

```java
public interface EventPublisher {
    void publish(EventMessage message); // doit lever une exception si échec
}
```

Tout le reste du système (relay, processor, métier, API) ne dépend **que** de cette interface.
L'implémentation par défaut,
[`LoggingEventPublisher`](../src/main/java/misterbil/eventing/publisher/LoggingEventPublisher.java),
se contente d'écrire un log — parfait pour le dev et les tests, sans broker à installer.

La bascule se fait via
[`PublisherConfiguration`](../src/main/java/misterbil/eventing/publisher/PublisherConfiguration.java) :

```java
@Bean
@ConditionalOnMissingBean(EventPublisher.class)   // « seulement si personne d'autre n'en fournit un »
public EventPublisher loggingEventPublisher() { return new LoggingEventPublisher(); }
```

Donc pour passer en production avec Kafka, il suffit d'ajouter une classe :

```java
@Component
class KafkaEventPublisher implements EventPublisher {
    public void publish(EventMessage m) {
        kafkaTemplate.send("evenements", m.id().toString(), m.payload());
    }
}
```

`@ConditionalOnMissingBean` désactive automatiquement le logger. **Relay, métier et API : zéro
ligne modifiée.** C'est l'inversion de dépendance — le cœur du système dépend d'une
abstraction, pas d'un détail d'infrastructure.

Le **contrat** est essentiel : `publish` doit **lever une exception** en cas d'échec. C'est ce
signal que le `ResilientPublisher` intercepte pour déclencher retry / circuit breaker. Une
implémentation qui avalerait ses erreurs casserait toute la résilience.

---

## 6. Le rejeu (replay) : pourquoi conserver le payload

Comme le **payload est gardé en base** après publication, on peut **rejouer** un évènement.
[`ReplayService`](../src/main/java/misterbil/eventing/replay/ReplayService.java) est minuscule :

```java
event.resetForReplay();  // statut -> PENDING, retryCount=0, lastError effacé
```

C'est tout : remettre en `PENDING` suffit, le relay le reprendra au prochain tick et le
republiera à l'identique. Utile pour : un consommateur a perdu des messages, un évènement
`DEAD` qu'on veut relancer après avoir réparé le fournisseur, un bug aval corrigé.

L'API l'expose : `POST /api/events/{id}/replay`.

---

## 7. Récapitulatif : quel problème résout chaque pièce

| Pièce | Problème résolu |
|---|---|
| **Outbox + même transaction** (`OutboxEventService`, `@Transactional`) | L'impossibilité d'écrire atomiquement dans la base **et** dans un broker. Plus de perte ni d'incohérence métier/évènement. |
| **Statut + payload persistés** (`OutboxEvent`, `EventStatus`) | Observabilité et reprise après crash : l'état est en base, pas en mémoire volatile. |
| **Relay asynchrone** (`OutboxRelay`) | Découple le temps métier (rapide, synchrone) de l'envoi (lent, faillible). Le client n'attend pas Kafka. |
| **1 transaction / évènement** (`OutboxEventProcessor`) | Isolation des pannes : un évènement raté n'entraîne pas les autres. |
| **Retry + Circuit Breaker + backoff** (`ResilientPublisher`) | Pannes transitoires (retry), pannes durables (circuit breaker), persistance de l'intention de réessayer (backoff en base). |
| **Interface `EventPublisher` + `@ConditionalOnMissingBean`** | Verrouillage à un fournisseur : on change Kafka → RabbitMQ → webhook sans toucher au cœur. |
| **Replay** (`ReplayService`) | Réémettre un évènement (perte aval, `DEAD` réparé) sans recréer la donnée métier. |
| **Verrou optimiste** (`@Version` sur `OutboxEvent`) | Double-publication si plusieurs instances du relay tournent en parallèle. |

---

## 8. Voir tourner

```bash
mvn spring-boot:run
```

```bash
# 1. Crée une commande -> écrit "CommandeCreee" dans l'outbox (statut PENDING)
curl -X POST localhost:8080/api/demo/commandes \
  -H 'Content-Type: application/json' -d '{"client":"Alice","montant":99.90}'

# 2. ~2 s plus tard, le relay l'a publié -> log [OUTBOX-PUBLISH], statut PUBLISHED
curl localhost:8080/api/events

# 3. Rejouer
curl -X POST localhost:8080/api/events/<id>/replay
```

- Swagger UI : <http://localhost:8080/swagger-ui.html>
- Console H2 (inspecter `OUTBOX_EVENT` en direct) : <http://localhost:8080/h2-console>
  (`jdbc:h2:mem:eventing`, user `sa`, sans mot de passe)
- État des circuits : <http://localhost:8080/actuator/circuitbreakers>

### Paramètres (`application.yml`)

| Clé | Effet |
|---|---|
| `eventing.relay.poll-interval-ms` | Fréquence de scrutation de l'outbox (2000 ms). |
| `eventing.relay.batch-size` | Nombre d'évènements traités par tick (50). |
| `eventing.relay.max-retries` | Au-delà → statut `DEAD` (5). |
| `eventing.relay.backoff-base-seconds` | Base du backoff exponentiel entre reprises (5 s). |
| `resilience4j.retry.instances.eventPublisher` | Tentatives **courtes** par publication (intra-tick). |
| `resilience4j.circuitbreaker.instances.eventPublisher` | Seuil/fenêtre d'ouverture du circuit. |

---

## 9. Exploiter les évènements : projection (read model) + évènements typés

Jusqu'ici la chaîne s'arrêtait à un log. Pour *exploiter* les évènements on a ajouté un
**consommateur** qui les transforme en **vue de lecture** (read model). Cela illustre le vrai
intérêt de l'évènementiel : **découpler le métier des calculs aval**.

### 9.1 Évènements typés (sealed interface)

Plutôt qu'un `Map` anonyme, chaque évènement est un **type** fermé
([`DomainEvent`](../src/main/java/misterbil/eventing/demo/DomainEvent.java)) :

```java
public sealed interface DomainEvent permits CommandeCreee, CommandeAnnulee {
    String type();
}
public record CommandeCreee(String commandeId, String client, BigDecimal montant) implements DomainEvent { ... }
public record CommandeAnnulee(String commandeId, String client, BigDecimal montant, String raison) implements DomainEvent { ... }
```

`sealed` = la liste des évènements possibles est **fermée et connue à la compilation**. Bénéfice
concret : un `switch` dessus est **exhaustif**, le compilateur **refuse de compiler** si un cas
n'est pas traité. Ajouter un évènement signale aussitôt les handlers à mettre à jour.

L'outbox, elle, reste neutre : le payload est toujours du JSON. Le typage n'intervient qu'à la
(dé)sérialisation, via un registre nom → classe
([`EventTypeRegistry`](../src/main/java/misterbil/eventing/consumer/EventTypeRegistry.java),
impl [`CommandeEventTypes`](../src/main/java/misterbil/eventing/demo/CommandeEventTypes.java)).

### 9.2 Un seul handler pour plusieurs types d'évènement

Oui, c'est possible et recommandé quand **plusieurs évènements alimentent la même projection**.
Le handler déclare l'ensemble des types qu'il gère, puis aiguille par pattern matching —
[`CommandeProjectionHandler`](../src/main/java/misterbil/eventing/demo/CommandeProjectionHandler.java) :

```java
@Override public Set<String> handledTypes() {
    return Set.of(CommandeCreee.TYPE, CommandeAnnulee.TYPE);
}

@Override @Transactional public void handle(Object event) {
    if (!(event instanceof DomainEvent de)) return;
    switch (de) {                                  // exhaustif car DomainEvent est sealed
        case CommandeCreee c  -> /* +1, montant += */ ;
        case CommandeAnnulee a -> /* +1 annulée, montant -= */ ;
    }
}
```

Deux écoles, selon le cas :
- **Un handler par type** : responsabilité unique, idéal si les types n'ont rien à voir.
- **Un handler multi-types** (ici) : quand les évènements touchent **la même donnée dérivée**
  (les stats du client) — on garde la cohérence de la projection en un seul endroit.

### 9.3 Comment ça se branche (sans toucher au relay)

```
OutboxEventProcessor ─> ResilientPublisher ─> EventPublisher
                                               └─ DispatchingEventPublisher   (← nouveau défaut)
                                                     │ resolve(eventType) -> Class
                                                     │ JSON -> objet typé
                                                     ▼
                                                EventHandler(s) filtrés par type
                                                     └─ CommandeProjectionHandler
                                                            └─ CommandeStats (table read model)
```

[`DispatchingEventPublisher`](../src/main/java/misterbil/eventing/consumer/DispatchingEventPublisher.java)
est une simple implémentation d'`EventPublisher`. Grâce à `@ConditionalOnMissingBean`, il
**remplace** le `LoggingEventPublisher` — **relay, processor et métier inchangés**. C'est le
même point d'extension que pour brancher Kafka (section 5), mais utilisé pour **consommer en
local** au lieu d'expédier dehors.

Point clé : un handler qui échoue **lève une exception** → elle remonte au `ResilientPublisher`
→ retry, puis `RETRYING` dans l'outbox. La projection ne se désynchronise donc pas en silence.

### 9.4 Voir tourner

```bash
# Crée 2 commandes pour Alice
curl -X POST localhost:8080/api/demo/commandes -H 'Content-Type: application/json' -d '{"client":"Alice","montant":100}'
curl -X POST localhost:8080/api/demo/commandes -H 'Content-Type: application/json' -d '{"client":"Alice","montant":50}'

# ~2 s plus tard, la projection est alimentée par le relay
curl localhost:8080/api/projections/commandes
# -> [{"client":"Alice","nbCreees":2,"nbAnnulees":0,"montantNet":150,...}]

# Annuler une commande (émet CommandeAnnulee, géré par le MÊME handler)
curl -X POST localhost:8080/api/demo/commandes/<id>/annuler -H 'Content-Type: application/json' -d '{"raison":"client absent"}'
curl localhost:8080/api/projections/commandes
# -> nbAnnulees:1, montantNet décrémenté
```

Couvert par
[`CommandeProjectionIntegrationTest`](../src/test/java/misterbil/eventing/demo/CommandeProjectionIntegrationTest.java)
(création projetée + un seul handler gérant création **et** annulation + rejeu sans double-comptage).

### 9.5 Idempotence : ne pas double-compter sur un rejeu

La projection incrémente des compteurs : **non idempotente** par nature. Or la livraison est
« au moins une fois » (un rejeu, ou une reprise après crash, peut redélivrer le **même**
évènement). Sans garde, un `CommandeCreee` reçu deux fois compterait deux commandes.

Parade : une table de déduplication
([`ProcessedEvent`](../src/main/java/misterbil/eventing/consumer/ProcessedEvent.java),
clé composite **consommateur + id d'évènement**). Le handler, **dans la même transaction** que
la projection :

```java
ProcessedEventId key = new ProcessedEventId(CONSUMER, eventId);
if (processedRepository.existsById(key)) return;   // doublon -> on s'abstient
// ... applique la projection ...
processedRepository.save(new ProcessedEvent(key));  // marque traité
```

Pourquoi l'`eventId` est la bonne clé : il est **stable au rejeu** (le replay republie le même
id). Pourquoi la **même transaction** : si le crash survient entre la projection et le marquage,
les deux sont annulés ensemble → l'évènement repassera et sera traité proprement une fois. La
clé primaire composite donne en plus une garantie au niveau base contre deux instances
concurrentes.

> À retenir : le système ne peut garantir « exactement une fois » à la *livraison* — on
> l'obtient « exactement une fois » à l'*effet*, côté consommateur, par l'idempotence.

> **Note de conception.** Ici producteur et consommateur tournent dans le **même processus**
> (suffisant pour un projet d'étude sur H2). Dans un vrai système distribué, le consommateur
> serait un service séparé lisant un broker (`@KafkaListener`). Le découpage du code
> (`EventHandler`, registre typé, projection) resterait identique — seul le transport
> changerait.

## 10. Limites et pistes (projet d'étude)

- **Stockage H2 en mémoire** : tout disparaît au redémarrage. Une vraie base (Postgres)
  rendrait la durabilité réelle.
- **Pas de nettoyage** des `PUBLISHED` : en prod, une purge/archivage périodique éviterait
  que `outbox_event` enfle.
- **Polling** : simple et robuste, mais latence ≈ intervalle de poll. Une approche CDC
  (Debezium lisant le journal de transactions) supprimerait ce délai pour de gros volumes.
- **« Au moins une fois »** : ce pattern garantit la livraison *au moins une fois*, donc des
  doublons sont possibles (ex. crash après envoi, avant `markPublished`). Les consommateurs
  doivent être **idempotents** (l'`id` d'évènement sert de clé de déduplication).
- **Déduplication** (résolue, §9.5) : la table `processed_event` n'est jamais purgée — en prod
  elle grossit indéfiniment ; il faudrait une rétention (TTL) au-delà de la fenêtre de rejeu
  plausible. Alternative pour certaines projections : les rendre intrinsèquement idempotentes
  (affectation d'état plutôt qu'incrément) et se passer de table.
