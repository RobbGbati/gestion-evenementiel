# Ajouter un nouvel évènement métier (template)

> Document pratique. Objectif : suivre, étape par étape, **comment ajouter un nouveau type
> d'évènement** au projet et le brancher au cœur évènementiel — de la définition du type jusqu'au
> test. Le fil rouge est l'exemple **Paiement** (`PaiementEffectue`).
>
> Pour comprendre *pourquoi* le pattern existe (Transactional Outbox, relay, résilience), lire
> d'abord [gestion-evenementielle.md](gestion-evenementielle.md). Ici on suppose ces bases acquises.

## Le cadre en une minute

Dans ce projet, un « évènement » est un fait métier **passé** (« une commande a été créée »,
« un paiement a été effectué »). Concrètement c'est :

- un `record` Java qui implémente l'interface scellée `DomainEvent` ;
- sérialisé en **JSON** et rangé dans la table outbox (`OutboxEvent`), dans la **même transaction**
  que la donnée métier — d'où l'atomicité ;
- publié de façon **asynchrone** par le relay, puis consommé par des **handlers** (projections,
  envois externes…).

Le flux d'un nouvel évènement traverse toujours les mêmes pièces :

```
  Service métier
     │  outboxEventService.record("Commande", id, type, event)
     ▼
  [ outbox : OutboxEvent (PENDING) ]
     │  OutboxRelay (@Scheduled) → OutboxEventProcessor
     ▼
  EventPublisher → DispatchingEventPublisher
     │  EventTypeRegistry.resolve("PaiementEffectue") → désérialise le JSON
     ▼
  EventHandler.handle(eventId, event)   ← projection / effet métier (idempotent)
```

Ajouter un évènement = **brancher un nouveau type dans cette chaîne**. Les étapes 1 à 4 sont
obligatoires (l'évènement existe et part dans l'outbox) ; l'étape 5 le rend *utile* (quelqu'un le
consomme) ; les étapes 6 et 7 l'exposent et le verrouillent.

---

## 1. Créer le record d'évènement

Un évènement est un `record` **auto-suffisant** : il porte tout ce qu'un consommateur doit savoir
pour agir, sans relire la base métier. Il déclare une constante `TYPE` (le nom métier, aligné avec
`OutboxEvent.eventType`) et implémente `type()`.

Modèle : [CommandeCreee.java](../src/main/java/misterbil/eventing/demo/event/dto/CommandeCreee.java).

Nouveau fichier `src/main/java/misterbil/eventing/demo/event/dto/PaiementEffectue.java` :

```java
package misterbil.eventing.demo.event.dto;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Un paiement a ete effectue pour une commande. Evenement auto-suffisant : porte le client,
 * le montant et la methode, pour qu'un consommateur agisse sans relire la base metier.
 */
public record PaiementEffectue(
        String commandeId,
        String client,
        BigDecimal montant,
        String methode,        // ex: "CARTE", "VIREMENT"
        Instant datePaiement)
        implements DomainEvent {

    public static final String TYPE = "PaiementEffectue";

    @Override
    public String type() {
        return TYPE;
    }
}
```

**Convention de nommage** : CamelCase au **passé composé** (`CommandeCreee`, `CommandeAnnulee`,
`PaiementEffectue`). Le `TYPE` reprend le nom de la classe.

---

## 2. Sceller l'interface `DomainEvent`

`DomainEvent` est `sealed` : la liste des évènements possibles est **fermée et connue à la
compilation**. Ajouter le nouveau type au `permits`.

Fichier : [DomainEvent.java](../src/main/java/misterbil/eventing/demo/event/dto/DomainEvent.java).

```java
public sealed interface DomainEvent
        permits CommandeCreee, CommandeAnnulee, PaiementEffectue {   // ← AJOUT

    String type();
}
```

**Pourquoi c'est précieux.** Tout `switch` exhaustif sur un `DomainEvent` (cf. étape 5) **ne
compilera plus** tant que le nouveau cas n'est pas traité. Le compilateur vous indique lui-même
tous les handlers à mettre à jour : impossible d'oublier silencieusement un consommateur.

---

## 3. Enregistrer le type dans le registre

Côté consommateur, le payload outbox n'est que du JSON neutre. Le **registre** fait le lien
`"PaiementEffectue"` (string stockée dans `eventType`) → `PaiementEffectue.class`, pour que le
dispatcher sache en quoi désérialiser le JSON.

Fichier : [CommandeEventTypes.java](../src/main/java/misterbil/eventing/demo/event/dto/CommandeEventTypes.java).

```java
private static final Map<String, Class<?>> TYPES = Map.of(
        CommandeCreee.TYPE, CommandeCreee.class,
        CommandeAnnulee.TYPE, CommandeAnnulee.class,
        PaiementEffectue.TYPE, PaiementEffectue.class);   // ← AJOUT
```

> ⚠️ Oublier cette ligne : l'évènement partira bien dans l'outbox, mais le dispatcher ne saura pas
> le désérialiser → aucun handler ne sera appelé. C'est l'oubli le plus courant.

---

## 4. Émettre l'évènement depuis le service métier

Le cœur du pattern : la donnée métier **et** l'évènement sont persistés dans la **même
transaction** (`@Transactional`). Soit les deux sont validés, soit aucun.

L'émission passe toujours par `OutboxEventService.record(...)` — 4 arguments, le payload est
sérialisé en JSON automatiquement :

```java
public OutboxEvent record(String aggregateType,   // "Commande"
                          String aggregateId,      // id de l'agregat concerne
                          String eventType,        // event.type()
                          Object payload)          // le record, serialise en JSON
```

Modèle : [CommandeService.java](../src/main/java/misterbil/eventing/demo/CommandeService.java)
(méthodes `creer` / `annuler`). Ajouter une méthode dans le service :

```java
@Transactional
public void enregistrerPaiement(UUID commandeId, String methode) {
    Commande commande = commandeRepository.findById(commandeId)
            .orElseThrow(() -> new NoSuchElementException("Commande introuvable : " + commandeId));

    PaiementEffectue event = new PaiementEffectue(
            commande.getId().toString(),
            commande.getClient(),
            commande.getMontant(),
            methode,
            Instant.now());

    // Meme transaction que la lecture/modification metier : atomicite garantie.
    outboxEventService.record("Commande", commande.getId().toString(), event.type(), event);
}
```

`aggregateType` regroupe les évènements d'un même contexte (ici toujours `"Commande"`) ;
`aggregateId` identifie l'instance concernée (utile pour tracer/rejouer par agrégat).

---

## 5. Consommer l'évènement (handler / projection)

Émettre ne suffit pas : il faut quelqu'un qui **réagit**. Un handler implémente `EventHandler`,
déclare les types qu'il traite via `handledTypes()`, et reçoit chaque évènement dans `handle(...)`.
Deux options.

### Option A — Étendre le handler existant

Si le paiement doit alimenter le **même read model** que les commandes, on ajoute le type et un
`case` au switch de
[CommandeProjectionHandler.java](../src/main/java/misterbil/eventing/demo/event/handler/CommandeProjectionHandler.java) :

```java
@Override
public Set<String> handledTypes() {
    return Set.of(CommandeCreee.TYPE, CommandeAnnulee.TYPE, PaiementEffectue.TYPE);  // ← AJOUT
}

// ... dans handle(...), le switch sur le scelle :
switch (domainEvent) {
    case CommandeCreee c   -> { /* existant */ }
    case CommandeAnnulee a -> { /* existant */ }
    case PaiementEffectue p -> {                       // ← AJOUT (sinon : ne compile pas)
        CommandeStats stats = statsFor(p.client());
        stats.enregistrerPaiement(montant(p.montant()));   // methode a ajouter sur CommandeStats
        statsRepository.save(stats);
    }
}
```

C'est ici que la sécurité du `sealed` joue : tant que le `case PaiementEffectue` manque, le fichier
ne compile pas.

### Option B — Créer un handler dédié

Si le paiement a sa **propre logique** (historique, notification…), créer un handler indépendant
avec son **propre identifiant de consommateur** (`CONSUMER`) :

```java
@Component
@RequiredArgsConstructor
public class PaiementHandler implements EventHandler {

    private static final String CONSUMER = "paiement-handler";   // identifie ce consommateur
    private final ProcessedEventRepository processedRepository;
    // ... vos dependances (repository d'historique, service de notif, etc.)

    @Override
    public Set<String> handledTypes() {
        return Set.of(PaiementEffectue.TYPE);
    }

    @Override
    @Transactional
    public void handle(UUID eventId, Object event) {
        if (!(event instanceof PaiementEffectue p)) {
            return;
        }
        // Garde d'idempotence : ce consommateur a-t-il deja traite cet evenement ?
        ProcessedEventId dedupKey = new ProcessedEventId(CONSUMER, eventId);
        if (processedRepository.existsById(dedupKey)) {
            return;
        }

        // ... effet metier (enregistrer l'historique, notifier, etc.)

        processedRepository.save(new ProcessedEvent(dedupKey));  // marque traite, meme transaction
    }
}
```

> **Idempotence — non négociable.** Le relay peut rejouer un évènement (panne, retry). Un handler
> à effet non rejouable (incrémenter un compteur, débiter…) **doit** dédupliquer sur l'`eventId`
> via `ProcessedEvent`, dans la **même transaction** que son effet. Chaque consommateur a son
> propre `CONSUMER` : un même évènement est ainsi traité **une fois par handler**.

---

## 6. (Optionnel) Exposer un endpoint REST

Pour déclencher le paiement depuis l'extérieur, un contrôleur mince qui délègue au service. DTO
en `record` avec validation Jakarta (`@NotBlank`, `@NotNull`…), annotations `@Tag`/`@Operation`
pour Swagger.

Modèle : [CommandeController.java](../src/main/java/misterbil/eventing/demo/CommandeController.java).

```java
@RestController
@RequestMapping("/api/demo/paiements")
@Tag(name = "Demo", description = "Enregistrer un paiement emet un evenement PaiementEffectue")
public class PaiementController {

    private final CommandeService commandeService;

    public PaiementController(CommandeService commandeService) {
        this.commandeService = commandeService;
    }

    @Operation(summary = "Enregistrer un paiement (declenche un evenement PaiementEffectue)")
    @PostMapping("/{commandeId}")
    public ResponseEntity<Void> enregistrer(@PathVariable UUID commandeId,
                                            @Valid @RequestBody PaiementRequest request) {
        commandeService.enregistrerPaiement(commandeId, request.methode());
        return ResponseEntity.noContent().build();
    }

    public record PaiementRequest(@NotBlank String methode) {
    }
}
```

La validation rejette automatiquement les requêtes invalides (`400`) via `ApiExceptionHandler`
(réponse au format `ProblemDetail`, RFC 7807).

---

## 7. Tester

Un test d'intégration `@SpringBootTest` vérifie le bout du tunnel : on enregistre l'évènement, on
force le traitement (`processor.process(id)`), on assert sur l'effet (projection / historique).

Modèle : [CommandeProjectionIntegrationTest.java](../src/test/java/misterbil/eventing/demo/CommandeProjectionIntegrationTest.java).

```java
@Test
void paiement_alimente_la_projection() {
    PaiementEffectue event = new PaiementEffectue(
            "cmd-1", "Alice", new BigDecimal("99.90"), "CARTE", Instant.now());

    OutboxEvent stored = eventService.record("Commande", "cmd-1", PaiementEffectue.TYPE, event);
    processor.process(stored.getId());

    CommandeStats stats = statsRepository.findById("Alice").orElseThrow();
    // assertions sur l'effet attendu...
}

@Test
void rejouer_un_paiement_ne_double_compte_pas() {
    OutboxEvent stored = eventService.record("Commande", "cmd-1", PaiementEffectue.TYPE, event);
    processor.process(stored.getId());

    replayService.replay(stored.getId());   // remet PENDING
    processor.process(stored.getId());      // rejoue

    // l'effet ne doit PAS etre applique deux fois (idempotence)
}
```

Lancer : `mvn test`.

### Vérifier à la main (`mvn spring-boot:run`)

```bash
# créer une commande puis enregistrer son paiement
curl -X POST http://localhost:8080/api/demo/commandes \
  -H 'Content-Type: application/json' -d '{"client":"Alice","montant":99.90}'
# → récupérer l'id retourné, puis :
curl -X POST http://localhost:8080/api/demo/paiements/<id> \
  -H 'Content-Type: application/json' -d '{"methode":"CARTE"}'

# l'évènement apparaît dans l'outbox
curl 'http://localhost:8080/api/events?status=PUBLISHED'
```

Inspection possible aussi via la console H2 (`http://localhost:8080/h2-console`) et Swagger
(`http://localhost:8080/swagger-ui.html`).

---

## Checklist récap

| # | Étape | Fichier | Obligatoire |
|---|-------|---------|:-----------:|
| 1 | Créer le record `implements DomainEvent` | `demo/event/dto/PaiementEffectue.java` (nouveau) | ✅ |
| 2 | Ajouter au `permits` du scellé | `demo/event/dto/DomainEvent.java` | ✅ |
| 3 | Enregistrer dans la map `TYPES` | `demo/event/dto/CommandeEventTypes.java` | ✅ |
| 4 | Émettre via `outboxEventService.record(...)` | `demo/CommandeService.java` | ✅ |
| 5 | Consommer (case au switch **ou** handler dédié) | `...handler/CommandeProjectionHandler.java` **ou** nouveau handler | ✅ pour servir à qqch |
| 6 | Exposer un endpoint REST | nouveau `@RestController` | ⬜ optionnel |
| 7 | Tester (intégration + idempotence) | `src/test/.../*IntegrationTest.java` | ✅ |

> Règle d'or : les étapes **1→2→3** vont toujours ensemble. Si le compilateur se plaint d'un
> `switch` non exhaustif après l'étape 2, c'est normal — il vous montre exactement le handler de
> l'étape 5 à compléter.
