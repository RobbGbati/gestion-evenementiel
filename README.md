# gestion-evenementiel

Socle Spring Boot (projet d'étude) pour un **module de publication d'évènements** résilient et **découplé du fournisseur**. Implémente le pattern **Transactional Outbox + relay** : les évènements sont stockés en base (payload + statut), publiés de façon asynchrone, réessayés en cas d'échec, et rejouables.

## Pourquoi ce design

| Besoin | Réponse |
|---|---|
| Changer de fournisseur sans toucher au métier | Interface `EventPublisher` ; l'impl par défaut (`LoggingEventPublisher`) est remplaçable via `@ConditionalOnMissingBean`. |
| Ne perdre aucun évènement | Écriture de l'évènement dans la **même transaction** que la donnée métier (outbox). |
| Résilience (retry + catch) | Resilience4j : `@Retry` (ratés ponctuels) + `@CircuitBreaker` (fournisseur durablement HS), + backoff exponentiel géré par le relay. |
| Rejouer un évènement | Payload conservé ; `POST /api/events/{id}/replay` le remet en `PENDING`. |
| Suivre le statut | Statuts `PENDING → PUBLISHING → PUBLISHED / RETRYING / DEAD`, audit en base. |

## Architecture

```
métier ──tx──> outbox_event (payload, status, retryCount, nextAttemptAt)
                       │
                 OutboxRelay (@Scheduled poll)
                       │
                 OutboxEventProcessor (1 tx / évènement)
                       │
                 ResilientPublisher (@Retry + @CircuitBreaker)
                       │
                 EventPublisher (interface)
                  ├─ LoggingEventPublisher   (défaut, dev/H2/test)
                  └─ KafkaEventPublisher      (à ajouter)
```

Packages (`misterbil.eventing`) : `outbox` (entité, repo, service, relay, processor), `publisher` (interface + impl + resilience), `replay`, `api`, `demo`.

## Lancer

```bash
mvn spring-boot:run
```

- App : `http://localhost:8080`
- **Swagger UI : `http://localhost:8080/swagger-ui.html`** (tester les endpoints depuis le navigateur)
- Spec OpenAPI : `http://localhost:8080/v3/api-docs`
- Console H2 : `http://localhost:8080/h2-console` (JDBC `jdbc:h2:mem:eventing`, user `sa`, pas de mot de passe)
- Actuator : `http://localhost:8080/actuator/circuitbreakers`, `/actuator/retries`

## Endpoints

| Méthode | URL | Rôle |
|---|---|---|
| `POST` | `/api/events` | Créer un évènement de test |
| `GET` | `/api/events?status=&page=&size=` | Lister (filtrable par statut) |
| `GET` | `/api/events/{id}` | Détail (payload, statut, erreurs…) |
| `POST` | `/api/events/{id}/replay` | Rejouer |
| `POST` | `/api/demo/commandes` | Démo : crée une commande → émet un évènement |

### Exemple

```bash
# Crée une commande -> émet "CommandeCreee" dans l'outbox
curl -X POST localhost:8080/api/demo/commandes \
  -H 'Content-Type: application/json' \
  -d '{"client":"Alice","montant":99.90}'

# ~2s plus tard, l'évènement est PUBLISHED (voir les logs [OUTBOX-PUBLISH])
curl localhost:8080/api/events

# Rejouer
curl -X POST localhost:8080/api/events/<id>/replay
```

## Brancher Kafka (ou autre fournisseur)

1. Ajouter la dépendance (`spring-kafka`).
2. Créer une impl :

```java
@Component
public class KafkaEventPublisher implements EventPublisher {
    public void publish(EventMessage message) {
        kafkaTemplate.send("evenements", message.id().toString(), message.payload());
    }
}
```

3. C'est tout : `@ConditionalOnMissingBean` désactive le `LoggingEventPublisher`. **Relay, métier et API restent inchangés.**

## Configuration (`application.yml`)

- `eventing.relay.*` : `poll-interval-ms`, `batch-size`, `max-retries`, `backoff-base-seconds`.
- `resilience4j.retry.instances.eventPublisher` : tentatives courtes par publication.
- `resilience4j.circuitbreaker.instances.eventPublisher` : seuil/fenêtre d'ouverture du circuit.

## Tests

```bash
mvn test
```

- `OutboxRelayIntegrationTest` (H2) : succès, `RETRYING → DEAD`, rejeu.
- `OutboxEventControllerTest` : API création/consultation/rejeu, 404, démo.
