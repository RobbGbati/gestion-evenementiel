package misterbil.eventing.publisher;

import java.util.Map;
import java.util.UUID;

/**
 * Message neutre transmis au fournisseur de publication.
 *
 * <p>Volontairement decouple de l'entite de persistance {@code OutboxEvent} : le
 * fournisseur (log, Kafka, RabbitMQ...) ne connait que ce contrat, jamais la base.
 *
 * @param id          identifiant unique de l'evenement (= id outbox)
 * @param aggregateType type d'agregat source (ex: "Commande")
 * @param aggregateId   identifiant de l'agregat source
 * @param eventType   type metier de l'evenement (ex: "CommandeCreee")
 * @param payload     charge utile serialisee en JSON
 * @param headers     metadonnees libres (cle/valeur)
 */
public record EventMessage(
        UUID id,
        String aggregateType,
        String aggregateId,
        String eventType,
        String payload,
        Map<String, String> headers
) {
}
