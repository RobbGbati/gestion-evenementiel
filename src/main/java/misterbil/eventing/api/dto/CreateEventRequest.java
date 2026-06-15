package misterbil.eventing.api.dto;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

/**
 * Requete de creation d'un evenement de test.
 *
 * @param aggregateType type d'agregat (ex: "Commande")
 * @param aggregateId   identifiant de l'agregat
 * @param eventType     type metier de l'evenement
 * @param payload       charge utile JSON libre
 */
public record CreateEventRequest(
        @NotBlank String aggregateType,
        @NotBlank String aggregateId,
        @NotBlank String eventType,
        @NotNull JsonNode payload
) {
}
