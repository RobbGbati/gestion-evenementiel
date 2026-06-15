package misterbil.eventing.demo.event.dto;

import misterbil.eventing.consumer.EventTypeRegistry;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Optional;

/**
 * Registre des evenements du contexte Commande : nom de type metier -> classe concrete.
 *
 * <p>Source unique de verite cote consommateur. Ajouter un evenement = une ligne ici
 * (plus le {@code permits} de {@link DomainEvent} et le handler concerne).
 */
@Component
public class CommandeEventTypes implements EventTypeRegistry {

    private static final Map<String, Class<?>> TYPES = Map.of(
            CommandeCreee.TYPE, CommandeCreee.class,
            CommandeAnnulee.TYPE, CommandeAnnulee.class);

    @Override
    public Optional<Class<?>> resolve(String eventType) {
        return Optional.ofNullable(TYPES.get(eventType));
    }
}
