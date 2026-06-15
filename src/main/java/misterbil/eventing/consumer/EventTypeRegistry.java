package misterbil.eventing.consumer;

import java.util.Optional;

/**
 * Fait le lien entre le nom de type metier ({@code eventType}, une chaine) et la classe Java
 * concrete a utiliser pour deserialiser le payload JSON.
 *
 * <p>C'est le point ou le payload "neutre" de l'outbox redevient un evenement <b>type</b>.
 */
public interface EventTypeRegistry {

    /**
     * @param eventType nom de type metier (ex: {@code "CommandeCreee"})
     * @return la classe cible si connue, sinon vide (type ignore par ce service)
     */
    Optional<Class<?>> resolve(String eventType);
}
