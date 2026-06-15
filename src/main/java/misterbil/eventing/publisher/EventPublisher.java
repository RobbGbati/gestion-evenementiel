package misterbil.eventing.publisher;

/**
 * Point d'extension fournisseur : abstraction de la cible de publication.
 *
 * <p>Le reste de l'application (relay, metier) ne depend que de cette interface.
 * Brancher un nouveau fournisseur = fournir une implementation (ex: {@code KafkaEventPublisher})
 * sans rien changer ailleurs.
 *
 * <p>Contrat : {@link #publish(EventMessage)} doit lever une exception si la publication
 * echoue. Le relay s'appuie sur cette exception pour declencher retry / circuit breaker.
 */
public interface EventPublisher {

    /**
     * Publie le message vers la cible.
     *
     * @param message message a publier
     * @throws RuntimeException si la publication echoue (timeout, broker indisponible...)
     */
    void publish(EventMessage message);
}
