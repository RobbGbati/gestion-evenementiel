package misterbil.eventing.publisher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation par defaut : journalise simplement le message.
 *
 * <p>Declaree via {@link PublisherConfiguration} avec {@code @ConditionalOnMissingBean} :
 * elle n'est utilisee que si aucun autre {@link EventPublisher} n'existe. Ajouter par
 * exemple un {@code KafkaEventPublisher} la remplace, sans toucher au relay ni au metier.
 */
public class LoggingEventPublisher implements EventPublisher {

    private static final Logger log = LoggerFactory.getLogger(LoggingEventPublisher.class);

    @Override
    public void publish(EventMessage message) {
        log.info("[OUTBOX-PUBLISH] type={} aggregate={}#{} id={} payload={}",
                message.eventType(),
                message.aggregateType(),
                message.aggregateId(),
                message.id(),
                message.payload());
    }
}
