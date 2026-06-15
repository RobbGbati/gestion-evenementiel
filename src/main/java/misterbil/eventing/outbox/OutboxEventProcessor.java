package misterbil.eventing.outbox;

import misterbil.eventing.publisher.EventMessage;
import misterbil.eventing.publisher.ResilientPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.UUID;

/**
 * Traite un evenement de l'outbox, chacun dans sa <b>propre</b> transaction.
 *
 * <p>Isole du relay : un echec sur un evenement n'annule pas le traitement des autres
 * du meme lot (transactions courtes et independantes).
 */
@Component
public class OutboxEventProcessor {

    private static final Logger log = LoggerFactory.getLogger(OutboxEventProcessor.class);

    private final OutboxEventRepository repository;
    private final ResilientPublisher resilientPublisher;
    private final RelayProperties properties;

    public OutboxEventProcessor(OutboxEventRepository repository,
                                ResilientPublisher resilientPublisher,
                                RelayProperties properties) {
        this.repository = repository;
        this.resilientPublisher = resilientPublisher;
        this.properties = properties;
    }

    /**
     * Publie un evenement et met a jour son statut.
     */
    @Transactional
    public void process(UUID eventId) {
        OutboxEvent event = repository.findById(eventId).orElse(null);
        if (event == null) {
            return;
        }
        // Garde-fou : un autre noeud a peut-etre deja traite l'evenement.
        if (event.getStatus() == EventStatus.PUBLISHED || event.getStatus() == EventStatus.DEAD) {
            return;
        }

        event.markPublishing();
        EventMessage message = toMessage(event);
        try {
            resilientPublisher.publish(message);
            event.markPublished();
            log.debug("Evenement {} publie", eventId);
        } catch (Exception e) {
            Instant nextAttempt = computeNextAttempt(event.getRetryCount());
            event.markFailure(describe(e), nextAttempt);
            log.warn("Echec publication evenement {} (tentative {}/{}) -> {} : {}",
                    eventId, event.getRetryCount(), event.getMaxRetries(),
                    event.getStatus(), describe(e));
        }
        repository.save(event);
    }

    private EventMessage toMessage(OutboxEvent event) {
        return new EventMessage(
                event.getId(),
                event.getAggregateType(),
                event.getAggregateId(),
                event.getEventType(),
                event.getPayload(),
                Map.of("retryCount", String.valueOf(event.getRetryCount())));
    }

    /** Backoff exponentiel : base * 2^retryCount secondes. */
    private Instant computeNextAttempt(int retryCount) {
        long delaySeconds = properties.backoffBaseSeconds() * (long) Math.pow(2, retryCount);
        return Instant.now().plus(delaySeconds, ChronoUnit.SECONDS);
    }

    private String describe(Exception e) {
        return e.getClass().getSimpleName() + ": " + e.getMessage();
    }
}
