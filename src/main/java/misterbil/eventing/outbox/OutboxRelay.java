package misterbil.eventing.outbox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;

/**
 * Relay periodique : scrute l'outbox et publie les evenements dus.
 *
 * <p>Selectionne les evenements {@code PENDING} et {@code RETRYING} dont l'echeance est
 * atteinte, puis delegue chacun a {@link OutboxEventProcessor} (transaction par evenement).
 */
@Component
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final OutboxEventRepository repository;
    private final OutboxEventProcessor processor;
    private final RelayProperties properties;

    public OutboxRelay(OutboxEventRepository repository,
                       OutboxEventProcessor processor,
                       RelayProperties properties) {
        this.repository = repository;
        this.processor = processor;
        this.properties = properties;
    }

    @Scheduled(fixedDelayString = "${eventing.relay.poll-interval-ms:2000}")
    public void poll() {
        List<OutboxEvent> due = repository.findByStatusInAndNextAttemptAtBeforeOrderByCreatedAtAsc(
                List.of(EventStatus.PENDING, EventStatus.RETRYING),
                Instant.now(),
                PageRequest.of(0, properties.batchSize()));

        if (due.isEmpty()) {
            return;
        }
        log.debug("Relay : {} evenement(s) a publier", due.size());
        for (OutboxEvent event : due) {
            processor.process(event.getId());
        }
    }
}
