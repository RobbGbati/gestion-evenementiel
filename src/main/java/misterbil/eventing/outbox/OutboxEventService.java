package misterbil.eventing.outbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Ecriture des evenements dans l'outbox.
 *
 * <p>{@link #record} s'execute avec {@link Propagation#REQUIRED} : appele depuis une
 * transaction metier, l'evenement est persiste dans la <b>meme</b> transaction que la
 * donnee metier. C'est la garantie d'atomicite du pattern Transactional Outbox.
 */
@Service
@RequiredArgsConstructor
public class OutboxEventService {

    private final OutboxEventRepository repository;
    private final ObjectMapper objectMapper;
    private final RelayProperties properties;

    /**
     * Enregistre un evenement a partir d'un objet payload (serialise en JSON).
     */
    @Transactional(propagation = Propagation.REQUIRED)
    public OutboxEvent record(String aggregateType,
                              String aggregateId,
                              String eventType,
                              Object payload) {
        return record(aggregateType, aggregateId, eventType, toJson(payload));
    }

    /**
     * Enregistre un evenement a partir d'un payload deja serialise (JSON brut).
     */
    @Transactional(propagation = Propagation.REQUIRED)
    public OutboxEvent record(String aggregateType,
                              String aggregateId,
                              String eventType,
                              String payloadJson) {
        OutboxEvent event = OutboxEvent.create(
                aggregateType, aggregateId, eventType, payloadJson, properties.maxRetries());
        return repository.save(event);
    }

    private String toJson(Object payload) {
        if (payload == null) {
            return "null";
        }
        if (payload instanceof String s) {
            return s;
        }
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Payload non serialisable en JSON", e);
        }
    }
}
