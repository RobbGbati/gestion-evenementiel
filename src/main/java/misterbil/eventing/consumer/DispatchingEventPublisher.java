package misterbil.eventing.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import misterbil.eventing.publisher.EventMessage;
import misterbil.eventing.publisher.EventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Implementation d'{@link EventPublisher} qui <b>consomme</b> l'evenement en local : elle le
 * retype puis le route vers les {@link EventHandler} interesses (ex: projections / read models).
 *
 * <p>C'est ici que l'outbox cesse d'etre un simple "tuyau de sortie" : au lieu d'expedier vers
 * un broker, on exploite l'evenement dans l'application meme. Comme c'est un {@code @Component}
 * de type {@link EventPublisher}, il remplace le {@code LoggingEventPublisher} par defaut
 * (via {@code @ConditionalOnMissingBean}), sans toucher au relay.
 *
 * <p>Resilience preservee : toute exception d'un handler remonte, donc le
 * {@code ResilientPublisher} reessaie et l'outbox repasse l'evenement en {@code RETRYING}.
 */
@Component
public class DispatchingEventPublisher implements EventPublisher {

    private static final Logger log = LoggerFactory.getLogger(DispatchingEventPublisher.class);

    private final EventTypeRegistry registry;
    private final List<EventHandler> handlers;
    private final ObjectMapper objectMapper;

    public DispatchingEventPublisher(EventTypeRegistry registry,
                                     List<EventHandler> handlers,
                                     ObjectMapper objectMapper) {
        this.registry = registry;
        this.handlers = handlers;
        this.objectMapper = objectMapper;
    }

    @Override
    public void publish(EventMessage message) {
        Class<?> targetType = registry.resolve(message.eventType()).orElse(null);
        if (targetType == null) {
            log.info("[DISPATCH] type inconnu '{}' ignore (id={})", message.eventType(), message.id());
            return;
        }

        Object event = deserialize(message.payload(), targetType);

        List<EventHandler> matched = handlers.stream()
                .filter(h -> h.handledTypes().contains(message.eventType()))
                .toList();
        if (matched.isEmpty()) {
            log.info("[DISPATCH] aucun handler pour '{}' (id={})", message.eventType(), message.id());
            return;
        }

        for (EventHandler handler : matched) {
            handler.handle(message.id(), event); // exception eventuelle => retry / RETRYING
        }
        log.debug("[DISPATCH] '{}' transmis a {} handler(s)", message.eventType(), matched.size());
    }

    private Object deserialize(String payloadJson, Class<?> targetType) {
        try {
            return objectMapper.readValue(payloadJson, targetType);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Payload illisible pour " + targetType.getSimpleName(), e);
        }
    }
}
