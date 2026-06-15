package misterbil.eventing.api.dto;

import misterbil.eventing.outbox.EventStatus;
import misterbil.eventing.outbox.OutboxEvent;

import java.time.Instant;
import java.util.UUID;

/**
 * Vue API d'un evenement de l'outbox.
 */
public record EventResponse(
        UUID id,
        String aggregateType,
        String aggregateId,
        String eventType,
        String payload,
        EventStatus status,
        int retryCount,
        int maxRetries,
        Instant createdAt,
        Instant updatedAt,
        Instant publishedAt,
        Instant nextAttemptAt,
        String lastError
) {
    public static EventResponse from(OutboxEvent e) {
        return new EventResponse(
                e.getId(),
                e.getAggregateType(),
                e.getAggregateId(),
                e.getEventType(),
                e.getPayload(),
                e.getStatus(),
                e.getRetryCount(),
                e.getMaxRetries(),
                e.getCreatedAt(),
                e.getUpdatedAt(),
                e.getPublishedAt(),
                e.getNextAttemptAt(),
                e.getLastError());
    }
}
