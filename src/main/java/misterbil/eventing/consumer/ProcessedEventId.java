package misterbil.eventing.consumer;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

/**
 * Cle composite de deduplication : (consommateur, id d'evenement).
 *
 * <p>Le {@code consumer} fait partie de la cle car plusieurs handlers peuvent consommer le
 * <b>meme</b> evenement independamment : chacun doit pouvoir le marquer "traite" de son cote.
 */
@Embeddable
public class ProcessedEventId implements Serializable {

    @Column(name = "consumer", nullable = false, updatable = false)
    private String consumer;

    @Column(name = "event_id", nullable = false, updatable = false)
    private UUID eventId;

    protected ProcessedEventId() {
        // requis par JPA
    }

    public ProcessedEventId(String consumer, UUID eventId) {
        this.consumer = consumer;
        this.eventId = eventId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ProcessedEventId that)) {
            return false;
        }
        return Objects.equals(consumer, that.consumer) && Objects.equals(eventId, that.eventId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(consumer, eventId);
    }
}
