package misterbil.eventing.consumer;

import jakarta.persistence.Column;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;

import java.time.Instant;

/**
 * Trace qu'un evenement a deja ete traite par un consommateur donne.
 *
 * <p>Brique d'idempotence : avant d'appliquer un effet non rejouable (incrementer un compteur,
 * inserer une ligne), un handler verifie l'absence de cette trace, applique l'effet, puis
 * l'enregistre — le tout dans la <b>meme transaction</b>. Un doublon (rejeu, reprise) trouve
 * alors la trace et s'abstient.
 */
@Entity
@Table(name = "processed_event")
public class ProcessedEvent {

    @EmbeddedId
    private ProcessedEventId id;

    @Column(name = "processed_at", nullable = false)
    private Instant processedAt;

    protected ProcessedEvent() {
        // requis par JPA
    }

    public ProcessedEvent(ProcessedEventId id) {
        this.id = id;
        this.processedAt = Instant.now();
    }
}
