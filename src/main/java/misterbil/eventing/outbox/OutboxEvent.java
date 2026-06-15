package misterbil.eventing.outbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Lob;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import lombok.Getter;

import java.time.Instant;
import java.util.UUID;

/**
 * Evenement persiste dans l'outbox.
 *
 * <p>Ecrit dans la meme transaction que la donnee metier (atomicite), puis publie
 * de maniere asynchrone par {@link OutboxRelay}. Conserve le payload et l'historique
 * de statut, ce qui permet l'audit et le rejeu.
 */
@Entity
@Table(name = "outbox_event")
@Getter
public class OutboxEvent {

    @Id
    @Column(nullable = false, updatable = false)
    private UUID id;

    @Column(name = "aggregate_type", nullable = false)
    private String aggregateType;

    @Column(name = "aggregate_id", nullable = false)
    private String aggregateId;

    @Column(name = "event_type", nullable = false)
    private String eventType;

    /** Charge utile serialisee en JSON. */
    @Lob
    @Column(nullable = false)
    private String payload;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private EventStatus status;

    @Column(name = "retry_count", nullable = false)
    private int retryCount;

    @Column(name = "max_retries", nullable = false)
    private int maxRetries;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    @Column(name = "published_at")
    private Instant publishedAt;

    /** Date a partir de laquelle l'evenement est eligible a une (nouvelle) tentative. */
    @Column(name = "next_attempt_at", nullable = false)
    private Instant nextAttemptAt;

    @Lob
    @Column(name = "last_error")
    private String lastError;

    /** Verrou optimiste : evite la double-publication si plusieurs instances tournent. */
    @Version
    @Column(nullable = false)
    private long version;

    protected OutboxEvent() {
        // requis par JPA
    }

    /**
     * Cree un evenement pret a publier (statut {@link EventStatus#PENDING}).
     */
    public static OutboxEvent create(String aggregateType,
                                     String aggregateId,
                                     String eventType,
                                     String payload,
                                     int maxRetries) {
        Instant now = Instant.now();
        OutboxEvent event = new OutboxEvent();
        event.id = UUID.randomUUID();
        event.aggregateType = aggregateType;
        event.aggregateId = aggregateId;
        event.eventType = eventType;
        event.payload = payload;
        event.status = EventStatus.PENDING;
        event.retryCount = 0;
        event.maxRetries = maxRetries;
        event.createdAt = now;
        event.updatedAt = now;
        event.nextAttemptAt = now;
        return event;
    }

    /** Passe en cours de publication. */
    public void markPublishing() {
        this.status = EventStatus.PUBLISHING;
        this.updatedAt = Instant.now();
    }

    /** Marque la publication reussie. */
    public void markPublished() {
        Instant now = Instant.now();
        this.status = EventStatus.PUBLISHED;
        this.publishedAt = now;
        this.updatedAt = now;
        this.lastError = null;
    }

    /**
     * Enregistre un echec : planifie une reprise ou abandonne si le quota est atteint.
     *
     * @param error          message d'erreur
     * @param nextAttemptAt  date de la prochaine tentative (en cas de RETRYING)
     */
    public void markFailure(String error, Instant nextAttemptAt) {
        this.retryCount++;
        this.lastError = error;
        this.updatedAt = Instant.now();
        if (this.retryCount >= this.maxRetries) {
            this.status = EventStatus.DEAD;
        } else {
            this.status = EventStatus.RETRYING;
            this.nextAttemptAt = nextAttemptAt;
        }
    }

    /** Reinitialise l'evenement pour un rejeu manuel. */
    public void resetForReplay() {
        Instant now = Instant.now();
        this.status = EventStatus.PENDING;
        this.retryCount = 0;
        this.nextAttemptAt = now;
        this.updatedAt = now;
        this.lastError = null;
        this.publishedAt = null;
    }
}
