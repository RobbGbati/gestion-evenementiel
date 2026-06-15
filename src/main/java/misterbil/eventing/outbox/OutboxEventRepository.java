package misterbil.eventing.outbox;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, UUID> {

    /**
     * Evenements eligibles a publication (statut dans la liste et echeance atteinte),
     * les plus anciens d'abord.
     */
    List<OutboxEvent> findByStatusInAndNextAttemptAtBeforeOrderByCreatedAtAsc(
            Collection<EventStatus> statuses, Instant before, Pageable pageable);

    /** Liste paginee filtree par statut (pour l'API). */
    Page<OutboxEvent> findByStatus(EventStatus status, Pageable pageable);
}
