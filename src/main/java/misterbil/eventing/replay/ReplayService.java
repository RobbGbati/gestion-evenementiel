package misterbil.eventing.replay;

import misterbil.eventing.outbox.OutboxEvent;
import misterbil.eventing.outbox.OutboxEventRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.NoSuchElementException;
import java.util.UUID;

/**
 * Rejeu d'un evenement deja stocke.
 *
 * <p>Le payload etant conserve, on remet simplement l'evenement en {@code PENDING} :
 * le relay le reprend au prochain tick et le republie a l'identique.
 */
@Service
public class ReplayService {

    private final OutboxEventRepository repository;

    public ReplayService(OutboxEventRepository repository) {
        this.repository = repository;
    }

    @Transactional
    public OutboxEvent replay(UUID eventId) {
        OutboxEvent event = repository.findById(eventId)
                .orElseThrow(() -> new NoSuchElementException("Evenement introuvable : " + eventId));
        event.resetForReplay();
        return repository.save(event);
    }
}
