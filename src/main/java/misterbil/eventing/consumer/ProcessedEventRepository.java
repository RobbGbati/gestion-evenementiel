package misterbil.eventing.consumer;

import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Acces aux traces de deduplication. La cle primaire composite (consommateur + id d'evenement)
 * garantit, au niveau base, qu'un evenement n'est marque traite qu'une fois par consommateur.
 */
public interface ProcessedEventRepository extends JpaRepository<ProcessedEvent, ProcessedEventId> {
}
