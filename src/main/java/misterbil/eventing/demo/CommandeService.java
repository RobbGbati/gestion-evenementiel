package misterbil.eventing.demo;

import lombok.RequiredArgsConstructor;
import misterbil.eventing.demo.event.dto.CommandeAnnulee;
import misterbil.eventing.demo.event.dto.CommandeCreee;
import misterbil.eventing.outbox.OutboxEventService;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.NoSuchElementException;
import java.util.UUID;

/**
 * Service metier de demonstration.
 *
 * <p>Illustre le coeur du pattern : la commande et son evenement sont persistes dans
 * la <b>meme</b> transaction. Soit les deux sont valides, soit aucun (atomicite). Les
 * evenements emis sont <b>types</b> ({@link CommandeCreee}, {@link CommandeAnnulee}) ; le
 * payload JSON en outbox est leur serialisation.
 */
@Service
@RequiredArgsConstructor
public class CommandeService {

    private final CommandeRepository commandeRepository;
    private final OutboxEventService outboxEventService;

    @Transactional
    public Commande creer(String client, BigDecimal montant) {
        Commande commande = commandeRepository.save(new Commande(client, montant));

        CommandeCreee event = new CommandeCreee(
                commande.getId().toString(), commande.getClient(), commande.getMontant());
        outboxEventService.record("Commande", commande.getId().toString(), event.type(), event);

        return commande;
    }

    /**
     * Annule une commande existante et emet {@link CommandeAnnulee} dans la meme transaction.
     */
    @Transactional
    public void annuler(UUID commandeId, String raison) {
        Commande commande = commandeRepository.findById(commandeId)
                .orElseThrow(() -> new NoSuchElementException("Commande introuvable : " + commandeId));

        CommandeAnnulee event = new CommandeAnnulee(
                commande.getId().toString(), commande.getClient(), commande.getMontant(), raison);
        outboxEventService.record("Commande", commande.getId().toString(), event.type(), event);
    }
}
