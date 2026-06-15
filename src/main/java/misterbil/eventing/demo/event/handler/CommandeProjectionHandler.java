package misterbil.eventing.demo.event.handler;

import lombok.RequiredArgsConstructor;
import misterbil.eventing.consumer.EventHandler;
import misterbil.eventing.consumer.ProcessedEvent;
import misterbil.eventing.consumer.ProcessedEventId;
import misterbil.eventing.consumer.ProcessedEventRepository;
import misterbil.eventing.demo.CommandeStats;
import misterbil.eventing.demo.CommandeStatsRepository;
import misterbil.eventing.demo.event.dto.CommandeAnnulee;
import misterbil.eventing.demo.event.dto.CommandeCreee;
import misterbil.eventing.demo.event.dto.DomainEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.Set;
import java.util.UUID;

/**
 * Projette les evenements Commande dans le read model {@link CommandeStats}.
 *
 * <p>Exemple de handler <b>multi-types</b> : il declare deux types et les aiguille via un
 * {@code switch} a patterns. Comme {@link DomainEvent} est {@code sealed}, le switch est
 * <b>exhaustif</b> sans branche {@code default} : si on ajoute un evenement au scelle, ce
 * fichier ne compilera plus tant qu'on ne l'aura pas traite. Le typage devient un filet de
 * securite a la compilation.
 *
 * <p><b>Idempotence.</b> Ses effets (incrementer des compteurs) ne sont pas rejouables : recevoir
 * deux fois le meme evenement fausserait les stats. On <b>deduplique</b> donc sur l'{@code eventId}
 * via {@link ProcessedEvent}. Verification de la trace, projection et marquage se font dans la
 * <b>meme transaction</b> ({@code @Transactional}) : soit tout est applique une fois, soit rien
 * (et l'evenement sera rejoue). Un doublon trouve la trace et s'abstient.
 */
@Component
@RequiredArgsConstructor
public class CommandeProjectionHandler implements EventHandler {

    private static final Logger log = LoggerFactory.getLogger(CommandeProjectionHandler.class);

    /** Identifie ce consommateur dans la table de deduplication. */
    private static final String CONSUMER = "commande-projection";

    private final CommandeStatsRepository statsRepository;
    private final ProcessedEventRepository processedRepository;

    @Override
    public Set<String> handledTypes() {
        return Set.of(CommandeCreee.TYPE, CommandeAnnulee.TYPE);
    }

    @Override
    @Transactional
    public void handle(UUID eventId, Object event) {
        // Le dispatcher ne nous envoie que nos types ; on retombe sur le scelle pour
        // beneficier de l'exhaustivite du switch.
        if (!(event instanceof DomainEvent domainEvent)) {
            return;
        }

        // Garde d'idempotence : deja projete ? on ignore ce doublon.
        ProcessedEventId dedupKey = new ProcessedEventId(CONSUMER, eventId);
        if (processedRepository.existsById(dedupKey)) {
            log.debug("[PROJECTION] evenement {} deja traite, ignore", eventId);
            return;
        }

        switch (domainEvent) {
            case CommandeCreee c -> {
                CommandeStats stats = statsFor(c.client());
                stats.enregistrerCreation(montant(c.montant()));
                statsRepository.save(stats);
            }
            case CommandeAnnulee a -> {
                CommandeStats stats = statsFor(a.client());
                stats.enregistrerAnnulation(montant(a.montant()));
                statsRepository.save(stats);
            }
        }

        // Marque l'evenement traite, dans la meme transaction que la projection.
        processedRepository.save(new ProcessedEvent(dedupKey));
    }

    /** Charge les stats du client ou en cree de nouvelles (upsert). */
    private CommandeStats statsFor(String client) {
        return statsRepository.findById(client)
                .orElseGet(() -> new CommandeStats(client));
    }

    private BigDecimal montant(BigDecimal value) {
        return value != null ? value : BigDecimal.ZERO;
    }
}
