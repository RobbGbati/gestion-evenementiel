package misterbil.eventing.demo;

import misterbil.eventing.demo.event.dto.CommandeAnnulee;
import misterbil.eventing.demo.event.dto.CommandeCreee;
import misterbil.eventing.demo.event.dto.DomainEvent;
import misterbil.eventing.consumer.ProcessedEventRepository;
import misterbil.eventing.outbox.OutboxEvent;
import misterbil.eventing.outbox.OutboxEventProcessor;
import misterbil.eventing.outbox.OutboxEventRepository;
import misterbil.eventing.outbox.OutboxEventService;
import misterbil.eventing.replay.ReplayService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifie le point "exploitation des evenements" : un evenement publie est consomme par le
 * {@code DispatchingEventPublisher}, retype, puis projete dans le read model {@link CommandeStats}.
 *
 * <p>Ici aucun {@code EventPublisher} de test n'est fourni : c'est le vrai dispatcher (avec ses
 * handlers) qui s'execute, comme en production.
 */
@SpringBootTest
class CommandeProjectionIntegrationTest {

    @Autowired
    private OutboxEventService eventService;

    @Autowired
    private OutboxEventProcessor processor;

    @Autowired
    private OutboxEventRepository outboxRepository;

    @Autowired
    private CommandeStatsRepository statsRepository;

    @Autowired
    private ProcessedEventRepository processedRepository;

    @Autowired
    private ReplayService replayService;

    @BeforeEach
    void setUp() {
        outboxRepository.deleteAll();
        statsRepository.deleteAll();
        processedRepository.deleteAll();
    }

    @Test
    void evenement_CommandeCreee_alimente_la_projection() {
        publier(new CommandeCreee("cmd-1", "Alice", new BigDecimal("99.90")));

        CommandeStats stats = statsRepository.findById("Alice").orElseThrow();
        assertThat(stats.getNbCreees()).isEqualTo(1);
        assertThat(stats.getNbAnnulees()).isZero();
        assertThat(stats.getMontantNet()).isEqualByComparingTo("99.90");
    }

    @Test
    void un_seul_handler_traite_creation_et_annulation() {
        publier(new CommandeCreee("cmd-1", "Bob", new BigDecimal("100.00")));
        publier(new CommandeCreee("cmd-2", "Bob", new BigDecimal("50.00")));
        publier(new CommandeAnnulee("cmd-1", "Bob", new BigDecimal("100.00"), "client absent"));

        CommandeStats stats = statsRepository.findById("Bob").orElseThrow();
        assertThat(stats.getNbCreees()).isEqualTo(2);
        assertThat(stats.getNbAnnulees()).isEqualTo(1);
        assertThat(stats.getMontantNet()).isEqualByComparingTo("50.00"); // 100 + 50 - 100
    }

    @Test
    void rejouer_un_evenement_ne_double_compte_pas_la_projection() {
        // Livraison initiale
        OutboxEvent stored = eventService.record("Commande", "agg",
                CommandeCreee.TYPE, new CommandeCreee("cmd-1", "Carol", new BigDecimal("30.00")));
        processor.process(stored.getId());

        // Rejeu du MEME evenement (meme id) : remet en PENDING puis republie
        replayService.replay(stored.getId());
        processor.process(stored.getId());

        // Grace a la deduplication, la projection ne compte qu'une fois
        CommandeStats stats = statsRepository.findById("Carol").orElseThrow();
        assertThat(stats.getNbCreees()).isEqualTo(1);
        assertThat(stats.getMontantNet()).isEqualByComparingTo("30.00");
    }

    /** Enregistre l'evenement type dans l'outbox puis declenche sa publication. */
    private void publier(DomainEvent event) {
        OutboxEvent stored = eventService.record("Commande", "agg", event.type(), event);
        processor.process(stored.getId());
    }
}
