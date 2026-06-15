package misterbil.eventing.outbox;

import misterbil.eventing.publisher.EventPublisher;
import misterbil.eventing.replay.ReplayService;
import misterbil.eventing.support.ControllableEventPublisher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifie le cycle de vie d'un evenement : succes, reprises, abandon (DEAD) et rejeu.
 *
 * <p>Le relay planifie est neutralise (poll-interval tres long) ; on declenche le
 * traitement via {@link OutboxEventProcessor#process} pour des transitions deterministes.
 */
@SpringBootTest
class OutboxRelayIntegrationTest {

    @TestConfiguration
    static class PublisherTestConfig {
        @Bean
        @Primary
        EventPublisher controllableEventPublisher() {
            return new ControllableEventPublisher();
        }
    }

    @Autowired
    private OutboxEventService eventService;

    @Autowired
    private OutboxEventProcessor processor;

    @Autowired
    private OutboxEventRepository repository;

    @Autowired
    private ReplayService replayService;

    @Autowired
    private EventPublisher publisher;

    private ControllableEventPublisher controllable() {
        return (ControllableEventPublisher) publisher;
    }

    @BeforeEach
    void setUp() {
        repository.deleteAll();
        controllable().reset();
    }

    @Test
    void publie_avec_succes() {
        controllable().succeedAlways();
        UUID id = record();

        processor.process(id);

        OutboxEvent event = repository.findById(id).orElseThrow();
        assertThat(event.getStatus()).isEqualTo(EventStatus.PUBLISHED);
        assertThat(event.getPublishedAt()).isNotNull();
        assertThat(controllable().publishedCount()).isEqualTo(1);
    }

    @Test
    void passe_en_RETRYING_puis_DEAD_apres_max_tentatives() {
        controllable().alwaysFail();
        UUID id = record();

        // max-retries = 3 (application.yml de test)
        processor.process(id);
        assertThat(repository.findById(id).orElseThrow().getStatus()).isEqualTo(EventStatus.RETRYING);

        processor.process(id);
        assertThat(repository.findById(id).orElseThrow().getStatus()).isEqualTo(EventStatus.RETRYING);

        processor.process(id);
        OutboxEvent dead = repository.findById(id).orElseThrow();
        assertThat(dead.getStatus()).isEqualTo(EventStatus.DEAD);
        assertThat(dead.getRetryCount()).isEqualTo(3);
        assertThat(dead.getLastError()).contains("Echec simule");
    }

    @Test
    void rejeu_remet_en_PENDING_et_republie() {
        controllable().alwaysFail();
        UUID id = record();
        processor.process(id);
        processor.process(id);
        processor.process(id);
        assertThat(repository.findById(id).orElseThrow().getStatus()).isEqualTo(EventStatus.DEAD);

        // Le fournisseur se retablit, on rejoue
        controllable().succeedAlways();
        replayService.replay(id);

        OutboxEvent replayed = repository.findById(id).orElseThrow();
        assertThat(replayed.getStatus()).isEqualTo(EventStatus.PENDING);
        assertThat(replayed.getRetryCount()).isZero();

        processor.process(id);
        assertThat(repository.findById(id).orElseThrow().getStatus()).isEqualTo(EventStatus.PUBLISHED);
    }

    private UUID record() {
        return eventService.record("Commande", "c-1", "CommandeCreee",
                Map.of("montant", 42)).getId();
    }
}
