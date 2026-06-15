package misterbil.eventing.support;

import misterbil.eventing.publisher.EventMessage;
import misterbil.eventing.publisher.EventPublisher;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Fournisseur pilotable pour les tests : echoue un nombre configurable de fois,
 * puis reussit. Compte aussi les publications effectivement recues.
 *
 * <p>Declare comme {@code @Bean EventPublisher} dans les tests, il remplace
 * automatiquement le LoggingEventPublisher (via @ConditionalOnMissingBean).
 */
public class ControllableEventPublisher implements EventPublisher {

    private final AtomicInteger failuresRemaining = new AtomicInteger(0);
    private final AtomicInteger publishedCount = new AtomicInteger(0);
    private volatile boolean alwaysFail = false;

    @Override
    public void publish(EventMessage message) {
        if (alwaysFail || failuresRemaining.getAndUpdate(n -> n > 0 ? n - 1 : 0) > 0) {
            throw new IllegalStateException("Echec simule du fournisseur");
        }
        publishedCount.incrementAndGet();
    }

    public void failNextTimes(int times) {
        this.alwaysFail = false;
        this.failuresRemaining.set(times);
    }

    public void alwaysFail() {
        this.alwaysFail = true;
    }

    public void succeedAlways() {
        this.alwaysFail = false;
        this.failuresRemaining.set(0);
    }

    public int publishedCount() {
        return publishedCount.get();
    }

    public void reset() {
        this.alwaysFail = false;
        this.failuresRemaining.set(0);
        this.publishedCount.set(0);
    }
}
