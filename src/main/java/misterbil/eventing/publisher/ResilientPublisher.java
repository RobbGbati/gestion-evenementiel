package misterbil.eventing.publisher;

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import org.springframework.stereotype.Component;

/**
 * Enveloppe l'appel au fournisseur avec les politiques de resilience.
 *
 * <p>Le {@link Retry} rejoue les ratures ponctuelles (timeout reseau...) ; le
 * {@link CircuitBreaker} ouvre le circuit quand le fournisseur est durablement HS,
 * pour ne pas le marteler. Les deux instances sont configurees sous le nom
 * {@code eventPublisher} dans application.yml.
 *
 * <p>Composant distinct du relay : les annotations Resilience4j passent par un proxy AOP,
 * inoperant sur un appel interne a la meme classe.
 */
@Component
public class ResilientPublisher {

    private final EventPublisher delegate;

    public ResilientPublisher(EventPublisher delegate) {
        this.delegate = delegate;
    }

    @Retry(name = "eventPublisher")
    @CircuitBreaker(name = "eventPublisher")
    public void publish(EventMessage message) {
        delegate.publish(message);
    }
}
