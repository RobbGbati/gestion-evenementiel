package misterbil.eventing.publisher;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Declare le fournisseur par defaut.
 *
 * <p>{@link ConditionalOnMissingBean} garantit le decouplage : si l'application fournit
 * sa propre implementation d'{@link EventPublisher} (ex: Kafka), celle-ci prime et le
 * {@link LoggingEventPublisher} n'est pas instancie.
 */
@Configuration
public class PublisherConfiguration {

    @Bean
    @ConditionalOnMissingBean(EventPublisher.class)
    public EventPublisher loggingEventPublisher() {
        return new LoggingEventPublisher();
    }
}
