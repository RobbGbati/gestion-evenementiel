package misterbil.eventing;

import misterbil.eventing.outbox.RelayProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Point d'entree de l'application.
 *
 * <p>{@link EnableScheduling} active le relay periodique
 * ({@link misterbil.eventing.outbox.OutboxRelay}) qui publie les evenements de l'outbox.
 */
@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties(RelayProperties.class)
public class EventingApplication {

    public static void main(String[] args) {
        SpringApplication.run(EventingApplication.class, args);
    }
}
