package misterbil.eventing.config;

import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Info;
import org.springframework.context.annotation.Configuration;

/**
 * Metadonnees de la documentation OpenAPI / Swagger UI.
 *
 * <p>Interface accessible une fois l'application demarree : {@code /swagger-ui.html}.
 * Specification brute : {@code /v3/api-docs}.
 */
@Configuration
@OpenAPIDefinition(info = @Info(
        title = "Gestion evenementiel - API Outbox",
        version = "0.0.1",
        description = "Module de publication d'evenements (Transactional Outbox + relay). "
                + "Permet de creer, consulter et rejouer des evenements."))
public class OpenApiConfig {
}
