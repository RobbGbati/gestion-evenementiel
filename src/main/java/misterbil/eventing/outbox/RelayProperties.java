package misterbil.eventing.outbox;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Parametres du relay (prefixe {@code eventing.relay} dans application.yml).
 *
 * @param pollIntervalMs   periode de scrutation de l'outbox (ms)
 * @param batchSize        nombre d'evenements traites par tick
 * @param maxRetries       nombre maximal de tentatives avant statut DEAD
 * @param backoffBaseSeconds base du backoff exponentiel entre 2 reprises (s)
 */
@ConfigurationProperties(prefix = "eventing.relay")
public record RelayProperties(
        long pollIntervalMs,
        int batchSize,
        int maxRetries,
        long backoffBaseSeconds
) {
    public RelayProperties {
        if (pollIntervalMs <= 0) {
            pollIntervalMs = 2000;
        }
        if (batchSize <= 0) {
            batchSize = 50;
        }
        if (maxRetries <= 0) {
            maxRetries = 5;
        }
        if (backoffBaseSeconds <= 0) {
            backoffBaseSeconds = 5;
        }
    }
}
