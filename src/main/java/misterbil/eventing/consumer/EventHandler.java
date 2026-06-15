package misterbil.eventing.consumer;

import java.util.Set;
import java.util.UUID;

/**
 * Reagit a un ou plusieurs types d'evenement.
 *
 * <p>Un meme handler peut declarer <b>plusieurs</b> types via {@link #handledTypes()} : utile
 * quand plusieurs evenements alimentent la <b>meme</b> projection (ex: {@code CommandeCreee} et
 * {@code CommandeAnnulee} mettent a jour les memes statistiques). Le dispatcher leur transmet
 * l'evenement deja deserialise dans son type concret.
 *
 * <p>Contrat : {@link #handle(UUID, Object)} doit <b>lever une exception</b> en cas d'echec.
 * Elle remonte jusqu'au {@code ResilientPublisher} (retry / circuit breaker) et laisse
 * l'evenement en {@code RETRYING} dans l'outbox.
 *
 * <p>Livraison "au moins une fois" : un meme {@code eventId} peut etre recu plusieurs fois
 * (rejeu, reprise apres crash). Les handlers a effet non idempotent (compteurs, insertions)
 * doivent <b>dedupliquer</b> sur {@code eventId} (voir {@link ProcessedEventRepository}).
 */
public interface EventHandler {

    /** Types d'evenement (au sens {@code OutboxEvent.eventType}) traites par ce handler. */
    Set<String> handledTypes();

    /**
     * Traite l'evenement deserialise.
     *
     * @param eventId identifiant unique de l'evenement (= id outbox), stable au rejeu ;
     *                sert de cle de deduplication
     * @param event   instance typee (ex: {@code CommandeCreee})
     */
    void handle(UUID eventId, Object event);
}
