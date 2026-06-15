package misterbil.eventing.demo.event.dto;

/**
 * Evenement metier <b>type</b> du contexte Commande.
 *
 * <p>Interface {@code sealed} : la liste des evenements possibles est <b>fermee</b> et connue
 * a la compilation. Avantage cle pour les consommateurs : un {@code switch} sur un
 * {@code DomainEvent} est <b>exhaustif</b> (le compilateur exige de traiter tous les cas),
 * donc ajouter un nouvel evenement signale immediatement les handlers a mettre a jour.
 *
 * <p>Cote outbox, le payload reste du JSON neutre ; le typage n'intervient qu'a la
 * (de)serialisation, via {@link misterbil.eventing.consumer.EventTypeRegistry}.
 */
public sealed interface DomainEvent permits CommandeCreee, CommandeAnnulee {

    /** Nom de type metier, aligne avec {@code OutboxEvent.eventType} et le registre. */
    String type();
}
