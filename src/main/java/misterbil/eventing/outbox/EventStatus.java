package misterbil.eventing.outbox;

/**
 * Cycle de vie d'un evenement dans l'outbox.
 */
public enum EventStatus {

    /** Cree, en attente de publication par le relay. */
    PENDING,

    /** Pris en charge par le relay, publication en cours. */
    PUBLISHING,

    /** Publie avec succes aupres du fournisseur. */
    PUBLISHED,

    /** Echec, une nouvelle tentative est planifiee ({@code nextAttemptAt}). */
    RETRYING,

    /** Nombre maximal de tentatives depasse : abandon (dead-letter logique). */
    DEAD
}
