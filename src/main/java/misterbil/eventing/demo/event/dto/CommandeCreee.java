package misterbil.eventing.demo.event.dto;

import java.math.BigDecimal;

/**
 * Une commande a ete creee. Porte tout ce qu'un consommateur doit savoir pour agir,
 * sans avoir a relire la base metier (evenement auto-suffisant).
 */
public record CommandeCreee(String commandeId, String client, BigDecimal montant)
        implements DomainEvent {

    public static final String TYPE = "CommandeCreee";

    @Override
    public String type() {
        return TYPE;
    }
}
