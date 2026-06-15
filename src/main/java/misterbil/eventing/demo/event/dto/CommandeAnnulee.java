package misterbil.eventing.demo.event.dto;

import java.math.BigDecimal;

/**
 * Une commande a ete annulee. Embarque {@code client} et {@code montant} (connus au moment
 * de l'emission) pour que la projection puisse se mettre a jour sans relire la base.
 */
public record CommandeAnnulee(String commandeId, String client, BigDecimal montant, String raison)
        implements DomainEvent {

    public static final String TYPE = "CommandeAnnulee";

    @Override
    public String type() {
        return TYPE;
    }
}
