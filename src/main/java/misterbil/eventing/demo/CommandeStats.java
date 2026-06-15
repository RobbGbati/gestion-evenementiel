package misterbil.eventing.demo;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Read model (projection) : statistiques de commandes par client.
 *
 * <p>Ce n'est <b>pas</b> une donnee metier source : c'est une <b>vue derivee</b>, reconstruite
 * uniquement a partir des evenements ({@code CommandeCreee}, {@code CommandeAnnulee}). Interet du
 * pattern : le service metier ignore tout de ce calcul. On peut ajouter, changer ou rejouer
 * cette projection sans toucher au code de creation de commande.
 */
@Entity
@Table(name = "commande_stats")
@Getter
@NoArgsConstructor
public class CommandeStats {

    @Id
    @Column(nullable = false, updatable = false)
    private String client;

    @Column(name = "nb_creees", nullable = false)
    private long nbCreees;

    @Column(name = "nb_annulees", nullable = false)
    private long nbAnnulees;

    /** Somme des montants crees moins les montants annules. */
    @Column(name = "montant_net", nullable = false)
    private BigDecimal montantNet;

    @Column(name = "derniere_maj", nullable = false)
    private Instant derniereMaj;

    public CommandeStats(String client) {
        this.client = client;
        this.nbCreees = 0;
        this.nbAnnulees = 0;
        this.montantNet = BigDecimal.ZERO;
        this.derniereMaj = Instant.now();
    }

    public void enregistrerCreation(BigDecimal montant) {
        this.nbCreees++;
        this.montantNet = this.montantNet.add(montant);
        this.derniereMaj = Instant.now();
    }

    public void enregistrerAnnulation(BigDecimal montant) {
        this.nbAnnulees++;
        this.montantNet = this.montantNet.subtract(montant);
        this.derniereMaj = Instant.now();
    }
}
