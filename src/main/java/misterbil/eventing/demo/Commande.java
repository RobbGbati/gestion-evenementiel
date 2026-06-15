package misterbil.eventing.demo;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Agregat metier de demonstration.
 *
 * <p>Sa creation emet un evenement {@code CommandeCreee} via l'outbox, dans la meme
 * transaction (voir {@link CommandeService}).
 */
@Entity
@Table(name = "commande")
@Getter
public class Commande {

    @Id
    @Column(nullable = false, updatable = false)
    private UUID id;

    @Column(nullable = false)
    private String client;

    @Column(nullable = false)
    private BigDecimal montant;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    protected Commande() {
        // requis par JPA
    }

    public Commande(String client, BigDecimal montant) {
        this.id = UUID.randomUUID();
        this.client = client;
        this.montant = montant;
        this.createdAt = Instant.now();
    }
}
