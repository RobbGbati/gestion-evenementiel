package misterbil.eventing.demo;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import jakarta.validation.Valid;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * API de demonstration : creer une commande declenche un evenement via l'outbox.
 */
@RestController
@RequestMapping("/api/demo/commandes")
@Tag(name = "Demo", description = "Exemple metier : creer une commande emet un evenement via l'outbox")
public class CommandeController {

    private final CommandeService commandeService;

    public CommandeController(CommandeService commandeService) {
        this.commandeService = commandeService;
    }

    @Operation(summary = "Creer une commande (declenche un evenement CommandeCreee)")
    @PostMapping
    public ResponseEntity<CommandeResponse> creer(@Valid @RequestBody CreateCommandeRequest request) {
        Commande commande = commandeService.creer(request.client(), request.montant());
        return ResponseEntity.status(HttpStatus.CREATED).body(CommandeResponse.from(commande));
    }

    @Operation(summary = "Annuler une commande (declenche un evenement CommandeAnnulee)")
    @PostMapping("/{id}/annuler")
    public ResponseEntity<Void> annuler(@PathVariable UUID id,
                                        @Valid @RequestBody AnnulerCommandeRequest request) {
        commandeService.annuler(id, request.raison());
        return ResponseEntity.noContent().build();
    }

    public record CreateCommandeRequest(
            @NotBlank String client,
            @NotNull @Positive BigDecimal montant) {
    }

    public record AnnulerCommandeRequest(@NotBlank String raison) {
    }

    public record CommandeResponse(UUID id, String client, BigDecimal montant, Instant createdAt) {
        static CommandeResponse from(Commande c) {
            return new CommandeResponse(c.getId(), c.getClient(), c.getMontant(), c.getCreatedAt());
        }
    }
}
