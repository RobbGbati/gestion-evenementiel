package misterbil.eventing.demo;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import misterbil.eventing.demo.event.handler.CommandeProjectionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

/**
 * Expose le read model alimente par les evenements (voir {@link CommandeProjectionHandler}).
 *
 * <p>Donnee uniquement <b>derivee</b> des evenements : aucune ecriture metier ici, on lit la
 * projection. Comparer avec {@code /api/demo/commandes} qui, lui, ecrit la donnee source.
 */
@RestController
@RequestMapping("/api/projections/commandes")
@Tag(name = "Projection", description = "Read model derive des evenements Commande")
public class ProjectionController {

    private final CommandeStatsRepository statsRepository;

    public ProjectionController(CommandeStatsRepository statsRepository) {
        this.statsRepository = statsRepository;
    }

    @Operation(summary = "Statistiques de commandes par client (vue construite a partir des evenements)")
    @GetMapping
    public List<StatsResponse> stats() {
        return statsRepository.findAll().stream().map(StatsResponse::from).toList();
    }

    public record StatsResponse(String client, long nbCreees, long nbAnnulees,
                                BigDecimal montantNet, Instant derniereMaj) {
        static StatsResponse from(CommandeStats s) {
            return new StatsResponse(s.getClient(), s.getNbCreees(), s.getNbAnnulees(),
                    s.getMontantNet(), s.getDerniereMaj());
        }
    }
}
