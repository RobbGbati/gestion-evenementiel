package misterbil.eventing.demo;

import org.springframework.data.jpa.repository.JpaRepository;

/** Acces au read model des statistiques de commandes (cle = client). */
public interface CommandeStatsRepository extends JpaRepository<CommandeStats, String> {
}
