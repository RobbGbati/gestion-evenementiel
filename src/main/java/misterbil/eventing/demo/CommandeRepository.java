package misterbil.eventing.demo;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface CommandeRepository extends JpaRepository<Commande, UUID> {
}
