package misterbil.eventing.api;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import misterbil.eventing.api.dto.CreateEventRequest;
import misterbil.eventing.api.dto.EventResponse;
import misterbil.eventing.outbox.EventStatus;
import misterbil.eventing.outbox.OutboxEvent;
import misterbil.eventing.outbox.OutboxEventRepository;
import misterbil.eventing.outbox.OutboxEventService;
import misterbil.eventing.replay.ReplayService;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import java.util.NoSuchElementException;
import java.util.UUID;

/**
 * API de consultation et de rejeu des evenements de l'outbox.
 */
@RestController
@RequestMapping("/api/events")
@RequiredArgsConstructor
@Tag(name = "Evenements", description = "Consultation et rejeu des evenements de l'outbox")
public class OutboxEventController {

    private final OutboxEventRepository repository;
    private final OutboxEventService eventService;
    private final ReplayService replayService;

    @Operation(summary = "Lister les evenements (filtrable par statut)")
    @GetMapping
    public Page<EventResponse> list(@RequestParam(required = false) EventStatus status,
                                    @RequestParam(defaultValue = "0") int page,
                                    @RequestParam(defaultValue = "20") int size) {
        PageRequest pageable = PageRequest.of(page, size);
        Page<OutboxEvent> events = (status == null)
                ? repository.findAll(pageable)
                : repository.findByStatus(status, pageable);
        return events.map(EventResponse::from);
    }

    @Operation(summary = "Detail d'un evenement")
    @GetMapping("/{id}")
    public EventResponse get(@PathVariable UUID id) {
        return repository.findById(id)
                .map(EventResponse::from)
                .orElseThrow(() -> new NoSuchElementException("Evenement introuvable : " + id));
    }

    @Operation(summary = "Creer un evenement de test (passe par l'outbox)")
    @PostMapping
    public ResponseEntity<EventResponse> create(@Valid @RequestBody CreateEventRequest request) {
        OutboxEvent event = eventService.record(
                request.aggregateType(),
                request.aggregateId(),
                request.eventType(),
                request.payload().toString());
        return ResponseEntity.status(HttpStatus.CREATED).body(EventResponse.from(event));
    }

    @Operation(summary = "Rejouer un evenement (le remet en attente de publication)")
    @PostMapping("/{id}/replay")
    public EventResponse replay(@PathVariable UUID id) {
        return EventResponse.from(replayService.replay(id));
    }
}
