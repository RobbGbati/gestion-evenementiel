package misterbil.eventing.api;

import misterbil.eventing.outbox.OutboxEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.hamcrest.Matchers.is;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class OutboxEventControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private OutboxEventRepository repository;

    @BeforeEach
    void setUp() {
        repository.deleteAll();
    }

    @Test
    void cree_consulte_et_rejoue_un_evenement() throws Exception {
        // Creation
        String body = """
                {"aggregateType":"Commande","aggregateId":"c-1","eventType":"CommandeCreee","payload":{"montant":42}}
                """;
        String location = mockMvc.perform(post("/api/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.status", is("PENDING")))
                .andExpect(jsonPath("$.eventType", is("CommandeCreee")))
                .andReturn().getResponse().getContentAsString();

        String id = location.replaceAll(".*\"id\":\"([^\"]+)\".*", "$1");

        // Consultation
        mockMvc.perform(get("/api/events/" + id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.aggregateId", is("c-1")));

        // Rejeu
        mockMvc.perform(post("/api/events/" + id + "/replay"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status", is("PENDING")))
                .andExpect(jsonPath("$.retryCount", is(0)));
    }

    @Test
    void retourne_404_si_evenement_inconnu() throws Exception {
        mockMvc.perform(get("/api/events/00000000-0000-0000-0000-000000000000"))
                .andExpect(status().isNotFound());
    }

    @Test
    void creation_commande_demo_emet_un_evenement() throws Exception {
        String body = """
                {"client":"Alice","montant":99.90}
                """;
        mockMvc.perform(post("/api/demo/commandes")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.client", is("Alice")));

        mockMvc.perform(get("/api/events"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalElements", is(1)))
                .andExpect(jsonPath("$.content[0].eventType", is("CommandeCreee")));
    }
}
