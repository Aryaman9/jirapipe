package com.jirapipe.admin;

import com.jirapipe.embedding.EmbeddingService;
import com.jirapipe.vectorstore.VectorStoreRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
public class BackfillService {

    private static final Logger log = LoggerFactory.getLogger(BackfillService.class);

    private final JdbcTemplate jdbcTemplate;
    private final EmbeddingService embeddingService;
    private final VectorStoreRepository vectorStore;

    public BackfillService(JdbcTemplate jdbcTemplate,
                           EmbeddingService embeddingService,
                           VectorStoreRepository vectorStore) {
        this.jdbcTemplate = jdbcTemplate;
        this.embeddingService = embeddingService;
        this.vectorStore = vectorStore;
    }

    @Async
    public void backfillEmbeddings() {
        List<Map<String, Object>> tickets = jdbcTemplate.queryForList("""
            SELECT t.id, t.summary, t.description
            FROM tickets t
            LEFT JOIN ticket_embeddings te ON te.ticket_id = t.id
            WHERE te.id IS NULL AND t.pipeline_status = 'RESOLVED'
            LIMIT 500
            """);

        log.info("Starting backfill for {} tickets without embeddings", tickets.size());

        int success = 0;
        int failed = 0;

        for (Map<String, Object> ticket : tickets) {
            try {
                UUID ticketId = (UUID) ticket.get("id");
                String summary = (String) ticket.get("summary");
                String description = (String) ticket.get("description");
                String text = summary + " " + (description != null ? description : "");

                float[] embedding = embeddingService.embed(text);
                if (embedding != null) {
                    String contentHash = sha256(text);
                    vectorStore.store(ticketId, embedding, contentHash);
                    success++;
                }
            } catch (Exception e) {
                failed++;
                log.warn("Backfill failed for ticket: {}", e.getMessage());
            }
        }

        log.info("Backfill complete: {} success, {} failed", success, failed);
    }

    private String sha256(String input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (Exception e) {
            return "unknown";
        }
    }
}
