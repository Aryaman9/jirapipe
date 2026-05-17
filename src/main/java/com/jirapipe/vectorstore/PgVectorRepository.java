package com.jirapipe.vectorstore;

import com.jirapipe.vectorstore.dto.SimilarTicket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public class PgVectorRepository implements VectorStoreRepository {

    private static final Logger log = LoggerFactory.getLogger(PgVectorRepository.class);

    private final JdbcTemplate jdbcTemplate;

    public PgVectorRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public List<SimilarTicket> findSimilar(float[] embedding, int limit) {
        String vectorLiteral = toVectorLiteral(embedding);

        return jdbcTemplate.query("""
            SELECT te.ticket_id, t.jira_key, t.summary,
                   r.resolution_text,
                   1 - (te.embedding <=> ?::vector) AS similarity
            FROM ticket_embeddings te
            JOIN tickets t ON t.id = te.ticket_id
            LEFT JOIN resolutions r ON r.ticket_id = te.ticket_id AND r.applied = TRUE
            WHERE te.flagged = FALSE
            ORDER BY te.embedding <=> ?::vector
            LIMIT ?
            """,
                (rs, rowNum) -> new SimilarTicket(
                        UUID.fromString(rs.getString("ticket_id")),
                        rs.getString("jira_key"),
                        rs.getString("summary"),
                        rs.getString("resolution_text"),
                        rs.getDouble("similarity")
                ),
                vectorLiteral, vectorLiteral, limit);
    }

    @Override
    public void store(UUID ticketId, float[] embedding, String contentHash) {
        String vectorLiteral = toVectorLiteral(embedding);

        jdbcTemplate.update("""
            INSERT INTO ticket_embeddings (ticket_id, embedding, content_hash)
            VALUES (?, ?::vector, ?)
            ON CONFLICT (ticket_id) DO UPDATE SET
                embedding = EXCLUDED.embedding,
                content_hash = EXCLUDED.content_hash
            """,
                ticketId, vectorLiteral, contentHash);

        log.debug("Stored embedding for ticket {}", ticketId);
    }

    private String toVectorLiteral(float[] embedding) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < embedding.length; i++) {
            if (i > 0) sb.append(",");
            sb.append(embedding[i]);
        }
        sb.append("]");
        return sb.toString();
    }
}
