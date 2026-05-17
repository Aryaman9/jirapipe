package com.jirapipe.vectorstore;

import com.jirapipe.vectorstore.dto.SimilarTicket;

import java.util.List;
import java.util.UUID;

public interface VectorStoreRepository {

    List<SimilarTicket> findSimilar(float[] embedding, int limit);

    void store(UUID ticketId, float[] embedding, String contentHash);
}
