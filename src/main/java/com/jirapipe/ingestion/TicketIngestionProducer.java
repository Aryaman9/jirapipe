package com.jirapipe.ingestion;

import com.jirapipe.config.JiraPipeProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class TicketIngestionProducer {

    private static final Logger log = LoggerFactory.getLogger(TicketIngestionProducer.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String topic;

    public TicketIngestionProducer(KafkaTemplate<String, String> kafkaTemplate,
                                   JiraPipeProperties properties) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = properties.getKafka().getTopics().getTicketIngestion();
    }

    public void publish(String payload) {
        log.debug("Publishing ticket payload to topic: {}", topic);
        kafkaTemplate.send(topic, payload)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish to Kafka topic {}: {}", topic, ex.getMessage());
                    } else {
                        log.debug("Published to partition {} offset {}",
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    }
                });
    }
}
