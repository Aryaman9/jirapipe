package com.jirapipe.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfig {

    private final JiraPipeProperties properties;

    public KafkaConfig(JiraPipeProperties properties) {
        this.properties = properties;
    }

    @Bean
    public NewTopic ticketIngestionTopic() {
        return TopicBuilder.name(properties.getKafka().getTopics().getTicketIngestion())
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic deadLetterTopic() {
        return TopicBuilder.name(properties.getKafka().getTopics().getDeadLetter())
                .partitions(1)
                .replicas(1)
                .build();
    }
}
