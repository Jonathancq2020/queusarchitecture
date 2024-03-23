package com.lab.litethinking.kafkaconsumer.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Value("${spring.kafka.topic.name}")
    private String topicName;

    @Value("${spring.kafka.topic-json.name}")
    private String topicJsonName;

    @Value("${spring.kafka.msgpack.name}")
    private String topicMsgPackName;


    @Bean
    public NewTopic javaguidesTopic(){
        return TopicBuilder.name(topicName).partitions(2).replicas(1)
                .build();
    }

    @Bean
    public NewTopic javaguidesJsonTopic(){
        return TopicBuilder.name(topicJsonName)
                .partitions(6).replicas(3)
                .config("retention.ms", "86400000") // retención durante 1 día (en milisegundos)
                .build();
    }

    @Bean
    public NewTopic javaguidesMsgPack(){
        return TopicBuilder.name(topicMsgPackName).partitions(2).replicas(1)
                .build();
    }

}