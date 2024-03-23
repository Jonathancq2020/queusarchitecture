package com.lab.litethinking.kafkaconsumer.kafka;

import com.lab.litethinking.kafkaconsumer.dto.People;
import com.lab.litethinking.kafkaconsumer.dto.User;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;

import java.io.*;
import java.net.URL;

@Service
public class AvroKafkaProducer {

    @Value("${spring.kafka.avro.name}")
    private String topicAvroName;
    private static final Logger LOGGER = LoggerFactory.getLogger(AvroKafkaProducer.class);

    @Autowired
    private KafkaTemplate<String, People> kafkaTemplate;

    public void sendMessage(People people){
        LOGGER.info(String.format("Message avro sent -> %s", people.toString()));
        kafkaTemplate.send(new ProducerRecord<>(topicAvroName, people.getName().toString(), people));
    }


}
