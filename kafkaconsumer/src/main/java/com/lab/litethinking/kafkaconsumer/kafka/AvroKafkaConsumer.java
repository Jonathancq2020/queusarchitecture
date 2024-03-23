package com.lab.litethinking.kafkaconsumer.kafka;

import com.lab.litethinking.kafkaconsumer.dto.People;
import com.lab.litethinking.kafkaconsumer.dto.User;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

@Service
public class AvroKafkaConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(AvroKafkaConsumer.class);

    @KafkaListener(topics = "${spring.kafka.avro.name}", groupId = "${spring.kafka.consumer.group-id}")
    public void consume(ConsumerRecord<String, People> record){
        People people = record.value();
        LOGGER.info("Received message avro => " + people.toString());
    }

}
