package com.lab.litethinking.kafkaconsumer.kafka;

import com.lab.litethinking.kafkaconsumer.dto.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

@Service
public class JsonKafkaConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(JsonKafkaConsumer.class);

    @KafkaListener(topics = "${spring.kafka.topic-json.name}", groupId = "${spring.kafka.consumer.group-id}")
    public void consume(@Payload byte[] data){
        try{
            User user = (User) convertBytesToObject(data);
            System.out.println("Mensaje recibido => " + user);
        }catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private Object convertBytesToObject(byte[] data) throws IOException, ClassNotFoundException{
        try (ByteArrayInputStream bis = new ByteArrayInputStream(data);
             ObjectInputStream ois = new ObjectInputStream(bis)) {
            return ois.readObject();
        }
    }

}
