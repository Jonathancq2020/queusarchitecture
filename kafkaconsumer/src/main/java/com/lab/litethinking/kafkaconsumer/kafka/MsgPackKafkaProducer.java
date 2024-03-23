package com.lab.litethinking.kafkaconsumer.kafka;

import com.lab.litethinking.kafkaconsumer.dto.People;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

@Service
public class MsgPackKafkaProducer {

    @Value("${spring.kafka.msgpack.name}")
    private String topic;

    private static final Logger LOGGER = LoggerFactory.getLogger(MsgPackKafkaProducer.class);

    @Autowired
    private KafkaTemplate<String, byte[]> kafkaTemplate;

    public void sendMessage(People people)  {

        try{

            LOGGER.info(String.format("Sending message: %s", people.toString()));
            byte[] msgPackData = this.serializePeople(people);
            kafkaTemplate.send(topic, msgPackData);
            LOGGER.info("Message sent successfully");

        }catch (Exception e){
            LOGGER.error("Failed to send message: " + e.getMessage());
        }
    }

    public byte[] serializePeople(People people) throws IOException {

        MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
        packer.packMapHeader(2);
        packer.packString("name").packString(people.getName());
        packer.packString("age").packInt(people.getAge());

        return packer.toByteArray();
    }


}
