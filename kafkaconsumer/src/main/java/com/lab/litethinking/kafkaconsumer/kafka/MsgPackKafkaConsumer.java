package com.lab.litethinking.kafkaconsumer.kafka;

import com.lab.litethinking.kafkaconsumer.dto.People;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class MsgPackKafkaConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(MsgPackKafkaConsumer.class);

    @KafkaListener(topics = "${spring.kafka.msgpack.name}", groupId = "${spring.kafka.consumer.group-id}")
    public void consume(byte[] message)  {
        MessageUnpacker desempaquetador = MessagePack.newDefaultUnpacker(message);
        try {
            Value valor = desempaquetador.unpackValue();
            LOGGER.info("Received object msgpack => " + valor);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
