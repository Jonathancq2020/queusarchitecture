package com.lab.litethinking.kafkaconsumer.kafka;

import com.lab.litethinking.kafkaconsumer.dto.Product;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.client.RestTemplate;
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

    @Value("${api.endpoint.report}")
    private String apiUrl;

    private static final Logger LOGGER = LoggerFactory.getLogger(JsonKafkaConsumer.class);

    @KafkaListener(topics = "${spring.kafka.topic-json.name}", groupId = "${spring.kafka.consumer.group-id}")
    public void consume(@Payload byte[] data){
        try{
            Product product = (Product) convertBytesToObject(data);
            System.out.println("Mensaje recibido => " + product.getName());

            if(product.getStock() > 0) {

                RestTemplate restTemplate=new RestTemplate();

                String result = restTemplate.getForObject(apiUrl+"?message="+product.getName(),String.class);

                LOGGER.info(String.format("Resultado del siguiente flujo =>: %s", result));

            }else{
                LOGGER.info("Producto no tiene un stock para generar un reporte");
            }

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
