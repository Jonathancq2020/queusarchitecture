package com.lab.litethinking.kafkaconsumer.controller;

import com.lab.litethinking.kafkaconsumer.dto.People;
import com.lab.litethinking.kafkaconsumer.dto.Product;
import com.lab.litethinking.kafkaconsumer.dto.User;
import com.lab.litethinking.kafkaconsumer.kafka.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@CrossOrigin(origins = "*", allowedHeaders = "*")
@RestController
@RequestMapping("/api/v1/kafka")
public class MessageController {

    private KafkaProducer kafkaProducer;

    private JsonKafkaProducer jsonKafkaProducer;

    private AvroKafkaProducer avroKafkaProducer;

    private MsgPackKafkaProducer msgpackKafkaProducer;

    public MessageController(KafkaProducer kafkaProducer, JsonKafkaProducer jsonKafkaProducer, AvroKafkaProducer avroKafkaProducer,
                             MsgPackKafkaProducer msgpackKafkaProducer){
        this.kafkaProducer = kafkaProducer;
        this.jsonKafkaProducer = jsonKafkaProducer;
        this.avroKafkaProducer = avroKafkaProducer;
        this.msgpackKafkaProducer = msgpackKafkaProducer;
    }

    @PostMapping("/publish-json")
    public ResponseEntity<String> publish_json(@RequestBody Product product){;
        jsonKafkaProducer.sendMessage(product);
        return ResponseEntity.ok("Message product sent to the topic");
    }


    @GetMapping("/publish")
    public ResponseEntity<String> publish(@RequestParam("message") String message){

        for (int i = 0; i < 10000; i++) {
            kafkaProducer.sendMessage("Mensaje es: " + i + " => "+message);
        }

        return ResponseEntity.ok("Todos los mensajes fueron enviados correctamente");
    }

    @PostMapping("/buy")
    public ResponseEntity<String> publish_serializer(@RequestBody User user){
        jsonKafkaProducer.sendMessage(user);
        return ResponseEntity.ok("Information of the buy has already ok ");
    }

    @PostMapping("/avro")
    public ResponseEntity<String> publish_avro(@RequestBody People people){
        avroKafkaProducer.sendMessage(people);
        return ResponseEntity.ok("Information of the buy has already ok ");
    }

    @PostMapping("/msgpack")
    public ResponseEntity<String> publish_msgpack(@RequestBody People people){
        try{
            msgpackKafkaProducer.sendMessage(people);
        }catch (Exception e){
            e.printStackTrace();
        }

        return ResponseEntity.ok("Information of the buy has already ok ");
    }

    @GetMapping("/reports")
    public ResponseEntity<String> report_product(@RequestParam("message") String product){
        try{
            kafkaProducer.sendMessage("Producto => " +product);
        }catch (Exception e){
            e.printStackTrace();
        }

        return ResponseEntity.ok("Información enviada correctamente ");
    }

}
