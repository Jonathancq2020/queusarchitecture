package com.lab.litethinking.kafkaconsumer.controller;

import com.lab.litethinking.kafkaconsumer.dto.User;
import com.lab.litethinking.kafkaconsumer.kafka.JsonKafkaConsumer;
import com.lab.litethinking.kafkaconsumer.kafka.JsonKafkaProducer;
import com.lab.litethinking.kafkaconsumer.kafka.KafkaProducer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@CrossOrigin(origins = "*", allowedHeaders = "*")
@RestController
@RequestMapping("/api/v1/kafka")
public class MessageController {

    private KafkaProducer kafkaProducer;

    private JsonKafkaProducer jsonKafkaProducer;

    public MessageController(KafkaProducer kafkaProducer, JsonKafkaProducer jsonKafkaProducer){
        this.kafkaProducer = kafkaProducer;
        this.jsonKafkaProducer = jsonKafkaProducer;
    }

    @GetMapping("/publish")
    public ResponseEntity<String> publish(@RequestParam("message") String message){
        System.out.println("DATA 1.0 "+message);
        kafkaProducer.sendMessage(message);
        return ResponseEntity.ok("Message sent to the topic");
    }

    @PostMapping("/buy")
    public ResponseEntity<String> publish(@RequestBody User user){
        jsonKafkaProducer.sendMessage(user);
        return ResponseEntity.ok("Information of the buy has already ok ");
    }

}
