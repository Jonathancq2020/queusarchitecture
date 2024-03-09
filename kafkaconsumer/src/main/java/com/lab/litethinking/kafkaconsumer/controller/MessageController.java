package com.lab.litethinking.kafkaconsumer.controller;

import com.lab.litethinking.kafkaconsumer.kafka.KafkaProducer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@CrossOrigin(origins = "*", allowedHeaders = "*")
@RestController
@RequestMapping("/api/v1/kafka")
public class MessageController {

    private KafkaProducer kafkaProducer;

    public MessageController(KafkaProducer kafkaProducer){
        this.kafkaProducer = kafkaProducer;
    }

    @GetMapping("/publish")
    public ResponseEntity<String> publish(@RequestParam("message") String message){
        System.out.println("DATA"+message);
        kafkaProducer.sendMessage(message);
        return ResponseEntity.ok("Message sent to the topic");
    }

}
