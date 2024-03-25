package com.lab.litethinking.kafkaconsumer.controller;

import com.lab.litethinking.kafkaconsumer.dto.Product;
import com.lab.litethinking.kafkaconsumer.dto.Venta;
import com.lab.litethinking.kafkaconsumer.kafka.JsonKafkaProducer;
import com.lab.litethinking.kafkaconsumer.kafka.KafkaProducer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@CrossOrigin(origins = "*", allowedHeaders = "*")
@RestController
@RequestMapping("/api/v1/example")
public class ExampleController {

    private JsonKafkaProducer jsonKafkaProducer;

    private KafkaProducer kafkaProducer;


    public ExampleController(JsonKafkaProducer jsonKafkaProducer, KafkaProducer kafkaProducer) {
        this.jsonKafkaProducer = jsonKafkaProducer;
        this.kafkaProducer = kafkaProducer;
    }

    @PostMapping("/producto")
    public ResponseEntity<String> publish_json(@RequestBody Product product){;
        jsonKafkaProducer.sendMessage(product);
        return ResponseEntity.ok("Message product sent to the topic");
    }

    @GetMapping("/ventas")
    public ResponseEntity<String> publish_json(@RequestParam("nombre") String nombre){;
        kafkaProducer.sendMessage(nombre);
        return ResponseEntity.ok("Venta completada");
    }


}
