package com.lab.litethinking.kafkaconsumer.dto;

import java.io.Serializable;

public class Venta implements Serializable{

    String name;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

}
