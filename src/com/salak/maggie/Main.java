package com.salak.maggie;

public class Main {

    public static void main(String[] args) {
        System.out.println("Starting the consumer...");
        DataConsumer consumer = new DataConsumer("unique-visits-topic");
        consumer.start();
    }
}
