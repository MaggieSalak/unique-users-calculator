package com.salak.maggie;

public class Main {

    public static void main(String[] args) {
        System.out.println("ok!");
        DataConsumer consumer = new DataConsumer("data-challenge-topic");
        consumer.start();
    }
}
