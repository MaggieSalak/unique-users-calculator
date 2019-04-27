package com.salak.maggie;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import kafka.utils.ShutdownableThread;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

public class DataConsumer extends ShutdownableThread {

    private final String topic;
    private final KafkaConsumer consumer;
    private final EventAggregator eventAggregator;

    // Kafka consumer config parameters
    private static final String KAFKA_SERVER_URL = "localhost";
    private static final int KAFKA_SERVER_PORT = 9092;
    private static final String CLIENT_ID = "DataConsumer";

    private static final String KEY_DESERIALIZER = "org.apache.kafka.common.serialization.IntegerDeserializer";
    private static final String VALUE_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";

    private static final int CONSUMER_TIMEOUT_MILLIS = 1000;

    public DataConsumer(String topic) {
        super("doodle-data-challenge", false);
        Properties properties = initProperties();
        this.consumer = new KafkaConsumer(properties);
        this.topic = topic;
        this.eventAggregator = new EventAggregator();
    }

    private static Properties initProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER_URL + ":" + KAFKA_SERVER_PORT);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, CLIENT_ID);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KEY_DESERIALIZER);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, VALUE_DESERIALIZER);
        return properties;
    }

    @Override
    public void doWork() {
        consumer.subscribe(Collections.singletonList(this.topic));
        ConsumerRecords<Integer, String> records = consumer.poll(CONSUMER_TIMEOUT_MILLIS);
        for (ConsumerRecord record : records) {
            processMessage(record.value().toString());
        }
    }

    // Converts the message to an Event and adds it to the event aggregator
    private void processMessage(String message) {
        GsonBuilder builder = new GsonBuilder();
        builder.setPrettyPrinting();

        Gson gson = builder.create();
        Event event = gson.fromJson(message, Event.class);

        this.eventAggregator.AddEvent(event);
    }

//    @Override
//    public String name() {
//        return null;
//    }
//
//    @Override
//    public boolean isInterruptible() {
//        return false;
//    }
}
