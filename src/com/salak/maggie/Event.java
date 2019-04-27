package com.salak.maggie;

/**
 * Event represents a timestamp and uid of a message consumed from Kafka
 */
public class Event {
    private long ts;
    private String uid;

    public Event() { }

    public long getTs() {
        return ts;
    }

    public String getUid() {
        return uid;
    }

    public String toString() {
        return this.ts + ", " + this.uid;
    }
}
