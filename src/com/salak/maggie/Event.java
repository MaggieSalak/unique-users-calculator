package com.salak.maggie;

public class Event {
    private String ts;
    private String uid;

    public Event() { }

    public String getTs() {
        return ts;
    }

    public String getUid() {
        return uid;
    }

    public void setTs(String ts) {
        this.ts = ts;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String toString() {
        return this.ts + ", " + this.uid;
    }
}
