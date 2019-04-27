package com.salak.maggie;

import java.util.*;
import java.util.TimerTask;

/**
 * EventAggregator is responsible for managing
 * unique events (uids) per required time slot (1 min)
 */
public class EventAggregator {
    // Unique events (uids) per minute
    private Map<Long, Set<String>> events;

    public EventAggregator() {
        this.events = new HashMap<>();

        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                printEventCountPerMinute();
            }
        };
        Timer timer = new Timer();
        timer.schedule(task, new Date(), 1000);
    }

    // Adds a new event to the set of events that happened in the same minute
    public void AddEvent(Event event) {
        Calendar cal = getTimeWithRoundedSeconds(event.getTs());
        long time = cal.getTimeInMillis();
        if (events.containsKey(time)) {
            Set<String> concurrentEvents = events.get(time);
            concurrentEvents.add(event.getUid());
            events.put(time, concurrentEvents);
            //System.out.println("contains " + time + " " + events.get(time).size());
        } else {
            Set<String> concurrentEvents = new HashSet<>(Arrays.asList(event.getUid()));
            events.put(time, concurrentEvents);
            System.out.println("Not contains " + time + " " + events.get(time).size());
            System.out.println(cal.getTime().getDay() + " " + cal.getTime().getHours() + " " +
                    cal.getTime().getMinutes() + " " + cal.getTime().getSeconds());
        }
    }

    // Outputs number of unique users per minute given the events consumed
    public void printEventCountPerMinute() {
        for (Map.Entry<Long, Set<String>> entry : events.entrySet()) {
            System.out.println("Timestamp " + entry.getKey() + ": " + entry.getValue().size() + " unique users");
        }
    }

    // Converts unix timestamp to current time in milliseconds rounded to a full minute
    private static Calendar getTimeWithRoundedSeconds(long timestamp) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(timestamp*1000); // multiply to get milliseconds
        calendar.set(Calendar.SECOND, 0);

        return calendar;
    }
}
