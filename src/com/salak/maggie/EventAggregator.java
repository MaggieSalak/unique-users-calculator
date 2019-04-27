package com.salak.maggie;

import java.util.*;
import java.util.TimerTask;

/**
 * EventAggregator is responsible for managing
 * unique users (uids) per required time slot (1 min)
 */
public class EventAggregator {
    // Unique users (represented as uids) per minute
    private final Map<Long, Set<String>> events = new HashMap<>();

    private static final long MAX_EVENT_AGE_MILLIS = 5000L;

    public EventAggregator() {
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                printEventCountPerMinute();
                deleteOutdatedEvents();
            }
        };
        Timer timer = new Timer();
        timer.schedule(task, new Date(), 1000);
    }

    // Adds a new event to the set of events that happened in the same minute
    public void addEvent(Event event) {
        Calendar cal = getTimeWithRoundedSeconds(event.getTs());
        long time = cal.getTimeInMillis();
        synchronized (events) {
            if (events.containsKey(time)) {
                Set<String> concurrentEvents = events.get(time);
                concurrentEvents.add(event.getUid());
                events.put(time, concurrentEvents);
            } else {
                Set<String> concurrentEvents = new HashSet<>(Arrays.asList(event.getUid()));
                events.put(time, concurrentEvents);
                System.out.println("Not contains " + time + " " + events.get(time).size());
                System.out.println(cal.getTime().getDay() + " " + cal.getTime().getHours() + " " +
                        cal.getTime().getMinutes() + " " + cal.getTime().getSeconds());
            }
        }
    }

    // Outputs number of unique users per minute given the events consumed
    private void printEventCountPerMinute() {
        synchronized (events) {
            for (Map.Entry<Long, Set<String>> entry : events.entrySet()) {
                System.out.println("Timestamp " + entry.getKey() + ": " + entry.getValue().size() + " unique users");
            }
        }
    }

    // Deletes events that are older than 5 sec from the aggregator
    private void deleteOutdatedEvents() {
        long currentTimeMillis = System.currentTimeMillis();
        synchronized (events) {
            Iterator<Long> iterator = events.keySet().iterator();
            while (iterator.hasNext()) {
                long time = iterator.next();
                if (currentTimeMillis - time > MAX_EVENT_AGE_MILLIS) {
                    iterator.remove();
                }
            }
        }
    }

    // Converts unix timestamp to current time in milliseconds rounded to a full minute
    private static Calendar getTimeWithRoundedSeconds(long timestamp) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(timestamp*1000); // multiply unix time to get milliseconds
        calendar.set(Calendar.SECOND, 0);

        return calendar;
    }
}
