package com.salak.maggie;

import java.util.*;

/**
 * EventAggregator is responsible for managing
 * unique users (uids) per required time slot (1 min)
 */
public class EventAggregator {
    // Unique users (represented as uids) per minute (represented in milliseconds)
    private final Map<Long, Set<String>> events = new HashMap<>();

    private static final long MAX_EVENT_AGE_MILLIS = 5000L;
    private static final long EVENT_PRINT_FREQUENCY_MILLIS = 1000L;

    public EventAggregator() {
        // Scheduled task that prints user count per minute (executed every 1 sec)
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                printEventCountPerMinute();
            }
        };
        Timer timer = new Timer();
        timer.schedule(task, new Date(), EVENT_PRINT_FREQUENCY_MILLIS);
    }

    // Adds a new event to the set of events that happened in the same minute
    public void addEvent(Event event) {
        long time = getTimeWithRoundedSeconds(event.getTs());
        synchronized (events) {
            if (events.containsKey(time)) {
                Set<String> concurrentEvents = events.get(time);
                concurrentEvents.add(event.getUid());
                events.put(time, concurrentEvents);
            } else {
                Set<String> concurrentEvents = new HashSet<>(Arrays.asList(event.getUid()));
                events.put(time, concurrentEvents);
            }
        }
    }

    // Outputs number of unique users per minute;
    // allows max latency of 5 sec before printing users for a given minute.
    // After printing the result per minute, the entry associated with that time slot
    // is immediately deleted as all events are expected to arrive with max latency of 5 sec.
    private void printEventCountPerMinute() {
        synchronized (events) {
            long currentTimeMillis = System.currentTimeMillis();
            Iterator<Long> iterator = events.keySet().iterator();
            while (iterator.hasNext()) {
                long time = iterator.next();
                if (currentTimeMillis - time > MAX_EVENT_AGE_MILLIS) {
                    int eventCount = events.get(time).size();
                    System.out.println("Timestamp " + time + ": " + eventCount + " unique users");
                    iterator.remove();
                }
            }
        }
    }

    // Converts unix timestamp to current time in milliseconds rounded to a full minute
    private static long getTimeWithRoundedSeconds(long timestamp) {
        Calendar calendar = Calendar.getInstance();
        // multiply unix time to get milliseconds
        calendar.setTimeInMillis(timestamp*1000);
        // set seconds to 0 as all events within the same minute should belong to the same time slot
        calendar.set(Calendar.SECOND, 0);
        return calendar.getTimeInMillis();
    }
}
