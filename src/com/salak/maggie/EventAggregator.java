package com.salak.maggie;

import java.text.DateFormat;
import java.util.*;

/**
 * EventAggregator is responsible for managing
 * unique users (uids) per required time slot (1 min)
 */
public class EventAggregator {

    // LastUpdatedUidSet represents a set of user ids
    // and a timestamp when the set was last updated
    private class LastUpdatedUidSet {
        Set<String> uids;
        long lastUpdatedMillis;

        LastUpdatedUidSet(String uid) {
            uids = new HashSet<>(Arrays.asList(uid));
            lastUpdatedMillis = System.currentTimeMillis();
        }

        void addUid(String uid) {
            uids.add(uid);
            lastUpdatedMillis = System.currentTimeMillis();
        }
    }

    // Unique users (represented as uids) per minute (represented in milliseconds)
    private final Map<Long, LastUpdatedUidSet> events = new HashMap<>();

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
                LastUpdatedUidSet eventsSet = events.get(time);
                eventsSet.addUid(event.getUid());
                events.put(time, eventsSet);
            } else {
                LastUpdatedUidSet eventsSet = new LastUpdatedUidSet(event.getUid());
                events.put(time, eventsSet);
            }
        }
    }

    // Outputs number of unique users per minute;
    // allows max latency of 5 sec  before printing users for a given minute,
    // counting from when the last event was received for the respective time slot.
    // After printing the result per minute, the entry associated with that time slot is deleted.
    private void printEventCountPerMinute() {
        synchronized (events) {
            long currentTimeMillis = System.currentTimeMillis();

            Iterator<Long> iterator = events.keySet().iterator();
            while (iterator.hasNext()) {
                long timestamp = iterator.next();
                LastUpdatedUidSet eventsSet = events.get(timestamp);
                long lastUpdatedTime = eventsSet.lastUpdatedMillis;

                if (currentTimeMillis - lastUpdatedTime > MAX_EVENT_AGE_MILLIS) {
                    String time = timestampToDate(timestamp);
                    int eventCount = eventsSet.uids.size();
                    System.out.println(time + ": " + eventCount + " unique users");
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

    private static String timestampToDate(long timestamp) {
        return DateFormat.getDateTimeInstance().format(new Date(timestamp));
    }
}
