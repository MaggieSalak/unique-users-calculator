# unique-users-calculator

## About

unique-users-calculator is a standalone Java application that consumes messages from a Kafka topic which contain information about users visiting a website at specific times. The program calculates the number of unique users that visited the website in 1-minute intervals and outputs the result.

## Setup

The solution is provided as a desktop Java application.  
The source code can be found in the `src/com.salak.maggie/` directory.  
To run the application, simply run the `main()` method in `src/com.salak.maggie/Main.java`. Results (unique user count per minute) are printed to stdout.  
All configuration needed for the Kafka consumer is already set up in the `DataConsumer.java` class. The topic name is configured in `Main.java` and is set to `unique-users-topic`.  
All libraries needed to run the application are in the `libs/` directory.

*The producer that emits messages to the topic is not part of the application. The producer needs to be started separately, e.g., using the standard Kafka producer as described in [Kafka documentation](https://kafka.apache.org/quickstart).*

## Approach

The Kafka consumer is implemented in the `DataConsumer.java` class. Every consumed message is converted to an `Event` object that contains the timestamp and user ID extracted from the message.

The event is then added to the `EventAggregator` that is responsible for the management of the state of received events.  
This is accomplished with a map where timestamps are mapped to sets of unique user IDs and the time when the entry was last updated (`LastUpdatedSet` class).  
In order to allow event aggregation per minute, timestamps are always rounded to a full minute, so that events with timestamps within the same minute are added to the same time slot by the aggregator.  
To allow processing historical data, we also keep track of when each entry in the state map was last updated (otherwise, all events would immediately be 'outdated').

The events state is processed every second. Since we can expect that 99.9% of events arrive with a maximum latency of 5 seconds, the unique user count is printed when 5 seconds have elapsed since the entry was last updated.  
After printing the result, the entry is deleted, since no more events with this timestamp are expected to arrive. In case this happens, the new event would be treated as a new entry in the state map and would be printed after the allowed timeout of 5 seconds, but without including the past events that were already deleted.
