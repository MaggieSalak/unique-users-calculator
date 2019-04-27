# doodle-data-challenge

## Setup
The solution is provided as a desktop Java application.
The source code can be found in `src/com.salak.maggie/` directory. 
To run the application, simply run the `main()` method in `src/com.salak.maggie/Main.java`. Results (unique user count per minute) are printed to stdout.
All configuration needed for the Kafka consumer is already set up in the `DataConsumer.java` class. The topic name is configured in `Main.java` and is set to `data-challenge-topic`.
All libraries needed to run the application are in the `libs/` directory.

## Approach
The Kafka consumer is implemented in the `DataConsumer.java` class. Every consumed message is converted to an `Event` object that contains the timestamp and user id extracted from the message.

The event is then added to the `EventAggregator` that is responsible for the management of the state of received events.
This is accomplished with a map where timestamps are mapped to sets of unique user ids and the time when entry was last updated (`LastUpdatedSet` class).
In order to allow event aggragation per minute, timestamps are always rounded to a full minute, so that events with timestamps within the same minute are added to the same time slot by the aggreagator.
To allow processing historical data, we also keep track of when each entry in the state map was last updated (otherise all events would immediately be 'outdated').

The events state is processed every second. Since we can expect that 99.9% of events arrive with a maximum latency of 5 seconds, unique user count is printed when 5 seconds have elapsed since the entry was last updated. 
After printing the result, the entry is deleted, since no more events with this timestamp are expected to arrive. In case this happens, the new event would be treated as a new entry in the state map, and would be printed after the allowed timeout of 5 seconds, but without including the past events that were already deleted.

## Possible improvements
The current solution only allows events aggregation per second. In order to support aggregation per minute, day, week, month, year, one possible solution could be to implement several aggregators, where each of them would be responsible for aggregation on the respective level (e.g. per hour).
In this approach every event would be added to each of the aggregators with the timestamp rounded up accordingly (e.g. to a full hour). In this solution every event would be stored in memory in several copies, however, printing of the results would require a single iteration over each of the states and the lookup of a single entry in the state map having `O(1)` complexity (since we are using a HashMap).

In the current solution, calculation errors may occur since not 100% of events are guaranteed to arrive within 5 seconds. 
To address this problem, one possible approach could be to, instead of just deleting outdated observations from memory, store them in a file or database. In that case, when an 'old' event is received (for which we have no data in memory), instead of creating a fresh entry in the state map, we could check if there is already an entry for this timestamp the file/table.



