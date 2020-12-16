package net.intelie.challenges.impl;

import net.intelie.challenges.Event;
import net.intelie.challenges.EventIterator;
import net.intelie.challenges.EventStore;

import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;


/**
 *
 * I considered using the TreeMap that costs O (log n) in its operations, but it is not Thread-safe
 * and if it becomes synchronized, it will create a lock for the entire structure.
 * So I chose ConcurrentSkipListSet which has an average O (log n) cost during searches,
 * but it does not create a lock in the entire structure. More details of Skip List implementation https://en.wikipedia.org/wiki/Skip_list.
 *
 */
public class EventStoreImpl implements EventStore {

    private final ConcurrentSkipListMap<String, ConcurrentSkipListMap<Long, Event>> store = new ConcurrentSkipListMap<>();

    private final boolean IS_START_TIME_INCLUSIVE = true;
    private final boolean IS_END_TIME_INCLUSIVE = false;

    @Override
    public void insert(Event event) {
        // ConcurrentSkipListMap does not permit to use of null keys or values, so it was necessary to check null
        Objects.requireNonNull(event, "event argument should not be null");
        Objects.requireNonNull(event.type(), "event.type argument should not be null");

        store.compute(event.type(), (key, list) -> createListIfNeededAndInsert(event, list));
    }

    private ConcurrentSkipListMap createListIfNeededAndInsert(Event insertedEvent, ConcurrentSkipListMap<Long, Event> list) {
        list = Optional.ofNullable(list).orElse(new ConcurrentSkipListMap<>());
        //  I considered event timestamp is unique by event. This simplify search implementation in EventStore.query method.
        list.put(insertedEvent.timestamp(), insertedEvent);
        return list;
    }

    /**
     *
     * @param type Should be not null
     */
    @Override
    public void removeAll(String type) {
        Objects.requireNonNull(type, "type argument should not be null");
        // Map.remove turns removeAll more simple.
        store.remove(type);
    }

    /**
     *
     * @param type      The type we are querying for. Should be not null
     * @param startTime Start timestamp (inclusive).
     * @param endTime   End timestamp (exclusive).
     * @return
     */
    @Override
    public EventIterator query(String type, long startTime, long endTime) {
        Objects.requireNonNull(type, "type argument should not be null");

        ConcurrentNavigableMap<Long, Event> events = store.get(type);

        if (events != null) {
            ConcurrentNavigableMap<Long, Event> subMap = events.subMap(startTime, IS_START_TIME_INCLUSIVE, endTime, IS_END_TIME_INCLUSIVE);
            return new EventIteratorAdapter(subMap.values().iterator());
        } else {
            // I decide return a empty collection to provide idempotent response
            List<Event> empty = Collections.emptyList();
            return new EventIteratorAdapter(empty.iterator());
        }
    }
}
