package net.intelie.challenges;

import net.intelie.challenges.impl.EventStoreImpl;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class EventStoreImplTest {

    @Test
    public void when_insert_event_with_type_and_timestamp_should_success() {
        final String type = "FAKE_TYPE";
        final long timestamp = 1100;

        Event event = new Event(type, timestamp);
        EventStore eventStore = new EventStoreImpl();
        eventStore.insert(event);

        EventIterator it = eventStore.query(type, 1000, 1200);
        it.moveNext();
        Event result = it.current();

        assertEquals(1100L, result.timestamp());
        assertEquals(type, result.type());
    }

    @Test(expected = NullPointerException.class)
    public void when_insert_event_without_type_should_throw_NullPointException() {
        final long timestamp = 1100;

        Event event = new Event(null, timestamp);
        EventStore eventStore = new EventStoreImpl();
        eventStore.insert(event);
    }

    @Test
    public void when_removeAll_by_type_should_success() {
        final String type = "FAKE_TYPE";
        final long timestamp = 1100;

        Event event = new Event(type, timestamp);
        EventStore eventStore = new EventStoreImpl();
        eventStore.insert(event);

        eventStore.removeAll(type);

        EventIterator it = eventStore.query(type, 1000, 1200);

        assertFalse("EventStore is not empty after removeAll by type '" + type + "'", it.moveNext());
    }

    @Test
    public void when_removeAll_and_type_is_null_should_throw_NullPointException() {

        try {
            EventStore eventStore = new EventStoreImpl();
            eventStore.removeAll(null);
        } catch (NullPointerException e) {
            assertEquals("type argument should not be null", e.getMessage());
        }
    }

    @Test
    public void when_timestamp_equals_endTime_should_return_empty() {
        final String type = "FAKE_TYPE";
        final long timestamp = 1200;

        Event event = new Event(type, timestamp);
        EventStore eventStore = new EventStoreImpl();
        eventStore.insert(event);

        EventIterator it = eventStore.query(type, 1000, 1200);
        assertFalse(it.moveNext());
    }

    @Test
    public void when_type_not_exist_should_return_empty() {
        final String typeForQuery = "FAKE_TYPE_NOT_EXIST";
        final String type = "FAKE_TYPE";
        final long timestamp = 1100;

        Event event = new Event(type, timestamp);
        EventStore eventStore = new EventStoreImpl();
        eventStore.insert(event);

        EventIterator it = eventStore.query(typeForQuery, 1000, 1200);
        assertFalse(it.moveNext());
    }

    @Test
    public void when_timestamp_equals_startTime_should_return_event() {
        final String type = "FAKE_TYPE";
        final long timestamp = 1000;

        Event event = new Event(type, timestamp);
        EventStore eventStore = new EventStoreImpl();
        eventStore.insert(event);

        EventIterator it = eventStore.query(type, 1000, 1200);
        it.moveNext();
        Event result = it.current();
        assertEquals(type, result.type());
        assertEquals(timestamp, result.timestamp());
    }

    @Test
    public void when_timestamp_between_startTime_and_endTime_should_return_event() {
        final String type = "FAKE_TYPE";
        final long timestamp = 1100;

        Event event = new Event(type, timestamp);
        EventStore eventStore = new EventStoreImpl();
        eventStore.insert(event);

        EventIterator it = eventStore.query(type, 1000, 1200);
        it.moveNext();
        Event result = it.current();
        assertEquals(type, result.type());
        assertEquals(timestamp, result.timestamp());
    }

}
