package net.intelie.challenges;

import net.intelie.challenges.impl.EventIteratorAdapter;
import net.intelie.challenges.impl.EventStoreImpl;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class EventIteratorAdapterTest {

    @Test
    public void when_moveNext_is_true_and_call_current_should_success() {
        final String type = "FAKE_TYPE";
        final long timestamp = 120;

        Event event = new Event(type, timestamp);

        EventStore eventStore = new EventStoreImpl();
        eventStore.insert(event);

        EventIterator it = new EventIteratorAdapter(Arrays.asList(event).iterator());

        try {
            it.moveNext();
            Event current = it.current();
            assertEquals(timestamp, current.timestamp());
            assertEquals("FAKE_TYPE", current.type());

        } catch (IllegalStateException e) {
            Assert.fail("This test should be not raise a IllegalStateException" + e);
        }
    }

    @Test
    public void when_moveNext_is_true_and_remove_should_success() {
        final String type = "FAKE_TYPE";
        final long timestamp = 120;

        Event event = new Event(type, timestamp);

        EventStore eventStore = new EventStoreImpl();
        eventStore.insert(event);

        EventIterator it = new EventIteratorAdapter(new ArrayList(Arrays.asList(event)).iterator());

        try {
            it.moveNext();
            it.remove();
            Assert.assertFalse(it.moveNext());

        } catch (IllegalStateException e) {
            Assert.fail("This test should be not raise a IllegalStateException" + e);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void when_moveNext_is_false_call_current_should_throw_IllegalStateException() {
        final String type = "FAKE_TYPE";

        Event event = new Event(type, 123L);

        EventStore eventStore = new EventStoreImpl();
        eventStore.insert(event);

        EventIterator it = new EventIteratorAdapter(Arrays.asList(event).iterator());

        while (it.moveNext()) {
        }
        it.current();// should throw IllegalStateException
    }

    @Test(expected = IllegalStateException.class)
    public void when_moveNext_is_never_called_and_get_current_should_throw_IllegalStateException() {
        final String type = "FAKE_TYPE";

        Event event = new Event(type, 123L);

        EventStore eventStore = new EventStoreImpl();
        eventStore.insert(event);

        EventIterator it = new EventIteratorAdapter(Arrays.asList(event).iterator());

        it.current();// should throw IllegalStateException
    }

    @Test(expected = IllegalStateException.class)
    public void when_moveNext_is_never_called_and_remove_should_throw_IllegalStateException() {
        final String type = "FAKE_TYPE";

        Event event = new Event(type, 123L);

        EventStore eventStore = new EventStoreImpl();
        eventStore.insert(event);

        EventIterator it = new EventIteratorAdapter(Arrays.asList(event).iterator());

        it.remove();// should throw IllegalStateException
    }
}
