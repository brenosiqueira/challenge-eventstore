package net.intelie.challenges.impl;

import net.intelie.challenges.Event;
import net.intelie.challenges.EventIterator;

import java.util.Iterator;

public final class EventIteratorAdapter implements EventIterator {

    private final Iterator<Event> events;
    private Event currentEvent = null;

    public EventIteratorAdapter(Iterator<Event> events) {
        this.events = events;
    }

    @Override
    public boolean moveNext() {
        if (events.hasNext()) {
            currentEvent = events.next();
            return true;
        }
        currentEvent = null;
        return false;
    }

    @Override
    public Event current() {
        checkStoppedIterator();

        return currentEvent;
    }

    @Override
    public void remove() {
        checkStoppedIterator();

        events.remove();
    }

    @Override
    public void close() throws Exception {
        // Not implemented
    }

    private void checkStoppedIterator() {
        if (currentEvent == null) {
            throw new IllegalStateException("Method moveNext is never called or iterator is over.");
        }
    }
}