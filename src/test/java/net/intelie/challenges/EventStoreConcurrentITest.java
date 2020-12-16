package net.intelie.challenges;

import net.intelie.challenges.impl.EventStoreImpl;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * This test class cover concurrent scenarios on my EventStore implementation
 */
public class EventStoreConcurrentITest {

    private class WorkerThreadToInsert implements Runnable {

        private final String id;
        private final EventStore store;
        private final AtomicLong fakeTimeStampCounter;

        public WorkerThreadToInsert(String id, EventStore store, AtomicLong fakeTimeStampCounter) {
            this.id = id;
            this.store = store;
            this.fakeTimeStampCounter = fakeTimeStampCounter;
        }

        @Override
        public void run() {
            for (int i = 0; i < 1000; i++) {
                System.out.println(Thread.currentThread().getName() + " WorkerName=" + id);
                long eventFakeTimestamp = fakeTimeStampCounter.getAndIncrement();
                Event event = new Event("TEST_TYPE", eventFakeTimestamp);
                store.insert(event);
            }
        }
    }

    @Test
    public void when_inserts_with_concurrent_should_be_consistent() throws InterruptedException {
        long fakeTimestampInitialValue = 1; // I use a counter to fake a timestamp to validate consistent during insertions
        final AtomicLong fakeTimeStampCounter = new AtomicLong(fakeTimestampInitialValue);
        final EventStore store = new EventStoreImpl();

        ExecutorService executor = Executors.newScheduledThreadPool(20);
        for (int i = 0; i < 30; i++) {
            executor.execute(new WorkerThreadToInsert(String.valueOf(i), store, fakeTimeStampCounter));
        }

        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.MINUTES);

        long endTimeStampPlusOne = fakeTimeStampCounter.get() + 1l;
        EventIterator it = store.query("TEST_TYPE", 1, endTimeStampPlusOne);

        int resultCount = 0;
        while (it.moveNext()) {
            resultCount++;
            Event current = it.current();
            System.out.println("Event type=" + current.type() + " time=" + current.timestamp());
        }

        // I know how many records by ("the last fake timestamp" - 1) and I assert here
        assertEquals(fakeTimeStampCounter.get() - 1, resultCount);
    }

    @Test
    public void when_execute_all_operations_in_concurrent_should_be_success() throws InterruptedException {
        final String type = "TEST_TYPE";
        final AtomicBoolean hasExceptionDuringInsertion = new AtomicBoolean(false);
        final AtomicBoolean hasExceptionDuringRemoveAll = new AtomicBoolean(false);
        final AtomicBoolean hasExceptionDuringQuery = new AtomicBoolean(false);

        final EventStore store = new EventStoreImpl();

        final long endTime = System.currentTimeMillis() + 1000;

        Runnable insertTask = () -> {
            try {
                Event event = new Event(type, System.currentTimeMillis());
                store.insert(event);
            } catch (Exception e) {
                hasExceptionDuringInsertion.set(true);
                e.printStackTrace();
            }
        };

        Runnable removeAllTask = () -> {
            try {
                store.removeAll(type);
            } catch (IllegalStateException e) {
                hasExceptionDuringRemoveAll.set(true);
                e.printStackTrace();
            }
        };

        Runnable queryTask = () -> {
            try {
                store.query(type, System.currentTimeMillis(), endTime);
            } catch (IllegalStateException e) {
                hasExceptionDuringQuery.set(true);
                e.printStackTrace();
            }
        };

        ExecutorService executor = Executors.newScheduledThreadPool(20);
        for (int i = 0; i < 30; i++) {
            executor.execute(insertTask);
            executor.execute(removeAllTask);
            executor.execute(queryTask);
        }

        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.MINUTES);

        assertFalse("has a exception during insertion", hasExceptionDuringInsertion.get());
        assertFalse("has a exception during removeAll",  hasExceptionDuringRemoveAll.get());
        assertFalse("has a exception during query", hasExceptionDuringQuery.get());
    }
}