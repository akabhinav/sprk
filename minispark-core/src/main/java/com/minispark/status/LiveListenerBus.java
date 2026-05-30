package com.minispark.status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Asynchronous fan-out of {@link SchedulerEvent}s to registered
 * {@link SchedulerListener}s. Posting is non-blocking (events go onto a queue);
 * a single daemon thread drains the queue and notifies listeners in order.
 *
 * <p><b>Why async, single-consumer.</b> The scheduler posts events from hot
 * paths (RPC threads, the DAG thread). If listeners ran inline, a slow listener
 * would stall scheduling. Draining on one thread keeps event ordering simple
 * and listeners single-threaded (no locking needed in a listener).
 *
 * Real Spark equivalent: org.apache.spark.scheduler.LiveListenerBus
 */
public final class LiveListenerBus {

    private static final Logger LOG = LoggerFactory.getLogger(LiveListenerBus.class);
    private static final SchedulerEvent POISON = new SchedulerEvent.JobEnd(-1, false, 0);

    private final List<SchedulerListener> listeners = new CopyOnWriteArrayList<>();
    private final LinkedBlockingQueue<SchedulerEvent> queue = new LinkedBlockingQueue<>();
    private final Thread dispatcher;
    private volatile boolean stopped = false;

    public LiveListenerBus() {
        this.dispatcher = new Thread(this::dispatchLoop, "listener-bus");
        this.dispatcher.setDaemon(true);
        this.dispatcher.start();
    }

    public void addListener(SchedulerListener listener) { listeners.add(listener); }

    /** Non-blocking. Dropped silently after {@link #stop()}. */
    public void post(SchedulerEvent event) {
        if (stopped) return;
        queue.offer(event);
    }

    private void dispatchLoop() {
        while (true) {
            SchedulerEvent event;
            try {
                event = queue.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            if (event == POISON) return;
            for (SchedulerListener l : listeners) {
                try {
                    l.onEvent(event);
                } catch (Throwable t) {
                    LOG.warn("Listener {} threw on {}: {}", l, event, t.toString());
                }
            }
        }
    }

    public void stop() {
        stopped = true;
        queue.offer(POISON);
    }
}
