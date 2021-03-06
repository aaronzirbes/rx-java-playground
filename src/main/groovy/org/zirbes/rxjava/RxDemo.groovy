package org.zirbes.rxjava

import com.fasterxml.jackson.databind.ObjectMapper

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

import org.joda.time.LocalDateTime

import rx.Scheduler
import rx.schedulers.Schedulers

import rx.schedulers.CachedThreadScheduler
import rx.schedulers.ImmediateScheduler
import rx.internal.schedulers.EventLoopsScheduler
import rx.schedulers.NewThreadScheduler

@CompileStatic
@Slf4j
class RxDemo {

    protected ObjectMapper objectMapper = new ObjectMapper()

    ObservableQueue queue = new ObservableQueue<Bird>()

    protected TimeUnit throttleUnit = TimeUnit.SECONDS
    protected int throttleTime = 20

    protected TimeUnit queueFlushUnit = TimeUnit.SECONDS
    protected int queueFlushTime = 22

    final NavigableMap<LocalDateTime, String> initialKeyTime = new ConcurrentSkipListMap<LocalDateTime, String>()
    final Map<String, ObservableQueue> pushQueues = new ConcurrentHashMap<String, ObservableQueue>()

    protected final Scheduler observe = Schedulers.from(ObservableExecutor.create('observe'))
    protected final Scheduler subscribe = Schedulers.from(ObservableExecutor.create('subscribe'))
    protected final Scheduler throttle = Schedulers.from(ObservableExecutor.create('throttle'))

    RxDemo() { }

    void run() {
        int counter = 0
        while (true) {
            counter++
            Bird bird = new Bird(counter)
            log.info "                     ++++++++ Queueing bird ${bird.name}"
            queueBird(bird)
            Thread.sleep(50)
        }
    }

    /** Queue the bird update for publishing to the event ledger */
    void queueBird(Bird bird) {
        String key = bird.name
        getQueue(key).add(bird)
        cleanUpQueues(key)
    }

    /** Clean up any other queues that have passed the queue expiration threshold */
    protected void cleanUpQueues(String queueKey) {
        int throttle = (int) throttleUnit.toMillis(throttleTime)
        int queueFlush = (int) queueFlushUnit.toMillis(queueFlushTime)

        LocalDateTime threshold = LocalDateTime.now().minusMillis(throttle).minusMillis(queueFlush)
        Map<LocalDateTime, String> expiredKeyMap = initialKeyTime.headMap(threshold)
        // Don't remove self
        LocalDateTime self = expiredKeyMap.find{ k, v -> (v == queueKey) }?.key
        if (self) { expiredKeyMap.remove(self) }
        // get the list of keys to remove
        Set<LocalDateTime> expiredTimes = expiredKeyMap.keySet()
        Collection<String> expiredKeys = expiredKeyMap.values()
        // Unsubscribe to all expired observables
        expiredKeys.each{ String key -> pushQueues[key]?.unsubscribe() }
        // Remove them from the queue map
        expiredKeys.each{ pushQueues.remove(it) }
        // Remove them from the time map
        expiredTimes.each{ initialKeyTime.remove(it) }
        //log.info "initialKeyTime size: ${initialKeyTime.size()}, pushQueues size: ${pushQueues.size()}"
    }

    /** Get an existing observable queue, or create a new one */
    protected ObservableQueue getQueue(String queueKey) {
        return pushQueues.compute(queueKey) { String k, ObservableQueue v ->
            if (v) { return v }
            // Log create time so we can clean it up
            initialKeyTime[LocalDateTime.now()] = queueKey
            // Create the observable queue
            return getWorkerQueue()
        }
    }

    /** Get an observable queue and subscribe to it's throttled puts to publish to the event ledger */
    protected ObservableQueue getWorkerQueue() {

        ObservableQueue queue = new ObservableQueue<Bird>().observeOn(observe)
                                                           .subscribeOn(subscribe)
                                                           .throttleLast(throttleUnit, throttleTime, throttle)
        return pushUpdatesFromQueue(queue)
    }

    protected ObservableQueue pushUpdatesFromQueue(ObservableQueue queue) {
        queue.subscribe(
            { Bird bird ->
                sendBird(bird)
            }, { Throwable t ->
                log.error "Error while sending updated bird ${t.class}", t
            }
        )
        return queue
    }

    // Computational Thread Pool (computation)
    protected void sendBird(Bird bird) {
        log.info "         >>>>>>>> Sending ${bird}"
    }

}
