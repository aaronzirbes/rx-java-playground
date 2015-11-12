package org.zirbes.rxjava

import groovy.util.logging.Slf4j

import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

import rx.Observable
import rx.Scheduler
import rx.Subscriber
import rx.internal.schedulers.SchedulerLifecycle

@Slf4j
class ObservableQueue<T> {

    protected long pollTimeout = 1

    @Delegate
    protected Observable observable
    protected LinkedBlockingQueue queue
    protected Subscriber subscriber

    ObservableQueue() {
        this.queue = new LinkedBlockingQueue<T>()
        this.observable = getObservableForQueue()
    }

    ObservableQueue onScheduler(Scheduler scheduler) {
        return observeOn(scheduler).subscribeOn(scheduler)
    }

    ObservableQueue observeOn(Scheduler scheduler) {
        observable = observable.observeOn(scheduler)
        return this
    }

    ObservableQueue subscribeOn(Scheduler scheduler) {
        observable = observable.subscribeOn(scheduler)
        return this
    }

    ObservableQueue throttled(TimeUnit timeUnit, long time) {
        observable = observable.throttleLast(time, timeUnit)
        return this
    }

    ObservableQueue wtihQueuePollTimeout(long seconds) {
        this.pollTimeout = seconds
        return this
    }

    void unsubscribe() {
        log.info "Unsubscribing from queue."
        subscriber?.unsubscribe()
    }

    boolean isUnsubscribed() {
        if (!subscriber) { return true }
        return subscriber.unsubscribed
    }

    void add(T thing) {
        queue.add(thing)
    }

    protected Observable getObservableForQueue() {
        return Observable.create({ Subscriber<T> subscriber ->
            this.subscriber = subscriber
            while (!subscriber.unsubscribed) {
                T thing = queue.poll(pollTimeout, TimeUnit.SECONDS)
                if (thing != null) { subscriber.onNext(thing) }
            }
            log.info "Exiting observable loop and clearing queue."
            queue.clear()
        } as Observable.OnSubscribe<T>)
    }

}

