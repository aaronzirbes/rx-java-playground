package org.zirbes.rxjava

import groovy.transform.CompileStatic

import java.util.concurrent.BlockingQueue
import java.util.concurrent.Executor
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.RejectedExecutionHandler
import java.util.concurrent.ThreadFactory
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

import rx.internal.util.RxThreadFactory

@CompileStatic
class ObservableExecutor {

    static final int CORE_POOL_SIZE = 32

    // ~2500 over 15 minutes = ~160 /min needed.
    static final int MAX_POOL_SIZE = 512

    /** Keep this small so as not to leave too many items in the queue **/
    static final int QUEUE_CAPACITY = 32

    /** Keep alive for unused threads */
    static final long KEEP_ALIVE = 60
    static final TimeUnit KEEP_ALIVE_UNIT = TimeUnit.SECONDS

    static Executor simple() {
        Executors.newCachedThreadPool()
    }

    static Executor create(String name) {
        BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<Runnable>(QUEUE_CAPACITY)
        ThreadFactory factory = new RxThreadFactory(name)
        RejectedExecutionHandler handler = new ThreadPoolExecutor.DiscardOldestPolicy()
        return new ThreadPoolExecutor(CORE_POOL_SIZE, MAX_POOL_SIZE, KEEP_ALIVE, KEEP_ALIVE_UNIT, workQueue, factory)
    }
}
