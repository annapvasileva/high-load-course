package ru.quipy.payments.logic

import ru.quipy.common.utils.SlidingWindowRateLimiter
import java.time.Duration
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.Semaphore
import java.util.concurrent.SynchronousQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

class PaymentRequestQueue(
    name: String,
    rateLimitPerSec: Int,
    parallelRequests: Int,
    queueCapacity: Int,
    private val averageProcessingTime: Duration,
    private val onExecute: (PaymentTask) -> Unit,
    private val onReject: (PaymentTask, String) -> Unit,
) {
    private val limiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))

    private val queue = ArrayBlockingQueue<PaymentTask>(queueCapacity)

    public fun pendingRequests(): Int { return queue.size }

    fun submit(task: PaymentTask) {
        if (!queue.offer(task)) {
            onReject(task, "Payment queue overflow")
            return
        }
    }

    @Volatile
    private var running = true

    private val slots = Semaphore(parallelRequests)

    private val dispatcher = Thread(::dispatcherLoop, "payment-$name-dispatcher").apply {
        isDaemon = true
        start()
    }

    private val executor = ThreadPoolExecutor(
        parallelRequests, parallelRequests,
        0L, TimeUnit.MILLISECONDS,
        SynchronousQueue(),
        { r -> Thread(r, "payment-$name-worker").apply { isDaemon = true } },
        ThreadPoolExecutor.AbortPolicy()
    )

    private fun dispatcherLoop() {
        while (running) {
            val task = try {
                queue.take()
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
                return
            }
            dispatch(task)
        }
    }

    private fun dispatch(task: PaymentTask) {
        try {
            slots.acquire()
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            return
        }

        val now = System.currentTimeMillis()
        val maxWait = Duration.ofMillis(task.deadline - now).minus(averageProcessingTime)

        if (!limiter.tickBlocking(maxWait)) {
            onReject(task, "no rate limit slot before deadline ${task.deadline} (now: $now)")
            slots.release()
            return
        }

        try {
            executor.execute {
                try {
                    onExecute(task)
                } finally {
                    slots.release()
                }
            }
        } catch (e: RejectedExecutionException) {
            slots.release()
            onReject(task, "executor is shut down")
        }
    }
}