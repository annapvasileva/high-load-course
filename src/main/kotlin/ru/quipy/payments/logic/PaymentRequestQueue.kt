package ru.quipy.payments.logic

import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingQuantile
import ru.quipy.common.utils.SlidingWindowRateLimiter
import java.time.Duration
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.Semaphore
import java.util.concurrent.SynchronousQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

enum class PaymentRejectPhase {
    ENQUEUE,
    DISPATCH,
}

class PaymentRequestQueue(
    name: String,
    rateLimitPerSec: Int,
    parallelRequests: Int,
    queueCapacity: Int,
    private val averageProcessingTime: Duration,
    private val onExecute: (PaymentTask) -> Unit,
    private val onReject: (PaymentTask, String, PaymentRejectPhase) -> Unit,
) {
    companion object {
        val logger = LoggerFactory.getLogger(PaymentSystemImpl::class.java)
    }

    private val limiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))

    private val throughputPerSec: Double = minOf(
        rateLimitPerSec.toDouble(),
        parallelRequests * 1000.0 / averageProcessingTime.toMillis(),
    )

    private val queue = ArrayBlockingQueue<PaymentTask>(queueCapacity)

    private val slidingQuantile = SlidingQuantile(
        windowSize  = 1000,
        bucketWidth = averageProcessingTime.toMillis() / 3,
        numBuckets  = 30
    )

    public fun pendingRequests(): Int { return queue.size }

    fun submit(task: PaymentTask) {
        val now = System.currentTimeMillis()
        val processingTimeMillis = task.deadline - now
        val n = processingTimeMillis / 1000.0 * throughputPerSec
        if (queue.size >= n * 10000) {
            onReject(
                task,
                "queue ahead ${queue.size} >= N=$n (v=$throughputPerSec) before deadline ${task.deadline} (now: $now)",
                PaymentRejectPhase.ENQUEUE,
            )
            return
        }

        if (!queue.offer(task)) {
            onReject(task, "Payment queue overflow", PaymentRejectPhase.ENQUEUE)
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
        logger.info("Q: ${slidingQuantile.quantile(0.97)} - BS: ${averageProcessingTime.toMillis() / 4} - Last: ${slidingQuantile.quantile(1.0)}")
        val maxWait = Duration.ofMillis(task.deadline - now - slidingQuantile.quantile(0.97))

        if (!limiter.tickBlocking(maxWait)) {
            try {
                onReject(
                    task,
                    "no rate limit slot before deadline ${task.deadline} (now: $now)",
                    PaymentRejectPhase.DISPATCH,
                )
            } finally {
                slots.release()
            }
            return
        }

        try {
            executor.execute {
                try {
                    val before = System.currentTimeMillis()
                    onExecute(task)
                    slidingQuantile.add(System.currentTimeMillis() - before)
                } finally {
                    slots.release()
                }
            }
        } catch (e: RejectedExecutionException) {
            slots.release()
            onReject(task, "executor is shut down", PaymentRejectPhase.DISPATCH)
        }
    }
}