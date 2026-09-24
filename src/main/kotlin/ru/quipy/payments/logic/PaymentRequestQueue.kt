package ru.quipy.payments.logic

import ru.quipy.common.utils.SlidingWindowRateLimiter
import java.time.Duration
import java.util.concurrent.ArrayBlockingQueue

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

    @Volatile
    private var running = true

    private val workers: List<Thread> = (0 until parallelRequests).map { idx ->
        Thread({ workerLoop() }, "payment-$name-worker-$idx").apply {
            isDaemon = true
            start()
        }
    }

    fun submit(task: PaymentTask) {
        if (!queue.offer(task)) {
            onReject(task, "Payment queue overflow")
            return
        }
    }

    fun pendingRequests(): Int = queue.size

    fun shutdown() {
        running = false
        workers.forEach { it.interrupt() }
    }

    private fun workerLoop() {
        while (running) {
            val task = try {
                queue.take()
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
                return
            }

            processTask(task)
        }
    }

    private fun processTask(task: PaymentTask) {
        val now = System.currentTimeMillis()
        val maxWait = Duration.ofMillis(task.deadline - now)
            .minus(averageProcessingTime)

        if (!limiter.tickBlocking(maxWait)) {
            onReject(task, "Could not acquire rate limit slot before deadline ${task.deadline} (now: $now)")
            return
        }

        onExecute(task)
    }
}
