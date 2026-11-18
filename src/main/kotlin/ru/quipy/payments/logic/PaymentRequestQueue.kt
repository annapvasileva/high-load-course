package ru.quipy.payments.logic

import io.micrometer.core.instrument.Clock
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.distribution.TimeWindowMax
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingAverage
import ru.quipy.common.utils.SlidingQuantile
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.payments.logic.PaymentExternalSystemAdapterImpl.Companion.MAX_METRIC_BUFFER_LENGTH
import ru.quipy.payments.logic.PaymentExternalSystemAdapterImpl.Companion.MAX_METRIC_ROTATION_MILLIS
import java.time.Duration
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.comparisons.maxOf
import kotlin.math.max

enum class PaymentRejectPhase {
    ENQUEUE,
    DISPATCH,
}

sealed interface SubmitResult {
    data object Accepted : SubmitResult

    data class Retryable(
        val reason: String,
        val retryAfterSeconds: Long,
    ) : SubmitResult

    data class BadRequest(
        val reason: String,
        val minRequiredDeadlineMs: Long,
    ) : SubmitResult
}

class PaymentRequestQueue(
    name: String,
    rateLimitPerSec: Int,
    parallelRequests: Int,
    queueCapacity: Int,
    private val meterRegistry: MeterRegistry,
    private val averageProcessingTime: Duration,
    private val onExecute: (PaymentTask, () -> Unit) -> Unit,
    private val onReject: (PaymentTask, String, PaymentRejectPhase) -> Unit,
) {
    companion object {
        private val logger =
            LoggerFactory.getLogger(PaymentRequestQueue::class.java)

        private const val OVERLOAD_THRESHOLD = 0.95
        private const val MAX_RETRY_AFTER_SEC = 30L
        private const val MIN_DRAIN_FRACTION = 0.1
        private const val RETRY_JITTER_FRACTION = 0.3

        private const val DEADLINE_REJECT_QUANTILE = 0.90
        private const val QUANTILE_COLD_START_SIZE = 100
        private const val QUANTILE_COLD_START_AVERAGE_FACTOR = 1.2
    }

    init {
        require(rateLimitPerSec > 0)
        require(parallelRequests > 0)
        require(queueCapacity > 0)
        require(averageProcessingTime.toMillis() > 0)
    }

    private val limiter = SlidingWindowRateLimiter(
        rateLimitPerSec.toLong(),
        Duration.ofSeconds(1),
    )

    private val avgMillis = averageProcessingTime.toMillis()

    private val coldStartTimeMillis: Long = (avgMillis * QUANTILE_COLD_START_AVERAGE_FACTOR).toLong()

    private val throughputPerSec = minOf(
        rateLimitPerSec.toDouble(),
        parallelRequests * 1000.0 / avgMillis,
    )

    private val arrivalTracker = SlidingAverage(bucketCount = 10, bucketNanos = 100_000_000L)

    private val queue = ArrayBlockingQueue<PaymentTask>(queueCapacity)

    private val queueSizeMax = TimeWindowMax(
        Clock.SYSTEM,
        MAX_METRIC_ROTATION_MILLIS,
        MAX_METRIC_BUFFER_LENGTH,
    )
    private val queueSizeMaxMetric = Gauge.builder("payment_request_queue_size_max",) { queueSizeMax.poll() }
        .tags("account", name)
        .register(meterRegistry)

    private val processingTimes = SlidingQuantile(
        windowSize = 1000,
        bucketWidth = max(1L, avgMillis / 10),
        numBuckets = 100,
    )

    private fun reserveMillis(): Long {
        // Prevent wrong quantile on small dataset
        if (processingTimes.count() < QUANTILE_COLD_START_SIZE) {
            return coldStartTimeMillis
        }

        // Considering coldStartTimeMillis is basically less than quantile
        // But to prevent quantile from degradation into a "deny all" scenario we use maxOf
        return maxOf(processingTimes.quantile(DEADLINE_REJECT_QUANTILE), coldStartTimeMillis)
    }

    fun pendingRequests(): Int = queue.size

    private fun retryAfterSeconds(excess: Double): Long {
        if (excess <= 0.0) return 1L
        val lambda = arrivalTracker.ratePerSec()
        val loadFactor = lambda / throughputPerSec

        if (loadFactor >= OVERLOAD_THRESHOLD) return MAX_RETRY_AFTER_SEC

        val drain = (throughputPerSec - lambda)
            .coerceAtLeast(throughputPerSec * MIN_DRAIN_FRACTION)

        val baseSec = excess / drain
        val jittered = baseSec * (1.0 + ThreadLocalRandom.current().nextDouble(0.0, RETRY_JITTER_FRACTION))
        return kotlin.math.ceil(jittered).toLong().coerceIn(1L, MAX_RETRY_AFTER_SEC)
    }

    private fun reject(
        task: PaymentTask,
        reason: String,
        phase: PaymentRejectPhase,
    ) {
        try {
            onReject(task, reason, phase)
        } catch (e: Exception) {
            logger.error(
                "Failed to record rejection for payment ${task.paymentId}, " +
                        "phase=$phase, reason=$reason",
                e,
            )
        }
    }

    fun submit(task: PaymentTask): SubmitResult {
        arrivalTracker.record()

        val now = System.currentTimeMillis()
        val reserve = reserveMillis()
        val remaining = task.deadline - now - reserve

        if (remaining <= 0) {
            val reason = "not enough time left at enqueue (now: $now, reserve: $reserve)"
            reject(task, reason, PaymentRejectPhase.ENQUEUE)
            logger.error(reason)
            return SubmitResult.BadRequest(reason, minRequiredDeadlineMs = reserve)
        }

        // Submit reject if we suppose that queue size is large enough to block request for deadline time
        // remaining is time that we can wait until it would be too late (after that there would be reject by deadline)
        // maxQueueAhead is how many requests in average we can process with remaining time
        // If we can process fewer requests than we actually have, we preventively deny submitting
        val maxQueueAhead = remaining / 1000.0 * throughputPerSec
        if (queue.size >= maxQueueAhead) {
            val excess = queue.size - maxQueueAhead
            val reason = "queue ahead ${queue.size} >= N=$maxQueueAhead (v=$throughputPerSec)"
            reject(task, reason, PaymentRejectPhase.ENQUEUE)
            return SubmitResult.Retryable(reason, retryAfterSeconds(excess))
        }

        if (!queue.offer(task)) {
            val reason = "Payment queue overflow"
            reject(task, reason, PaymentRejectPhase.ENQUEUE)
            return SubmitResult.Retryable(reason, retryAfterSeconds(queue.size.toDouble()))
        }
        queueSizeMax.record(queue.size.toDouble())

        return SubmitResult.Accepted
    }

    private val slots = Semaphore(parallelRequests)

    private val dispatcher = Thread(
        ::dispatcherLoop,
        "payment-$name-dispatcher",
    ).apply {
        isDaemon = true
        start()
    }

    private fun dispatcherLoop() {
        while (true) {
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
        // Limit for parallel at time
        try {
            slots.acquire()
        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()

            reject(
                task,
                "dispatcher interrupted while acquiring worker slot",
                PaymentRejectPhase.DISPATCH,
            )
            return
        }

        val finished = AtomicBoolean(false)
        var startedAtNanos: Long? = null
        var handedOff = false

        fun finish() {
            if (!finished.compareAndSet(false, true)) {
                return
            }

            startedAtNanos?.let { started ->
                val elapsedMillis = TimeUnit.NANOSECONDS.toMillis(
                    System.nanoTime() - started,
                )
                processingTimes.add(elapsedMillis)
            }

            slots.release()
        }

        try {
            val maxWaitMillis = task.deadline - System.currentTimeMillis() - reserveMillis()

            if (!limiter.tickBlocking(Duration.ofMillis(maxWaitMillis))) {
                reject(
                    task,
                    "no rate limit slot before deadline",
                    PaymentRejectPhase.DISPATCH,
                )
                return
            }

            startedAtNanos = System.nanoTime()

            onExecute(task, ::finish)

            handedOff = true
        } catch (e: Exception) {
            logger.error(
                "Failed to start payment processing for ${task.paymentId}",
                e,
            )

            reject(
                task,
                "failed to start payment request: ${e.message}",
                PaymentRejectPhase.DISPATCH,
            )
        } finally {
            if (!handedOff) {
                finish()
            }
        }
    }
}
