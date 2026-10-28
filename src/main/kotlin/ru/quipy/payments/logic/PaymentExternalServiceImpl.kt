package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.UUID
import io.micrometer.core.instrument.Clock
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.distribution.TimeWindowMax
import java.net.SocketTimeoutException
import java.net.URI
import java.net.URLEncoder
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.net.http.HttpTimeoutException
import java.nio.charset.StandardCharsets
import java.util.concurrent.CompletionException
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val meterRegistry: MeterRegistry,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val mapper = ObjectMapper().registerKotlinModule()

        // HTTP client is shared, because we are still limited by processors,
        // but async operations allow enough parallel requests
        private val httpExecutor: ExecutorService =
            Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2) { r ->
                Thread(r, "payment-http").apply { isDaemon = true }
            }
        private val client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .followRedirects(HttpClient.Redirect.NORMAL)
            .version(HttpClient.Version.HTTP_2)
            .executor(httpExecutor)
            .build()

        public const val MAX_METRIC_ROTATION_MILLIS = 4_000L
        public const val MAX_METRIC_BUFFER_LENGTH = 4
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    // TODO: Use dynamic queue size
    private val capacityMultiplier = 1000
    private val requestQueue = PaymentRequestQueue(
        name = accountName,
        parallelRequests = parallelRequests,
        rateLimitPerSec = rateLimitPerSec,
        queueCapacity = rateLimitPerSec * capacityMultiplier,
        meterRegistry = meterRegistry,
        averageProcessingTime = requestAverageProcessingTime,
        onExecute = ::processPayment,
        onReject = ::rejectPayment,
    )

    private val incomingCounter = counter("incoming")
    private val outgoingCounter = counter("outgoing")
    private val rejectedEnqueueCounter = counter("rejected_enqueue")
    private val rejectedDispatchCounter = counter("rejected_dispatch")

    private fun counter(type: String) =
        meterRegistry.counter(
            "payment_sys_requests",
            "account", accountName,
            "type", type,
        )

    private val inFlightRequests = AtomicInteger()

    private val inFlightMax = TimeWindowMax(
        Clock.SYSTEM,
        MAX_METRIC_ROTATION_MILLIS,
        MAX_METRIC_BUFFER_LENGTH,
    )
    private val inFlightMaxMetric = Gauge.builder(
        "payment_sys_in_flight_max",
    ) {
        inFlightMax.poll()
    }
        .tags("account", accountName)
        .register(meterRegistry)

    override fun performPaymentAsync(
        paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long,
    ): SubmitResult {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")
        incomingCounter.increment()

        val transactionId = UUID.randomUUID()

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        return requestQueue.submit(
            PaymentTask(
                paymentId = paymentId,
                amount = amount,
                paymentStartedAt = paymentStartedAt,
                deadline = deadline,
                transactionId = transactionId,
            )
        )
    }

    private fun rejectPayment(
        task: PaymentTask,
        reason: String,
        phase: PaymentRejectPhase,
    ) {
        when (phase) {
            PaymentRejectPhase.ENQUEUE ->
                rejectedEnqueueCounter.increment()

            PaymentRejectPhase.DISPATCH ->
                rejectedDispatchCounter.increment()
        }

        logger.warn(
            "[$accountName] Rejected payment ${task.paymentId}, " +
                    "txId: ${task.transactionId}, phase: $phase, " +
                    "reason: $reason"
        )

        paymentESService.update(task.paymentId) {
            it.logProcessing(
                false,
                now(),
                task.transactionId,
                reason = reason,
            )
        }
    }

    private fun recordProcessingResult(
        task: PaymentTask,
        success: Boolean,
        reason: String?,
    ) {
        try {
            paymentESService.update(task.paymentId) {
                it.logProcessing(
                    success,
                    now(),
                    task.transactionId,
                    reason = reason,
                )
            }
        } catch (e: Exception) {
            logger.error(
                "[$accountName] Failed to persist payment result: " +
                        "payment=${task.paymentId}, " +
                        "txId=${task.transactionId}, success=$success",
                e,
            )
        }
    }

    private fun processPayment(
        task: PaymentTask,
        onTaskFinished: () -> Unit,
    ) {
        val request = try {
            buildPaymentRequest(task)
        } catch (e: Exception) {
            try {
                recordPaymentResult(task, failureResponse(task, e))
            } finally {
                onTaskFinished()
            }
            return
        }

        val future = try {
            client.sendAsync(
                request,
                HttpResponse.BodyHandlers.ofString(),
            )
        } catch (e: Exception) {
            try {
                recordPaymentResult(task, failureResponse(task, e))
            } finally {
                onTaskFinished()
            }
            return
        }
        outgoingCounter.increment()

        val current = inFlightRequests.incrementAndGet()
        inFlightMax.record(current.toDouble())

        future.whenComplete { response, error ->
            inFlightRequests.decrementAndGet()

            try {
                val result = if (error != null) {
                    failureResponse(task, error)
                } else {
                    parsePaymentResponse(task, response!!)
                }

                recordPaymentResult(task, result)
            } catch (e: Exception) {
                logger.error(
                    "[$accountName] Unexpected response-processing failure: " +
                            "txId=${task.transactionId}, payment=${task.paymentId}",
                    e,
                )

                recordProcessingResult(
                    task = task,
                    success = false,
                    reason = e.message ?: "Failed to process external response.",
                )
            } finally {
                onTaskFinished()
            }
        }
    }

    private fun buildPaymentRequest(task: PaymentTask): HttpRequest {
        val query = listOf(
            "serviceName" to serviceName,
            "token" to token,
            "accountName" to accountName,
            "transactionId" to task.transactionId.toString(),
            "paymentId" to task.paymentId.toString(),
            "amount" to task.amount.toString(),
        ).joinToString("&") { (key, value) ->
            "$key=${URLEncoder.encode(value, StandardCharsets.UTF_8)}"
        }

        val remaining = Duration.ofMillis(task.deadline - System.currentTimeMillis())

        return HttpRequest.newBuilder()
            .uri(
                URI.create(
                    "http://$paymentProviderHostPort/external/process?$query",
                ),
            )
            .timeout(remaining)
            .POST(HttpRequest.BodyPublishers.noBody())
            .build()
    }

    private fun parsePaymentResponse(
        task: PaymentTask,
        response: HttpResponse<String>,
    ): ExternalSysResponse {
        val responseText = response.body()

        return try {
            mapper.readValue(
                responseText,
                ExternalSysResponse::class.java,
            )
        } catch (e: Exception) {
            logger.error(
                "[$accountName] Invalid provider response: " +
                        "txId=${task.transactionId}, " +
                        "HTTP ${response.statusCode()}, body=$responseText",
                e,
            )

            ExternalSysResponse(
                task.transactionId.toString(),
                task.paymentId.toString(),
                false,
                e.message,
            )
        }
    }

    private fun failureResponse(
        task: PaymentTask,
        error: Throwable,
    ): ExternalSysResponse {
        val cause = (error as? CompletionException)?.cause ?: error

        val reason = when (cause) {
            is HttpTimeoutException -> "Request timeout."
            is SocketTimeoutException -> "Request socket timeout."
            else -> cause.message ?: "External payment request failed."
        }

        logger.error(
            "[$accountName] External payment failed: " +
                    "txId=${task.transactionId}, payment=${task.paymentId}",
            cause,
        )

        return ExternalSysResponse(
            task.transactionId.toString(),
            task.paymentId.toString(),
            false,
            reason,
        )
    }

    private fun recordPaymentResult(
        task: PaymentTask,
        result: ExternalSysResponse,
    ) {
        logger.info(
            "[$accountName] Payment processed: " +
                    "txId=${task.transactionId}, " +
                    "payment=${task.paymentId}, " +
                    "success=${result.result}, " +
                    "message=${result.message}",
        )

        recordProcessingResult(
            task = task,
            success = result.result,
            reason = result.message,
        )
    }

    override fun pendingRequests(): Int =
        requestQueue.pendingRequests()

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName
}

public fun now(): Long = System.currentTimeMillis()