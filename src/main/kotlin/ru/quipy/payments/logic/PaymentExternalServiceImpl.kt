package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.Call
import okhttp3.Callback
import okhttp3.Dispatcher
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import okhttp3.Response
import org.slf4j.LoggerFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.time.Duration
import java.util.UUID

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val client = OkHttpClient.Builder()
        .dispatcher(Dispatcher().apply {
            maxRequests = 200
            maxRequestsPerHost = 100
        })
        .build()

    private val capacityMultiplier = 1000
    private val requestQueue = PaymentRequestQueue(
        name = accountName,
        parallelRequests = parallelRequests,
        rateLimitPerSec = rateLimitPerSec,
        queueCapacity = rateLimitPerSec * capacityMultiplier,
        averageProcessingTime = requestAverageProcessingTime,
        onExecute = ::processPayment,
        onReject = ::rejectPayment,
    )

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        requestQueue.submit(
            PaymentTask(
                paymentId = paymentId,
                amount = amount,
                paymentStartedAt = paymentStartedAt,
                deadline = deadline,
                transactionId = transactionId,
            )
        )
    }

    private fun rejectPayment(task: PaymentTask, reason: String) {
        logger.warn("[$accountName] Rejected payment ${task.paymentId}, txId: ${task.transactionId}, reason: $reason")
        paymentESService.update(task.paymentId) {
            it.logProcessing(false, now(), task.transactionId, reason = reason)
        }
    }

    private fun processPayment(task: PaymentTask) {
        val paymentId = task.paymentId
        val transactionId = task.transactionId
        val amount = task.amount

        val request = Request.Builder().run {
            url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
            post(emptyBody)
        }.build()

        client.newCall(request).enqueue(object : Callback {
            override fun onFailure(call: Call, e: IOException) {
                logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = e.message)
                }
            }

            override fun onResponse(call: Call, response: Response) {
                response.use {
                    val body = try {
                        mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                    }

                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                    paymentESService.update(paymentId) {
                        it.logProcessing(body.result, now(), transactionId, reason = body.message)
                    }
                }
            }
        })
    }

    override fun pendingRequests(): Int = requestQueue.pendingRequests()

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName
}

public fun now() = System.currentTimeMillis()