package ru.quipy.apigateway

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import ru.quipy.orders.repository.OrderRepository
import ru.quipy.payments.logic.OrderPayer
import ru.quipy.payments.logic.SubmitResult
import java.util.*

@RestController
class APIController {

    val logger: Logger = LoggerFactory.getLogger(APIController::class.java)

    @Autowired
    private lateinit var orderRepository: OrderRepository

    @Autowired
    private lateinit var orderPayer: OrderPayer

    @PostMapping("/users")
    fun createUser(@RequestBody req: CreateUserRequest): User {
        return User(UUID.randomUUID(), req.name)
    }

    data class CreateUserRequest(val name: String, val password: String)

    data class User(val id: UUID, val name: String)

    @PostMapping("/orders")
    fun createOrder(@RequestParam userId: UUID, @RequestParam price: Int): Order {
        val order = Order(
            UUID.randomUUID(),
            userId,
            System.currentTimeMillis(),
            OrderStatus.COLLECTING,
            price,
        )
        return orderRepository.save(order)
    }

    data class Order(
        val id: UUID,
        val userId: UUID,
        val timeCreated: Long,
        val status: OrderStatus,
        val price: Int,
    )

    enum class OrderStatus {
        COLLECTING,
        PAYMENT_IN_PROGRESS,
        PAID,
    }

    @PostMapping("/orders/{orderId}/payment")
    fun payOrder(
        @PathVariable orderId: UUID,
        @RequestParam deadline: Long,
    ): ResponseEntity<Any> {
        val paymentId = UUID.randomUUID()
        val order = orderRepository.findById(orderId)?.let {
            orderRepository.save(it.copy(status = OrderStatus.PAYMENT_IN_PROGRESS))
            it
        } ?: throw IllegalArgumentException("No such order $orderId")

        val outcome = orderPayer.processPayment(orderId, order.price, paymentId, deadline)

        return when (val r = outcome.submitResult) {
            is SubmitResult.Accepted ->
                ResponseEntity.ok(PaymentSubmissionDto(outcome.createdAt, paymentId))

            is SubmitResult.Retryable -> {
                orderRepository.save(order.copy(status = OrderStatus.COLLECTING))
                ResponseEntity
                    .status(HttpStatus.TOO_MANY_REQUESTS)
                    .header(HttpHeaders.RETRY_AFTER, r.retryAfterSeconds.toString())
                    .body(
                        mapOf(
                            "error" to "payment_enqueue_rejected",
                            "reason" to r.reason,
                            "retryAfterSeconds" to r.retryAfterSeconds,
                        )
                    )
            }

            is SubmitResult.BadRequest -> {
                orderRepository.save(order.copy(status = OrderStatus.COLLECTING))
                ResponseEntity
                    .status(HttpStatus.UNPROCESSABLE_ENTITY)
                    .body(
                        mapOf(
                            "error" to "bad_deadline",
                            "reason" to r.reason,
                            "minRequiredDeadlineMs" to r.minRequiredDeadlineMs,
                        )
                    )
            }
        }
    }

    class PaymentSubmissionDto(
        val timestamp: Long,
        val transactionId: UUID
    )
}