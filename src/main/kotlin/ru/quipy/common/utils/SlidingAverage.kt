package ru.quipy.common.utils

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicLongArray

public class SlidingAverage(
    private val bucketCount: Int,
    private val bucketNanos: Long,
) {
    private val windowNanos: Long = bucketNanos * bucketCount
    private val buckets = AtomicLongArray(bucketCount)
    private val currentBucket = AtomicLong(Long.MIN_VALUE / 2)

    fun record() {
        rotateIfNeeded()
        buckets.incrementAndGet(bucketIndex(System.nanoTime()))
    }

    fun ratePerSec(): Double {
        rotateIfNeeded()
        var sum = 0L
        for (i in 0 until bucketCount) sum += buckets.get(i)
        return sum * 1_000_000_000.0 / windowNanos
    }

    private fun bucketIndex(nowNanos: Long): Int =
        ((nowNanos / bucketNanos) % bucketCount).toInt()

    private fun rotateIfNeeded() {
        val nowBucket = System.nanoTime() / bucketNanos
        val last = currentBucket.get()
        if (nowBucket == last) return
        if (!currentBucket.compareAndSet(last, nowBucket)) return

        val gap = (nowBucket - last).coerceAtMost(bucketCount.toLong())
        for (i in 0 until gap) {
            buckets.set(((last + 1 + i) % bucketCount).toInt(), 0L)
        }
    }
}
