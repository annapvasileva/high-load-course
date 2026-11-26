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
        val nowNanos = System.nanoTime()
        rotateIfNeeded(nowNanos)
        buckets.incrementAndGet(bucketIndex(nowNanos))
    }

    fun ratePerSec(): Double {
        rotateIfNeeded(System.nanoTime())
        var sum = 0L
        for (i in 0 until bucketCount) sum += buckets.get(i)
        return sum * 1_000_000_000.0 / windowNanos
    }

    private fun bucketIndex(nowNanos: Long): Int =
        Math.floorMod(nowNanos / bucketNanos, bucketCount.toLong()).toInt()

    private fun rotateIfNeeded(nowNanos: Long) {
        val nowBucket = nowNanos / bucketNanos
        val last = currentBucket.get()
        if (nowBucket == last) return
        if (!currentBucket.compareAndSet(last, nowBucket)) return

        val gap = (nowBucket - last).coerceAtMost(bucketCount.toLong())
        for (i in 0 until gap) {
            buckets.set(
                Math.floorMod(last + 1 + i, bucketCount.toLong()).toInt(),
                0L,
            )
        }
    }
}
