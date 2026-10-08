package ru.quipy.common.utils

import java.util.ArrayDeque
import kotlin.math.ceil

class SlidingQuantile(
    private val windowSize: Int,
    private val bucketWidth: Long,
    private val numBuckets: Int
) {
    private val ring = ArrayDeque<Int>()
    private val counts = IntArray(numBuckets)
    private var total = 0

    private fun bucket(v: Long): Int =
        (v / bucketWidth).toInt().coerceIn(0, numBuckets - 1)

    fun add(v: Long) {
        if (total == windowSize) {
            counts[ring.removeFirst()]--
            total--
        }
        val b = bucket(v)
        ring.addLast(b)
        counts[b]++
        total++
    }

    fun quantile(q: Double): Long {
        if (total == 0) return 0
        val target = ceil(q * total).toInt().coerceAtLeast(1)
        var cum = 0
        for (i in 0 until numBuckets) {
            cum += counts[i]
            if (cum >= target) {
                return i.toLong() * bucketWidth
            }
        }
        return 0
    }
}
