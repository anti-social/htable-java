package net.uaprom.htable

import groovy.transform.CompileStatic

import org.junit.Test

import javax.xml.bind.annotation.XmlType


@CompileStatic
abstract class HashTableBenchmark {
    protected static final long LOOKUP_KEY = 1L
    protected static final byte[] DEFAULT_VALUE = [0xff, 0xff, 0xff, 0xff] as byte[]
    protected static final int WARMUP_CYCLES = 100_000
    protected static final int CYCLES = 1_000_000

    protected static final byte[] BIT_MASKS = [
            0b00000001,
            0b00000010,
            0b00000100,
            0b00001000,
            0b00010000,
            0b00100000,
            0b01000000,
            0b10000000,
    ] as byte[]

    abstract protected HashTable.Writer getWriter()

    abstract protected HashTable.Reader getReader(byte[] data)

    @Test
    void benchmarkPrimes_Reader_get() {
        long[] keys = getPrimes(1000000)
        int[] values = keys.collect { (int) it } as int[]
        byte[] data = getWriter().dumpInts(keys, values)
        HashTable.Reader reader = getReader(data)
        println "Created hash table with $keys.length elements (prime numbers), data size: $data.length bytes"

        final CycleAction action = new CycleAction() {
            @Override
            final void execute(int count) {
                for (int i = 0; i < count; i++) {
                    reader.get(LOOKUP_KEY, DEFAULT_VALUE)
                }
            }
        }
        runBenchmark(action)
    }

    @Test
    void benchmarkEvenNumbers_Reader_get() {
        long[] keys = (0..<2_000_000).step(2).collect { it }
        int[] values = keys.collect { (int) it }
        byte[] data = getWriter().dumpInts(keys, values)
        HashTable.Reader reader = getReader(data)
        println "Created hash table with $keys.length elements (even numbers), data size: $data.length bytes"

        final CycleAction action = new CycleAction() {
            @Override
            final void execute(int count) {
                for (int i = 0; i < count; i++) {
                    reader.get(LOOKUP_KEY, DEFAULT_VALUE)
                }
            }
        }
        runBenchmark(action)
    }

    private static long[] getPrimes(int n) {
        // Sieve of Sundaram algorithm
        int bitmapSize = (int)(n % 8 == 0 ? (n / 8) : (n / 8 + 1))
        byte[] bitmap = new byte[bitmapSize]
        int maxI = (int)(((int)Math.sqrt(2 * n + 1) - 1) / 2)
        (1..maxI).each { int i ->
            int maxJ = (int)((n - i) / (2 * i + 1))
            (i..maxJ).each { int j ->
                int m = (int)(i + j + 2 * i * j)
                int byteIx = (int)(m / 8)
                int bitIx = (int)(m % 8)
                if (byteIx < bitmapSize) {
                    bitmap[byteIx] = (byte) (bitmap[byteIx] | BIT_MASKS[bitIx])
                }
            }
        }
        bitmap[0] = (byte)(bitmap[0] | 0b00000011)
        def res = [2L]
        bitmap.eachWithIndex { byte b, int byteIx ->
            BIT_MASKS.eachWithIndex { byte mask, int bitIx ->
                if ((~b & mask) != 0) {
                    long p = byteIx * 8 + bitIx
                    res.add(p * 2 + 1)
                }
            }
        }
        return res as long[]
    }

    // protected long benchmarkWriter_dump(HashTable.Writer writer) {
    //     // warmup
    //     cycle(WARMUP_CYCLES)

    //     // measure
    //     long startTime = System.nanoTime()
    //     byte[] value = cycle(CYCLES)
    //     long endTime = System.nanoTime()
    //     return (long) ((endTime - startTime) / 1_000_000)
    // }

    protected void runBenchmark(CycleAction action) {
        // warmup
        cycle(action, WARMUP_CYCLES)

        // measure
        long duration = cycle(action, CYCLES)

        println "Run ${CYCLES} cycles: ${duration} ms"
        println "${(long) (CYCLES / (duration / 1000))} operations per second"
    }

    protected long cycle(CycleAction action, int count) {
        long startTime = System.nanoTime()
        action.execute(count)
        long endTime = System.nanoTime()
        return (long) ((endTime - startTime) / 1_000_000)
    }

    abstract class CycleAction {
        abstract void execute(int count);
    }
}
