package net.uaprom.htable

import groovy.transform.CompileStatic


class HashTableBenchmark {
    protected HashTable.Reader reader;

    protected static final long lookupKey = 268_435_456L
    protected static final long[] keys = [1L, lookupKey]
    protected static final byte[][] values = [[0, 0, 0, 1] as byte[], [0, 0, 0, 2] as byte[]]
    protected static final byte[] defaultValue = [0xff, 0xff, 0xff, 0xff] as byte[]
    protected static final int WARMUP_CYCLES = 100_000
    protected static final int CYCLES = 1_000_000

    @CompileStatic
    protected long benchmark() {
        // warmup
        cycle(WARMUP_CYCLES)

        // measure
        long startTime = System.nanoTime()
        byte[] value = cycle(CYCLES)
        long endTime = System.nanoTime()
        return (long) ((endTime - startTime) / 1_000_000)
    }

    @CompileStatic
    protected byte[] cycle(int count) {
        byte[] value
        for (int i = 0; i < count; i++) {
            value = reader.get(lookupKey, defaultValue)
        }
        return value
    }
}
