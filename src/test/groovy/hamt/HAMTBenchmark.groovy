package hamt

import groovy.transform.CompileStatic

import org.junit.Before
import org.junit.Test


class HAMTBenchmark {
    private HAMT.Reader reader;

    private static final long lookupKey = 268_435_456L
    private static final byte[] defaultValue = [0xff, 0xff, 0xff, 0xff] as byte[]
    private static final int WARMUP_CYCLES = 100_000
    private static final int CYCLES = 1_000_000

    @Before
    void setUp() {
        def keys = [1L, lookupKey]
        def values = [[0, 0, 0, 1] as byte[], [0, 0, 0, 2] as byte[]]
        this.reader = new HAMT.Reader(new HAMT.Writer(2, 4).dump(keys, values))
    }

    @Test
    @CompileStatic
    void benchmark() {
        // warmup
        cycle(WARMUP_CYCLES)

        // measure
        long startTime = System.nanoTime()
        byte[] value = cycle(CYCLES)
        long endTime = System.nanoTime()
        println "Run ${CYCLES} cycles: ${(endTime - startTime) / 1000000} ms"
        println "Value is: ${value}"
    }

    @CompileStatic
    byte[] cycle(int count) {
        byte[] value
        for (int i = 0; i < count; i++) {
            value = this.reader.get(lookupKey, defaultValue)
        }
        return value
    }
}
