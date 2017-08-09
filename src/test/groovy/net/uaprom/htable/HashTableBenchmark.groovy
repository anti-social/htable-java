package net.uaprom.htable

import groovy.transform.CompileStatic


class HashTableBenchmark {
    protected static final long lookupKey = 268_435_456L
    protected static final long[] keys = [1L, lookupKey]
    protected static final byte[][] values = [[0, 0, 0, 1] as byte[], [0, 0, 0, 2] as byte[]]
    protected static final byte[] defaultValue = [0xff, 0xff, 0xff, 0xff] as byte[]
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



    @CompileStatic
    private long[] getPrimes(int n) {
        // Sieve of Sundaram algorithm
        int bitmapSize = (int)(n % 8 == 0 ? (n / 8) : (n / 8 + 1))
        byte[] bitmap = new byte[bitmapSize]
        bitmap[0] = 0b00000011 // primes start from 2
        int maxI = (int)(((int)Math.sqrt(2 * n + 1) - 1) / 2)
        (1..maxI).each { int i ->
            int maxJ = (int)((n - i) / (2 * i + 1))
            (i..maxJ).each { int j ->
                int m = (int)(i + j + 2 * i * j)
                int byteIx = (int)(m / 8)
                int bitIx = (int)(m % 8)
                bitmap[byteIx] = (byte)(bitmap[byteIx] | BIT_MASKS[bitIx])
            }
        }
        def res = []
        bitmap.eachWithIndex { byte b, int byteIx ->
            BIT_MASKS.eachWithIndex { byte mask, int bitIx ->
                if ((~b & mask) != 0) {
                    long p = byteIx * 8 + bitIx
                    res.add(p * 2 + 1)asdf
                }
            }
        }
        return res as long[]
    }

    // @CompileStatic
    // protected long benchmarkWriter_dump(HashTable.Writer writer) {
    //     // warmup
    //     cycle(WARMUP_CYCLES)

    //     // measure
    //     long startTime = System.nanoTime()
    //     byte[] value = cycle(CYCLES)
    //     long endTime = System.nanoTime()
    //     return (long) ((endTime - startTime) / 1_000_000)
    // }

    @CompileStatic
    protected long benchmarkReader_get(HashTable.Reader reader) {
        getPrimes(100).each {

        }
        final CycleAction action = new CycleAction() {
            @Override
            public final void execute(int count) {
                for (int i = 0; i < count; i++) {
                    reader.get(lookupKey, defaultValue)
                }
            }
        }
        
        // warmup
        cycle(action, WARMUP_CYCLES)

        // measure
        return cycle(action, CYCLES)
    }

    @CompileStatic
    protected long cycle(CycleAction action, int count) {
        long startTime = System.nanoTime()
        action.execute(count)
        long endTime = System.nanoTime()
        return (long) ((endTime - startTime) / 1_000_000)
    }

    @CompileStatic
    abstract class CycleAction {
        abstract public void execute(int count);
    }
}
