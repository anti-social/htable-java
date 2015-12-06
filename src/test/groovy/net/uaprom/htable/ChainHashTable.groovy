package net.uaprom.htable

import org.junit.Before
import org.junit.Test


public class ChainHashTableBenchmark extends HashTableBenchmark {
    @Before
    void setUp() {
        reader = new ChainHashTable.Reader(new ChainHashTable.Writer(HashTable.ValueSize.INT).dump(keys, values))
    }

    @Test
    public void benchmarkReaderGet() {
        long milliseconds = benchmark()
        println "ChainHashTable.Reader.get benchmark:"
        println "Run ${CYCLES} cycles: ${milliseconds} ms"
        println "${(long) (CYCLES / (milliseconds / 1000))} operations per second"
        // println "Value is: ${value}"
    }
}
