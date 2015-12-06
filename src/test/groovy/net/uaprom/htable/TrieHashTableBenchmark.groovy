package net.uaprom.htable

import org.junit.Before
import org.junit.Test


public class TrieHashTableBenchmark extends HashTableBenchmark {
    @Before
    void setUp() {
        reader = new TrieHashTable.Reader(new TrieHashTable.Writer(2, 4).dump(keys, values))
    }

    @Test
    public void benchmarkReaderGet() {
        long milliseconds = benchmark()
        println "TrieHashTable.Reader.get benchmark:"
        println "Run ${CYCLES} cycles: ${milliseconds} ms"
        println "${(long) (CYCLES / (milliseconds / 1000))} operations per second"
        // println "Value is: ${value}"
    }
}
