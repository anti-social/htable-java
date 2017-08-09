package net.uaprom.htable

import org.junit.Before
import org.junit.Test


public class TrieHashTableBenchmark extends HashTableBenchmark {
    @Test
    public void benchmarkReader_get() {
        HashTable.Reader reader =
            new TrieHashTable.Reader(new TrieHashTable.Writer(HashTable.ValueSize.INT)
                                     .dump(keys, values))
        long milliseconds = benchmarkReader_get(reader)
        println "TrieHashTable.Reader.get benchmark:"
        println "Run ${CYCLES} cycles: ${milliseconds} ms"
        println "${(long) (CYCLES / (milliseconds / 1000))} operations per second"
        // println "Value is: ${value}"
    }
}
