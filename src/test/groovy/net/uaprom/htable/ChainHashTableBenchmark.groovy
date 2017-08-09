package net.uaprom.htable

import org.junit.Before
import org.junit.Test


public class ChainHashTableBenchmark extends HashTableBenchmark {
    // @Test
    // public void benchmarkWriter_dump() {
        
    // }

    @Test
    public void benchmarkReader_get() {
        HashTable.Reader reader =
            new ChainHashTable.Reader(new ChainHashTable.Writer(HashTable.ValueSize.INT)
                                      .dump(keys, values))
        long milliseconds = benchmarkReader_get(reader)
        println "ChainHashTable.Reader.get benchmark:"
        println "Run ${CYCLES} cycles: ${milliseconds} ms"
        println "${(long) (CYCLES / (milliseconds / 1000))} operations per second"
        // println "Value is: ${value}"
    }
}
