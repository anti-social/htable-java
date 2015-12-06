package net.uaprom.htable

import java.nio.ByteBuffer
import java.nio.ByteOrder
import spock.lang.Specification


class HashTableSpec extends Specification {
    void checkGet(reader, keys, values, range, defaultValue) {
        def map = [keys, values].transpose().collectEntries { it }
        for (k in range) {
            if (map.containsKey(k)) {
                assert reader.get(k, defaultValue) == map[k]
            } else {
                assert reader.get(k, defaultValue) == defaultValue
            }
        }
    }

    void checkExists(reader, keys, values, range) {
        def map = [keys, values].transpose().collectEntries { it }
        for (k in range) {
            if (map.containsKey(k)) {
                assert reader.exists(k) == true
            } else {
                assert reader.exists(k) == false
            }
        }
    }

    byte[] intToBytes(int v) {
        return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(v).array()
    }

    byte[] shortToBytes(short v) {
        return ByteBuffer.allocate(2).order(ByteOrder.LITTLE_ENDIAN).putShort(v).array()
    }

    def "test ChainHashTable.Writer.getHashTableSize"() {
        when:
        def htableWriter = new ChainHashTable.Writer(HashTable.ValueSize.INT, fillingRatio, minHashTableSize)
        then:
        htableWriter.getHashTableSize(numValues) == hashTableSize

        where:
        fillingRatio | minHashTableSize | numValues || hashTableSize
        1 | 2 | 0 || 0
        1 | 2 | 1 || 0
        1 | 2 | 2 || 2
        1 | 2 | 3 || 2
        1 | 2 | 4 || 4
        1 | 2 | 7 || 4
        1 | 2 | 8 || 8
        10 | 2 | 0 || 0
        10 | 2 | 1 || 0
        10 | 2 | 9 || 0
        10 | 2 | 10 || 0
        10 | 2 | 19 || 0
        10 | 2 | 20 || 2
        10 | 2 | 21 || 2
        10 | 2 | 39 || 2
        10 | 2 | 40 || 4
        10 | 2 | 79 || 4
        10 | 2 | 80 || 8
        10 | 2 | 159 || 8
        10 | 2 | 160 || 16
        10 | 2 | 319 || 16
        10 | 2 | 320 || 32
        10 | 2 | 10239 || 512
        10 | 2 | 10240 || 1024
        20 | 2 | 0 || 0
        20 | 2 | 1 || 0
        20 | 2 | 19 || 0
        20 | 2 | 39 || 0
        20 | 2 | 40 || 2
        20 | 2 | 79 || 2
        20 | 2 | 80 || 4
        20 | 2 | 159 || 4
        20 | 2 | 160 || 8
        50 | 2 | 0 || 0
        50 | 2 | 1 || 0
        50 | 2 | 50 || 0
        50 | 2 | 99 || 0
        50 | 2 | 100 || 2
        50 | 2 | 199 || 2
        50 | 2 | 200 || 4
        50 | 2 | 399 || 4
        50 | 2 | 400 || 8
        10 | 4 | 0 || 0
        10 | 4 | 1 || 0
        10 | 4 | 9 || 0
        10 | 4 | 10 || 0
        10 | 4 | 19 || 0
        10 | 4 | 20 || 0
        10 | 4 | 21 || 0
        10 | 4 | 39 || 0
        10 | 4 | 40 || 4
        10 | 4 | 79 || 4
        10 | 4 | 80 || 8
    }

    def "test ChainHashTable.Writer.dumpInts [valueSize: 4]"() {
        given:
        def htableWriter = new ChainHashTable.Writer(HashTable.ValueSize.INT)

        expect:
        htableWriter.dumpInts(keys, values).collect { it & 0xff } == bytes

        where:
        keys | values || bytes
        [0L] | [13] || [
            *shortToBytes((short) 0b0_00_00000_0_10_00_000),
            0, 13, 0, 0, 0
        ]
        [255L] | [13] || [
            *shortToBytes((short) 0b0_00_00000_0_10_00_000),
            255, 13, 0, 0, 0
        ]
        [256L] | [13] || [
            *shortToBytes((short) 0b0_00_00000_0_10_00_001),
            0, 1, 13, 0, 0, 0
        ]
        [0L, 2L] | [10, 12] || [
            *shortToBytes((short) 0b0_00_00000_0_10_00_000),
            0, 10, 0, 0, 0,
            2, 12, 0, 0, 0
        ]
    }

    def "test ChainHashTable.Writer.dumpInts [valueSize: 4, fillingRatio: 1]"() {
        given:
        def htableWriter = new ChainHashTable.Writer(HashTable.ValueSize.INT, 1)

        expect:
        htableWriter.dumpInts(keys, values).collect { it & 0xff } == bytes

        where:
        keys | values || bytes
        [0L, 5L] | [13, 15] || [
            *shortToBytes((short) 0b0_00_00001_0_10_00_000),
            2, 7,
            0, 13, 0, 0, 0,
            5, 15, 0, 0, 0
        ]
        [0L, 5L, 8L] | [13, 15, 18] || [
            *shortToBytes((short) 0b0_00_00001_0_10_00_000),
            2, 12,
            0, 13, 0, 0, 0, 8, 18, 0, 0, 0,
            5, 15, 0, 0, 0
        ]
        [0L, 5L, 8L, 11L] | [13, 15, 18, 21] || [
            *shortToBytes((short) 0b0_00_00010_0_10_00_000),
            4, 14, 0, 19,
            0, 13, 0, 0, 0, 8, 18, 0, 0, 0,
            5, 15, 0, 0, 0,
            11, 21, 0, 0, 0
        ]
    }

    def "test ChainHashTable.Reader().get [valueSize: 4]"() {
        given:
        def htableWriter = new ChainHashTable.Writer(HashTable.ValueSize.INT)

        when:
        def keys = keyRange.step(keyStep).collect { it }
        def values = keyRange.step(keyStep).collect { intToBytes((int) it * 2) }
        def reader = new ChainHashTable.Reader(htableWriter.dump(keys, values))
        then:
        checkGet(reader, keys, values, keyRange, defaultValue)

        where:
        keyRange | keyStep | defaultValue
        1L..1L | 1 | [0xff, 0xff, 0xff, 0xff] as byte[]
        1L..10L | 3 | [0xff, 0xff, 0xff, 0xff] as byte[]
        1L..20L | 1 | [0xff, 0xff, 0xff, 0xff] as byte[]
        0L..1000L | 3 | [0xff, 0xff, 0xff, 0xff] as byte[]
        0L..100000L | 7 | [0xff, 0xff, 0xff, 0xff] as byte[]
    }
}
