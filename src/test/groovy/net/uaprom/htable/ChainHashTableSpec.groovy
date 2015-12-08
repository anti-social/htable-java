package net.uaprom.htable


class HashTableSpec extends BaseSpecification {
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
            *shortToBytes((short) 0b0_00_00000_00_000_0_10),
            0, 13, 0, 0, 0
        ]
        [255L] | [13] || [
            *shortToBytes((short) 0b0_00_00000_00_000_0_10),
            255, 13, 0, 0, 0
        ]
        [256L] | [13] || [
            *shortToBytes((short) 0b0_00_00000_00_001_0_10),
            0, 1, 13, 0, 0, 0
        ]
        [0L, 2L] | [10, 12] || [
            *shortToBytes((short) 0b0_00_00000_00_000_0_10),
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
            *shortToBytes((short) 0b0_00_00001_00_000_0_10),
            4, 9,
            0, 13, 0, 0, 0,
            5, 15, 0, 0, 0
        ]
        [0L, 5L, 8L] | [13, 15, 18] || [
            *shortToBytes((short) 0b0_00_00001_00_000_0_10),
            4, 14,
            0, 13, 0, 0, 0, 8, 18, 0, 0, 0,
            5, 15, 0, 0, 0
        ]
        [0L, 5L, 8L, 11L] | [13, 15, 18, 21] || [
            *shortToBytes((short) 0b0_00_00010_00_000_0_10),
            6, 16, 0, 21,
            0, 13, 0, 0, 0, 8, 18, 0, 0, 0,
            5, 15, 0, 0, 0,
            11, 21, 0, 0, 0
        ]
    }

    def "test ChainHashTable.Reader().get [valueSize: 4] with data offset"() {
        given:
        def htableWriter = new ChainHashTable.Writer(HashTable.ValueSize.INT)

        when:
        def keysRange = keys.min()..keys.max()
        def garbage = [0xff as byte] * 10 as byte[]
        def data = htableWriter.dumpInts(keys, values)
        def reader = new ChainHashTable.Reader([*garbage, *data] as byte[], garbage.length, data.length)
        then:
        assertReader_getInt(reader, keys, values, keysRange, defaultValue)

        where:
        keys | values | defaultValue
        [1L] | [156] | -2
        1L..20L | 1..20 | -2
    }

    def "test ChainHashTable.Reader().get [valueSize: 4]"() {
        given:
        def htableWriter = new ChainHashTable.Writer(HashTable.ValueSize.INT)

        when:
        def keys = keyRange.step(keyStep).collect { it }
        def values = keyRange.step(keyStep).collect { intToBytes((int) it * 2) }
        def reader = new ChainHashTable.Reader(htableWriter.dump(keys, values))
        then:
        assertReader_get(reader, keys, values, keyRange, defaultValue)

        where:
        keyRange | keyStep | defaultValue
        1L..1L | 1 | [0xff, 0xff, 0xff, 0xff] as byte[]
        1L..10L | 3 | [0xff, 0xff, 0xff, 0xff] as byte[]
        1L..20L | 1 | [0xff, 0xff, 0xff, 0xff] as byte[]
        1L..67L | 1 | [0xff, 0xff, 0xff, 0xff] as byte[] // miminum data for pointer size == 2
        0L..1000L | 3 | [0xff, 0xff, 0xff, 0xff] as byte[]
        0L..100000L | 7 | [0xff, 0xff, 0xff, 0xff] as byte[]
    }
}
