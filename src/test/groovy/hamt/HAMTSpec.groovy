package hamt

import java.nio.ByteBuffer
import java.nio.ByteOrder
import spock.lang.Specification


class HAMTSpec extends Specification {
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

    // def "test new HAMT.Writer(1, 4).getLevels"() {
    //     given:
    //     def hamtWriter = new HAMT.Writer(1, 4)

    //     expect:
    //     hamtWriter.getLevels(maxKey) == levels

    //     where:
    //     maxKey | levels
    //     0 | 1
    //     1 | 1
    //     2 | 1
    //     7 | 1
    //     8 | 2
    //     63 | 2
    //     64 | 3
    //     511 | 3
    //     512 | 4
    //     4095 | 4
    //     4096 | 5
    //     32767 | 5
    //     32768 | 6
    //     262143 | 6
    //     262144 | 7
    //     2097151 | 7
    //     2097152 | 8
    //     16777215 | 8
    //     16777216 | 9
    //     134217727 | 9
    //     134217728 | 10
    //     1073741823 | 10
    //     1073741824 | 11
    // }

    // def "test new HAMT.Writer(2, 4).getLevels"() {
    //     given:
    //     def hamtWriter = new HAMT.Writer(2, 4)

    //     expect:
    //     hamtWriter.getLevels(maxKey) == levels

    //     where:
    //     maxKey | levels
    //     1 | 1
    //     2 | 1
    //     15 | 1
    //     16 | 2
    //     32 | 2
    //     255 | 2
    //     256 | 3
    //     4096 | 4
    //     65535 | 4
    //     65536 | 5
    //     1048575 | 5
    //     1048576 | 6
    //     16777215 | 6
    //     16777216 | 7
    //     268435455 | 7
    //     268435456 | 8
    // }

    // def "test new HAMT.Writer(4, 4).getLevels"() {
    //     given:
    //     def hamtWriter = new HAMT.Writer(4, 4)

    //     expect:
    //     hamtWriter.getLevels(maxKey) == levels

    //     where:
    //     maxKey | levels
    //     0 | 1
    //     1 | 1
    //     2 | 1
    //     31 | 1
    //     32 | 2
    //     1023 | 2
    //     1024 | 3
    //     32767 | 3
    //     32768 | 4
    //     1048575 | 4
    //     1048576 | 5
    //     33554431 | 5
    //     33554432 | 6
    //     1073741823 | 6
    //     1073741824 | 7
    // }

    // def "test new HAMT.Writer(8, 4).getLevels"() {
    //     given:
    //     def hamtWriter = new HAMT.Writer(8, 4)

    //     expect:
    //     hamtWriter.getLevels(maxKey) == levels

    //     where:
    //     maxKey | levels
    //     0 | 1
    //     1 | 1
    //     2 | 1
    //     63 | 1
    //     64 | 2
    //     4095 | 2
    //     4096 | 3
    //     262143 | 3
    //     262144 | 4
    //     16777215 | 4
    //     16777216 | 5
    //     1073741823 | 5
    //     1073741824 | 6
    // }

    // def "test new HAMT.Writer(2, 4).getHeader"() {
    //     given:
    //     def hamtWriter = new HAMT.Writer(2, 4)

    //     expect:
    //     Integer.toBinaryString((int)hamtWriter.getHeader(levels, ptrSize)) == Integer.toBinaryString(header)

    //     where:
    //     levels | ptrSize || header
    //     1      | 1       || 0b000_0_10_00_001_00001
    //     1      | 2       || 0b000_0_10_01_001_00001
    //     1      | 3       || 0b000_0_10_10_001_00001
    //     1      | 4       || 0b000_0_10_11_001_00001
    //     2      | 1       || 0b000_0_10_00_001_00010
    //     3      | 1       || 0b000_0_10_00_001_00011
    //     4      | 1       || 0b000_0_10_00_001_00100
    //     31     | 1       || 0b000_0_10_00_001_11111
    // }

    // def "test new HAMT.Writer(1, 1).dump"() {
    //     given:
    //     def hamtWriter = new HAMT.Writer(1, 1)

    //     expect:
    //     hamtWriter.dump(keys, values).collect { it & 0xff } == bytes

    //     where:
    //     keys | values | bytes
    //     // keys: 0b00_000_000
    //     [0L] | [[3] as byte[]] | [
    //         *shortToBytes((short) 0b000_0_00_00_000_00001),
    //         0b0000_0001, 3
    //     ]
    // }

    // def "test new HAMT.Writer(1, 4).dump"() {
    //     given:
    //     def hamtWriter = new HAMT.Writer(1, 4)

    //     expect:
    //     hamtWriter.dump(keys, values).collect { it & 0xff } == bytes

    //     where:
    //     keys | values | bytes
    //     // keys: 0b00_000_000
    //     [0L] | [[3, 0, 0, 0] as byte[]] | [
    //         *shortToBytes((short) 0b000_0_10_00_000_00001),
    //         0b0000_0001, 3, 0, 0, 0
    //     ]
    //     // keys: 0b00_000_111
    //     [7L] | [[3, 0, 0, 0] as byte[]] | [
    //         *shortToBytes((short) 0b000_0_10_00_000_00001),
    //         0b1000_0000, 3, 0, 0, 0,
    //     ]
    //     // keys: 0b00_001_000
    //     [8L] | [[3, 0, 0, 0] as byte[]] | [
    //         *shortToBytes((short) 0b000_0_10_00_000_00010),
    //         0b0000_0010, 2,
    //         0b0000_0001, 3, 0, 0, 0,
    //     ]
    //     // keys: 0b01_000_000
    //     [64L] | [[3, 0, 0, 0] as byte[]] | [
    //         *shortToBytes((short) 0b000_0_10_00_000_00011),
    //         0b0000_0010, 2,
    //         0b0000_0001, 4,
    //         0b0000_0001, 3, 0, 0, 0,
    //     ]
    //     // keys: 0b00_000_000, 0b00_001_101, 0b10_011_111
    //     [0L, 13L, 159L] | [
    //         [1, 0, 0, 0] as byte[],
    //         [2, 0, 0, 0] as byte[],
    //         [3, 0, 0, 0] as byte[]
    //     ] | [
    //          *shortToBytes((short) 0b000_0_10_00_000_00011),
    //          0b0000_0101, 3, 6,       // (0, 13), 159
    //          0b0000_0011, 8, 13,      // 0, 13
    //          0b0000_1000, 18,         // 159
    //          0b0000_0001, 1, 0, 0, 0, // 0
    //          0b0010_0000, 2, 0, 0, 0, // 13
    //          0b1000_0000, 3, 0, 0, 0  // 159
    //     ]
    // }

    // def "test new HAMT.Writer(2, 4).dump"() {
    //     given:
    //     def hamtWriter = new HAMT.Writer(2, 4)

    //     expect:
    //     hamtWriter.dump(keys, values).collect { it & 0xff } == bytes

    //     where:
    //     keys | values | bytes
    //     [1L] | [[3, 0, 0, 0] as byte[]] | [
    //         *shortToBytes((short) 0b000_0_10_00_001_00001),
    //         0b0000_0010, 0b0000_0000, 3, 0, 0, 0
    //     ]
    //     [0L, 13L] | [
    //         [3, 0, 0, 0] as byte[],
    //         [1, 0, 0, 0] as byte[]
    //     ] | [
    //         *shortToBytes((short) 0b000_0_10_00_001_00001),
    //         0b0000_0001, 0b0010_0000, 3, 0, 0, 0, 1, 0, 0, 0
    //     ]
    //     // keys: 0b0000_0000, 0b0000_1101, 0b0001_1111
    //     [0L, 13L, 31L] | [
    //         [3, 0, 0, 0] as byte[],
    //         [1, 0, 0, 0] as byte[],
    //         [2, 0, 0, 0] as byte[]
    //     ] | [
    //         *shortToBytes((short) 0b000_0_10_00_001_00010),
    //         0b0000_0011, 0b0000_0000, 4, 14,
    //         0b0000_0001, 0b0010_0000, 3, 0, 0, 0, 1, 0, 0, 0,
    //         0b0000_0000, 0b1000_0000, 2, 0, 0, 0
    //     ]
    // }

    // def "test new HAMT.Writer(4, 1).dump"() {
    //     given:
    //     def hamtWriter = new HAMT.Writer(4, 1)

    //     expect:
    //     hamtWriter.dump(keys, values).collect { it & 0xff } == bytes

    //     where:
    //     keys | values | bytes
    //     // keys: 0b000_00000
    //     [0L] | [[3] as byte[]] | [
    //         *shortToBytes((short) 0b000_0_00_00_010_00001),
    //         0b0000_0001, 0b0000_0000, 0b0000_0000, 0b0000_0000, 3
    //     ]
    //     // keys: 0b001_00000
    //     [32L] | [[3] as byte[]] | [
    //         *shortToBytes((short) 0b000_0_00_00_010_00010),
    //         0b0000_0010, 0b0000_0000, 0b0000_0000, 0b0000_0000, 5,
    //         0b0000_0001, 0b0000_0000, 0b0000_0000, 0b0000_0000, 3
    //     ]
    // }

    // def "test new HAMT.Writer(8, 1).dump"() {
    //     given:
    //     def hamtWriter = new HAMT.Writer(8, 1)

    //     expect:
    //     hamtWriter.dump(keys, values).collect { it & 0xff } == bytes

    //     where:
    //     keys | values | bytes
    //     // keys: 0b000_00000
    //     [0L] | [[3] as byte[]] | [
    //         *shortToBytes((short) 0b000_0_00_00_011_00001),
    //         0b0000_0001, 0b0000_0000, 0b0000_0000, 0b0000_0000, 0b0000_0000, 0b0000_0000, 0b0000_0000, 0b0000_0000, 3
    //     ]
    //     // keys: 0b001_00000
    //     [64L] | [[3] as byte[]] | [
    //         *shortToBytes((short) 0b000_0_00_00_011_00010),
    //         0b0000_0010, 0b0000_0000, 0b0000_0000, 0b0000_0000, 0b0000_0000, 0b0000_0000, 0b0000_0000, 0b0000_0000, 9,
    //         0b0000_0001, 0b0000_0000, 0b0000_0000, 0b0000_0000, 0b0000_0000, 0b0000_0000, 0b0000_0000, 0b0000_0000, 3
    //     ]
    // }

    // def "test new HAMT.Reader().exists"() {
    //     given:
    //     def hamtWriter = new HAMT.Writer(2, 4)

    //     when:
    //     def reader = new HAMT.Reader(hamtWriter.dump(keys, values))
    //     then:
    //     checkExists(reader, keys, values, 0L..100L)

    //     where:
    //     keys | values
    //     [0L, 13L, 31L] | [
    //         [3, 0, 0, 0] as byte[],
    //         [1, 0, 0, 0] as byte[],
    //         [2, 0, 0, 0] as byte[]
    //     ]
    // }

    // def "test new HAMT.Reader().get [bitmaskSize: 1, valueSize: 1]"() {
    //     given:
    //     def hamtWriter = new HAMT.Writer(1, 1)

    //     when:
    //     def reader = new HAMT.Reader(hamtWriter.dump(keys, values))
    //     then:
    //     checkGet(reader, keys, values, 0L..100L, defaultValue)

    //     where:
    //     keys | values | defaultValue
    //     [0L, 13L, 31L] |
    //     [
    //         [1] as byte[],
    //         [2] as byte[],
    //         [3] as byte[]
    //     ] |
    //     [0xff] as byte[]
    // }

    // def "test new HAMT.Reader().get [bitmaskSize: 1, valueSize: 4]"() {
    //     given:
    //     def hamtWriter = new HAMT.Writer(1, 4)

    //     when:
    //     def reader = new HAMT.Reader(hamtWriter.dump(keys, values))
    //     then:
    //     checkGet(reader, keys, values, 0L..100L, defaultValue)

    //     where:
    //     keys | values | defaultValue
    //     [0L, 13L, 31L] |
    //     [
    //         [0, 0, 0, 3] as byte[],
    //         [0, 0, 0, 1] as byte[],
    //         [0, 0, 0, 2] as byte[]
    //     ] |
    //     [0xff, 0xff, 0xff, 0xff] as byte[]
    // }

    // def "test new HAMT.Reader().get [bitmaskSize: 2, valueSize: 4]"() {
    //     given:
    //     def hamtWriter = new HAMT.Writer(2, 4)

    //     when:
    //     def reader = new HAMT.Reader(hamtWriter.dump(keys, values))
    //     then:
    //     checkGet(reader, keys, values, 0L..100L, defaultValue)

    //     where:
    //     keys | values | defaultValue
    //     [0L, 13L, 31L] |
    //     [
    //         [0, 0, 0, 3] as byte[],
    //         [0, 0, 0, 1] as byte[],
    //         [0, 0, 0, 2] as byte[]
    //     ] |
    //     [0xff, 0xff, 0xff, 0xff] as byte[]
    // }

    def "test new HAMT.Reader().get [bitmaskSize: 2, valueSize: 4] with different ptrSize"() {
        given:
        def hamtWriter = new HAMT.Writer(2, 4)

        when:
        def keys = keyRange.step(keyStep).collect { it }
        def values = keyRange.step(keyStep).collect { intToBytes((int) it * 2) }
        def reader = new HAMT.Reader(hamtWriter.dump(keys, values))
        then:
        checkGet(reader, keys, values, keyRange, defaultValue)

        where:
        keyRange | keyStep | defaultValue
        0L..1000L | 3 | [0xff, 0xff, 0xff, 0xff] as byte[]
        0L..100000L | 7 | [0xff, 0xff, 0xff, 0xff] as byte[]
    }
}
