package hamt

import spock.lang.Specification


class HAMTSpec extends Specification {
    def "test new HAMT(1, 4).getLevels"() {
        setup:
        def hamtWriter = new HAMT(1, 4)

        expect:
        hamtWriter.getLevels(map) == levels

        where:
        map | levels
        [(0):1] | 1
        [(1):1] | 1
        [(2):1] | 1
        [(7):1] | 1
        [(8):1] | 2
        [(63):1] | 2
        [(64):1] | 3
        [(511):1] | 3
        [(512):1] | 4
        [(4095):1] | 4
        [(4096):1] | 5
        [(32767):1] | 5
        [(32768):1] | 6
        [(262143):1] | 6
        [(262144):1] | 7
        [(2097151):1] | 7
        [(2097152):1] | 8
        [(16777215):1] | 8
        [(16777216):1] | 9
        [(134217727):1] | 9
        [(134217728):1] | 10
        [(1073741823):1] | 10
        [(1073741824):1] | 11
    }

    def "test new HAMT(2, 4).getLevels"() {
        setup:
        def hamtWriter = new HAMT(2, 4)

        expect:
        hamtWriter.getLevels(map) == levels

        where:
        map | levels
        [(1):1] | 1
        [(2):1] | 1
        [(15):1] | 1
        [(16):1] | 2
        [(32):1] | 2
        [(255):1] | 2
        [(256):1] | 3
        [(4096):1] | 4
        [(65535):1] | 4
        [(65536):1] | 5
        [(1048575):1] | 5
        [(1048576):1] | 6
        [(16777215):1] | 6
        [(16777216):1] | 7
        [(268435455):1] | 7
        [(268435456):1] | 8
    }

    def "test new HAMT(4, 4).getLevels"() {
        setup:
        def hamtWriter = new HAMT(4, 4)

        expect:
        hamtWriter.getLevels(map) == levels

        where:
        map | levels
        [(0):1] | 1
        [(1):1] | 1
        [(2):1] | 1
        [(31):1] | 1
        [(32):1] | 2
        [(1023):1] | 2
        [(1024):1] | 3
        [(32767):1] | 3
        [(32768):1] | 4
        [(1048575):1] | 4
        [(1048576):1] | 5
        [(33554431):1] | 5
        [(33554432):1] | 6
        [(1073741823):1] | 6
        [(1073741824):1] | 7
    }

    def "test new HAMT(8, 4).getLevels"() {
        setup:
        def hamtWriter = new HAMT(8, 4)

        expect:
        hamtWriter.getLevels(map) == levels

        where:
        map | levels
        [(0):1] | 1
        [(1):1] | 1
        [(2):1] | 1
        [(63):1] | 1
        [(64):1] | 2
        [(4095):1] | 2
        [(4096):1] | 3
        [(262143):1] | 3
        [(262144):1] | 4
        [(16777215):1] | 4
        [(16777216):1] | 5
        [(1073741823):1] | 5
        [(1073741824):1] | 6
    }

    def "test new HAMT(2, 4).getHeader"() {
        setup:
        def hamtWriter = new HAMT(2, 4)

        expect:
        Integer.toBinaryString((int)hamtWriter.getHeader(levels, ptrSize)) == Integer.toBinaryString(header)

        where:
        levels | ptrSize | header
        1 | 1 | 0b010_00_01_00001_0000
        1 | 2 | 0b010_01_01_00001_0000
        1 | 3 | 0b010_10_01_00001_0000
        1 | 4 | 0b010_11_01_00001_0000
        2 | 1 | 0b010_00_01_00010_0000
        3 | 1 | 0b010_00_01_00011_0000
        4 | 1 | 0b010_00_01_00100_0000
        31 | 1 | 0b010_00_01_11111_0000
    }

    def "test new HAMT(1, 1).dump"() {
        setup:
        def hamtWriter = new HAMT(1, 1)

        expect:
        hamtWriter.dump(map).collect { it & 0xff } == bytes

        where:
        map | bytes
        // keys: 0b00_000_000
        [(0): [3] as byte[]] | [
            0b0_00_00_00_0, 0b0001_0000,
            0b0000_0001, 3
        ]
    }

    def "test new HAMT(1, 4).dump"() {
        setup:
        def hamtWriter = new HAMT(1, 4)

        expect:
        hamtWriter.dump(map).collect { it & 0xff } == bytes

        where:
        map | bytes
        // keys: 0b00_000_000
        [(0): [0, 0, 0, 3] as byte[]] | [
            0b010_00_00_0, 0b0001_0000,
            0b0000_0001, 0, 0, 0, 3
        ]
        // keys: 0b00_000_111
        [(7): [0, 0, 0, 3] as byte[]] | [
            0b010_00_00_0, 0b0001_0000,
            0b1000_0000, 0, 0, 0, 3,
        ]
        // keys: 0b00_001_000
        [(8): [0, 0, 0, 3] as byte[]] | [
            0b010_00_00_0, 0b0010_0000,
            0b0000_0010, 2,
            0b0000_0001, 0, 0, 0, 3
        ]
        // keys: 0b01_000_000
        [(64): [0, 0, 0, 3] as byte[]] | [
            0b010_00_00_0, 0b0011_0000,
            0b0000_0010, 2,
            0b0000_0001, 4,
            0b0000_0001, 0, 0, 0, 3
        ]
        // keys: 0b00_000_000, 0b00_001_101, 0b10_011_111
        [(0): [0, 0, 0, 1] as byte[],
         (13): [0, 0, 0, 2] as byte[],
         (159): [0, 0, 0, 3] as byte[]] | [
            0b010_00_00_0, 0b0011_0000,
            0b0000_0101, 3, 6,       // (0, 13), 159
            0b0000_0011, 8, 13,      // 0, 13
            0b0000_1000, 18,         // 159
            0b0000_0001, 0, 0, 0, 1, // 0
            0b0010_0000, 0, 0, 0, 2, // 13
            0b1000_0000, 0, 0, 0, 3  // 159
        ]
    }

    def "test new HAMT(2, 4).dump"() {
        setup:
        def hamtWriter = new HAMT(2, 4)

        expect:
        hamtWriter.dump(map).collect { it & 0xff } == bytes

        where:
        map | bytes
        [(1): [0, 0, 0, 3] as byte[]] | [
            0b010_00_01_0, 0b0001_0000,
            0b0000_0010, 0b0000_0000, 0, 0, 0, 3
        ]
        [(0): [0, 0, 0, 3] as byte[],
         (13): [0, 0, 0, 1] as byte[]] | [
            0b010_00_01_0, 0b0001_0000,
            0b0000_0001, 0b0010_0000, 0, 0, 0, 3, 0, 0, 0, 1
        ]
        // keys: 0b0000_0000, 0b0000_1101, 0b0001_1111
        [(0): [0, 0, 0, 3] as byte[],
         (13): [0, 0, 0, 1] as byte[],
         (31): [0, 0, 0, 2] as byte[]] | [
            0b010_00_01_0, 0b0010_0000,
            0b0000_0011, 0b0000_0000, 4, 14,
            0b0000_0001, 0b0010_0000, 0, 0, 0, 3, 0, 0, 0, 1,
            0b0000_0000, 0b1000_0000, 0, 0, 0, 2
        ]
    }

    def "test new HAMT(4, 1).dump"() {
        setup:
        def hamtWriter = new HAMT(4, 1)

        expect:
        hamtWriter.dump(map).collect { it & 0xff } == bytes

        where:
        map | bytes
        // keys: 0b000_00000
        [(0): [3] as byte[]] | [
            0b0_00_00_10_0, 0b0001_0000,
            0b0000_0001, 0b0000_0000, 0b0000_0000, 0b0000_0000, 3
        ]
        // keys: 0b001_00000
        [(32): [3] as byte[]] | [
            0b0_00_00_10_0, 0b0010_0000,
            0b0000_0010, 0b0000_0000, 0b0000_0000, 0b0000_0000, 5,
            0b0000_0001, 0b0000_0000, 0b0000_0000, 0b0000_0000, 3
        ]
    }

    def "test new HAMT(8, 1).dump"() {
        setup:
        def hamtWriter = new HAMT(8, 1)

        expect:
        hamtWriter.dump(map).collect { it & 0xff } == bytes

        where:
        map | bytes
        // keys: 0b000_00000
        [(0): [3] as byte[]] | [
            0b0_00_00_11_0, 0b0001_0000,
            0b0000_0001, 0b0000_0000, 0b0000_0000, 0b0000_0000, 0b0000_0000, 0b0000_0000, 0b0000_0000, 0b0000_0000, 3
        ]
        // keys: 0b001_00000
        [(64): [3] as byte[]] | [
            0b0_00_00_11_0, 0b0010_0000,
            0b0000_0010, 0b0000_0000, 0b0000_0000, 0b0000_0000, 0b0000_0000, 0b0000_0000, 0b0000_0000, 0b0000_0000, 9,
            0b0000_0001, 0b0000_0000, 0b0000_0000, 0b0000_0000, 0b0000_0000, 0b0000_0000, 0b0000_0000, 0b0000_0000, 3
        ]
    }

    def "test HAMT.Reader.get"() {
        setup:
        def hamtWriter = new HAMT(2, 4)

        when:
        def reader = new HAMT.Reader(hamtWriter.dump(map))
        then:
        for (k in 0..100) {
            if (map.containsKey(k)) {
                assert reader.exists(k) == true
            } else {
                assert reader.exists(k) == false
            }
        }

        where:
        map | _
        [
            (0): [0, 0, 0, 3] as byte[],
            (13): [0, 0, 0, 1] as byte[],
            (31): [0, 0, 0, 2] as byte[]
        ] | _
    }
}
