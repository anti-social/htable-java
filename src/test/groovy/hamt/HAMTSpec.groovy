package hamt

import spock.lang.Specification


class HAMTSpec extends Specification {
    def testGetLevels() {
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
        [(65536):1] | 5
        [(268435455):1] | 7
        [(268435456):1] | 8
    }

    def testGetHeader() {
        setup:
        def hamtWriter = new HAMT(2, 4)

        expect:
        Integer.toBinaryString((int)hamtWriter.getHeader(levels, ptrSize)) == Integer.toBinaryString(header)

        where:
        levels | ptrSize | header
        1 | 1 | 0b010_00_01_00001_0000
    }

    def testDump() {
        setup:
        def hamtWriter = new HAMT(2, 4)

        expect:
        hamtWriter.dump(map).collect { it & 0xff } == bytes

        where:
        map | bytes
        [(1): 3] | [0b010_00_01_0, 0b0001_0000, 0b0000_0000, 0b0000_0010, 0, 0, 0, 3]
        [(0): 3, (13): 1] | [0b010_00_01_0, 0b0001_0000, 0b0010_0000, 0b0000_0001, 0, 0, 0, 3, 0, 0, 0, 1]
        // 0b0000_0000, 0b0000_1101, 0b0001_1111
        [(0): 3, (13): 1, (31): 2] | [
            0b010_00_01_0, 0b0010_0000,
            0b0000_0000, 0b0000_0011, 4, 14,
            0b0010_0000, 0b0000_0001, 0, 0, 0, 3, 0, 0, 0, 1,
            0b1000_0000, 0b0000_0000, 0, 0, 0, 2
        ]
    }
}
