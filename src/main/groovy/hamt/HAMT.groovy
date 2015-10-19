package hamt

import java.nio.ByteBuffer
import java.nio.ByteOrder


/**
 *  <Header><Data>
 *
 *  Header:
 *
 *  |b|2b|2b|2b|-5b--|-4b-|
 *     |  |  |   |     |
 *     |  |  |   |     Version
 *     |  |  |   |
 *     |  |  |   Number of levels (n+1)
 *     |  |  |
 *     |  |  Bitmask size in bytes (2^n)
 *     |  |
 *     |  Pointer size in bytes (n+1)
 *     |
 *     Value size (2^n)
 *
 *  Data:
 *
 *  [<Bitmask><LevelData>]
 */
class HAMT {
    private int bitmaskSize
    private int shift
    private int valueSize
    def levelDataFactory

    static private int VERSION = 0
    static private int[] BITMASK_SIZES = [0, 1, -1, 2, -1, -1, -1, 3] as int[]
    static private int[] POINTER_SIZES = [0, 1, 2] as int[]
    static private int[] VALUE_SIZES = [0, 1, -1, 2, -1, -1, -1, 3] as int[]

    public HAMT(bitmaskSize, valueSize) {
        assert bitmaskSize == 1 || bitmaskSize == 2 || bitmaskSize == 4 || bitmaskSize == 8
        assert valueSize == 1 || valueSize == 2 || valueSize == 4 || valueSize == 8
        this.bitmaskSize = bitmaskSize
        this.shift = BITMASK_SIZES[bitmaskSize - 1] + 3
        this.levelDataFactory = new LevelDataFactory2()
        this.valueSize = valueSize
    }

    def getLevels(map) {
        def maxKey = map.max { it.key }.key
        def levels = 1
        def key = maxKey >>> this.shift
        while (key != 0) {
            levels++
            key = key >>> this.shift
        }
        return levels
    }

    def getHeader(levels, ptrSize) {
        assert 1 <= ptrSize && ptrSize <= 4
        short header = 0
        header = header | VERSION
        header = header | (levels << 4)
        header = header | (BITMASK_SIZES[this.bitmaskSize - 1] << 9)
        header = header | ((ptrSize - 1) << 11)
        header = header | (VALUE_SIZES[this.valueSize - 1] << 13)
        return header
    }

    def dump(map) {
        int levels = getLevels(map)
        short header = getHeader(levels, 1)
        def rootLevel = this.levelDataFactory.create()
        def values = []
        for (e in map) {
            int k = e.key >>> ((levels - 1) * this.shift)
            rootLevel.bitmask = rootLevel.bitmask | (1 << k)
            values.add(e.value)
        }
        def buffer = ByteBuffer.allocate(2 + 2 + values.size() * this.valueSize)
        buffer.putShort(header)
        buffer.putShort(rootLevel.bitmask)
        for (v in values) {
            buffer.putInt(v)
        }
        return buffer.array()
    }

    class LevelData2 {
        public short bitmask
        
    }

    class LevelDataFactory2 {
        def create() {
            return new LevelData2()
        }
    }
}
