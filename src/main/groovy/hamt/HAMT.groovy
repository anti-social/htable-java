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
    private int shift_mask
    private int valueSize
    def levelDataFactory

    static private int VERSION = 0
    static private int[] BITMASK_SIZES = [0, 1, -1, 2, -1, -1, -1, 3] as int[]
    static private int[] POINTER_SIZES = [0, 1, 2] as int[]
    static private int[] VALUE_SIZES = [0, 1, -1, 2, -1, -1, -1, 3] as int[]
    static private def SHIFT_MASKS = [3: 0b0000_0111, 4: 0b0000_1111, 5: 0b0001_1111, 6: 0b0011_1111]

    public HAMT(bitmaskSize, valueSize) {
        assert bitmaskSize == 1 || bitmaskSize == 2 || bitmaskSize == 4 || bitmaskSize == 8
        assert valueSize == 1 || valueSize == 2 || valueSize == 4 || valueSize == 8
        this.bitmaskSize = bitmaskSize
        this.shift = BITMASK_SIZES[bitmaskSize - 1] + 3
        this.shift_mask = SHIFT_MASKS[this.shift]
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

    def getPtrSize(layers) {
        int ptrSize
        for (ps in (0..3)) {
            ptrSize = ps + 1
            def maxSize = 1 << (8 * ptrSize)
            def size = 0
            for (l in layers) {
                size += l.size(ptrSize, this.valueSize)
                if (size > maxSize) {
                    break
                }
            }
            if (size > maxSize) {
                continue
            } else {
                break
            }
        }
        return ptrSize
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
        def layers = [new LevelData2()]
        def layersMap = [:]
        for (e in map) {
            layersMap[e.key] = layers[0]
        }
        for (int l = levels; l > 0; l--) {
            def prevSubLayer
            for (e in map) {
                int k = e.key >>> ((l - 1) * this.shift) & this.shift_mask
                def layer = layersMap[e.key]
                if (l == 1) {
                    layer.addValue(e.value)
                } else {
                    def subLayer = layer.newLayer(k)
                    if (!subLayer.is(prevSubLayer)) {
                        layers.add(subLayer)
                    }
                    prevSubLayer = subLayer
                    layersMap[e.key] = subLayer
                }
                layer.setBit(k)
            }
        }
        def ptrSize = getPtrSize(layers)

        def bufferSize = 2
        for (layer in layers) {
            def layerSize = layer.size(ptrSize, valueSize)
            layer.setOffset(bufferSize - 2)
            bufferSize += layerSize
        }
        def buffer = ByteBuffer.allocate(bufferSize)
        // buffer.order(ByteOrder.LITTLE_ENDIAN)
        short header = getHeader(levels, ptrSize)
        buffer.putShort(header)
        for (layer in layers) {
            layer.dump(buffer)
        }
        return buffer.array()
    }

    class LevelData2 {
        public short bitmask
        public int offset
        public def layers = []
        public def values = []

        def setBit(n) {
            this.bitmask |= 1 << n
        }

        def newLayer(n) {
            if ((this.bitmask & (1 << n)) != 0) {
                return this.layers.last()
            }
            else {
                def l = new LevelData2()
                this.layers.add(l)
                return l
            }
        }

        def addValue(v) {
            this.values.add(v)
        }

        def setOffset(o) {
            this.offset = o
        }

        def size(ptrSize, valueSize) {
            return 2 + layers.size() * ptrSize + values.size() * valueSize
        }
        
        def dump(buffer) {
            buffer.putShort(this.bitmask)
            if (!layers.isEmpty()) {
                for (l in layers) {
                    buffer.put((byte)(l.offset))
                }
            } else {
                for (v in values) {
                    buffer.putInt(v)
                }
            }
        }
    }
}
