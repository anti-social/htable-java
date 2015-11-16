package hamt

import java.nio.ByteBuffer
import java.nio.ByteOrder


/**
 *  Hash array mapped trie implementation in Java
 *
 *  <Header><Data>
 *
 *  Header:
 *
 *  |-4b-|b|2b|2b|2b|-5b--|
 *     |  |  |  |  |   |
 *     |  |  |  |  |   Number of levels (n)
 *     |  |  |  |  |
 *     |  |  |  |  Bitmask size in bytes (2^n)
 *     |  |  |  |
 *     |  |  |  Pointer size in bytes (n+1)
 *     |  |  |
 *     |  |  Value size (2^n)
 *     |  Variable value size flag (not implemented yet)
 *     |
 *     Reserved
 *
 *  Data:
 *
 *  [<Bitmask><LayerData>]
 */
class HAMT {
    static private int NUM_LEVELS_OFFSET = 0
    static private int BITMASK_SIZE_OFFSET = 5
    static private int PTR_SIZE_OFFSET = 7
    static private int VALUE_SIZE_OFFSET = 9
    static private int[] BITMASK_SIZES = [0, 1, -1, 2, -1, -1, -1, 3] as int[]
    static private int[] POINTER_SIZES = [0, 1, 2] as int[]
    static private int[] VALUE_SIZES = [0, 1, -1, 2, -1, -1, -1, 3] as int[]
    static private def SHIFT_MASKS = [3: 0b0000_0111, 4: 0b0000_1111, 5: 0b0001_1111, 6: 0b0011_1111]

    static enum BitmaskSize {
        BYTE(1), SHORT(2), INT(4), LONG(8)

        private final int size

        BitmaskSize(int size) { this.size = size }

        int size() { return this.size }
    }
    
    static enum ValueSize {
        BYTE(1), SHORT(2), INT(4), LONG(8), VAR(-1)

        private final int size

        ValueSize(int size) { this.size = size }

        int size() { return this.size }
    }
    
    static class Writer {
        private final int bitmaskSize
        private final int shift
        private final int shift_mask
        private final int valueSize

        public Writer(BitmaskSize bitmaskSize, ValueSize valueSize) {
            this(bitmaskSize.size(), valueSize.size())
        }

        private Writer(int bitmaskSize, int valueSize) {
            this.bitmaskSize = bitmaskSize
            this.shift = BITMASK_SIZES[this.bitmaskSize - 1] + 3
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

        def getHeader(int numLevels, int ptrSize) {
            assert 1 <= ptrSize && ptrSize <= 4
            short header = 0
            header |= numLevels << NUM_LEVELS_OFFSET
            header |= BITMASK_SIZES[this.bitmaskSize - 1] << BITMASK_SIZE_OFFSET
            header |= (ptrSize - 1) << PTR_SIZE_OFFSET
            header |= VALUE_SIZES[this.valueSize - 1] << VALUE_SIZE_OFFSET
            return header
        }

        def dump(map) {
            int numLevels = getLevels(map)
            def layers = [new LayerData(this.bitmaskSize)]
            def layersMap = [:]
            for (e in map) {
                layersMap[e.key] = layers[0]
            }
            for (int l = numLevels; l > 0; l--) {
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
            buffer.order(ByteOrder.LITTLE_ENDIAN)
            buffer.putShort(getHeader(numLevels, ptrSize))
            for (layer in layers) {
                layer.dump(buffer, ptrSize)
            }
            return buffer.array()
        }

        class LayerData {
            public byte[] bitmask
            public int offset
            public def layers = []
            public def values = []

            LayerData(int bitmaskSize) {
                this.bitmask = new byte[bitmaskSize]
            }
        
            def setBit(int k) {
                int n = k >>> 3
                int b = k & 0b0000_0111
                this.bitmask[n] |= 1 << b
            }

            def newLayer(int k) {
                int n = k >>> 3
                int b = k & 0b0000_0111
                if ((this.bitmask[n] & (1 << b)) != 0) {
                    return this.layers.last()
                }
                else {
                    def l = new LayerData(this.bitmask.length)
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

            def size(int ptrSize, int valueSize) {
                return bitmask.length + layers.size() * ptrSize + values.size() * valueSize
            }
        
            def dump(ByteBuffer buffer, int ptrSize) {
                buffer.put(this.bitmask)
                if (!layers.isEmpty()) {
                    for (l in layers) {
                        buffer.put(Utils.ptrToByteArrayLE(l.offset, ptrSize))
                    }
                } else {
                    for (v in values) {
                        buffer.put(v)
                    }
                }
            }
        }
    }

    static class Reader {
        private final int numLevels;
        private final int bitmaskSize;
        private final int ptrSize;
        private final int valueSize;
        private final ByteBuffer buffer

        private static int LEVELS_MASK = 0b0001_1111
        private static int BITMASK_SIZE_MASK = 0b0000_0011
        private static int PTR_SIZE_MASK = 0b0000_0011
        private static int VALUE_SIZE_MASK = 0b0000_0011

        private static byte[] BIT_COUNT_MASKS = [
            0b0000_0000,
            0b0000_0001,
            0b0000_0011,
            0b0000_0111,
            0b0000_1111,
            0b0001_1111,
            0b0011_1111,
            0b0111_1111
        ]
        private static byte[] BIT_COUNTS = new byte[256]
        static {
            for (i in 0..255) {
                BIT_COUNTS[i] = Integer.bitCount(i)
            }
        }

        public Reader(byte[] data) {
            buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN)
            short header = buffer.getShort()
            this.numLevels = ((header >>> NUM_LEVELS_OFFSET) & LEVELS_MASK)
            this.bitmaskSize = 1 << ((header >>> BITMASK_SIZE_OFFSET) & BITMASK_SIZE_MASK)
            this.ptrSize = ((header >>> PTR_SIZE_OFFSET) & PTR_SIZE_MASK) + 1
            this.valueSize = 1 << ((header >>> VALUE_SIZE_OFFSET) & VALUE_SIZE_MASK)
            this.buffer = buffer.slice()
        }

        def getValueOffset(int key) {
            this.buffer.position(0)
            int shift = BITMASK_SIZES[this.bitmaskSize - 1] + 3
            int shift_mask = SHIFT_MASKS[shift]

            if (key >>> (numLevels * shift) > 0) {
                return -1
            }

            int layerOffset = 0
            int ptrOffset = 0
            byte[] bitmask = new byte[bitmaskSize]
            for (int level = numLevels - 1; level >= 0; level--) {
                int k = key >>> (level * shift) & shift_mask
                int nByte = k >>> 3
                int nBit = k & 0b0000_0111
                this.buffer.position(layerOffset)
                this.buffer.get(bitmask)
                if ((bitmask[nByte] & (1 << nBit)) == 0) {
                    return -1
                }
                ptrOffset = BIT_COUNTS[bitmask[nByte] & BIT_COUNT_MASKS[nBit]]
                for (int bitmaskIx = nByte - 1; bitmaskIx >= 0; bitmaskIx--) {
                    byte bitmaskByte = bitmask[bitmaskIx]
                    ptrOffset += BIT_COUNTS[bitmaskByte]
                }
                if (level != 0) {
                    this.buffer.position(layerOffset + bitmask.length + ptrOffset * ptrSize)
                    byte[] nextLayoutOffsetBuffer = new byte[ptrSize]
                    this.buffer.get(nextLayoutOffsetBuffer)
                    layerOffset = Utils.byteArrayToPtrLE(nextLayoutOffsetBuffer)
                }
            }
            return layerOffset + bitmask.length + ptrOffset * valueSize
        }

        def exists(int key) {
            int valueOffset = getValueOffset(key)
            return valueOffset > 0 ? true : false
        }

        def get(int key, byte[] defaultValue) {
            int valueOffset = getValueOffset(key)
            if (valueOffset > 0) {
                byte[] value = new byte[this.valueSize]
                this.buffer.position(valueOffset)
                this.buffer.get(value)
                return value
            }
            return defaultValue
        }
    }

    static class Utils {
        static byte[] ptrToByteArrayLE(int ptr, int ptrSize) {
            byte[] res = new byte[ptrSize]
            for (i in 0..<ptrSize) {
                res[i] = (ptr >>> (i * 8)) & 0xff
            }
            return res
        }

        static int byteArrayToPtrLE(byte[] array) {
            int ptrSize = array.length
            int ptr = array[0] & 0xff
            for (i in 0..<ptrSize) {
                ptr |= (array[i] & 0xff) << (i * 8)
            }
            return ptr
        }

        static byte[] ptrToByteArrayBE(int ptr, int ptrSize) {
            byte[] res = new byte[ptrSize]
            for (i in 0..<ptrSize) {
                res[ptrSize - i - 1] = (ptr >>> (i * 8)) & 0xff
            }
            return res
        }

        static int byteArrayToPtrBE(byte[] array) {
            int ptrSize = array.length
            int ptr = (array[0] & 0xff) << ((ptrSize - 1) * 8)
            for (i in 0..<ptrSize) {
                ptr |= (array[i] & 0xff) << ((ptrSize - 1 - i) * 8)
            }
            return ptr
        }
    }
}
