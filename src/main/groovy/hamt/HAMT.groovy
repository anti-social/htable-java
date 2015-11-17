package hamt

import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.HashMap
import java.util.List
import java.util.Map

import groovy.transform.CompileStatic


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
@CompileStatic
class HAMT {
    static public final int NUM_LEVELS_OFFSET = 0
    static public final int BITMASK_SIZE_OFFSET = 5
    static public final int PTR_SIZE_OFFSET = 7
    static public final int VALUE_SIZE_OFFSET = 9
    static public final int VARIABLE_VALUE_SIZE_OFFSET = 11
    static public final int[] POINTER_SIZES = [0, 1, 2] as int[]

    static enum BitmaskSize {
        BYTE(1), SHORT(2), INT(4), LONG(8)

        public final int size
        public final int shiftBits
        public final int shiftMask

        BitmaskSize(int size) {
            this.size = size
            this.shiftBits = 31 - Integer.numberOfLeadingZeros(this.size << 3)
            this.shiftMask = (1 << this.shiftBits) - 1
        }

        int encode() {
            return this.shiftBits - 3
        }

        static BitmaskSize get(int size) {
            for (BitmaskSize bitmaskSize : values()) {
                if (bitmaskSize.size == size) {
                    return bitmaskSize
                }
            }
            return null
        }
    }
    
    static enum ValueSize {
        BYTE(1), SHORT(2), INT(4), LONG(8), VAR(-1)

        public final int size
        public final int shiftBits

        ValueSize(int size) {
            this.size = size
            this.shiftBits = 31 - Integer.numberOfLeadingZeros(this.size << 3)
        }

        int encode() {
            return this.shiftBits - 3
        }

        static ValueSize get(int size) {
            for (ValueSize valueSize : values()) {
                if (valueSize.size == size) {
                    return valueSize
                }
            }
            return null
        }
    }
    
    static class Writer {
        private final BitmaskSize bitmaskSize
        private final ValueSize valueSize

        public Writer(BitmaskSize bitmaskSize, ValueSize valueSize) {
            this.bitmaskSize = bitmaskSize
            this.valueSize = valueSize
        }

        private Writer(int bitmaskSize, int valueSize) {
            this(BitmaskSize.get(bitmaskSize), ValueSize.get(valueSize))
        }

        int getLevels(Map<Long,byte[]> map) {
            long maxKey = map.max { it.key }.key
            int levels = 1
            long key = maxKey >>> this.bitmaskSize.shiftBits
            while (key != 0) {
                levels++
                    key = key >>> this.bitmaskSize.shiftBits
            }
            return levels
        }

        int getPtrSize(List<LayerData> layers) {
            int ptrSize
            for (ps in (0..3)) {
                ptrSize = ps + 1
                int maxSize = 1 << (8 * ptrSize)
                int size = 0
                for (LayerData l : layers) {
                    size += l.size(ptrSize, this.valueSize.size)
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

        short getHeader(int numLevels, int ptrSize) {
            assert 1 <= ptrSize && ptrSize <= 4
            int header = 0
            header |= numLevels << NUM_LEVELS_OFFSET
            header |= this.bitmaskSize.encode() << BITMASK_SIZE_OFFSET
            header |= (ptrSize - 1) << PTR_SIZE_OFFSET
            header |= this.valueSize.encode() << VALUE_SIZE_OFFSET
            return (short) header
        }

        byte[] dump(Map<Long,byte[]> map) {
            int numLevels = getLevels(map)
            List<LayerData> layers = [new LayerData(this.bitmaskSize.size)]
            Map<Long,LayerData> layersMap = [:]
            for (e in map) {
                layersMap[e.key] = layers[0]
            }
            for (int l = numLevels; l > 0; l--) {
                LayerData prevSubLayer
                for (Map.Entry<Long,byte[]> e : map) {
                    int k = (int) e.key >>> ((l - 1) * this.bitmaskSize.shiftBits) & this.bitmaskSize.shiftMask
                    LayerData layer = layersMap[e.key]
                    if (l == 1) {
                        layer.addValue(e.value)
                    } else {
                        LayerData subLayer = layer.newLayer(k)
                        if (!subLayer.is(prevSubLayer)) {
                            layers.add(subLayer)
                        }
                        prevSubLayer = subLayer
                        layersMap[e.key] = subLayer
                    }
                    layer.setBit(k)
                }
            }
            int ptrSize = getPtrSize(layers)

            int bufferSize = 2
            for (layer in layers) {
                def layerSize = layer.size(ptrSize, valueSize.size)
                layer.setOffset(bufferSize - 2)
                bufferSize += layerSize
            }
            ByteBuffer buffer = ByteBuffer.allocate(bufferSize)
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
            public List<LayerData> layers = []
            public List<byte[]> values = []

            LayerData(int bitmaskSize) {
                this.bitmask = new byte[bitmaskSize]
            }
        
            void setBit(int k) {
                int n = k >>> 3
                int b = k & 0b0000_0111
                this.bitmask[n] = (byte) (this.bitmask[n] | (1 << b))
            }

            LayerData newLayer(int k) {
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

            void addValue(byte[] v) {
                this.values.add(v)
            }

            void setOffset(int o) {
                this.offset = o
            }

            int size(int ptrSize, int valueSize) {
                return bitmask.length + layers.size() * ptrSize + values.size() * valueSize
            }
        
            void dump(ByteBuffer buffer, int ptrSize) {
                buffer.put(this.bitmask)
                if (!this.layers.isEmpty()) {
                    for (LayerData l : this.layers) {
                        buffer.put(Utils.ptrToByteArrayLE(l.offset, ptrSize))
                    }
                } else {
                    for (byte[] v : this.values) {
                        buffer.put(v)
                    }
                }
            }
        }
    }

    static class Reader {
        private final int numLevels;
        private final BitmaskSize bitmaskSize;
        private final int ptrSize;
        private final ValueSize valueSize;
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
                BIT_COUNTS[i] = Integer.bitCount(i) as byte
            }
        }

        public Reader(byte[] data) {
            ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN)
            short header = buffer.getShort()
            this.numLevels = ((header >>> NUM_LEVELS_OFFSET) & LEVELS_MASK)
            this.bitmaskSize = BitmaskSize.get(1 << ((header >>> BITMASK_SIZE_OFFSET) & BITMASK_SIZE_MASK))
            this.ptrSize = ((header >>> PTR_SIZE_OFFSET) & PTR_SIZE_MASK) + 1
            this.valueSize = ValueSize.get(1 << ((header >>> VALUE_SIZE_OFFSET) & VALUE_SIZE_MASK))
            this.buffer = buffer.slice()
        }

        int getValueOffset(long key) {
            this.buffer.position(0)

            if (
                this.numLevels * this.bitmaskSize.shiftBits < 64
                && key >>> (this.numLevels * this.bitmaskSize.shiftBits) > 0
            ) {
                return -1
            }

            int layerOffset = 0
            int ptrOffset = 0
            byte[] bitmask = new byte[this.bitmaskSize.size]
            for (int level = numLevels - 1; level >= 0; level--) {
                long k = key >>> (level * this.bitmaskSize.shiftBits) & this.bitmaskSize.shiftMask
                int nByte = (int) (k >>> 3)
                int nBit = (int) (k & 0b0000_0111)
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
            return layerOffset + bitmask.length + ptrOffset * this.valueSize.size
        }

        boolean exists(int key) {
            int valueOffset = getValueOffset(key)
            return valueOffset > 0 ? true : false
        }

        byte[] get(int key, byte[] defaultValue) {
            int valueOffset = getValueOffset(key)
            if (valueOffset > 0) {
                byte[] value = new byte[this.valueSize.size]
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
                res[i] = ((ptr >>> (i * 8)) & 0xff) as byte
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
                res[ptrSize - i - 1] = ((ptr >>> (i * 8)) & 0xff) as byte
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
