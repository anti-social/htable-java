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
 *  |3b-|b|2b|2b|3b-|-5b--|
 *    |  |  |  |  |   |
 *    |  |  |  |  |   Number of levels (n)
 *    |  |  |  |  |
 *    |  |  |  |  Bitmask size in bytes (2^n)
 *    |  |  |  |
 *    |  |  |  Pointer size in bytes (n+1)
 *    |  |  |
 *    |  |  Value size (2^n)
 *    |  Variable value size flag (not implemented yet)
 *    |
 *     Reserved
 *
 *  Data:
 *
 *  [<Bitmask><LayerData>]
 */
@CompileStatic
public class HAMT {
    public static final int NUM_LEVELS_OFFSET = 0
    public static final int BITMASK_SIZE_OFFSET = 5
    public static final int PTR_SIZE_OFFSET = 8
    public static final int VALUE_SIZE_OFFSET = 10
    public static final int VARIABLE_VALUE_SIZE_OFFSET = 12
    public static final int LEVELS_MASK = 0b0001_1111
    public static final int BITMASK_SIZE_MASK = 0b0000_0111
    public static final int PTR_SIZE_MASK = 0b0000_0011
    public static final int VALUE_SIZE_MASK = 0b0000_0011

    public static enum BitmaskSize {
        BYTE(1), SHORT(2), INT(4), LONG(8)

        private static final Map<Integer,BitmaskSize> sizesMap = new HashMap<>()
        static {
            for (BitmaskSize bitmaskSize : values()) {
                sizesMap.put(bitmaskSize.size, bitmaskSize)
            }
        }

        public final int size
        public final int shiftBits
        public final int shiftMask

        public BitmaskSize(int size) {
            this.size = size
            this.shiftBits = 31 - Integer.numberOfLeadingZeros(this.size << 3)
            this.shiftMask = (1 << this.shiftBits) - 1
        }

        public int encode() {
            return this.shiftBits - 3
        }

        public static BitmaskSize get(int size) {
            return sizesMap.get(size)
        }

        public static BitmaskSize decode(int value) {
            return BitmaskSize.get(1 << value)
        }
    }
    
    public static enum ValueSize {
        BYTE(1), SHORT(2), INT(4), LONG(8), VAR(-1)

        private static final Map<Integer,ValueSize> sizesMap = new HashMap<>()
        static {
            for (ValueSize valueSize : values()) {
                sizesMap.put(valueSize.size, valueSize)
            }
        }

        public final int size
        public final int shiftBits

        public ValueSize(int size) {
            this.size = size
            this.shiftBits = 31 - Integer.numberOfLeadingZeros(this.size << 3)
        }

        public int encode() {
            return this.shiftBits - 3
        }

        public static ValueSize get(int size) {
            return sizesMap.get(size)
        }

        public static ValueSize decode(int value) {
            return ValueSize.get(1 << value)
        }
    }
    
    public static class Writer {
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
            for (int ps = 0; ps <= 3; ps++) {
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

        public byte[] dump(Map<Long,byte[]> map) {
            int numLevels = getLevels(map)
            List<LayerData> layers = [new LayerData(this.bitmaskSize.size)]
            Map<Long,LayerData> layersMap = [:]
            for (Map.Entry<Long,byte[]> e : map) {
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
            for (LayerData layer : layers) {
                int layerSize = layer.size(ptrSize, valueSize.size)
                layer.setOffset(bufferSize - 2)
                bufferSize += layerSize
            }
            ByteBuffer buffer = ByteBuffer.allocate(bufferSize)
            buffer.order(ByteOrder.LITTLE_ENDIAN)
            buffer.putShort(getHeader(numLevels, ptrSize))
            for (LayerData layer : layers) {
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
                    LayerData l = new LayerData(this.bitmask.length)
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

    public static class Reader {
        private final int numLevels;
        private final BitmaskSize bitmaskSize;
        private final int ptrSize;
        private final ValueSize valueSize;
        private final ByteBuffer buffer

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
            for (int i = 0; i <= 255; i++) {
                BIT_COUNTS[i] = Integer.bitCount(i) as byte
            }
        }

        public Reader(byte[] data) {
            ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN)
            short header = buffer.getShort()
            this.numLevels = ((header >>> NUM_LEVELS_OFFSET) & LEVELS_MASK)
            this.bitmaskSize = BitmaskSize.decode((header >>> BITMASK_SIZE_OFFSET) & BITMASK_SIZE_MASK)
            this.ptrSize = ((header >>> PTR_SIZE_OFFSET) & PTR_SIZE_MASK) + 1
            this.valueSize = ValueSize.decode((header >>> VALUE_SIZE_OFFSET) & VALUE_SIZE_MASK)
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

        public boolean exists(int key) {
            int valueOffset = getValueOffset(key)
            return valueOffset > 0 ? true : false
        }

        public byte[] get(int key, byte[] defaultValue) {
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
            for (int i = 0; i < ptrSize; i++) {
                res[i] = ((ptr >>> (i * 8)) & 0xff) as byte
            }
            return res
        }

        static int byteArrayToPtrLE(byte[] array) {
            int ptrSize = array.length
            int ptr = array[0] & 0xff
            for (int i = 0; i < ptrSize; i++) {
                ptr |= (array[i] & 0xff) << (i * 8)
            }
            return ptr
        }

        static byte[] ptrToByteArrayBE(int ptr, int ptrSize) {
            byte[] res = new byte[ptrSize]
            for (int i = 0; i < ptrSize; i++) {
                res[ptrSize - i - 1] = ((ptr >>> (i * 8)) & 0xff) as byte
            }
            return res
        }

        static int byteArrayToPtrBE(byte[] array) {
            int ptrSize = array.length
            int ptr = (array[0] & 0xff) << ((ptrSize - 1) * 8)
            for (int i = 0; i < ptrSize; i++) {
                ptr |= (array[i] & 0xff) << ((ptrSize - 1 - i) * 8)
            }
            return ptr
        }
    }
}
