package net.uaprom.htable;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 *  Array mapped trie implementation in Java
 *
 *  <Header><Data>
 *
 *  Header:
 *
 *  |3b-|-5b--|2b|3b-|b|2b|
 *    |   |    |  |   | |
 *    |   |    |  |   | Value size (2^n)
 *    |   |    |  |   Variable value size flag (not implemented yet)             
 *    |   |    |  |
 *    |   |    |  Bitmask size in bytes (2^n)
 *    |   |    |
 *    |   |    Pointer size in bytes (n+1)
 *    |   |
 *    |   Number of levels (n)
 *    |
 *    Reserved
 *
 *  Data:
 *
 *  [<Bitmask><LayerData>]
 */
public class TrieHashTable extends HashTable {
    private static final int HEADER_SIZE = 2;
    private static final int VALUE_SIZE_OFFSET = 0;
    private static final int VARIABLE_VALUE_SIZE_OFFSET = 2;
    private static final int BITMASK_SIZE_OFFSET = 3;
    private static final int PTR_SIZE_OFFSET = 6;
    private static final int NUM_LEVELS_OFFSET = 8;
    private static final int NUM_LEVELS_MASK = 0b0001_1111;
    private static final int BITMASK_SIZE_MASK = 0b0000_0111;
    private static final int PTR_SIZE_MASK = 0b0000_0011;
    private static final int VALUE_SIZE_MASK = 0b0000_0011;

    public enum BitmaskSize {
        BYTE(1), SHORT(2), INT(4), LONG(8);

        private static final Map<Integer,BitmaskSize> sizesMap = new HashMap<>();
        static {
            for (BitmaskSize bitmaskSize : values()) {
                sizesMap.put(bitmaskSize.size, bitmaskSize);
            }
        }

        public final int size;
        public final int shiftBits;
        public final int shiftMask;

        BitmaskSize(int size) {
            this.size = size;
            this.shiftBits = 31 - Integer.numberOfLeadingZeros(this.size << 3);
            this.shiftMask = (1 << this.shiftBits) - 1;
        }

        public int encode() {
            return this.shiftBits - 3;
        }

        public static BitmaskSize get(int size) {
            return sizesMap.get(size);
        }

        public static BitmaskSize decode(int value) {
            return BitmaskSize.get(1 << value);
        }
    }
    
    public static final class Writer extends HashTable.Writer {
        private final BitmaskSize bitmaskSize;

        public static final BitmaskSize DEFAULT_BITMASK_SIZE = BitmaskSize.SHORT;

        public Writer(ValueSize valueSize) {
            this(valueSize, DEFAULT_BITMASK_SIZE);
        }

        public Writer(ValueSize valueSize, BitmaskSize bitmaskSize) {
            super(valueSize);
            this.bitmaskSize = bitmaskSize;
        }

        private int getLevels(long maxKey) {
            int levels = 1;
            long key = maxKey >>> this.bitmaskSize.shiftBits;
            while (key != 0) {
                levels++;
                key = key >>> this.bitmaskSize.shiftBits;
            }
            return levels;
        }

        private int getPtrSize(List<LayerData> layers) {
            int ptrSize = 0;
            for (int ps = 0; ps <= 3; ps++) {
                ptrSize = ps + 1;
                int maxSize = 1 << (8 * ptrSize);
                int size = 0;
                for (LayerData l : layers) {
                    size += l.size(ptrSize, this.valueSize.size);
                    if (size > maxSize) {
                        break;
                    }
                }
                if (size > maxSize) {
                    continue;
                } else {
                    break;
                }
            }
            return ptrSize;
        }

        private short getHeader(int numLevels, int ptrSize) {
            assert 1 <= ptrSize && ptrSize <= 4;

            int header = 0;
            header |= numLevels << NUM_LEVELS_OFFSET;
            header |= this.bitmaskSize.encode() << BITMASK_SIZE_OFFSET;
            header |= (ptrSize - 1) << PTR_SIZE_OFFSET;
            header |= this.valueSize.encode() << VALUE_SIZE_OFFSET;
            return (short) header;
        }

        @Override
        public byte[] dump(long[] keys, byte[][] values) {
            assert keys.length == values.length;

            if (keys.length == 0) {
                return new byte[0];
            }

            long maxKey = keys[keys.length - 1];
            int numLevels = getLevels(maxKey);
            List<LayerData> layers = new ArrayList<>();
            layers.add(new LayerData(this.bitmaskSize.size));
            Map<Long,LayerData> layersMap = new HashMap<>();
            for (long key : keys) {
                layersMap.put(key, layers.get(0));
            }
            for (int l = numLevels; l > 0; l--) {
                LayerData prevSubLayer = null;
                int i = 0;
                for (long key : keys) {
                    int k = (int) (key >>> ((l - 1) * this.bitmaskSize.shiftBits) & this.bitmaskSize.shiftMask);
                    LayerData layer = layersMap.get(key);
                    if (l == 1) {
                        layer.addValue(values[i]);
                    } else {
                        LayerData subLayer = layer.newLayer(k);
                        if (subLayer != prevSubLayer) {
                            layers.add(subLayer);
                        }
                        prevSubLayer = subLayer;
                        layersMap.put(key, subLayer);
                    }
                    layer.setBit(k);
                    i++;
                }
            }
            int ptrSize = getPtrSize(layers);

            int bufferSize = HEADER_SIZE;
            for (LayerData layer : layers) {
                int layerSize = layer.size(ptrSize, valueSize.size);
                layer.setOffset(bufferSize);
                bufferSize += layerSize;
            }

            ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            buffer.putShort(getHeader(numLevels, ptrSize));
            for (LayerData layer : layers) {
                layer.dump(buffer, ptrSize);
            }
            return buffer.array();
        }

        private class LayerData {
            public byte[] bitmask;
            public int offset;
            public List<LayerData> layers = new ArrayList<>();
            public List<byte[]> values = new ArrayList<>();

            public LayerData(int bitmaskSize) {
                this.bitmask = new byte[bitmaskSize];
            }
        
            private void setBit(int k) {
                int n = k >>> 3;
                int b = k & 0b0000_0111;
                this.bitmask[n] = (byte) (this.bitmask[n] | (1 << b));
            }

            private LayerData newLayer(int k) {
                int n = k >>> 3;
                int b = k & 0b0000_0111;
                if ((this.bitmask[n] & (1 << b)) != 0) {
                    return this.layers.get(this.layers.size() - 1);
                }
                else {
                    LayerData l = new LayerData(this.bitmask.length);
                    this.layers.add(l);
                    return l;
                }
            }

            private void addValue(byte[] v) {
                this.values.add(v);
            }

            private void setOffset(int o) {
                this.offset = o;
            }

            private int size(int ptrSize, int valueSize) {
                return bitmask.length + layers.size() * ptrSize + values.size() * valueSize;
            }
        
            private void dump(ByteBuffer buffer, int ptrSize) {
                buffer.put(this.bitmask);
                if (!this.layers.isEmpty()) {
                    for (LayerData l : this.layers) {
                        buffer.put(LONG_CODECS[ptrSize - 1].dump(l.offset));
                    }
                } else {
                    for (byte[] v : this.values) {
                        buffer.put(v);
                    }
                }
            }
        }
    }

    public static final class Reader extends HashTable.Reader {
        private final int numLevels;
        private final BitmaskSize bitmaskSize;
        private final int ptrSize;
        private final ValueSize valueSize;

        public Reader(byte[] data) {
            this(data, 0, data.length);
        }

        public Reader(byte[] data, int offset, int length) {
            this(ByteBuffer.wrap(data, offset, length).slice());
        }

        public Reader(ByteBuffer buffer) {
            super(buffer);
            this.buffer.order(ByteOrder.LITTLE_ENDIAN);
            short header = buffer.getShort(0);
            this.numLevels = ((header >>> NUM_LEVELS_OFFSET) & NUM_LEVELS_MASK);
            this.bitmaskSize = BitmaskSize.decode((header >>> BITMASK_SIZE_OFFSET) & BITMASK_SIZE_MASK);
            this.ptrSize = ((header >>> PTR_SIZE_OFFSET) & PTR_SIZE_MASK) + 1;
            this.valueSize = ValueSize.decode((header >>> VALUE_SIZE_OFFSET) & VALUE_SIZE_MASK);
        }

        public int numLevels() {
            return numLevels;
        }

        public BitmaskSize bitmaskSize() {
            return bitmaskSize;
        }

        public int ptrSize() {
            return ptrSize;
        }

        @Override
        public ValueSize valueSize() {
            return valueSize;
        }

        @Override
        public final int getValueOffset(long key) {
            if (
                    this.numLevels * this.bitmaskSize.shiftBits < 64 &&
                            key >>> (this.numLevels * this.bitmaskSize.shiftBits) > 0
            ) {
                return NOT_FOUND_OFFSET;
            }

            int layerOffset = HEADER_SIZE;
            int ptrIx = 0;
            byte[] bitmask = new byte[this.bitmaskSize.size];
            byte[] ptrBuf = new byte[this.ptrSize];
            LongCodec ptrCodec = LONG_CODECS[this.ptrSize - 1];
            for (int level = numLevels - 1; level >= 0; level--) {
                this.buffer.position(layerOffset);
                this.buffer.get(bitmask);
                long k = key >>> (level * this.bitmaskSize.shiftBits) & this.bitmaskSize.shiftMask;
                int nByte = (int) (k >>> 3);
                int nBit = (int) (k & 0b0000_0111);
                if ((bitmask[nByte] & (1 << nBit)) == 0) {
                    return NOT_FOUND_OFFSET;
                }
                ptrIx = BIT_COUNTERS[nByte].count(bitmask, 0, nByte, nBit);
                this.buffer.position(layerOffset + bitmask.length + ptrIx * this.ptrSize);
                this.buffer.get(ptrBuf);
                if (level != 0) {
                    layerOffset = (int) ptrCodec.load(ptrBuf, 0);
                }
            }
            return layerOffset + bitmask.length + ptrIx * this.valueSize.size;
        }

        private static final BitCounter DEFAULT_BIT_COUNTER = new BitCounter();
        private static final BitCounter[] BIT_COUNTERS = new BitCounter[] {
            new BitCounter() {
                @Override
                int count(byte[] bytes, int offset, int nByte, int nBit) {
                    return BIT_COUNTS[bytes[offset] & BIT_COUNT_MASKS[nBit]];
                }
            },
            new BitCounter() {
                @Override
                int count(byte[] bytes, int offset, int nByte, int nBit) {
                    return
                        BIT_COUNTS[bytes[offset] & 0xff] +
                        BIT_COUNTS[bytes[offset + 1] & BIT_COUNT_MASKS[nBit]];
                }
            },
            new BitCounter() {
                @Override
                int count(byte[] bytes, int offset, int nByte, int nBit) {
                    return
                        BIT_COUNTS[bytes[offset] & 0xff] +
                        BIT_COUNTS[bytes[offset + 1] & 0xff] +
                        BIT_COUNTS[bytes[offset + 2] & BIT_COUNT_MASKS[nBit]];
                }
            },
            new BitCounter() {
                @Override
                int count(byte[] bytes, int offset, int nByte, int nBit) {
                    return
                        BIT_COUNTS[bytes[offset] & 0xff] +
                        BIT_COUNTS[bytes[offset + 1] & 0xff] +
                        BIT_COUNTS[bytes[offset + 2] & 0xff] +
                        BIT_COUNTS[bytes[offset + 3] & BIT_COUNT_MASKS[nBit]];
                }
            },
            DEFAULT_BIT_COUNTER,
            DEFAULT_BIT_COUNTER,
            DEFAULT_BIT_COUNTER,
            DEFAULT_BIT_COUNTER,
        };

        static class BitCounter {
            static byte[] BIT_COUNT_MASKS = new byte[]{
                0b0000_0000,
                0b0000_0001,
                0b0000_0011,
                0b0000_0111,
                0b0000_1111,
                0b0001_1111,
                0b0011_1111,
                0b0111_1111
            };
            static byte[] BIT_COUNTS = new byte[256];
            static {
                for (int i = 0; i <= 255; i++) {
                    BIT_COUNTS[i] = (byte) Integer.bitCount(i);
                }
            }

            int count(byte[] bytes, int offset, int nByte, int nBit) {
                int count = BIT_COUNTS[bytes[offset + nByte] & BIT_COUNT_MASKS[nBit]];
                for (int byteIx = nByte - 1; byteIx >= 0; byteIx--) {
                    int b = bytes[offset + byteIx] & 0xff;
                    count += BIT_COUNTS[b];
                }
                return count;
            }
        }
    }
}
