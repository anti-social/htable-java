package hamt;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;


/**
 *  Hash array mapped trie implementation in Java
 *
 *  <Header><Data>
 *
 *  Header:
 *
 *  |3b-|-5b--|b|2b|2b|3b-|
 *   |    |    | |  |  |
 *   |    |    | |  |  Key size (n+1)
 *   |    |    | |  |
 *   |    |    | |  Pointer size in bytes (n+1)
 *   |    |    | |
 *   |    |    | Value size (2^n)
 *   |    |    Variable value size flag (not implemented yet)
 *   |    |
 *   |    Hash table size (2^n);
 *   |    note table size 1 means there is not hash table but only one SortedSet
 *   |
 *   Reserved
 *
 *  Data:
 *
 *  <HTable>[<SortedSet>]
 *
 *  Htable:
 *
 *  [<SortedSetPtr>]
 *
 *  SortedSet:
 *
 *  <Size>[<Key><Value>]
 */
public class HAMT {
    private static final int KEY_SIZE_OFFSET = 0;
    private static final int PTR_SIZE_OFFSET = 3;
    private static final int VALUE_SIZE_OFFSET = 5;
    private static final int VARIABLE_VALUE_SIZE_OFFSET = 7;
    private static final int HASH_TABLE_SIZE_OFFSET = 8;
    private static final int KEY_SIZE_MASK = 0b0000_0111;
    private static final int PTR_SIZE_MASK = 0b0000_0011;
    private static final int VALUE_SIZE_MASK = 0b0000_0011;
    private static final int HASH_TABLE_SIZE_MASK = 0b0001_1111;

    public static enum ValueSize {
        BYTE(1), SHORT(2), INT(4), LONG(8);//, VAR(-1);

        private static final Map<Integer,ValueSize> sizesMap = new HashMap<>();
        static {
            for (ValueSize valueSize : values()) {
                sizesMap.put(valueSize.size, valueSize);
            }
        }

        public final int size;
        public final int shiftBits;

        ValueSize(int size) {
            this.size = size;
            this.shiftBits = 31 - Integer.numberOfLeadingZeros(this.size << 3);
        }

        public int encode() {
            return this.shiftBits - 3;
        }

        public static ValueSize get(int size) {
            return sizesMap.get(size);
        }

        public static ValueSize decode(int value) {
            return ValueSize.get(1 << value);
        }
    }
    
    public static class Writer {
        private final int fillingRatio;
        private final ValueSize valueSize;

        public final int DEFAULT_FILLING_RATIO = 10;

        public Writer(ValueSize valueSize) {
            this(valueSize, DEFAULT_FILLING_RATIO);
        }

        public Writer(ValueSize valueSize, int fillingRatio) {
            this.valueSize = valueSize;
            this.fillingRation = fillingRatio
        }

        private Writer(int bitmaskSize, int valueSize) {
            this(BitmaskSize.get(bitmaskSize), ValueSize.get(valueSize));
        }

        public ValueSize valueSize() {
            return valueSize;
        }

        private int getKeySize(long maxKey) {
            int highestSetBit = Long.numberOfLeadingZeros(maxKey) + 1;
            return highestSetBit / 8 * 8 + (highestSetBit % 8 == 0 ? 0 : 8);
        }

        private int getDataSize(int hashTableSize, int ptrSize, int keySize, int valueSize, int numValues) {
            return hashTableSize * ptrSize + (keySize + valueSize) * numValues;
        }

        private int getPtrSize(int hashTableSize, int keySize, int valueSize, int numValues) {
            for (int ptrSize = 1; ptrSize < 4; ptrSize++) {
                if (getDataSize(hashTableSize, ptrSize, keySize, valueSize, numValues) <= (1 << ptrSize * 8)) {
                    return ptrSize;
                }
            }
            return 4;
        }

        private int getHashTableSize(int numValues) {
            return (numValues / fillingRatio) / 8 * 8;
        }

        private short getHeader(int keySize, int ptrSize, ValueSize valueSize, int hashTableSize) {
            assert 1 <= ptrSize && ptrSize <= 4;

            int header = 0;
            header |= (keySize - 1) << KEY_SIZE_OFFSET;
            header |= (ptrSize - 1) << PTR_SIZE_OFFSET;
            header |= this.valueSize.encode() << VALUE_SIZE_OFFSET;
            header |= hashTableSize << HASH_TABLE_SIZE_OFFSET;
            return (short) header;
        }

        public byte[] dumpBytes(Collection<Long> keys, Collection<Byte> values) {
            return dumpBytes(Utils.toLongArray(keys), Utils.toByteArray(values));
        }

        public byte[] dumpBytes(long[] keys, byte[] values) {
            assert valueSize == ValueSize.BYTE;
            byte[][] bytesArray = new byte[values.length][];
            for (int i = 0; i < values.length; i++) {
                bytesArray[i] = new byte[]{ values[i] };
            }
            return dump(keys, bytesArray);
        }

        public byte[] dumpShorts(List<Long> keys, List<Short> values) {
            return dumpShorts(Utils.toLongArray(keys), Utils.toShortArray(values));
        }

        public byte[] dumpShorts(long[] keys, short[] values) {
            assert valueSize == ValueSize.SHORT;
            byte[][] bytesArray = new byte[values.length][];
            for (int i = 0; i < values.length; i++) {
                bytesArray[i] = Utils.shortToBytes(values[i]);
            }
            return dump(keys, bytesArray);
        }

        public byte[] dumpInts(List<Long> keys, List<Integer> values) {
            return dumpInts(Utils.toLongArray(keys), Utils.toIntArray(values));
        }

        public byte[] dumpInts(long[] keys, int[] values) {
            assert valueSize == ValueSize.INT;
            byte[][] bytesArray = new byte[values.length][];
            for (int i = 0; i < values.length; i++) {
                bytesArray[i] = Utils.intToBytes(values[i]);
            }
            return dump(keys, bytesArray);
        }

        public byte[] dumpLongs(List<Long> keys, List<Long> values) {
            return dumpLongs(Utils.toLongArray(keys), Utils.toLongArray(values));
        }

        public byte[] dumpLongs(long[] keys, long[] values) {
            assert valueSize == ValueSize.LONG;
            byte[][] bytesArray = new byte[values.length][];
            for (int i = 0; i < values.length; i++) {
                bytesArray[i] = Utils.longToBytes(values[i]);
            }
            return dump(keys, bytesArray);
        }

        public byte[] dumpFloats(Collection<Long> keys, Collection<Float> values) {
            return dumpFloats(Utils.toLongArray(keys), Utils.toFloatArray(values));
        }

        public byte[] dumpFloats(long[] keys, float[] values) {
            assert valueSize == ValueSize.INT;
            byte[][] bytesArray = new byte[values.length][];
            for (int i = 0; i < values.length; i++) {
                bytesArray[i] = Utils.floatToBytes(values[i]);
            }
            return dump(keys, bytesArray);
        }

        public byte[] dumpDoubles(Collection<Long> keys, Collection<Double> values) {
            return dumpDoubles(Utils.toLongArray(keys), Utils.toDoubleArray(values));
        }

        public byte[] dumpDoubles(long[] keys, double[] values) {
            assert valueSize == ValueSize.LONG;
            byte[][] bytesArray = new byte[values.length][];
            for (int i = 0; i < values.length; i++) {
                bytesArray[i] = Utils.doubleToBytes(values[i]);
            }
            return dump(keys, bytesArray);
        }

        public byte[] dump(Collection<Long> keys, Collection<byte[]> values) {
            return dump(Utils.toLongArray(keys), Utils.toBytesArray(values));
        }

        public byte[] dump(SortedMap<Long, byte[]> entries) {
            return dump(Utils.toLongArray(entries.keySet()), Utils.toBytesArray(entries.values()));
        }

        public byte[] dump(long[] keys, byte[][] values) {
            assert keys.length == values.length;

            if (keys.length == 0) {
                return new byte[0];
            }

            long maxKey = keys[keys.length - 1];
            int keySize = getKeySize(maxKey);
            int hashTableSize = getHashTableSize(keys.length);
            int ptrSize = getPtrSize(hashTableSize, keySize, this.valueSize.size(), keys.length);

            HTable htable = new HTable(hashTableSize, ptrSize, keySize);
            for (int i = 0; i < keys.length; i++) {
                long key = keys[i];
                byte[] value = values[i];
                htable.put(key, value);
            }

            ByteBuffer buffer = ByteBuffer.allocate(2 + getDataSize(hashTableSize, ptrSize, keySize, this.valueSize.size(), keys.length));
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            buffer.putShort(getHeader(numLevels, ptrSize));
            buffer.put(htable.dump());
            return buffer.array();
        }

        class HTable {
            private final int ptrSize;
            private final int keySize;
            private final SortedLongs[] table;

            public HTable(int size, int ptrSize, int keySize) {
                this.table = new SortedSet[size];
                for (int i = 0; i < size; i++) {
                    this.table[i] = new SortedSet();
                }
            }

            private void put(long key, byte[] value) {
                this.table[key % size].add(key, value);
            }

            private void dump(ByteBuffer buffer) {
                ByteBuffer tableBuffer = buffer.slice();
                int kvListOffset = table.length * ptrSize;
                buffer.position(kvListOffset);
                ByteBuffer kvListBuffer = buffer.slice();
                for (SortedLongs kvList : table) {
                    tableBuffer.put(kvListBuffer.position() + kvListOffset);
                    kvList.dump(kvListBuffer);
                }
            }
        }

        class SortedLongs {
            private final List<KeyValue> kv = new ArrayList<>();

            private void add(long key, byte[] value) {
                this.kv.add(key)
            }

            private List<KeyValue> getKV() {
                return kv;
            }

            private void dump(ByteBuffer buffer) {
                buffer.put((byte) kv.size());
                for (KeyValue kv : kvList) {
                    kv.dump(buffer);
                }
            }
        }

        Class KeyValue {
            public final long key;
            public final byte[] value;

            public KeyValue(long key, byte[] value) {
                this.key = key;
                this.value = value;
            }

            private void dump(ByteBuffer buffer) {
                buffer.put(keyToBytes(key, keySize));
                buffer.put(value);
            }
        }
    }

    public static class Reader {
        private final int keySize;
        private final int ptrSize;
        private final ValueSize valueSize;
        private final int hashTableSize;
        private final ByteBuffer buffer;

        public static final int NOT_FOUND_OFFSET = -1;

        public Reader(byte[] data) {
            ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
            short header = buffer.getShort();
            this.keySize = ((header >>> KEY_SIZE_OFFSET) & KEY_SIZE_MASK) + 1;
            this.ptrSize = ((header >>> PTR_SIZE_OFFSET) & PTR_SIZE_MASK) + 1;
            this.valueSize = ValueSize.decode((header >>> VALUE_SIZE_OFFSET) & VALUE_SIZE_MASK);
            this.hashTableSize = ((header >>> HASH_TABLE_SIZE_OFFSET) & HASH_TABLE_SIZE__MASK) + 1;
            this.buffer = buffer.slice();
        }

        public int getValueOffset(long key) {
            int ptrOffset = (key % hashTableSize) * ptrSize;
            this.buffer.position(ptrOffset);
            byte[] kvListPtrBuf = new byte[ptrSize];
            this.buffer.get(kvListPtrBuf);
            int kvListPtr = bytesToPtr(kvListPtrBuf);

        }

        public boolean exists(long key) {
            int valueOffset = getValueOffset(key);
            return valueOffset > 0 ? true : false;
        }

        public byte getByte(int valueOffset) {
            assert this.valueSize == ValueSize.BYTE;
            return this.buffer.get(valueOffset);
        }

        public short getShort(int valueOffset) {
            assert this.valueSize == ValueSize.SHORT;
            return Utils.bytesToShort(get(valueOffset));
        }

        public int getInt(int valueOffset) {
            assert this.valueSize == ValueSize.INT;
            return Utils.bytesToInt(get(valueOffset));
        }

        public long getLong(int valueOffset) {
            assert this.valueSize == ValueSize.LONG;
            return Utils.bytesToLong(get(valueOffset));
        }

        public float getFloat(int valueOffset) {
            assert this.valueSize == ValueSize.INT;
            byte[] value = new byte[this.valueSize.size];
            return Utils.bytesToFloat(get(valueOffset));
        }

        public double getDouble(int valueOffset) {
            assert this.valueSize == ValueSize.LONG;
            return Utils.bytesToDouble(get(valueOffset));
        }

        public byte[] get(int valueOffset) {
            byte[] value = new byte[this.valueSize.size];
            this.buffer.position(valueOffset);
            this.buffer.get(value);
            return value;
        }

        public byte[] get(long key, byte[] defaultValue) {
            int valueOffset = getValueOffset(key);
            if (valueOffset == NOT_FOUND_OFFSET) {
                return defaultValue;
            }
            return get(valueOffset);
        }

        private static final BitCounter DEFAULT_BIT_COUNTER = new BitCounter();
        private static final BitCounter[] BIT_COUNTERS = new BitCounter[] {
            new BitCounter() {
                @Override
                int count(byte[] bytes, int nByte, int nBit) {
                    return BIT_COUNTS[bytes[0] & BIT_COUNT_MASKS[nBit]];
                }
            },
            new BitCounter() {
                @Override
                int count(byte[] bytes, int nByte, int nBit) {
                    return BIT_COUNTS[bytes[0] & 0xff] + BIT_COUNTS[bytes[1] & BIT_COUNT_MASKS[nBit]];
                }
            },
            new BitCounter() {
                @Override
                int count(byte[] bytes, int nByte, int nBit) {
                    return BIT_COUNTS[bytes[0] & 0xff] + BIT_COUNTS[bytes[1] & 0xff] + BIT_COUNTS[bytes[2] & BIT_COUNT_MASKS[nBit]];
                }
            },
            new BitCounter() {
                @Override
                int count(byte[] bytes, int nByte, int nBit) {
                    return BIT_COUNTS[bytes[0] & 0xff] + BIT_COUNTS[bytes[1] & 0xff] + BIT_COUNTS[bytes[2] & 0xff] + BIT_COUNTS[bytes[3] & BIT_COUNT_MASKS[nBit]];
                }
            },
            DEFAULT_BIT_COUNTER,
            DEFAULT_BIT_COUNTER,
            DEFAULT_BIT_COUNTER,
            DEFAULT_BIT_COUNTER,
        };

        static class BitCounter {
            protected static byte[] BIT_COUNT_MASKS = new byte[]{
                0b0000_0000,
                0b0000_0001,
                0b0000_0011,
                0b0000_0111,
                0b0000_1111,
                0b0001_1111,
                0b0011_1111,
                0b0111_1111
            };
            protected static byte[] BIT_COUNTS = new byte[256];
            static {
                for (int i = 0; i <= 255; i++) {
                    BIT_COUNTS[i] = (byte) Integer.bitCount(i);
                }
            }

            int count(byte[] bytes, int nByte, int nBit) {
                int count = BIT_COUNTS[bytes[nByte] & BIT_COUNT_MASKS[nBit]];
                for (int byteIx = nByte - 1; byteIx >= 0; byteIx--) {
                    int b = bytes[byteIx] & 0xff;
                    count += BIT_COUNTS[b];
                }
                return count;
            }
        }

    }

    private static final PointerCodec[] POINTER_CODECS = new PointerCodec[] {
        new PointerCodec() {
            @Override
            public byte[] dump(int ptr) {
                return new byte[]{ (byte) (ptr & 0xff) };
            }

            @Override
            public int load(byte[] array) {
                return array[0] & 0xff;
            }
        },
        new PointerCodec() {
            @Override
            public byte[] dump(int ptr) {
                return new byte[]{ (byte) (ptr & 0xff),
                                   (byte) ((ptr >>> 8) & 0xff) };
            }

            @Override
            public int load(byte[] array) {
                return (array[0] & 0xff) | ((array[1] & 0xff) << 8);
            }
        },
        new PointerCodec() {
            @Override
            public byte[] dump(int ptr) {
                return new byte[]{ (byte) (ptr & 0xff),
                                   (byte) ((ptr >>> 8) & 0xff),
                                   (byte) ((ptr >>> 16) & 0xff) };
            }

            @Override
            public int load(byte[] array) {
                return (array[0] & 0xff) | ((array[1] & 0xff) << 8) | ((array[2] & 0xff) << 16);
            }
        },
        new PointerCodec() {
            @Override
            public byte[] dump(int ptr) {
                return Utils.intToBytes(ptr);
            }

            @Override
            public int load(byte[] array) {
                return Utils.bytesToInt(array);
            }
        }
    };
        
    interface PointerCodec {
        byte[] encode(int ptr);
            
        int decode(byte[] array);
    }

    public static class Utils {
        public static byte[] shortToBytes(short v) {
            return new byte[]{ (byte) (v & 0xff),
                               (byte) ((v >>> 8) & 0xff) };
        }

        public static byte[] intToBytes(int v) {
            return new byte[]{ (byte) (v & 0xff),
                               (byte) ((v >>> 8) & 0xff),
                               (byte) ((v >>> 16) & 0xff),
                               (byte) ((v >>> 24) & 0xff) };
        }

        public static byte[] longToBytes(long v) {
            return new byte[]{ (byte) (v & 0xff),
                               (byte) ((v >>> 8) & 0xff),
                               (byte) ((v >>> 16) & 0xff),
                               (byte) ((v >>> 24) & 0xff),
                               (byte) ((v >>> 32) & 0xff),
                               (byte) ((v >>> 40) & 0xff),
                               (byte) ((v >>> 48) & 0xff),
                               (byte) ((v >>> 56) & 0xff) };
        }

        public static byte[] floatToBytes(float v) {
            return intToBytes(Float.floatToIntBits(v));
        }

        public static byte[] doubleToBytes(double v) {
            return longToBytes(Double.doubleToLongBits(v));
        }

        public static short bytesToShort(byte[] array) {
            return (short) ((array[0] & 0xff) | ((array[1] & 0xff) << 8));
        }

        public static int bytesToInt(byte[] array) {
            return
                (array[0] & 0xff) |
                ((array[1] & 0xff) << 8) |
                ((array[2] & 0xff) << 16) |
                ((array[3] & 0xff) << 24);
        }

        public static long bytesToLong(byte[] array) {
            return
                (array[0] & 0xffL) |
                ((array[1] & 0xffL) << 8) |
                ((array[2] & 0xffL) << 16) |
                ((array[3] & 0xffL) << 24) |
                ((array[4] & 0xffL) << 32) |
                ((array[5] & 0xffL) << 40) |
                ((array[6] & 0xffL) << 48) |
                ((array[7] & 0xffL) << 56);
        }

        public static float bytesToFloat(byte[] array) {
            return Float.intBitsToFloat(bytesToInt(array));
        }

        public static double bytesToDouble(byte[] array) {
            return Double.longBitsToDouble(bytesToLong(array));
        }

        public static byte[] toByteArray(Collection<Byte> values) {
            byte[] array = new byte[values.size()];
            int i = 0;
            for (byte v : values) {
                array[i] = v;
                i++;
            }
            return array;
        }

        public static byte[][] toBytesArray(Collection<byte[]> values) {
            byte[][] array = new byte[values.size()][];
            int i = 0;
            for (byte[] v : values) {
                array[i] = v;
                i++;
            }
            return array;
        }

        public static short[] toShortArray(Collection<Short> values) {
            short[] array = new short[values.size()];
            int i = 0;
            for (short v : values) {
                array[i] = v;
                i++;
            }
            return array;
        }

        public static int[] toIntArray(Collection<Integer> values) {
            int[] array = new int[values.size()];
            int i = 0;
            for (int v : values) {
                array[i] = v;
                i++;
            }
            return array;
        }

        public static long[] toLongArray(Collection<Long> values) {
            long[] array = new long[values.size()];
            int i = 0;
            for (long v : values) {
                array[i] = v;
                i++;
            }
            return array;
        }

        public static float[] toFloatArray(Collection<Float> values) {
            float[] array = new float[values.size()];
            int i = 0;
            for (float v : values) {
                array[i] = v;
                i++;
            }
            return array;
        }

        public static double[] toDoubleArray(Collection<Double> values) {
            double[] array = new double[values.size()];
            int i = 0;
            for (double v : values) {
                array[i] = v;
                i++;
            }
            return array;
        }
    }
}
