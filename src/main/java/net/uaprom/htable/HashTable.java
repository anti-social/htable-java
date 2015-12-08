package net.uaprom.htable;

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
 *  Abstract class for serializable hash tables
 *
 */
abstract public class HashTable {
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
    
    abstract public static class Writer {
        protected final ValueSize valueSize;

        public Writer(ValueSize valueSize) {
            this.valueSize = valueSize;
        }

        public ValueSize valueSize() {
            return valueSize;
        }

        public byte[] dumpBytes(Collection<Long> keys, Collection<Byte> values) {
            return dumpBytes(ByteUtils.toLongArray(keys), ByteUtils.toByteArray(values));
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
            return dumpShorts(ByteUtils.toLongArray(keys), ByteUtils.toShortArray(values));
        }

        public byte[] dumpShorts(long[] keys, short[] values) {
            assert valueSize == ValueSize.SHORT;
            byte[][] bytesArray = new byte[values.length][];
            for (int i = 0; i < values.length; i++) {
                bytesArray[i] = ByteUtils.shortToBytes(values[i]);
            }
            return dump(keys, bytesArray);
        }

        public byte[] dumpInts(List<Long> keys, List<Integer> values) {
            return dumpInts(ByteUtils.toLongArray(keys), ByteUtils.toIntArray(values));
        }

        public byte[] dumpInts(long[] keys, int[] values) {
            assert valueSize == ValueSize.INT;
            byte[][] bytesArray = new byte[values.length][];
            for (int i = 0; i < values.length; i++) {
                bytesArray[i] = ByteUtils.intToBytes(values[i]);
            }
            return dump(keys, bytesArray);
        }

        public byte[] dumpLongs(List<Long> keys, List<Long> values) {
            return dumpLongs(ByteUtils.toLongArray(keys), ByteUtils.toLongArray(values));
        }

        public byte[] dumpLongs(long[] keys, long[] values) {
            assert valueSize == ValueSize.LONG;
            byte[][] bytesArray = new byte[values.length][];
            for (int i = 0; i < values.length; i++) {
                bytesArray[i] = ByteUtils.longToBytes(values[i]);
            }
            return dump(keys, bytesArray);
        }

        public byte[] dumpFloats(Collection<Long> keys, Collection<Float> values) {
            return dumpFloats(ByteUtils.toLongArray(keys), ByteUtils.toFloatArray(values));
        }

        public byte[] dumpFloats(long[] keys, float[] values) {
            assert valueSize == ValueSize.INT;
            byte[][] bytesArray = new byte[values.length][];
            for (int i = 0; i < values.length; i++) {
                bytesArray[i] = ByteUtils.floatToBytes(values[i]);
            }
            return dump(keys, bytesArray);
        }

        public byte[] dumpDoubles(Collection<Long> keys, Collection<Double> values) {
            return dumpDoubles(ByteUtils.toLongArray(keys), ByteUtils.toDoubleArray(values));
        }

        public byte[] dumpDoubles(long[] keys, double[] values) {
            assert valueSize == ValueSize.LONG;
            byte[][] bytesArray = new byte[values.length][];
            for (int i = 0; i < values.length; i++) {
                bytesArray[i] = ByteUtils.doubleToBytes(values[i]);
            }
            return dump(keys, bytesArray);
        }

        public byte[] dump(Collection<Long> keys, Collection<byte[]> values) {
            return dump(ByteUtils.toLongArray(keys), ByteUtils.toBytesArray(values));
        }

        public byte[] dump(SortedMap<Long, byte[]> entries) {
            return dump(ByteUtils.toLongArray(entries.keySet()), ByteUtils.toBytesArray(entries.values()));
        }

        abstract public byte[] dump(long[] keys, byte[][] values);
    }

    abstract public static class Reader {
        protected final byte[] data;
        protected final int offset;
        protected final int length;
        
        public static final int NOT_FOUND_OFFSET = -1;

        public Reader(byte[] data, int offset, int length) {
            this.data = data;
            this.offset = offset;
            this.length = length;
        }

        abstract public ValueSize valueSize();

        abstract public int getValueOffset(long key);

        public boolean exists(long key) {
            int valueOffset = getValueOffset(key);
            return valueOffset > 0 ? true : false;
        }

        public byte getByte(long key, byte defaultValue) {
            int valueOffset = getValueOffset(key);
            if (valueOffset == NOT_FOUND_OFFSET) {
                return defaultValue;
            }
            return getByte(valueOffset);
        }

        public byte getByte(int valueOffset) {
            assert this.valueSize() == ValueSize.BYTE;
            return this.data[valueOffset];
        }

        public short getShort(long key, short defaultValue) {
            int valueOffset = getValueOffset(key);
            if (valueOffset == NOT_FOUND_OFFSET) {
                return defaultValue;
            }
            return getShort(valueOffset);
        }

        public short getShort(int valueOffset) {
            assert this.valueSize() == ValueSize.SHORT;
            return ByteUtils.bytesToShort(get(valueOffset));
        }

        public int getInt(long key, int defaultValue) {
            int valueOffset = getValueOffset(key);
            if (valueOffset == NOT_FOUND_OFFSET) {
                return defaultValue;
            }
            return getInt(valueOffset);
        }

        public int getInt(int valueOffset) {
            assert this.valueSize() == ValueSize.INT;
            return ByteUtils.bytesToInt(get(valueOffset));
        }

        public long getLong(long key, long defaultValue) {
            int valueOffset = getValueOffset(key);
            if (valueOffset == NOT_FOUND_OFFSET) {
                return defaultValue;
            }
            return getLong(valueOffset);
        }

        public long getLong(int valueOffset) {
            assert this.valueSize() == ValueSize.LONG;
            return ByteUtils.bytesToLong(get(valueOffset));
        }

        public float getFloat(long key, float defaultValue) {
            int valueOffset = getValueOffset(key);
            if (valueOffset == NOT_FOUND_OFFSET) {
                return defaultValue;
            }
            return getFloat(valueOffset);
        }

        public float getFloat(int valueOffset) {
            assert this.valueSize() == ValueSize.INT;
            return ByteUtils.bytesToFloat(get(valueOffset));
        }

        public double getDouble(long key, double defaultValue) {
            int valueOffset = getValueOffset(key);
            if (valueOffset == NOT_FOUND_OFFSET) {
                return defaultValue;
            }
            return getDouble(valueOffset);
        }

        public double getDouble(int valueOffset) {
            assert this.valueSize() == ValueSize.LONG;
            return ByteUtils.bytesToDouble(get(valueOffset));
        }

        public byte[] get(int valueOffset) {
            byte[] value = new byte[this.valueSize().size];
            System.arraycopy(this.data, valueOffset, value, 0, this.valueSize().size);
            return value;
        }

        public byte[] get(long key, byte[] defaultValue) {
            int valueOffset = getValueOffset(key);
            if (valueOffset == NOT_FOUND_OFFSET) {
                return defaultValue;
            }
            return get(valueOffset);
        }
    }

    protected static final LongCodec[] LONG_CODECS = new LongCodec[] {
        new LongCodec() {
            @Override
            public byte[] dump(long v) {
                return new byte[]{ (byte) (v & 0xff) };
            }

            @Override
            public long load(byte[] array, int offset) {
                return array[offset] & 0xff;
            }
        },
        new LongCodec() {
            @Override
            public byte[] dump(long v) {
                return new byte[]{ (byte) (v & 0xff),
                                   (byte) ((v >>> 8) & 0xff) };
            }

            @Override
            public long load(byte[] array, int offset) {
                return
                    (array[offset] & 0xff) |
                    ((array[offset+1] & 0xff) << 8);
            }
        },
        new LongCodec() {
            @Override
            public byte[] dump(long v) {
                return new byte[]{ (byte) (v & 0xff),
                                   (byte) ((v >>> 8) & 0xff),
                                   (byte) ((v >>> 16) & 0xff) };
            }

            @Override
            public long load(byte[] array, int offset) {
                return
                    (array[offset] & 0xff) |
                    ((array[offset+1] & 0xff) << 8) |
                    ((array[offset+2] & 0xff) << 16);
            }
        },
        new LongCodec() {
            @Override
            public byte[] dump(long v) {
                return new byte[]{ (byte) (v & 0xff),
                                   (byte) ((v >>> 8) & 0xff),
                                   (byte) ((v >>> 16) & 0xff),
                                   (byte) ((v >>> 24) & 0xff) };
            }

            @Override
            public long load(byte[] array, int offset) {
                return
                    (array[offset] & 0xff) |
                    ((array[offset+1] & 0xff) << 8) |
                    ((array[offset+2] & 0xff) << 16) |
                    ((array[offset+3] & 0xff) << 24);
            }
        },
        new LongCodec() {
            @Override
            public byte[] dump(long v) {
                return new byte[]{ (byte) (v & 0xff),
                                   (byte) ((v >>> 8) & 0xff),
                                   (byte) ((v >>> 16) & 0xff),
                                   (byte) ((v >>> 24) & 0xff),
                                   (byte) ((v >>> 32) & 0xff) };
            }

            @Override
            public long load(byte[] array, int offset) {
                return
                    (array[offset] & 0xff) |
                    ((array[offset+1] & 0xff) << 8) |
                    ((array[offset+2] & 0xff) << 16) |
                    ((array[offset+3] & 0xff) << 24) |
                    ((array[offset+4] & 0xff) << 32);
            }
        },
        new LongCodec() {
            @Override
            public byte[] dump(long v) {
                return new byte[]{ (byte) (v & 0xff),
                                   (byte) ((v >>> 8) & 0xff),
                                   (byte) ((v >>> 16) & 0xff),
                                   (byte) ((v >>> 24) & 0xff),
                                   (byte) ((v >>> 32) & 0xff),
                                   (byte) ((v >>> 40) & 0xff) };
            }

            @Override
            public long load(byte[] array, int offset) {
                return
                    (array[offset] & 0xff) |
                    ((array[offset+1] & 0xff) << 8) |
                    ((array[offset+2] & 0xff) << 16) |
                    ((array[offset+3] & 0xff) << 24) |
                    ((array[offset+4] & 0xff) << 32) |
                    ((array[offset+5] & 0xff) << 40);
            }
        },
        new LongCodec() {
            @Override
            public byte[] dump(long v) {
                return new byte[]{ (byte) (v & 0xff),
                                   (byte) ((v >>> 8) & 0xff),
                                   (byte) ((v >>> 16) & 0xff),
                                   (byte) ((v >>> 24) & 0xff),
                                   (byte) ((v >>> 32) & 0xff),
                                   (byte) ((v >>> 40) & 0xff),
                                   (byte) ((v >>> 48) & 0xff) };
            }

            @Override
            public long load(byte[] array, int offset) {
                return
                    (array[offset] & 0xff) |
                    ((array[offset+1] & 0xff) << 8) |
                    ((array[offset+2] & 0xff) << 16) |
                    ((array[offset+3] & 0xff) << 24) |
                    ((array[offset+4] & 0xff) << 32) |
                    ((array[offset+5] & 0xff) << 40) |
                    ((array[offset+6] & 0xff) << 48);
            }
        },
        new LongCodec() {
            @Override
            public byte[] dump(long v) {
                return ByteUtils.longToBytes(v);
            }

            @Override
            public long load(byte[] array, int offset) {
                return ByteUtils.bytesToLong(array, offset);
            }
        }
    };
        
    static abstract class LongCodec {
        abstract byte[] dump(long v);
            
        long load(byte[] array) {
            return load(array, 0);
        }

        abstract long load(byte[] array, int offset);
    }
}
