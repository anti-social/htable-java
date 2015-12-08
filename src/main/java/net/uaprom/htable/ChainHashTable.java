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
 *   |    note table size 1 means there is not hash table but only one SortedKeysValues
 *   |
 *   Reserved
 *
 *  Data:
 *
 *  <HTable>[<SortedKeysValues>]
 *
 *  Htable:
 *
 *  [<SortedKeysValuesPtr>]
 *
 *  SortedKeysValues:
 *
 *  [<Key><Value>]
 */
public class ChainHashTable extends HashTable {
    private static final int HEADER_SIZE = 2;
    private static final int KEY_SIZE_OFFSET = 0;
    private static final int PTR_SIZE_OFFSET = 3;
    private static final int VALUE_SIZE_OFFSET = 5;
    private static final int VARIABLE_VALUE_SIZE_OFFSET = 7;
    private static final int HASH_TABLE_SIZE_OFFSET = 8;
    private static final int KEY_SIZE_MASK = 0b0000_0111;
    private static final int PTR_SIZE_MASK = 0b0000_0011;
    private static final int VALUE_SIZE_MASK = 0b0000_0011;
    private static final int HASH_TABLE_SIZE_MASK = 0b0001_1111;

    public static final class Writer extends HashTable.Writer {
        private final int fillingRatio;
        private final int minHashTableSize;

        public static final int DEFAULT_FILLING_RATIO = 10;
        public static final int DEFAULT_MIN_HASH_TABLE_SIZE = 2;

        public Writer(ValueSize valueSize) {
            this(valueSize, DEFAULT_FILLING_RATIO);
        }

        public Writer(ValueSize valueSize, int fillingRatio) {
            this(valueSize, fillingRatio, DEFAULT_MIN_HASH_TABLE_SIZE);
        }

        public Writer(ValueSize valueSize, int fillingRatio, int minHashTableSize) {
            super(valueSize);
            assert fillingRatio > 0;
            assert minHashTableSize > 1;
            this.fillingRatio = fillingRatio;
            this.minHashTableSize = minHashTableSize;
        }

        public ValueSize valueSize() {
            return valueSize;
        }

        private int getHashTableSize(int numValues) {
            int size = Integer.highestOneBit(numValues / fillingRatio);
            if (size < minHashTableSize) {
                return 0;
            }
            return size;
        }

        private short getHeader(int keySize, int ptrSize, int hashTableSize) {
            assert 1 <= ptrSize && ptrSize <= 4;

            int header = 0;
            header |= (keySize - 1) << KEY_SIZE_OFFSET;
            header |= (ptrSize - 1) << PTR_SIZE_OFFSET;
            header |= this.valueSize.encode() << VALUE_SIZE_OFFSET;
            int encodedHashTableSize = (31 - Integer.numberOfLeadingZeros(hashTableSize));
            if (encodedHashTableSize < 0) {
                encodedHashTableSize = 0;
            }
            header |= encodedHashTableSize << HASH_TABLE_SIZE_OFFSET;
            return (short) header;
        }

        @Override
        public byte[] dump(long[] keys, byte[][] values) {
            assert keys.length == values.length;

            if (keys.length == 0) {
                return new byte[0];
            }

            long maxKey = keys[keys.length - 1];
            int keySize = ByteUtils.getMinimumNumberOfBytes(maxKey);
            int hashTableSize = getHashTableSize(keys.length);

            HTable htable = new HTable(hashTableSize, keySize, valueSize.size);
            for (int i = 0; i < keys.length; i++) {
                long key = keys[i];
                byte[] value = values[i];
                htable.put(key, value);
            }

            byte[] data = htable.dump();
            ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE + data.length);
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            buffer.putShort(getHeader(keySize, htable.getPtrSize(), hashTableSize));
            buffer.put(data);
            return buffer.array();
        }

        final class HTable {
            private final SortedKeysValues[] table;
            private final SortedKeysValues kvList;
            private final int keySize;
            private final int valueSize;
            private int ptrSize;

            public HTable(int size, int keySize, int valueSize) {
                this.table = new SortedKeysValues[size];
                for (int i = 0; i < size; i++) {
                    this.table[i] = new SortedKeysValues();
                }
                this.kvList = new SortedKeysValues();
                this.keySize = keySize;
                this.valueSize = valueSize;
            }

            private int hash(long key) {
                return (int) (key % table.length);
            }

            public void put(long key, byte[] value) {
                if (this.table.length > 0) {
                    this.table[hash(key)].add(key, value);
                } else {
                    kvList.add(key, value);
                }
            }

            public int getPtrSize() {
                return ptrSize;
            }

            public byte[] dump() {
                ptrSize = calcPtrSize();
                int bufferSize = calcBufferSize(ptrSize);
                LongCodec ptrCodec = HashTable.LONG_CODECS[ptrSize - 1];
                LongCodec keyCodec = HashTable.LONG_CODECS[keySize - 1];

                ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
                buffer.order(ByteOrder.LITTLE_ENDIAN);

                if (table.length > 0) {
                    ByteBuffer tableBuffer = buffer.slice();
                    int kvListOffset = table.length * ptrSize;
                    buffer.position(kvListOffset);
                    ByteBuffer kvListBuffer = buffer.slice();
                    for (SortedKeysValues kvList : table) {
                        int kvListPtr = 0;
                        if (!kvList.isEmpty()) {
                            kvListPtr = HEADER_SIZE + kvListBuffer.position() + kvListOffset;
                        }
                        tableBuffer.put(ptrCodec.dump(kvListPtr));
                        kvList.dump(kvListBuffer, keyCodec);
                    }
                } else {
                    kvList.dump(buffer, keyCodec);
                }
                return buffer.array();
            }

            private int calcPtrSize() {
                int ptrSize = 1;
                if (table.length == 0) {
                    return ptrSize;
                }
                int lastKvListSize = 0;
                for (int kvListIx = table.length - 1; kvListIx >= 0; kvListIx--){
                    lastKvListSize = table[kvListIx].calcBufferSize(keySize, valueSize);
                    if (lastKvListSize != 0) {
                        break;
                    }
                }
                for (; ptrSize <= 4; ptrSize++) {
                    int bufferSize = calcBufferSize(ptrSize);
                    if (HEADER_SIZE + bufferSize - lastKvListSize < (1 << ptrSize * 8)) {
                        break;
                    }
                }
                return ptrSize;
            }

            private int calcBufferSize(int ptrSize) {
                int bufferSize = 0;
                if (table.length > 0) {
                    bufferSize += table.length * ptrSize;
                    for (SortedKeysValues kvList : table) {
                        bufferSize += kvList.calcBufferSize(keySize, valueSize);
                    }
                } else {
                    bufferSize += this.kvList.calcBufferSize(keySize, valueSize);
                }
                return bufferSize;
            }
        }

        final class SortedKeysValues extends ArrayList<KeyValue> {
            public void add(long key, byte[] value) {
                add(new KeyValue(key, value));
            }

            public void dump(ByteBuffer buffer, LongCodec keyCodec) {
                for (KeyValue kv : this) {
                    kv.dump(buffer, keyCodec);
                }
            }

            public int calcBufferSize(int keySize, int valueSize) {
                if (isEmpty()) {
                    return 0;
                }
                return size() * (keySize + valueSize);
            }
        }

        final class KeyValue {
            public final long key;
            public final byte[] value;

            public KeyValue(long key, byte[] value) {
                this.key = key;
                this.value = value;
            }

            public void dump(ByteBuffer buffer, LongCodec keyCodec) {
                buffer.put(keyCodec.dump(key));
                buffer.put(value);
            }
        }
    }

    public static final class Reader extends HashTable.Reader {
        private final int keySize;
        private final LongCodec keyCodec;
        private final int ptrSize;
        private final LongCodec ptrCodec;
        private final ValueSize valueSize;
        private final int hashTableSize;
        private final int entrySize;

        public Reader(byte[] data) {
            this(data, 0, data.length);
        }

        public Reader(byte[] data, int offset, int length) {
            super(data, offset, length);
            short header = ByteUtils.bytesToShort(data, offset);
            this.keySize = ((header >>> KEY_SIZE_OFFSET) & KEY_SIZE_MASK) + 1;
            this.keyCodec = HashTable.LONG_CODECS[keySize - 1];
            this.ptrSize = ((header >>> PTR_SIZE_OFFSET) & PTR_SIZE_MASK) + 1;
            this.ptrCodec = HashTable.LONG_CODECS[ptrSize - 1];
            this.valueSize = ValueSize.decode((header >>> VALUE_SIZE_OFFSET) & VALUE_SIZE_MASK);
            this.hashTableSize = 1 << ((header >>> HASH_TABLE_SIZE_OFFSET) & HASH_TABLE_SIZE_MASK);
            this.entrySize = keySize + valueSize.size;
        }

        @Override
        public ValueSize valueSize() {
            return valueSize;
        }

        @Override
        public int getValueOffset(long key) {
            if (hashTableSize == 1) {
                return binarySearch(HEADER_SIZE, this.length - HEADER_SIZE, key);
            } else {
                int hashTableIx = (int) (key % hashTableSize);
                int ptrOffset = this.offset + HEADER_SIZE + hashTableIx * this.ptrSize;
                int kvListPtr = (int) ptrCodec.load(this.data, ptrOffset);
                if (kvListPtr == 0) {
                    return NOT_FOUND_OFFSET;
                }
                int kvListLength = getKvListLength(hashTableIx, kvListPtr);
                return binarySearch(kvListPtr, kvListLength, key);
            }
        }

        private long getKey(int offset, int entryIx) {
            return keyCodec.load(this.data, offset + entryIx * this.entrySize);
        }

        private int binarySearch(int kvListOffset, int kvListLength, long key) {
            int offset = this.offset + kvListOffset;
            int kvListSize = kvListLength / entrySize;
            int minEntryIx = 0, maxEntryIx = kvListSize - 1;
            while (minEntryIx <= maxEntryIx) {
                int currentEntryIx = (maxEntryIx + minEntryIx) >>> 1;
                long entryKey = getKey(offset, currentEntryIx);
                if (entryKey > key) {
                    maxEntryIx = currentEntryIx - 1;
                } else if (entryKey < key) {
                    minEntryIx = currentEntryIx + 1;
                } else {
                    return offset + currentEntryIx * entrySize + keySize;
                }
            }
            return NOT_FOUND_OFFSET;
        }

        private int getKvListLength(int hashTableIx, int kvListPtr) {
            for (int i = hashTableIx + 1; i < hashTableSize; i++) {
                int nextPtrOffset = this.offset + HEADER_SIZE + i * ptrSize;
                int nextKvListPtr = (int) ptrCodec.load(this.data, nextPtrOffset);
                if (nextKvListPtr == 0) {
                    continue;
                } else {
                    return nextKvListPtr - kvListPtr;
                }
            }
            return this.length - kvListPtr;
        }
    }
}
