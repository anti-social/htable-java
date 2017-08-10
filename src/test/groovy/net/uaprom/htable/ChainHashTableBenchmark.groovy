package net.uaprom.htable


class ChainHashTableBenchmark extends HashTableBenchmark {
    @Override
    protected HashTable.Writer getWriter() {
        return new ChainHashTable.Writer(HashTable.ValueSize.INT)
    }

    @Override
    protected HashTable.Reader getReader(byte[] data) {
        return new ChainHashTable.Reader(data)
    }
}
