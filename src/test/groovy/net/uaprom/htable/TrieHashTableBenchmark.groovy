package net.uaprom.htable


class TrieHashTableBenchmark extends HashTableBenchmark {
    @Override
    protected HashTable.Writer getWriter() {
        return new TrieHashTable.Writer(HashTable.ValueSize.INT)
    }

    @Override
    protected HashTable.Reader getReader(byte[] data) {
        return new TrieHashTable.Reader(data)
    }
}
