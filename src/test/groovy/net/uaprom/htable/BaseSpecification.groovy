package net.uaprom.htable

import java.nio.ByteBuffer
import java.nio.ByteOrder
import spock.lang.Specification


class BaseSpecification extends Specification {
    void assertReader_getInt(reader, keys, values, range, defaultValue) {
        def map = [keys, values].transpose().collectEntries { it }
        for (k in range) {
            if (map.containsKey(k)) {
                assert reader.getInt(k, defaultValue) == map[k]
            } else {
                assert reader.getInt(k, defaultValue) == defaultValue
            }
        }
    }

    void assertReader_get(reader, keys, values, range, defaultValue) {
        def map = [keys, values].transpose().collectEntries { it }
        for (k in range) {
            if (map.containsKey(k)) {
                assert reader.get(k, defaultValue) == map[k]
            } else {
                assert reader.get(k, defaultValue) == defaultValue
            }
        }
    }

    void assertReader_exists(reader, keys, values, range) {
        def map = [keys, values].transpose().collectEntries { it }
        for (k in range) {
            if (map.containsKey(k)) {
                assert reader.exists(k) == true
            } else {
                assert reader.exists(k) == false
            }
        }
    }

    byte[] intToBytes(int v) {
        return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(v).array()
    }

    byte[] shortToBytes(short v) {
        return ByteBuffer.allocate(2).order(ByteOrder.LITTLE_ENDIAN).putShort(v).array()
    }
}
