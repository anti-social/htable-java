package net.uaprom.htable

import spock.lang.Specification


class ByteUtilsSpec extends Specification {
    def "test ByteUtils.getMinimumNumberOfBytes int value"() {
        expect:
        ByteUtils.getMinimumNumberOfBytes(value) == bytesNum

        where:
        value | bytesNum
        0 | 1
        1 | 1
        2 | 1
        7 | 1
        8 | 1
        255 | 1
        256 | 2
        65535 | 2
        65536 | 3
        16777215 | 3
        16777216 | 4
        2147483647 | 4
    }
  
    def "test ByteUtils.getMinimumNumberOfBytes long value"() {
        expect:
        ByteUtils.getMinimumNumberOfBytes(value) == bytesNum

        where:
        value | bytesNum
        0L | 1
        1L | 1
        2L | 1
        7L | 1
        8L | 1
        255L | 1
        256L | 2
        65535L | 2
        65536L | 3
        16777215L | 3
        16777216L | 4
        2147483647L | 4
        2147483648L | 4
        4294967295L | 4
        4294967296L | 5
        2 ** 40 - 1 as long | 5
        2 ** 40 as long | 6
        2 ** 48 - 1 as long | 6
        2 ** 48 as long | 7
        2 ** 56 - 1 as long | 7
        2 ** 56 as long | 8
    }
}
