package hamt

import spock.lang.Specification


class HAMTSpec extends Specification {
    def testGetLevels() {
        expect:
        new HAMT().getLevels(map) == levels

        where:
        map | levels
        [(1):1] | 1
    }
}