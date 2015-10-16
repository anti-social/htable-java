package hamt


class HAMT {
    def getLevels(map) {
        def maxKey = map.max { it.key }.key
        def levels = maxKey.intdiv(16) + 1
        return levels
    }
}

