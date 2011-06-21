package net.joshdevins.hadoop.utils.io;

/**
 * A simple tuple-like object to hold a key/value pair.
 * 
 * @author Josh Devins
 */
public final class Pair<K, V> {

    private final K key;
    private final V value;

    public Pair(final K key, final V value) {
        this.key = key;
        this.value = value;
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }
}
