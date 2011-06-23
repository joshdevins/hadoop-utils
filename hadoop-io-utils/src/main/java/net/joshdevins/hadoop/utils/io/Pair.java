package net.joshdevins.hadoop.utils.io;

/**
 * A simple tuple-like object to hold a pair of objects like key and value.
 * 
 * @author Josh Devins
 */
public final class Pair<A, B> {

    private final A a;
    private final B b;

    public Pair(final A a, final B b) {
        this.a = a;
        this.b = b;
    }

    public A getA() {
        return a;
    }

    public B getB() {
        return b;
    }
}
