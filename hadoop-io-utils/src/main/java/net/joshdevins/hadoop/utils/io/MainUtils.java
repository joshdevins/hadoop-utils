package net.joshdevins.hadoop.utils.io;

/**
 * Simple utilities for drivers.
 * 
 * @author Josh Devins
 */
public final class MainUtils {

    private MainUtils() {
        throw new UnsupportedOperationException();
    }

    public static void exitWithError(final String message) {
        System.err.println(message);
        throw new ExitException(message);
    }

    public static void exitWithStackTraceAndError(final String message, final Exception e) {
        printStackTraceAndError(e, message);
        throw new ExitException(message, e);
    }

    public static void printStackTraceAndError(final Exception e, final String message) {
        e.printStackTrace();
        System.err.println(message);
    }
}
