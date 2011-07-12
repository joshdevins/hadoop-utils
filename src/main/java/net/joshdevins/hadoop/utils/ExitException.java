package net.joshdevins.hadoop.utils;

/**
 * Signal exception for an abnormal exit from a main method. Basically only useful for unit testing main methods.
 * 
 * @author Josh Devins
 */
public final class ExitException extends RuntimeException {

    private static final long serialVersionUID = -6023799377007723896L;

    public ExitException(final String message) {
        super(message);
    }

    public ExitException(final String message, final Exception cause) {
        super(message, cause);
    }
}
