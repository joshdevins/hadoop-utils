package net.joshdevins.hadoop.utils.io;

/**
 * Generic HTTP error exception.
 * 
 * @author Josh Devins
 */
public class HttpErrorException extends RuntimeException {

    private static final long serialVersionUID = -5191429036140674590L;

    private final int statusCode;

    public HttpErrorException(final int statusCode) {
        this.statusCode = statusCode;
    }

    public HttpErrorException(final int statusCode, final String message) {
        super(message);
        this.statusCode = statusCode;
    }

    public HttpErrorException(final int statusCode, final String message, final Throwable cause) {
        super(message, cause);
        this.statusCode = statusCode;
    }

    public int getStatusCode() {
        return statusCode;
    }
}
