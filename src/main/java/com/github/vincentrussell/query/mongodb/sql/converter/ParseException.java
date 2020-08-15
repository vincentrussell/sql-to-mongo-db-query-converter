package com.github.vincentrussell.query.mongodb.sql.converter;

/**
 * Exception that is thrown when there is an issue parsing a sql statement.
 */
public class ParseException extends Exception {
    private static final long serialVersionUID = 332235693126829948L;

    /**
     * Constructs a new exception with the specified detail message.  The
     * cause is not initialized, and may subsequently be initialized by
     * a call to {@link #initCause}.
     *
     * @param   message   the detail message. The detail message is saved for
     *          later retrieval by the {@link #getMessage()} method.
     */
    public ParseException(final String message) {
        super(message);
    }

    /**
     * Constructs a new exception with the specified detail message and
     * cause.  <p>Note that the detail message associated with
     * {@code cause} is <i>not</i> automatically incorporated in
     * this exception's detail message.
     *
     * @param  message the detail message (which is saved for later retrieval
     *         by the {@link #getMessage()} method).
     * @param  cause the cause (which is saved for later retrieval by the
     *         {@link #getCause()} method).  (A <tt>null</tt> value is
     *         permitted, and indicates that the cause is nonexistent or
     *         unknown.)
     */
    public ParseException(final String message, final Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs a new exception with the specified detail message and
     * cause.  <p>Note that the detail message associated with
     * {@code cause} is <i>not</i> automatically incorporated in
     * this exception's detail message.
     * @param e the caused-by throwable
     */
    public ParseException(final Throwable e) {
        super(fixErrorMessage(e));
    }

    private static Throwable fixErrorMessage(final Throwable e) {
        if (e.getMessage().startsWith("Encountered unexpected token: \"=\" \"=\"")) {
            return new ParseException("unable to parse complete sql string. one reason for this "
                    + "is the use of double equals (==).", e);
        }
        if (e.getMessage().startsWith("Encountered \" \"(\" \"( \"\"")) {
            return new ParseException("Only one simple table name is supported.", e);
        }
        if (e.getMessage().contains("Was expecting:" + System.getProperty("line.separator")
                + "    \"SELECT\"")) {
            return new ParseException("Only select statements are supported.", e);
        }

        return e;
    }
}
