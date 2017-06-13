package org.voltdb_testfuncs;

/** A simple user-defined (runtime) exception, used to test UDF's
 *  (user-defined exceptions) that throw such exceptions. */
public class UserDefinedTestException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public UserDefinedTestException() {
    }

    public UserDefinedTestException(String message) {
        super(message);
    }

    public UserDefinedTestException(Throwable cause) {
        super(cause);
    }

    public UserDefinedTestException(String message, Throwable cause) {
        super(message, cause);
    }
}
