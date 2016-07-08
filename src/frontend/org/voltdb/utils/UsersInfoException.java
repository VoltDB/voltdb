package org.voltdb.utils;

public class UsersInfoException extends Exception {
    String errMsg;

    public UsersInfoException() {
        super();
    }

    public UsersInfoException(String message) {
        super(message);
    }

    public UsersInfoException(String message, Throwable cause) {
        super(message, cause);
    }

    public UsersInfoException(Throwable cause) {
        super(cause);
    }
}
