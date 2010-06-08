package org.voltdb.twitter.server;

public class HttpStatus {
    
    private int status;
    private String message;
    
    public HttpStatus(int status) {
        this.status = status;
        this.message = getMessage(status);
    }
    
    private String getMessage(int status) {
        switch (status) {
        case 200:
            return "OK";
        default:
            return "";
        }
    }
    
    @Override
    public String toString() {
        return status + " " + message;
    }
    
}