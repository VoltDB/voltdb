package org.voltdb.twitter.server;

public class HttpContentType {
    
    public static enum Type {
        HTML, PNG, CSS, JS
    }
    
    private Type type;
    
    public HttpContentType(String type) {
        if (type.equals("html")) {
            this.type = Type.HTML;
        } else if (type.equals("png")) {
            this.type = Type.PNG;
        } else if (type.equals("css")) {
            this.type = Type.CSS;
        } else if (type.equals("js")) {
            this.type = Type.JS;
        }
    }
    
    @Override
    public String toString() {
        switch(type) {
        case HTML:
            return "text/html; charset=UTF-8";
        case PNG:
            return "image/png";
        case CSS:
            return "text/css";
        case JS:
            return "application/javascript";
        default:
            return "";
        }
    }
    
}