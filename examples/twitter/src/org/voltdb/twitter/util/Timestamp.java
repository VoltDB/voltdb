package org.voltdb.twitter.util;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Timestamp {
    
    public static final String RFC1123 = "EEE, dd MMM yyyyy HH:mm:ss z";
    
    private long timestamp;
    private SimpleDateFormat format;
    
    public Timestamp(String format) {
        this.timestamp = System.currentTimeMillis();
        this.format = new SimpleDateFormat(format);
    }
    
    @Override
    public String toString() {
        return format.format(new Date(timestamp));
    }
    
}