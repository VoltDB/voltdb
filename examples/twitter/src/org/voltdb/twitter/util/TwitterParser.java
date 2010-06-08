package org.voltdb.twitter.util;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.voltdb.twitter.database.DB;

public class TwitterParser {
    
    private DB db;
    private Pattern regex;
    private Matcher matcher;
    
    public TwitterParser(List<String> servers) {
        db = new DB(servers);
        
        // pound sign followed by alphabetic character (end hash tag), followed by punctuation, whitespace, or EOL
        regex = Pattern.compile("(#\\p{Alpha}\\S+?)(?:\\p{Punct}|\\s|$)");
    }
    
    public void parseHashTags(String message) {
        matcher = regex.matcher(message);
        while (matcher.find()) {
            String hashTag = matcher.group();
            
            // the regex will often capture an extra char
            char lastChar = hashTag.charAt(hashTag.length() - 1);
            if (!Character.isLetterOrDigit(lastChar) || Character.isWhitespace(lastChar)) {
                hashTag = hashTag.substring(0, hashTag.length() - 1);
            }
            
            // strip the pound sign and convert to lower case
            hashTag = hashTag.substring(1).toLowerCase();
            
            // unsure of max length of hashtag, trim to 32 to be safe
            if (hashTag.length() > 32) {
                hashTag = hashTag.substring(0, 32);
            }
            
            db.insertHashTag(hashTag);
        }
    }
    
}