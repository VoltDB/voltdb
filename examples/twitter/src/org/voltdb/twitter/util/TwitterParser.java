/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */
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
