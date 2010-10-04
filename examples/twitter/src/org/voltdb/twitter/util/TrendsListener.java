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

import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterException;

public class TrendsListener implements StatusListener {

    private TwitterParser twitterParser;

    public TrendsListener(List<String> servers) {
        this.twitterParser = new TwitterParser(servers);
    }

    @Override
    public void onDeletionNotice(StatusDeletionNotice arg0) {}

    @Override
    public void onException(Exception e) {
        if (e instanceof TwitterException) {
            if (((TwitterException) e).getStatusCode() == 401) {
                System.err.println("Invalid Twitter credentials. Did you edit build.xml?");
            } else {
                System.err.println("An unknown error occurred:");
                e.printStackTrace();
            }
        } else {
            System.err.println("An unknown error occurred:");
            e.printStackTrace();
        }

        System.exit(-1);
    }

    @Override
    public void onStatus(Status status) {
        twitterParser.parseStatus(status);
    }

    @Override
    public void onTrackLimitationNotice(int arg0) {}

}
