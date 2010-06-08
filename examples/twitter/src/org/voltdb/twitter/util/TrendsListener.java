package org.voltdb.twitter.util;

import java.util.List;

import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;

public class TrendsListener implements StatusListener {
    
    private TwitterParser twitterParser;
    
    public TrendsListener(List<String> servers) {
        this.twitterParser = new TwitterParser(servers);
    }
    
    @Override
    public void onDeletionNotice(StatusDeletionNotice arg0) {}
    
    @Override
    public void onException(Exception arg0) {}
    
    @Override
    public void onStatus(Status status) {
        twitterParser.parseHashTags(status.getText());
    }
    
    @Override
    public void onTrackLimitationNotice(int arg0) {}
    
}