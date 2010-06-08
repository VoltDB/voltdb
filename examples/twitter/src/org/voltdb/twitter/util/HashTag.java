package org.voltdb.twitter.util;

import org.voltdb.VoltTableRow;

public class HashTag implements Comparable<HashTag> {
    
    private String hashTag;
    private int count;
    private VoltTableRow row;
    
    public HashTag(String hashTag, int count) {
        this(hashTag, count, null);
    }
    
    public HashTag(String hashTag, int count, VoltTableRow row) {
        this.hashTag = hashTag;
        this.count = count;
        this.row = row;
    }
    
    public String getHashTag() {
        return hashTag;
    }
    
    public int getCount() {
        return count;
    }
    
    public VoltTableRow getRow() {
        return row;
    }
    
    @Override
    public int compareTo(HashTag other) {
        if (other.getCount() - count != 0) {
            return other.getCount() - count;
        } else {
            return hashTag.compareTo(other.getHashTag());
        }
    }
    
    @Override
    public String toString() {
        return hashTag + " " + count;
    }
    
}