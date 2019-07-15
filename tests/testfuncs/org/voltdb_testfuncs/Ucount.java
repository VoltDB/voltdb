package org.voltdb_testfuncs;

import java.io.Serializable;

public class Ucount implements Serializable {
    private int count = 0;

    public void start() {
        this.count = 0;
    }

    public void assemble (double value) {
        this.count++;
    }

    public void combine (Ucount other) {
        this.count += other.count;
    }

    public int end () {
        return count;
    }
}
