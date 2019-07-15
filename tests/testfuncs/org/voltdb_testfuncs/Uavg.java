package org.voltdb_testfuncs;

import java.io.Serializable;

public class Uavg implements Serializable {
    private int count = 0;
    private double sum = 0;

    public void start() {
        count = 0;
        sum = 0;
    }

    public void assemble (double value) {
        count++;
        sum += value;
    }

    public void combine (Uavg other) {
        count += other.count;
        sum += other.sum;
    }

    public double end () {
        return (double)sum/(double)count;
    }
}
