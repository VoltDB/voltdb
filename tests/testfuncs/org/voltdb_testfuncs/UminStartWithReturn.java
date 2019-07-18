package org.voltdb_testfuncs;

import java.io.Serializable;

public class UminStartWithReturn implements Serializable {
    private double min;

    public double start() {
        min = 2147483647;
        return min;
    }

    public void assemble (double value) {
        if (value < min) {
            min = value;
        }
    }

    public void combine (UminStartWithReturn other) {
        if (other.min < min) {
            min = other.min;
        }
    }

    public double end () {
        return min;
    }
}