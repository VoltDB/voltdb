package org.voltdb_testfuncs;

import java.io.Serializable;

public class UminStartOverload implements Serializable {
    private double min;

    public void start() {
        start(2147483647);
    }

    private void start(int min_in) {
        min = min_in;
    }

    public void assemble (double value) {
        if (value < min) {
            min = value;
        }
    }

    public void combine (UminStartOverload other) {
        if (other.min < min) {
            min = other.min;
        }
    }

    public double end () {
        return min;
    }
}