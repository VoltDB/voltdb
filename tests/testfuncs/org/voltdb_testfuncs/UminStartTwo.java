package org.voltdb_testfuncs;

import java.io.Serializable;

public class UminStartTwo implements Serializable {
    private double min;

    public void start() {
        min = 2147483647;
    }

    public double start(double value) {
        min = value;
        return min;
    }

    public void assemble (double value) {
        if (value < min) {
            min = value;
        }
    }

    public void combine (UminStartTwo other) {
        if (other.min < min) {
            min = other.min;
        }
    }

    public double end () {
        return min;
    }
}