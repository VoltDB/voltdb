package org.voltdb_testfuncs;

import java.io.Serializable;

public class UminStartWithParameter implements Serializable {
    private double min;

    public void start(double value) {
        min = value;
    }

    public void assemble (double value) {
        if (value < min) {
            min = value;
        }
    }

    public void combine (UminStartWithParameter other) {
        if (other.min < min) {
            min = other.min;
        }
    }

    public double end () {
        return min;
    }
}