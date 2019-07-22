package org.voltdb_testfuncs;

import java.io.Serializable;

public class UminStartMissing implements Serializable {
    private double min = 0;

    public void assemble (double value) {
        if (value < min) {
            min = value;
        }
    }

    public void combine (UminStartMissing other) {
        if (other.min < min) {
            min = other.min;
        }
    }

    public double end () {
        return min;
    }
}