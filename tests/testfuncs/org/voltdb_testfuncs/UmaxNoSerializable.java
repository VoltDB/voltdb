package org.voltdb_testfuncs;

import java.io.Serializable;

public class UmaxNoSerializable {
    private double max;

    public void start() {
        max = -2147483648;
    }

    public void assemble (double value) {
        if (value > max) {
            max = value;
        }
    }

    public void combine (UmaxNoSerializable other) {
        if (other.max > max) {
            max = other.max;
        }
    }

    public double end () {
        return max;
    }
}