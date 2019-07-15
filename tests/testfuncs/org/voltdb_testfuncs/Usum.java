package org.voltdb_testfuncs;

import java.io.Serializable;

public class Usum implements Serializable {
    private double intermediateResult;

    public void start() {
        intermediateResult = 0;
    }

    public void assemble (double value) {
        intermediateResult += value;
    }

    public void combine (Usum other) {
        intermediateResult += other.intermediateResult;
    }

    public double end () {
        return intermediateResult;
    }

}