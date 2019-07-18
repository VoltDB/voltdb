package org.voltdb_testfuncs;

import java.io.Serializable;

public class UcountNull implements Serializable {
    private int count = 0;

    public void start() {
        this.count = 0;
    }

    public void assemble (Double value) {
        if (value == null) {
            this.count++;
        }
    }

    public void combine (UcountNull other) {
        this.count += other.count;
    }

    public int end () {
        return count;
    }
}