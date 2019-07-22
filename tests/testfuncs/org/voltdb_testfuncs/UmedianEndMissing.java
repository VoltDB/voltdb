package org.voltdb_testfuncs;

import java.io.Serializable;
import java.lang.Integer;
import java.util.*;

public class UmedianEndMissing implements Serializable {
    private List<Integer> nums;

    public void start() {
        nums = new ArrayList<>();
    }

    public void assemble (Integer value) {
        nums.add(value);
    }

    public void combine (UmedianEndMissing other) {
        nums.addAll(other.nums);
    }
}
