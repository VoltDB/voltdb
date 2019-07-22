package org.voltdb_testfuncs;

import java.io.Serializable;
import java.lang.Integer;
import java.util.*;

public class UmedianEndUnsupportedReturn implements Serializable {
    private List<Integer> nums;

    public void start() {
        nums = new ArrayList<>();
    }

    public void assemble (Integer value) {
        nums.add(value);
    }

    public void combine (UmedianEndUnsupportedReturn other) {
        nums.addAll(other.nums);
    }

    public List<Integer> end () {
        Collections.sort(nums);
        return nums;
    }
}
