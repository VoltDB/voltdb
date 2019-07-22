package org.voltdb_testfuncs;

import java.io.Serializable;
import java.lang.Integer;
import java.util.*;

public class UmedianEndTwo implements Serializable {
    private List<Integer> nums;

    public void start() {
        nums = new ArrayList<>();
    }

    public void assemble (Integer value) {
        nums.add(value);
    }

    public void combine (UmedianEndTwo other) {
        nums.addAll(other.nums);
    }

    public double end () {
        Collections.sort(nums);
        if (nums.size() % 2 == 0) {
            return ((double)nums.get(nums.size()/2 - 1) + (double)nums.get(nums.size()/2)) / 2;
        }
        else {
            return (double)nums.get((nums.size() - 1)/2);
        }
    }

    public void end (Integer value) {
        nums.add(value);
    }
}
