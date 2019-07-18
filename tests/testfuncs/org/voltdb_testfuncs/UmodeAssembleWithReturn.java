package org.voltdb_testfuncs;

import java.io.Serializable;
import java.lang.Integer;
import java.util.*;

public class UmodeAssembleWithReturn implements Serializable {
    private List<Integer> nums;

    public void start() {
        nums = new ArrayList<>();
    }

    public Integer assemble (Integer value) {
        nums.add(value);
        return value;
    }

    public void combine (UmodeAssembleWithReturn other) {
        nums.addAll(other.nums);
    }

    public Integer end () {
        HashMap<Integer,Integer> hm = new HashMap<Integer,Integer>();
        int max  = 0;
        Integer temp = nums.size() > 0 ? nums.get(0) : null;

        for (int i = 0; i < nums.size(); i++) {

            if (hm.get(nums.get(i)) != null) {

                int count = hm.get(nums.get(i));
                count++;
                hm.put(nums.get(i), count);

                if (count > max) {
                    max  = count;
                    temp = nums.get(i);
                }
            }

            else {
                hm.put(nums.get(i),1);
            }
        }
        return temp;
    }
}
