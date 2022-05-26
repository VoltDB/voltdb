/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb_testfuncs;

import java.io.Serializable;
import java.lang.Integer;
import java.util.*;
import org.voltdb.VoltUDAggregate;

public class UmodeAssembleUnsupportedParameter implements Serializable, VoltUDAggregate<ArrayList<Integer>, UmodeAssembleUnsupportedParameter> {
    private List<Integer> nums;

    public void start() {
        nums = new ArrayList<>();
    }

    public void assemble (ArrayList<Integer> value) {
        for (int i = 0; i < value.size(); ++i) {
            nums.add(value.get(i));
        }
    }

    public void combine (UmodeAssembleUnsupportedParameter other) {
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
