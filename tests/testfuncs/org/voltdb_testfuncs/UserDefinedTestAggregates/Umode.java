//import org.voltdb.SQLStmt;
//import org.voltdb.VoltProcedure;
//import org.voltdb.VoltTable;
//import java.io.Serializable;
//import java.io.IOException;
//import java.lang.Integer;
//import java.lang.Float;
//import java.util.*;
//
//public class Umode implements Serializable {
//    private List<Integer> nums;
//
//    public void start() {
//        nums = new ArrayList<>();
//    }
//
//    public void assemble (Integer value) {
//        nums.add(value);
//    }
//
//    public void combine (Umode other) {
//        nums.addAll(other.nums);
//    }
//
//    public double end () {
//        HashMap<Integer,Integer> hm = new HashMap<Integer,Integer>();
//        int max  = 0;
//        Integer temp = nums.size() > 0 ? nums.get(0) : null;
//
//        for(int i = 0; i < nums.size(); i++) {
//
//            if (hm.get(nums.get(i)) != null) {
//
//                int count = hm.get(nums.get(i));
//                count++;
//                hm.put(nums.get(i), count);
//
//                if(count > max) {
//                    max  = count;
//                    temp = nums.get(i);
//                }
//            }
//
//            else 
//                hm.put(nums.get(i),1);
//        }
//        return temp;
//    }
//}
