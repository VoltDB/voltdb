import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import java.io.Serializable;
import java.io.IOException;
import java.lang.Integer;
import java.lang.Float;
import java.util.*;

public class Umedian implements Serializable {
   private List<Integer> nums;

   public void start() {
       nums = new ArrayList<>();
   }

   public void assemble (Integer value) {
       nums.add(value);
   }

   public void combine (Umedian other) {
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
}
