import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import java.io.Serializable;
import java.io.IOException;
import java.lang.Float;

public class Uavg implements Serializable {
   private int count = 0;
   private int sum = 0;

   public void start() {
       this.count = 0;
       this.sum = 0;
   }

   public void assemble (byte value) {
       this.count++;
       this.sum += value;
   }

   public void combine (Uavg other) {
   	this.count += other.count;
       this.sum += other.sum;
   }

   public double end () {
   	return (double)sum/(double)count;
   }
}
