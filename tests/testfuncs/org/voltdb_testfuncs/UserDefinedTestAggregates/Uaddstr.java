//import org.voltdb.SQLStmt;
//import org.voltdb.VoltProcedure;
//import org.voltdb.VoltTable;
//import java.io.Serializable;
//import java.io.IOException;
//import java.lang.String;
//
//public class Uaddstr implements Serializable {
//    private String overall;
//
//    public void start() {
//        overall = "";
//    }
//
//    public void assemble (String value) {
//        overall += " " + value;
//    }
//
//    public void combine (Uaddstr other) {
//    	overall += " " + other.overall;
//    }
//
//    public String end () {
//    	return overall;
//    }
//}
