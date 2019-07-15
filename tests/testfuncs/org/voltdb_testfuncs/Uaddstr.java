package org.voltdb_testfuncs;

import java.io.Serializable;
import java.lang.String;

public class Uaddstr implements Serializable {
   private String overall;

   public void start() {
       overall = "";
   }

   public void assemble (String value) {
       overall += " " + value;
   }

   public void combine (Uaddstr other) {
   	overall += " " + other.overall;
   }

   public String end () {
   	return overall;
   }
}
