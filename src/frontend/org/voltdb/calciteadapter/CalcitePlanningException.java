package org.voltdb.calciteadapter;

public class CalcitePlanningException extends RuntimeException{

    CalcitePlanningException(String msg) {
        super(msg);
    }
    CalcitePlanningException() {
        super();
    }
}
