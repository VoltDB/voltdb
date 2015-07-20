package org.voltdb.sqlparser.semantics.grammar;

import java.util.ArrayList;
import java.util.List;

public class NullInList {

    public static void main(String[] args) {
        List<String> alist = new ArrayList<String>();
        alist.add(null);
        alist.add(null);
        alist.add(null);
        System.out.printf("%d elements\n", alist.size());
        for (String str : alist) {
            if (str == null) {
                System.out.printf("Null\n");
            } else {
                System.out.printf("%s", str);
            }
        }
    }

}
