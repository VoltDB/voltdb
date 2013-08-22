package org.voltdb.compiler;

import junit.framework.TestCase;

public class TestClassMatcher extends TestCase {
    static String testClasses = "org.voltdb.utils.BinaryDeque\n" +
                                "org.voltdb.utils.BinaryDeque$BinaryDequeTruncator\n" +
                                "org.voltdb.utils.BuildDirectoryUtils\n" +
                                "org.voltdb.utils.ByteArrayUtils\n" +
                                "org.voltdb.utils.CLibrary\n" +
                                "org.voltdb.utils.CLibrary$Rlimit\n" +
                                "org.voltdb.utils.CSVLoader\n";


    public void testSimple() {


        ClassMatcher cm = new ClassMatcher();
        cm.m_classList = testClasses;

        cm.addPattern("org.**.B*");

        String[] out = cm.getMatchedClassList();
        for (String className : out) {
            System.out.println(className);
        }
    }
}
