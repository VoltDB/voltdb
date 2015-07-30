package org.voltdb.sqlparser.tools;

public class VoltXMLElementTestFromSQL {
    String m_sqlString;
    String m_sqlFileName;
    String m_className;


    public static void main(String[] args) {
        VoltXMLElementTestFromSQL elem = new VoltXMLElementTestFromSQL(args);
        elem.process();
    }

    public VoltXMLElementTestFromSQL(String[] args) {
        for (int idx = 0; idx < args.length; idx += 1) {
            if (args[idx] == "--sql") {
                m_sqlString = args[++idx];
            } else if (args[idx] == "--sql-file") {
                m_sqlFileName = args[++idx];
            } else if (args[idx] == "--class") {
                m_className = args[++idx];
            } else {
                System.err.printf("Unknown comand line parameter \"%s\"\n", args[idx]);
                System.exit(100);
            }
        }
    }

    private void process() {

    }
}
