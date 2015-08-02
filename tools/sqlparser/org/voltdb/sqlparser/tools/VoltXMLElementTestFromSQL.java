package org.voltdb.sqlparser.tools;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOError;
import java.io.PrintStream;

import org.hsqldb_voltpatches.HSQLInterface;
import org.hsqldb_voltpatches.HSQLInterface.HSQLParseException;
import org.hsqldb_voltpatches.VoltXMLElement;

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
        PrintStream os = null;
        try {
            os = new PrintStream(new FileOutputStream(m_sqlFileName));
        } catch (FileNotFoundException e) {
            System.err.println(e.getMessage());
            return;
        }
        HSQLInterface hif = HSQLInterface.loadHsqldb();
        try {
            VoltXMLElement elem = hif.getVoltXMLFromDQLUsingVoltSQLParser(m_sqlString, null);
            writePrefix(os);

        } catch (HSQLParseException e) {
           System.err.println(e.getMessage());
           return;
        } finally {
            try {
                os.close();
            } catch (IOError ex) {
                ;
            }
        }

    }

    private void writePrefix(PrintStream os) {
        // TODO Auto-generated method stub

    }
}
