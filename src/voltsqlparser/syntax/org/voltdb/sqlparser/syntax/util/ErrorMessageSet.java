package org.voltdb.sqlparser.syntax.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.voltdb.sqlparser.syntax.util.ErrorMessage.Severity;

public class ErrorMessageSet implements Iterable<ErrorMessage> {
    List<ErrorMessage> m_errorMessages = new ArrayList<ErrorMessage>();

    public void addError(int line,
                         int col,
                         String fmt,
                         Object ... args) {
        String msg = String.format(fmt, args);
        m_errorMessages.add(new ErrorMessage(line,
                                             col,
                                             Severity.Error,
                                             msg));
    }

    public void addWarning(int line, int col, String errorMessageFormat,
            Object[] args) {
        String msg = String.format(errorMessageFormat, args);
        m_errorMessages.add(new ErrorMessage(line,
                                             col,
                                             Severity.Warning,
                                             msg));
    }

    public int size() {
        // TODO Auto-generated method stub
        return m_errorMessages.size();
    }

    @Override
    public Iterator<ErrorMessage> iterator() {
        return m_errorMessages.iterator();
    }

}
