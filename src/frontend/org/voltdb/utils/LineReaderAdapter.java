package org.voltdb.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;

public class LineReaderAdapter implements SQLCommandLineReader {
    private final BufferedReader m_reader;
    private int m_lineNum = 0;

    @Override
    public int getLineNumber() {
        return m_lineNum;
    }

    public LineReaderAdapter(Reader reader) {
        m_reader = new BufferedReader(reader);
    }

    @Override
    public String readBatchLine() throws IOException {
        m_lineNum++;
        return m_reader.readLine();
    }

    void close() {
        try {
            m_reader.close();
        } catch (IOException e) { }
    }
}
