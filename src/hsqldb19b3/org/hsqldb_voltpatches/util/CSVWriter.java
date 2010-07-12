/* Copyright (c) 2001-2009, The HSQL Development Group
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of the HSQL Development Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL HSQL DEVELOPMENT GROUP, HSQLDB.ORG,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */


package org.hsqldb_voltpatches.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

/**
 * helper class to write table data to a csv-file (comma separated values).
 * the first line in file is a list of fieldnames, all following lines
 * are data lines.
 * a descptiontion of file format can be found on: http://www.wotsit.org/
 * usage: create a object using the constructor. call writeHeader
 * for writing the filename header then add data with writeData.
 * at the end close() closes the file.
 *
 *@author jeberle@users
 */
public class CSVWriter {

    private String             newline = System.getProperty("line.separator");
    private OutputStreamWriter writer  = null;
    private int                nbrCols = 0;
    private int                nbrRows = 0;

    /**
     * constructor.
     * creates a csv file for writing data to it
     * @param file the file to write data to
     * @param encoding encoding to use or null (=defualt)
     */
    public CSVWriter(File file, String encoding) throws IOException {

        if (encoding == null) {
            encoding = System.getProperty("file.encoding");
        }

        FileOutputStream fout = new FileOutputStream(file);

        writer = new OutputStreamWriter(fout, encoding);
    }

    /**
     * writes the csv header (fieldnames). should be called after
     * construction one time.
     * @param header String[] with fieldnames
     */
    public void writeHeader(String[] header) throws IOException {

        this.nbrCols = header.length;

        doWriteData(header);
    }

    /**
     * writes a data-record to the file. note that data[] must have
     * same number of elements as the header had.
     *
     * @param data data to write to csv-file
     */
    public void writeData(String[] data) throws IOException {
        doWriteData(data);
    }

    /**
     * closes the csv file.
     */
    public void close() throws IOException {
        this.writer.close();
    }

    private void doWriteData(String[] values) throws IOException {

        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                this.writer.write(";");
            }

            if (values[i] != null) {
                this.writer.write("\"");
                this.writer.write(this.toCsvValue(values[i]));
                this.writer.write("\"");
            }
        }

        this.writer.write(newline);

        this.nbrRows++;
    }

    private String toCsvValue(String str) {

        StringBuffer sb = new StringBuffer();

        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);

            sb.append(c);

            switch (c) {

                case '"' :
                    sb.append('"');
                    break;
            }
        }

        return sb.toString();
    }
}
