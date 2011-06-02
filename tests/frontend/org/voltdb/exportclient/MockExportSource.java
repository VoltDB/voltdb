/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.exportclient;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONObject;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.export.ExportProtoMessage;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.types.VoltDecimalHelper;
import org.voltdb.utils.VoltTypeUtil;

import au.com.bytecode.opencsv_voltpatches.CSVReader;

public class MockExportSource {

    static String readString(DataInputStream in) throws IOException {
        int len = in.readInt();
        byte[] strData = new byte[len];
        in.read(strData);
        String retval = new String(strData, "UTF-8");
        return retval;
    }

    static ExportProtoMessage getNextExportMessage(DataInputStream in) throws IOException {
        int messageLen = in.readInt();
        byte[] openMsgBytes = new byte[messageLen];
        in.read(openMsgBytes);
        FastDeserializer fds = new FastDeserializer(openMsgBytes);
        ExportProtoMessage m = ExportProtoMessage.readExternal(fds);
        return m;
    }

    public final static void acceptThread(Socket socket, DataGenerator dataGen) throws IOException {
        try {
            DataInputStream in = new DataInputStream(socket.getInputStream());
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());

            // read in auth request
            int messageLen = in.readInt();
            assert(messageLen == 35);
            byte version = in.readByte();
            assert(version == 0);
            String service = readString(in);
            assert(service.equals("export"));
            String username = readString(in);
            assert(username.equals(""));
            byte[] password = new byte[20];
            in.read(password);

            // reply with success
            byte buildString[] = "Dummy Buildstring".getBytes("UTF-8");
            out.writeInt(30 + buildString.length); //message length
            out.write((byte)0); //version
            out.write((byte)0); //success
            out.writeInt(0); // hostid
            out.writeLong(0); // connectionid
            out.writeLong(0); // instanceid (1/2)
            out.writeInt(0); // instanceid (2/2)
            out.writeInt(buildString.length);
            out.write(buildString);

            // receive the open message
            ExportProtoMessage m = getNextExportMessage(in);
            assert(m.isOpen());

            // send the open response message
            VoltTable schema = dataGen.tableForSchema();

            FastSerializer fs = new FastSerializer();
            // serialize an array of DataSources that are locally available
            fs.writeInt(1); // source count
            // BEGIN TABLE 1
            fs.writeLong(0); // generation
            fs.writeInt(0); // partition id
            String signature = dataGen.getSignature();
            fs.writeString(signature); // table signature
            fs.writeString("unnamed table"); // table name
            fs.writeLong(ManagementFactory.getRuntimeMXBean().getStartTime());
            fs.writeInt(schema.getColumnCount()); // number of columns
            for (int i = 0; i < schema.getColumnCount(); i++) {
                fs.writeString(schema.getColumnName(i)); // column name
                fs.writeInt(schema.getColumnType(i).getValue()); // column type
            }
            // END TABLE 1
            // serialize the makup of the cluster
            fs.writeInt(1); // number of hosts
            fs.writeString("127.0.0.1:21212:-1:-1"); // metadata for localhost
            // build the message and send it
            m = new ExportProtoMessage(0, 0, "i");
            m.openResponse(fs.getBuffer());
            fs = new FastSerializer();
            m.writeToFastSerializer(fs);
            byte[] openResponseBytes = fs.getBytes();
            out.write(openResponseBytes);

            while ((dataGen.eof() == false) && socket.isConnected()) {
                // get the ack/poll message
                m = getNextExportMessage(in);
                assert(m.isPoll());

                // send  a block
                VoltTable t = dataGen.nextBlock();
                // an empty table means no more csv
                if (t.getRowCount() == 0)
                    break;
                ByteBuffer buf = ExportEncoder.getEncodedTable(t);
                m = new ExportProtoMessage(0, 0, dataGen.getSignature());
                m.pollResponse(0, buf);
                fs = new FastSerializer();
                m.writeToFastSerializer(fs);
                byte[] pollResponseBytes = fs.getBytes();
                out.write(pollResponseBytes);
            }

            // sleep until the socket closes
            while(socket.isConnected())
                Thread.sleep(100);

        }
        catch (EOFException e){
            return;
        }
        catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public final static void run(int  delay, int blockSize, int tupleCount) {
        runInternal(delay, blockSize, tupleCount, null, null);
    }

    public final static void run(int delay, int blockSize, File csv, File schema) {
        runInternal(delay, blockSize, 0, csv, schema);
    }

    private final static void runInternal(int delay, int blockSize, int tupleCount, File csv, File schema) {
        try {
            ServerSocket listener = new ServerSocket(VoltDB.DEFAULT_PORT);

            while (true) {
                Socket sock = listener.accept();

                DataGenerator generator;
                if (csv != null)
                    generator = new DataGenerator(delay, blockSize, csv, schema);
                else
                    generator = new DataGenerator(delay, blockSize, tupleCount);

                acceptThread(sock, generator);
            }
        }
        catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    static class DataGenerator {
        final int m_blockSize;
        final int m_delay;
        final VoltTable m_schema;
        final CSVReader m_csv;
        long m_rowCounter = 0;
        final int m_tupleCount;

        public DataGenerator(int delay, int blockSize, int tupleCount) {
            m_delay = delay;
            m_blockSize = blockSize;
            m_tupleCount = tupleCount;

            m_schema = new VoltTable(
                    new VoltTable.ColumnInfo("foo", VoltType.BIGINT),
                    new VoltTable.ColumnInfo("bar", VoltType.STRING));

            m_csv = null;
        }

        public DataGenerator(int delay, int blockSize, File csvdata, File schema) {
            m_delay = delay;
            m_blockSize = blockSize;
            m_tupleCount = 0;

            VoltTable schemaTemp = null;
            CSVReader csvTemp = null;

            try {
                FileReader fr = new FileReader(schema);

                BufferedReader reader = new BufferedReader(fr);
                StringBuilder content = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    content.append(line);
                }
                fr.close();
                JSONObject jsonObj = new JSONObject(content.toString());
                JSONArray columns = jsonObj.getJSONArray("columns");
                VoltTable.ColumnInfo[] colInfo = new VoltTable.ColumnInfo[columns.length()];
                for (int i = 0; i < columns.length(); i++) {
                    JSONObject column = columns.getJSONObject(i);
                    assert(column != null);
                    String columnName = column.getString("name");
                    String columnType = column.getString("type");
                    VoltType vtype = VoltType.typeFromString(columnType);
                    colInfo[i] = new VoltTable.ColumnInfo(columnName, vtype);
                }
                schemaTemp = new VoltTable(colInfo);

                csvTemp = new CSVReader(new FileReader(csvdata));
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(-1);
            }

            m_schema = schemaTemp;
            m_csv = csvTemp;
        }

        public VoltTable tableForSchema() {
            return m_schema;
        }

        boolean addRow(VoltTable t) {
            if (m_csv != null) {
                try {
                    Object[] row = new Object[m_schema.getColumnCount()];
                    String[] values = m_csv.readNext();
                    if (values == null) return false;
                    for (int i = 0; i < m_schema.getColumnCount(); i++) {
                        VoltType type = m_schema.getColumnType(i);
                        if (type.isInteger()) {
                            row[i] = Long.parseLong(values[i]);
                        }
                        else if (type == VoltType.FLOAT) {
                            row[i] = Double.parseDouble(values[i]);
                        }
                        else if (type == VoltType.STRING) {
                            row[i] = values[i];
                        }
                        else if (type == VoltType.DECIMAL) {
                            row[i] = VoltDecimalHelper.deserializeBigDecimalFromString(values[i]);
                        }
                        else {
                            assert(false);
                            System.exit(-1);
                        }
                    }

                    // actually add the csv row
                    t.addRow(row);

                } catch (IOException e) {
                    e.printStackTrace();
                    System.exit(-1);
                }
            }
            else {
                // stop if we've done enough tuples
                if ((m_tupleCount > 0) && (m_rowCounter == m_tupleCount))
                    return false;

                if ((m_rowCounter % 2) == 0) {
                    t.addRow(m_rowCounter, "你好");
                }
                else {
                    t.addRow(m_rowCounter, String.valueOf(m_rowCounter));
                }
            }
            m_rowCounter++;
            return true;
        }

        public String getSignature() {
            ArrayList<VoltType> colTypes = new ArrayList<VoltType>();
            for (int i = 0; i < m_schema.getColumnCount(); i++) {
                colTypes.add(m_schema.getColumnType(i));
            }
            String signature = VoltTypeUtil.getSignatureForTable(colTypes);
            return signature;
        }

        public boolean eof() {
            return false;
        }

        public VoltTable nextBlock() {
            try {
                Thread.sleep(m_delay);
            }
            catch (InterruptedException e) {}

            VoltTable next = m_schema.clone(2048 * 1024);

            for (int i = 0; i < m_blockSize; i++) {
                if (!addRow(next))
                    return next;
            }

            return next;
        }
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        int blockSize = 5; // tuples / block
        File csv = null;
        File schema = null;
        int delay = 1000;

        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("--schema")) {
                i++;
                assert(args[i].startsWith("--") == false);
                schema = new File(args[i]);
            }
            else if (args[i].equals("--data")) {
                i++;
                assert(args[i].startsWith("--") == false);
                csv = new File(args[i]);
            }
            else if (args[i].equals("--delay")) {
                i++;
                assert(args[i].startsWith("--") == false);
                delay = Integer.parseInt(args[i]);
            }
            else if (args[i].equals("--blocksize")) {
                i++;
                assert(args[i].startsWith("--") == false);
                blockSize = Integer.parseInt(args[i]);
            }
            else {
                System.err.println("Error, only --schema, --data, --blocksize and --delay are supported.\n");
                System.exit(-1);
            }
        }

        run(delay, blockSize, csv, schema);
    }

}
