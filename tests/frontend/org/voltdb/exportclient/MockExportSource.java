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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;

import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.export.ExportProtoMessage;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FastSerializer;

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

    public final static void acceptThread(Socket socket) throws IOException {
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
            FastSerializer fs = new FastSerializer();
            // serialize an array of DataSources that are locally available
            fs.writeInt(1); // source count
            // BEGIN TABLE 1
            fs.writeLong(0); // generation
            fs.writeInt(0); // partition id
            fs.writeString("i"); // table signature
            fs.writeString("DUMMY"); // table name
            fs.writeLong(ManagementFactory.getRuntimeMXBean().getStartTime());
            fs.writeInt(1); // number of columns
            fs.writeString("A"); // column 1 name
            fs.writeInt(VoltType.INTEGER.getValue()); // column 1 type
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

            // get the ack/poll message
            m = getNextExportMessage(in);
            assert(m.isPoll());

            // send half of a block
            VoltTable t = new VoltTable(new VoltTable.ColumnInfo("foo", VoltType.INTEGER));
            t.addRow(5);
            ByteBuffer buf = ExportEncoder.getEncodedTable(t);
            m = new ExportProtoMessage(0, 0, "i");
            m.pollResponse(0, buf);
            fs = new FastSerializer();
            m.writeToFastSerializer(fs);
            byte[] pollResponseBytes = fs.getBytes();
            out.write(pollResponseBytes);

            // sleep until the socket closes
            while(socket.isConnected())
                Thread.sleep(100);

        }
        catch (EOFException e){
            System.out.println("Connection closed");
            System.out.flush();
            return;
        }
        catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public final static void run() {
        try {
            ServerSocket listener = new ServerSocket(VoltDB.DEFAULT_PORT);

            while (true) {
                Socket sock = listener.accept();
                acceptThread(sock);
            }
        }
        catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        run();
    }

}
