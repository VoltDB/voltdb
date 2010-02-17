/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.sysprocs.saverestore;

import java.io.BufferedInputStream;
import java.io.CharArrayWriter;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.zip.CRC32;

public class SnapshotDigestUtil {

    public static void recordSnapshotTableList(String path, String nonce, List<String> tables) throws IOException {
        final File f = new File(path, nonce + ".digest");
        if (f.exists()) {
            if (!f.delete()) {
                throw new IOException("Unable to write table list file " + f);
            }
        }
        FileOutputStream fos = new FileOutputStream(f);
        StringWriter sw = new StringWriter();
        for (int ii = 0; ii < tables.size(); ii++) {
            sw.append(tables.get(ii));
            if (!(ii == (tables.size() - 1))) {
                sw.append(',');
            } else {
                sw.append('\n');
            }
        }

        final byte tableListBytes[] = sw.getBuffer().toString().getBytes("UTF-8");
        final CRC32 crc = new CRC32();
        crc.update(tableListBytes);
        ByteBuffer fileBuffer = ByteBuffer.allocate(tableListBytes.length + 4);
        fileBuffer.putInt((int)crc.getValue());
        fileBuffer.put(tableListBytes);
        fileBuffer.flip();
        fos.getChannel().write(fileBuffer);
        fos.getFD().sync();
    }

    public static List<String> retrieveRelevantTableNames(String path,
            String nonce) throws Exception {
        return retrieveRelevantTableNames(new File(path, nonce + ".digest"));
    }

    public static List<String> retrieveRelevantTableNames(File f) throws Exception {
        final FileInputStream fis = new FileInputStream(f);
        try {
            final BufferedInputStream bis = new BufferedInputStream(fis);
            ByteBuffer crcBuffer = ByteBuffer.allocate(4);
            if (4 != bis.read(crcBuffer.array())) {
                throw new EOFException("EOF while attempting to read CRC from snapshot digest");
            }
            final int crc = crcBuffer.getInt();
            final InputStreamReader isr = new InputStreamReader(bis, "UTF-8");
            CharArrayWriter caw = new CharArrayWriter();
            while (true) {
                int nextChar = isr.read();
                if (nextChar == -1) {
                    throw new EOFException("EOF while reading snapshot digest");
                }
                if (nextChar == '\n') {
                    break;
                }
                caw.write(nextChar);
            }
            String tableList = caw.toString();
            byte tableListBytes[] = tableList.getBytes("UTF-8");
            CRC32 tableListCRC = new CRC32();
            tableListCRC.update(tableListBytes);
            tableListCRC.update("\n".getBytes("UTF-8"));
            final int calculatedValue = (int)tableListCRC.getValue();
            if (crc != calculatedValue) {
                throw new IOException("CRC of snapshot digest did not match digest contents");
            }
            String tableNames[] = tableList.split(",");
            return java.util.Arrays.asList(tableNames);
        } finally {
            try {
                if (fis != null) {
                    fis.close();
                }
            } catch (IOException e) {
            }
        }
    }
}
