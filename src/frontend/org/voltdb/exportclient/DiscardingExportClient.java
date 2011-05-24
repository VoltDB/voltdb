/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

package org.voltdb.exportclient;

import org.voltdb.export.ExportProtoMessage.AdvertisedDataSource;

public class DiscardingExportClient extends ExportClientBase {

    @Override
    public ExportDecoderBase constructExportDecoder(AdvertisedDataSource source) {
        // TODO Auto-generated method stub
        return null;
    }

    protected static void printHelpAndQuit(int code) {
        System.out
                .println("java -cp <classpath> org.voltdb.exportclient.ExportToFileClient "
                        + "--help");
        System.out
                .println("java -cp <classpath> org.voltdb.exportclient.ExportToFileClient "
                        + "--servers server1[,server2,...,serverN] "
                        + "--connect (admin|client) "
                        + "[--user export_username] "
                        + "[--password export_password]");
        System.out
                .println("java -cp <classpath> org.voltdb.exportclient.ExportToFileClient "
                        + "--servers server1[,server2,...,serverN] "
                        + "--connect (admin|client) "
                        + "--type (csv|tsv) "
                        + "--nonce file_prefix "
                        + "[--period rolling_period_in_minutes] "
                        + "[--dateformat date_pattern_for_file_name] "
                        + "[--outdir target_directory] "
                        + "[--skipinternals] "
                        + "[--user export_username] "
                        + "[--password export_password]");
        System.out.println("Note that server hostnames may be appended with a specific port:");
        System.out.println("  --servers server1:port1[,server2:port2,...,serverN:portN]");

        System.exit(code);
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub

    }

}
