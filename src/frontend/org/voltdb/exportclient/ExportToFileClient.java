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
package org.voltdb.exportclient;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.log4j.Logger;
import org.voltdb.VoltDB;
import org.voltdb.VoltType;
import org.voltdb.elt.ELTProtoMessage.AdvertisedDataSource;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.VoltLoggerFactory;
import org.voltdb.utils.CSVEscaperUtil.Escaper;
import org.voltdb.utils.CSVEscaperUtil.CSVEscaper;
import org.voltdb.utils.CSVEscaperUtil.TSVEscaper;

/**
 * Uses the Export feature of VoltDB to write exported tables to files.
 *
 * command line args:
 *   --servers {comma-separated list of VoltDB server to which to connect}
 *   --type [csv|tsv]
 *     csv for comma-separated values, tsv for tab-separated values
 *   --outdir {path where output files should be written}
 *   --nonce {string-to-unique-ify output files}
 *   --user {username for cluster export user}
 *   --password {password for cluster export user}
 *
 */

public class ExportToFileClient extends ExportClientBase
{
    private static final Logger m_logger =
        Logger.getLogger("ExportToFileClient",
                         VoltLoggerFactory.instance());

    private Escaper m_escaper;
    private String m_nonce;
    private File m_outDir;
    private HashMap<String, ExportToFileDecoder> m_tableDecoders;

    // This class outputs exported rows converted to CSV or TSV values
    // for the table named in the constructor's AdvertisedDataSource
    class ExportToFileDecoder extends ExportDecoderBase
    {
        private Escaper m_escaper;
        private File m_outFile;
        private FileOutputStream m_fos;

        public ExportToFileDecoder(AdvertisedDataSource source, String nonce,
                                   File outdir, Escaper escaper)
        {
            super(source);
            m_escaper = escaper;
            // Create the output file for this table
            String filename =
                nonce + "-" + source.tableName() + "." + escaper.getExtension();
            m_logger.info("Opening filename " + filename);
            m_outFile = new File(outdir.getPath() + File.separator + filename);
            boolean fail = false;
            try {
                if (!m_outFile.createNewFile()) {
                    m_logger.error("Error: Failed to create output file " +
                                   m_outFile.getPath() + " for table " +
                                   source.tableName() +
                                   ": File already exists");
                    fail = true;
                }
            } catch (IOException e) {
                m_logger.error(e.getMessage());
                m_logger.error("Error: Failed to create output file " +
                               m_outFile.getPath() + " for table " +
                               source.tableName());
                fail = true;
            }
            if (fail)
            {
                m_logger.error("Ha, writing to /dev/null");
                m_outFile = new File("/dev/null");
            }
            try
            {
                m_fos = new FileOutputStream(m_outFile, true);
            }
            catch (FileNotFoundException e)
            {
                String msg = "Unable to open output file: " + filename;
                m_logger.fatal(msg);
                throw new RuntimeException(msg);
            }
        }

        @Override
        public boolean processRow(int rowSize, byte[] rowData)
        {
            Object[] row = null;
            try
            {
                row = decodeRow(rowData);
            }
            catch (IOException e)
            {
                m_logger.error("Unable to decode row for table: " +
                               m_source.tableName());
                return false;
            }

            StringBuilder sb = new StringBuilder();
            final SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss:SSS:");
            for (int i = 0; i < m_tableSchema.size(); i++)
            {
                if (i != 0)
                {
                    sb.append(m_escaper.getDelimiter());
                }

                if (row[i] == null)
                {
                    sb.append("NULL");
                }
                else if (m_tableSchema.get(i) == VoltType.STRING)
                {
                    sb.append(m_escaper.escape((String)row[i]));
                }
                else if (m_tableSchema.get(i) == VoltType.TIMESTAMP)
                {
                    StringBuilder builder = new StringBuilder(64);
                    TimestampType timestamp = (TimestampType)row[i];
                    builder.append(sdf.format(timestamp.asApproximateJavaDate()));
                    builder.append(timestamp.getUSec());
                    sb.append(m_escaper.escape(builder.toString()));
                }
                else
                {
                    sb.append(row[i].toString());
                }
            }
            sb.append("\n");
            byte bytes[] = null;
            try {
                bytes = sb.toString().getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
                return false;
            }
            // write bytes to file
            try
            {
                m_fos.write(bytes);
            }
            catch (IOException e)
            {
                e.printStackTrace();
                throw new RuntimeException(e.getMessage());
            }
            return true;
        }
    }

    public void setVoltServers(String[] voltServers)
    {
        ArrayList<InetSocketAddress> servers =
            new ArrayList<InetSocketAddress>();
        for (int i = 0; i < voltServers.length; i++)
        {
            InetSocketAddress server =
                new InetSocketAddress(voltServers[i], VoltDB.DEFAULT_PORT);
            servers.add(server);
        }
        setServerInfo(servers);
    }

    public ExportToFileClient(Escaper escaper, String nonce, File outdir)
    {
        m_escaper = escaper;
        m_nonce = nonce;
        m_outDir = outdir;
        m_tableDecoders = new HashMap<String, ExportToFileDecoder>();
    }

    @Override
    public ExportDecoderBase constructELTDecoder(AdvertisedDataSource source)
    {
        // For every source that provides part of a table, use the same
        // export decoder.
        String table_name = source.tableName();
        if (!m_tableDecoders.containsKey(table_name))
        {
            m_tableDecoders.put(table_name,
                                new ExportToFileDecoder(source, m_nonce,
                                                        m_outDir, m_escaper));
        }
        return m_tableDecoders.get(table_name);
    }




    private static void printHelpAndQuit(int code)
    {
        System.out.println("java -cp <classpath> -Djava.library.path=<library path> org.voltdb.exportclient.ExportToFileClient --help");
        System.out.println("java -cp <classpath> -Djava.library.path=<library path> org.voltdb.exportclient.ExportToFileClient " +
                           "--servers server1,server2,... --type CSV|TSV " +
                           "--outdir dir --nonce any_string --user export_username " +
                           "--password export_password");
        System.exit(code);
    }

    public static void main(String[] args)
    {
        String[] volt_servers = null;
        String user = null;
        String password = null;
        String nonce = null;
        Escaper escaper = null;
        File outdir = null;

        for (int ii = 0; ii < args.length; ii++)
        {
            String arg = args[ii];
            if (arg.equals("--help"))
            {
                printHelpAndQuit(0);
            }
            else if (arg.equals("--servers"))
            {
                if (args.length < ii + 1) {
                    System.err.println("Error: Not enough args following --servers");
                    printHelpAndQuit(-1);
                }
                volt_servers = args[ii + 1].split(",");
                ii++;
            }
            else if (arg.equals("--type")) {
                if (args.length < ii + 1) {
                    System.err.println("Error: Not enough args following --type");
                    printHelpAndQuit(-1);
                }
                String type = args[ii + 1];
                if (type.equalsIgnoreCase("csv")) {
                    escaper = new CSVEscaper();
                } else if (type.equalsIgnoreCase("tsv")) {
                    escaper = new TSVEscaper();
                } else {
                    System.err.println("Error: --type must be one of CSV or TSV");
                    printHelpAndQuit(-1);
                }
                ii++;
            }
            else if (arg.equals("--outdir")) {
                if (args.length < ii + 1) {
                    System.err.println("Error: Not enough args following --outdir");
                    printHelpAndQuit(-1);
                }
                boolean invalidDir = false;
                outdir = new File(args[ii + 1]);
                if (!outdir.exists()) {
                    System.err.println("Error: " + outdir.getPath() + " does not exist");
                    invalidDir = true;
                }
                if (!outdir.canRead()) {
                    System.err.println("Error: " + outdir.getPath() + " does not have read permission set");
                    invalidDir = true;
                }
                if (!outdir.canExecute()) {
                    System.err.println("Error: " + outdir.getPath() + " does not have execute permission set");
                    invalidDir = true;
                }
                if (!outdir.canWrite()) {
                    System.err.println("Error: " + outdir.getPath() + " does not have write permission set");
                    invalidDir = true;
                }
                if (invalidDir) {
                    System.exit(-1);
                }
                ii++;
            }
            else if (arg.equals("--nonce"))
            {
                if (args.length < ii + 1) {
                    System.err.println("Error: Not enough args following --nonce");
                    printHelpAndQuit(-1);
                }
                nonce = args[ii + 1];
                ii++;
            }
            else if (arg.equals("--user"))
            {
                if (args.length < ii + 1) {
                    System.err.println("Error: Not enough args following --user");
                    printHelpAndQuit(-1);
                }
                user = args[ii + 1];
                ii++;
            }
            else if (arg.equals("--password"))
            {
                if (args.length < ii + 1) {
                    System.err.println("Error: Not enough args following --password");
                    printHelpAndQuit(-1);
                }
                password = args[ii + 1];
                ii++;
            }
        }
        // Check args for validity
        if (volt_servers == null || volt_servers.length < 1)
        {
            System.err.println("ExportToFile: must provide at least one VoltDB server");
            printHelpAndQuit(-1);
        }
        if (user == null)
        {
            System.err.println("ExportToFile: must provide a username");
            printHelpAndQuit(-1);
        }
        if (password == null)
        {
            System.err.println("ExportToFile: must provide a password");
            printHelpAndQuit(-1);
        }
        if (escaper == null)
        {
            System.err.println("ExportToFile: must provide an output type");
            printHelpAndQuit(-1);
        }
        if (nonce == null)
        {
            System.err.println("ExportToFile: must provide a filename nonce");
            printHelpAndQuit(-1);
        }
        if (outdir == null)
        {
            outdir = new File(".");
        }

        ExportToFileClient client =
            new ExportToFileClient(escaper, nonce, outdir);
        client.setVoltServers(volt_servers);
        try {
            client.connectToELServers(user, password);
        }
        catch (IOException e) {
            m_logger.fatal("Unable to connect to VoltDB servers for export");
            System.exit(-1);
        }
        client.run();
    }
}
