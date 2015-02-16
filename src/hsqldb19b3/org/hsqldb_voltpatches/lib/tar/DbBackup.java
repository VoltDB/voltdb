/* Copyright (c) 2001-2014, The HSQL Development Group
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


package org.hsqldb_voltpatches.lib.tar;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

import org.hsqldb_voltpatches.lib.InputStreamInterface;
import org.hsqldb_voltpatches.lib.InputStreamWrapper;

/**
 * Works with tar archives containing HSQLDB database instance backups.
 * Viz, creating, examining, or extracting these archives.
 * <P>
 * This class provides OO Tar backup-creation control.
 * The extraction and listing features are implemented only in static fashion
 * in the Main method, which provides a consistent interface for all three
 * features from the command-line.
 * </P> <P>
 * For tar creation, the default behavior is to fail if the target archive
 * exists, and to abort if any database change is detected.
 * Use the JavaBean setters to changes this behavior.
 * See the main(String[]) method for details about command-line usage.
 * </P>
 *
 * @see <a href="../../../../../guide/management-chapt.html#mtc_backup"
 *      target="guide">
 *     The database backup section of the HyperSQL User Guide</a>
 * @see DbBackupMain#main(String[])
 * @see #setOverWrite(boolean)
 * @see #setAbortUponModify(boolean)
 * @author Blaine Simpson (blaine dot simpson at admc dot com)
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.0
 * @since 2.0.0
 */
public class DbBackup {

    protected File         dbDir;
    protected File         archiveFile;
    protected String       instanceName;
    protected boolean      overWrite       = false;    // Defaults no NO OVERWRITE
    protected boolean      abortUponModify = true;     // Defaults to ABORT-UPON-MODIFY
    File[]                 componentFiles;
    InputStreamInterface[] componentStreams;
    boolean[]              existList;
    boolean[]              ignoreList;

    /**
     * Instantiate a DbBackup instance for creating a Database Instance backup.
     *
     * Much validation is deferred until the write() method, to prevent
     * problems with files changing between the constructor and the write call.
     */
    public DbBackup(File archiveFile, String dbPath) {

        this.archiveFile = archiveFile;

        File dbPathFile = new File(dbPath);

        dbDir          = dbPathFile.getAbsoluteFile().getParentFile();
        instanceName   = dbPathFile.getName();
        componentFiles = new File[] {
            new File(dbDir, instanceName + ".properties"),
            new File(dbDir, instanceName + ".script"),
            new File(dbDir, instanceName + ".data"),
            new File(dbDir, instanceName + ".backup"),
            new File(dbDir, instanceName + ".log"),
            new File(dbDir, instanceName + ".lobs")
        };
        componentStreams = new InputStreamInterface[componentFiles.length];
        existList        = new boolean[componentFiles.length];
        ignoreList       = new boolean[componentFiles.length];
    }

    /**
     * Used for SCRIPT backup
     */
    public DbBackup(File archiveFile, String dbPath, boolean script) {

        this.archiveFile = archiveFile;

        File dbPathFile = new File(dbPath);

        dbDir        = dbPathFile.getAbsoluteFile().getParentFile();
        instanceName = dbPathFile.getName();
        componentFiles = new File[]{
            new File(dbDir, instanceName + ".script"), };
        componentStreams = new InputStreamInterface[componentFiles.length];
        existList        = new boolean[componentFiles.length];
        ignoreList       = new boolean[componentFiles.length];
        abortUponModify  = false;
    }

    /**
     * Overrides file with stream.
     */
    public void setStream(String fileExtension, InputStreamInterface is) {

        for (int i = 0; i < componentFiles.length; i++) {
            if (componentFiles[i].getName().endsWith(fileExtension)) {
                componentStreams[i] = is;

                break;
            }
        }
    }

    public void setFileIgnore(String fileExtension) {

        for (int i = 0; i < componentFiles.length; i++) {
            if (componentFiles[i].getName().endsWith(fileExtension)) {
                ignoreList[i] = true;

                break;
            }
        }
    }

    /**
     * Defaults to false.
     *
     * If false, then attempts to write a tar file that already exist will
     * abort.
     */
    public void setOverWrite(boolean overWrite) {
        this.overWrite = overWrite;
    }

    /**
     * Defaults to true.
     *
     * If true, then the write() method will validate that the database is
     * closed, and it will verify that no DB file changes between when we
     * start writing the tar, and when we finish.
     */
    public void setAbortUponModify(boolean abortUponModify) {
        this.abortUponModify = abortUponModify;
    }

    public boolean getOverWrite() {
        return overWrite;
    }

    public boolean getAbortUponModify() {
        return abortUponModify;
    }

    /**
     * This method always backs up the .properties and .script files.
     * It will back up all of .backup, .data, and .log which exist.
     *
     * If abortUponModify is set, no tar file will be created, and this
     * method will throw.
     *
     * @throws IOException for any of many possible I/O problems
     * @throws IllegalStateException only if abortUponModify is set, and
     *                               database is open or is modified.
     */
    public void write() throws IOException, TarMalformatException {

        long startTime = new java.util.Date().getTime();

        checkEssentialFiles();

        TarGenerator generator = new TarGenerator(archiveFile, overWrite,
            Integer.valueOf(DbBackup.generateBufferBlockValue(componentFiles)));

        for (int i = 0; i < componentFiles.length; i++) {
            boolean exists = componentStreams[i] != null
                             || componentFiles[i].exists();

            if (!exists) {
                continue;

                // We've already verified that required files exist, therefore
                // there is no error condition here.
            }

            if (ignoreList[i]) {
                continue;
            }

            if (componentStreams[i] == null) {
                generator.queueEntry(componentFiles[i].getName(),
                                     componentFiles[i]);

                existList[i] = true;
            } else {
                generator.queueEntry(componentFiles[i].getName(),
                                     componentStreams[i]);
            }
        }

        generator.write();
        checkFilesNotChanged(startTime);
    }

    public void writeAsFiles() throws IOException {

        int bufferSize = 512
                         * DbBackup.generateBufferBlockValue(componentFiles);
        byte[] writeBuffer = new byte[bufferSize];

        checkEssentialFiles();
        FileOutputStream fileOut = null;

        for (int i = 0; i < componentFiles.length; i++) try {
            if (ignoreList[i]) {
                continue;
            }

            if (!componentFiles[i].exists()) {
                continue;
            }

            File outFile = new File(archiveFile, componentFiles[i].getName());
            fileOut = new FileOutputStream(outFile);

            if (componentStreams[i] == null) {
                componentStreams[i] = new InputStreamWrapper(
                    new FileInputStream(componentFiles[i]));
            }

            InputStreamInterface instream = componentStreams[i];

            while (true) {
                int count = instream.read(writeBuffer, 0, writeBuffer.length);

                if (count <= 0) {
                    break;
                }

                fileOut.write(writeBuffer, 0, count);
            }

            instream.close();
            fileOut.flush();
            fileOut.getFD().sync();
        } finally {
            if (fileOut != null) {
                fileOut.close();
                fileOut = null;
            }
        }
    }

    void checkEssentialFiles()
    throws FileNotFoundException, IllegalStateException {

        if (!componentFiles[0].getName().endsWith(".properties")) {
            return;
        }

        for (int i = 0; i < 2; i++) {
            boolean exists = componentStreams[i] != null
                             || componentFiles[i].exists();

            if (!exists) {

                // First 2 files are REQUIRED
                throw new FileNotFoundException(
                    RB.file_missing.getString(
                        componentFiles[i].getAbsolutePath()));
            }
        }

        if (!abortUponModify) {
            return;
        }

        Properties      p   = new Properties();
        FileInputStream fis = null;

        try {
            File propertiesFile = componentFiles[0];

            fis = new FileInputStream(propertiesFile);

            p.load(fis);
        } catch (IOException io) {}
        finally {
            try {
                if (fis != null) {
                    fis.close();
                }
            } catch (IOException io) {}
            finally {
                fis = null;    // Encourage buffer GC
            }
        }

        String modifiedString = p.getProperty("modified");

        if (modifiedString != null
                && (modifiedString.equalsIgnoreCase("yes")
                    || modifiedString.equalsIgnoreCase("true"))) {
            throw new IllegalStateException(
                RB.modified_property.getString(modifiedString));
        }
    }

    void checkFilesNotChanged(long startTime) throws FileNotFoundException {

        // abortUponModify is used with offline invocation only
        if (!abortUponModify) {
            return;
        }

        try {
            for (int i = 0; i < componentFiles.length; i++) {
                if (componentFiles[i].exists()) {
                    if (!existList[i]) {
                        throw new FileNotFoundException(
                            RB.file_disappeared.getString(
                                componentFiles[i].getAbsolutePath()));
                    }

                    if (componentFiles[i].lastModified() > startTime) {
                        throw new FileNotFoundException(
                            RB.file_changed.getString(
                                componentFiles[i].getAbsolutePath()));
                    }
                } else if (existList[i]) {
                    throw new FileNotFoundException(
                        RB.file_appeared.getString(
                            componentFiles[i].getAbsolutePath()));
                }
            }
        } catch (IllegalStateException ise) {
            if (!archiveFile.delete()) {
                System.out.println(
                    RB.cleanup_rmfail.getString(
                        archiveFile.getAbsolutePath()));

                // Be-it-known.  This method can write to stderr if
                // abortUponModify is true.
            }

            throw ise;
        }
    }

    /**
     * @todo - Supply a version of my MemTest program which people can run
     * one time when the server can be starved of RAM, and save the available
     * RAM quantity to a text file.  We can then really crank up the buffer
     * size to make transfers really efficient.
     */

    /**
     * Return a 512-block buffer size suggestion, based on the size of what
     * needs to be read or written, and default and typical JVM constraints.
     * <P>
     * <B>Algorithm details:</B>
     * </P> <P>
     * Minimum system I want support is a J2SE system with 256M physical
     * RAM.  This system can hold a 61 MB byte array (real 1024^2 M).
     * (61MB with Java 1.6, 62MB with Java 1.4).
     * This decreases to just 60 MB with (pre-production, non-optimized)
     * HSQLDB v. 1.9 on Java 1.6.
     * Allow the user 40 MB of for data (this only corresponds to a much
     * smaller quantity of real data due to the huge overhead of Java and
     * database structures).
     * This allows 20 MB for us to use.  User can easily use more than this
     * by raising JVM settings and/or getting more PRAM or VRAM.
     * Therefore, ceiling = 20MB = 20 MB / .5 Kb = 40 k blocks
     * </P> <P>
     * We make the conservative simplification that each data file contains
     * just one huge data entry component.  This is a good estimation, since in
     * most cases, the contents of the single largest file will be many orders
     * of magnitude larger than the other files and the single block entry
     * headers.
     * </P> <P>
     * We aim for reading or writing these biggest file with 10 reads/writes.
     * In the case of READING Gzip files, there will actually be many more
     * reads than this, but that's the price you pay for smaller file size.
     * </P>
     *
     * @param files  Null array elements are permitted.  They will just be
     *               skipped by the algorithm.
     */
    static protected int generateBufferBlockValue(File[] files) {

        long maxFileSize = 0;

        for (int i = 0; i < files.length; i++) {
            if (files[i] == null) {
                continue;
            }

            if (files[i].length() > maxFileSize) {
                maxFileSize = files[i].length();
            }
        }

        int idealBlocks = (int) (maxFileSize / (10L * 512L));

        // I.e., 1/10 of the file, in units of 512 byte blocks.
        // It's fine that operations will truncate down instead of round.
        if (idealBlocks < 1) {
            return 1;
        }

        if (idealBlocks > 40 * 1024) {
            return 40 * 1024;
        }

        return idealBlocks;
    }

    /**
     * Convenience wrapper for generateBufferBlockValue(File[]).
     *
     * @see #generateBufferBlockValue(File[])
     */
    static protected int generateBufferBlockValue(File file) {
        return generateBufferBlockValue(new File[]{ file });
    }
}
