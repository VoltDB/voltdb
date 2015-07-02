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


package org.hsqldb_voltpatches.lib;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.Random;

import org.hsqldb_voltpatches.lib.java.JavaSystem;

/**
 * A collection of file management methods.<p>
 * Also provides the default FileAccess implementation
 *
 * @author Campbell Boucher-Burnet (boucherb@users dot sourceforge.net)
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @author Ocke Janssen oj@openoffice.org
 * @version 2.3.0
 * @since 1.7.2
 */
public class FileUtil implements FileAccess {

    private static FileUtil      fileUtil      = new FileUtil();
    private static FileAccessRes fileAccessRes = new FileAccessRes();

    /** Creates a new instance of FileUtil */
    FileUtil() {}

    public static FileUtil getFileUtil() {
        return fileUtil;
    }

    public static FileAccess getFileAccess(boolean isResource) {
        return isResource ? (FileAccess) fileAccessRes
                          : (FileAccess) fileUtil;
    }

    public boolean isStreamElement(java.lang.String elementName) {
        return new File(elementName).exists();
    }

    public InputStream openInputStreamElement(java.lang.String streamName)
    throws java.io.IOException {

        try {
            return new FileInputStream(new File(streamName));
        } catch (Throwable e) {
            throw JavaSystem.toIOException(e);
        }
    }

    public void createParentDirs(String filename) {
        makeParentDirectories(new File(filename));
    }

    public void removeElement(String filename) {

        if (isStreamElement(filename)) {
            delete(filename);
        }
    }

    public void renameElement(java.lang.String oldName,
                              java.lang.String newName) {
        renameWithOverwrite(oldName, newName);
    }

    public java.io.OutputStream openOutputStreamElement(
            java.lang.String streamName) throws java.io.IOException {
        return new FileOutputStream(new File(streamName));
    }

    // end of FileAccess implementation
    // a new File("...")'s path is not canonicalized, only resolved
    // and normalized (e.g. redundant separator chars removed),
    // so as of JDK 1.4.2, this is a valid test for case insensitivity,
    // at least when it is assumed that we are dealing with a configuration
    // that only needs to consider the host platform's native file system,
    // even if, unlike for File.getCanonicalPath(), (new File("a")).exists() or
    // (new File("A")).exits(), regardless of the hosting system's
    // file path case sensitivity policy.
    public final boolean fsIsIgnoreCase =
        (new File("A")).equals(new File("a"));

    // posix separator normalized to File.separator?
    // CHECKME: is this true for every file system under Java?
    public final boolean fsNormalizesPosixSeparator =
        (new File("/")).getPath().endsWith(File.separator);

    // for JDK 1.1 createTempFile
    final Random random = new Random(System.currentTimeMillis());

    /**
     * Delete the named file
     */
    public boolean delete(String filename) {
        return new File(filename).delete();
    }

    /**
     * Requests, in a JDK 1.1 compliant way, that the file or directory denoted
     * by the given abstract pathname be deleted when the virtual machine
     * terminates. <p>
     *
     * Deletion will be attempted only for JDK 1.2 and greater runtime
     * environments and only upon normal termination of the virtual
     * machine, as defined by the Java Language Specification. <p>
     *
     * Once deletion has been sucessfully requested, it is not possible to
     * cancel the request. This method should therefore be used with care. <p>
     *
     * @param f the abstract pathname of the file be deleted when the virtual
     *       machine terminates
     */
    public void deleteOnExit(File f) {
        f.deleteOnExit();
    }

    /**
     * Return true or false based on whether the named file exists.
     */
    public boolean exists(String filename) {
        return new File(filename).exists();
    }

    public boolean exists(String fileName, boolean resource, Class cla) {

        if (fileName == null || fileName.length() == 0) {
            return false;
        }

        return resource ? null != cla.getResource(fileName)
                        : FileUtil.getFileUtil().exists(fileName);
    }

    /**
     * Rename the file with oldname to newname. If a file with newname already
     * exists, it is deleted before the renaming operation proceeds.
     *
     * If a file with oldname does not exist, no file will exist after the
     * operation.
     */
    private boolean renameWithOverwrite(String oldname, String newname) {

        File file = new File(oldname);

        delete(newname);

        boolean renamed = file.renameTo(new File(newname));

        if (renamed) {
            return true;
        }

        System.gc();
        delete(newname);

        if (exists(newname)) {
            new File(newname).renameTo(new File(newDiscardFileName(newname)));
        }

        return file.renameTo(new File(newname));
    }

    /**
     * Retrieves the absolute path, given some path specification.
     *
     * @param path the path for which to retrieve the absolute path
     * @return the absolute path
     */
    public String absolutePath(String path) {
        return new File(path).getAbsolutePath();
    }

    /**
     * Retrieves the canonical file for the given file, in a
     * JDK 1.1 complaint way.
     *
     * @param f the File for which to retrieve the absolute File
     * @return the canonical File
     */
    public File canonicalFile(File f) throws IOException {
        return new File(f.getCanonicalPath());
    }

    /**
     * Retrieves the canonical file for the given path, in a
     * JDK 1.1 complaint way.
     *
     * @param path the path for which to retrieve the canonical File
     * @return the canonical File
     */
    public File canonicalFile(String path) throws IOException {
        return new File(new File(path).getCanonicalPath());
    }

    /**
     * Retrieves the canonical path for the given File, in a
     * JDK 1.1 complaint way.
     *
     * @param f the File for which to retrieve the canonical path
     * @return the canonical path
     */
    public String canonicalPath(File f) throws IOException {
        return f.getCanonicalPath();
    }

    /**
     * Retrieves the canonical path for the given path, in a
     * JDK 1.1 complaint way.
     *
     * @param path the path for which to retrieve the canonical path
     * @return the canonical path
     */
    public String canonicalPath(String path) throws IOException {
        return new File(path).getCanonicalPath();
    }

    /**
     * Retrieves the canonical path for the given path, or the absolute
     * path if attempting to retrieve the canonical path fails.
     *
     * @param path the path for which to retrieve the canonical or
     *      absolute path
     * @return the canonical or absolute path
     */
    public String canonicalOrAbsolutePath(String path) {

        try {
            return canonicalPath(path);
        } catch (Exception e) {
            return absolutePath(path);
        }
    }

    public void makeParentDirectories(File f) {

        String parent = f.getParent();

        if (parent != null) {
            new File(parent).mkdirs();
        } else {

            // workaround for jdk 1.1 bug (returns null when there is a parent)
            parent = f.getPath();

            int index = parent.lastIndexOf('/');

            if (index > 0) {
                parent = parent.substring(0, index);

                new File(parent).mkdirs();
            }
        }
    }

    public static String makeDirectories(String path) {

        try {
            File file = new File(path);

            file.mkdirs();

            return file.getCanonicalPath();
        } catch (IOException e) {
            return null;
        }
    }

    public FileAccess.FileSync getFileSync(java.io.OutputStream os)
    throws java.io.IOException {
        return new FileSync((FileOutputStream) os);
    }

    public static class FileSync implements FileAccess.FileSync {

        FileDescriptor outDescriptor;

        FileSync(FileOutputStream os) throws IOException {
            outDescriptor = os.getFD();
        }

        public void sync() throws IOException {
            outDescriptor.sync();
        }
    }

    public static class FileAccessRes implements FileAccess {

        public boolean isStreamElement(String fileName) {

            URL url = null;

            try {
                url = getClass().getResource(fileName);

                if (url == null) {
                    ClassLoader cl =
                        Thread.currentThread().getContextClassLoader();

                    if (cl != null) {
                        url = cl.getResource(fileName);
                    }
                }
            } catch (Throwable t) {

                //
            }

            return url != null;
        }

        public InputStream openInputStreamElement(final String fileName)
        throws IOException {

            InputStream fis = null;

            try {
                fis = getClass().getResourceAsStream(fileName);

                if (fis == null) {
                    ClassLoader cl =
                        Thread.currentThread().getContextClassLoader();

                    if (cl != null) {
                        fis = cl.getResourceAsStream(fileName);
                    }
                }
            } catch (Throwable t) {

                //
            } finally {
                if (fis == null) {
                    throw new FileNotFoundException(fileName);
                }
            }

            return fis;
        }

        public void createParentDirs(java.lang.String filename) {}

        public void removeElement(java.lang.String filename) {}

        public void renameElement(java.lang.String oldName,
                                  java.lang.String newName) {}

        public java.io.OutputStream openOutputStreamElement(String streamName)
        throws IOException {
            throw new IOException();
        }

        public FileAccess.FileSync getFileSync(OutputStream os)
        throws IOException {
            throw new IOException();
        }
    }

    /**
     * Utility method for user applications. Attempts to delete all the files
     * for the database as listed by the getDatabaseFileList() method. If any
     * of the current, main database files cannot be deleted, it is renamed
     * by adding a suffixe containting a hexadecimal timestamp portion and
     * the ".old" extension. Also deletes the ".tmp" directory.
     *
     * @param dbNamePath full path or name of database (without a file extension)
     * @return currently always true
     */
    public static boolean deleteOrRenameDatabaseFiles(String dbNamePath) {

        DatabaseFilenameFilter filter = new DatabaseFilenameFilter(dbNamePath);
        File[] fileList = filter.getExistingFileListInDirectory();

        for (int i = 0; i < fileList.length; i++) {
            fileList[i].delete();
        }

        File tempDir = new File(filter.canonicalFile.getPath() + ".tmp");

        if (tempDir.isDirectory()) {
            File[] tempList = tempDir.listFiles();

            for (int i = 0; i < tempList.length; i++) {
                tempList[i].delete();
            }

            tempDir.delete();
        }

        fileList = filter.getExistingMainFileSetList();

        if (fileList.length == 0) {
            return true;
        }

        System.gc();

        for (int i = 0; i < fileList.length; i++) {
            fileList[i].delete();
        }

        fileList = filter.getExistingMainFileSetList();

        for (int i = 0; i < fileList.length; i++) {
            fileList[i].renameTo(
                new File(newDiscardFileName(fileList[i].getPath())));
        }

        return true;
    }

    /**
     * Utility method for user applications. Returns a list of files that
     * currently exist for a database. The list includes current database files
     * as well as ".new", and ".old" versions of the files, plus any app logs.
     *
     * @param dbNamePath full path or name of database (without a file extension)
     */
    public static File[] getDatabaseFileList(String dbNamePath) {

        DatabaseFilenameFilter filter = new DatabaseFilenameFilter(dbNamePath);

        return filter.getExistingFileListInDirectory();
    }

    /**
     * Returns a list of existing main files for a database. The list excludes
     * non-essential files.
     *
     * @param dbNamePath full path or name of database (without a file extension)
     */
    public static File[] getDatabaseMainFileList(String dbNamePath) {

        DatabaseFilenameFilter filter = new DatabaseFilenameFilter(dbNamePath,
            false);

        return filter.getExistingFileListInDirectory();
    }

    static int discardSuffixLength = 9;

    public static String newDiscardFileName(String filename) {

        String timestamp = StringUtil.toPaddedString(
            Integer.toHexString((int) System.currentTimeMillis()),
            discardSuffixLength - 1, '0', true);
        String discardName = filename + "." + timestamp + ".old";

        return discardName;
    }

    static class DatabaseFilenameFilter implements FilenameFilter {

        String[]        suffixes      = new String[] {
            ".backup", ".properties", ".script", ".data", ".log", ".lobs",
        };
        String[]        extraSuffixes = new String[] {
            ".lck", ".sql.log", ".app.log"
        };
        private String  dbName;
        private File    parent;
        private File    canonicalFile;
        private boolean extraFiles;

        DatabaseFilenameFilter(String dbNamePath) {
            this(dbNamePath, true);
        }

        DatabaseFilenameFilter(String dbNamePath, boolean extras) {

            canonicalFile = new File(dbNamePath);

            try {
                canonicalFile = canonicalFile.getCanonicalFile();
            } catch (Exception e) {}

            dbName     = canonicalFile.getName();
            parent     = canonicalFile.getParentFile();
            extraFiles = extras;
        }

        public File[] getCompleteMainFileSetList() {

            File[] fileList = new File[suffixes.length];

            for (int i = 0; i < suffixes.length; i++) {
                fileList[i] = new File(canonicalFile.getPath() + suffixes[i]);
            }

            return fileList;
        }

        public File[] getExistingMainFileSetList() {

            File[]        fileList = getCompleteMainFileSetList();
            HsqlArrayList list     = new HsqlArrayList();

            for (int i = 0; i < fileList.length; i++) {
                if (fileList[i].exists()) {
                    list.add(fileList[i]);
                }
            }

            fileList = new File[list.size()];

            list.toArray(fileList);

            return fileList;
        }

        public File[] getExistingFileListInDirectory() {

            File[] list = parent.listFiles(this);

            return list == null ? new File[]{}
                                : list;
        }

        /**
         * Accepts all main files as well as ".new" and ".old" versions.
         */
        public boolean accept(File dir, String name) {

            if (parent.equals(dir) && name.indexOf(dbName) == 0) {
                String suffix = name.substring(dbName.length());

                if (extraFiles) {
                    for (int i = 0; i < extraSuffixes.length; i++) {
                        if (suffix.equals(extraSuffixes[i])) {
                            return true;
                        }
                    }
                }

                for (int i = 0; i < suffixes.length; i++) {
                    if (suffix.equals(suffixes[i])) {
                        return true;
                    }

                    if (!extraFiles) {
                        continue;
                    }

                    if (suffix.startsWith(suffixes[i])) {
                        if (name.endsWith(".new")) {
                            if (suffix.length() == suffixes[i].length() + 4) {
                                return true;
                            }
                        } else if (name.endsWith(".old")) {
                            if (suffix.length()
                                    == suffixes[i].length()
                                       + discardSuffixLength + 4) {
                                return true;
                            }
                        }
                    }
                }
            }

            return false;
        }
    }
}
