/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

package org.voltdb.processtools;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.voltcore.logging.VoltLogger;

import com.google.common.base.Preconditions;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.ChannelSftp.LsEntrySelector;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;

/**
 * Utility class that aides the copying of files to remote hosts using SFTP.
 *
 * @author stefano
 *
 */
public class SFTPSession {

    /**
     * default logger
     */
    protected static final VoltLogger cmdLog = new VoltLogger(SFTPSession.class.getName());
    /*
     * regular expression that matches file names ending in jar, so, and jnilib
     */
    private final static Pattern ARTIFACT_REGEXP = Pattern.compile("\\.(?:jar|so|jnilib)\\Z");
    /*
     * JSch SFTP channel
     */
    private ChannelSftp m_channel;
    /*
     *  remote host name
     */
    private final String m_host;
    /*
     * instance logger
     */
    private final VoltLogger m_log;

    /**
     * Instantiate a wrapper around a JSch Sftp Channel
     *
     * @param user SFTP connection user name
     * @param key SFTP connection private key
     * @param host SFTP remote host name
     * @param port SFTP port
     * @param log logger
     *
     * @throws SFTPException when it cannot connect, and establish a SFTP
     *   session
     */
    public SFTPSession( String user, String key, String host, int port, VoltLogger log) {
        Preconditions.checkArgument(
                user != null && !user.trim().isEmpty(),
                "specified empty or null user"
                );
        Preconditions.checkArgument(
                host != null && !host.trim().isEmpty(),
                "specified empty or null host"
                );
        Preconditions.checkArgument(
                port < 1,
                "specified invalid port"
                );

        if (log == null) m_log = cmdLog;
        else m_log = log;

        JSch jsch = new JSch();

        if (key != null && !key.trim().isEmpty()) {
            try {
                jsch.addIdentity(key);
            } catch (JSchException jsex) {
                throw new SFTPException("add identity file " + key, jsex);
            }
        }

        Session session;
        try {
            session = jsch.getSession(user, host, port);
            session.setTimeout(15000);
            session.setConfig("StrictHostKeyChecking", "no");
            session.setDaemonThread(true);
        } catch (JSchException jsex) {
            throw new SFTPException("create a JSch session", jsex);
        }

        try {
            session.connect();
        } catch (JSchException jsex) {
            throw new SFTPException("connect a JSch session", jsex);
        }

        ChannelSftp channel;
        try {
            channel = (ChannelSftp)session.openChannel("sftp");
        } catch (JSchException jsex) {
            throw new SFTPException("create an SFTP channel", jsex);
        }

        try {
            channel.connect();
        } catch (JSchException jsex) {
            throw new SFTPException("open an SFTP channel", jsex);
        }
        m_host = host;
        m_channel = channel;
    }

    public SFTPSession( String user, String key, String host) {
        this(user, key, host, 22, null);
    }

    public SFTPSession( String user, String key, String host, VoltLogger log) {
        this(user, key, host, 22, log);
    }

    /**
     * Given a map where their keys contain source file, and their associated
     * values contain their respective destinations files (not directories)
     * ensure that the directories used in the destination files exist (creating
     * them if needed), removes previously installed artifacts files that end
     * with .so, .jar, and .jnilib, and copy over the source files to their
     * destination.
     *
     * @param files Map where their keys contain source file, and their associated
     * values contain their respective destinations files. NB destinations must not
     * be directory names, but fully specified file names
     *
     * @throws SFTPException when an error occurs during SFTP operations performed
     *   by this method
     */
    public void install( Map<File, File> files) {
        Preconditions.checkArgument(
                files != null, "null file collection"
                );

        ensureDirectoriesExistFor(files.values());
        deletePreviouslyInstalledArtifacts(files.values());
        copyOverFiles(files);
    }

    /**
     * Given a map where their keys contain source file, and their associated
     * values contain their respective destinations files (not directories)
     * copy over the source files to their destination.
     *
     * @param files Map where their keys contain source file, and their associated
     * values contain their respective destinations files. NB destinations must not
     * be directory names, but fully specified file names
     *
     * @throws SFTPException when an error occurs during SFTP operations performed
     *   by this method
     */
    public void copyOverFiles( Map<File, File> files) {
        Preconditions.checkArgument(
                files != null, "null file collection"
                );
        Preconditions.checkState(
                m_channel != null, "stale session"
                );

        for (Map.Entry<File, File> entry: files.entrySet()) {
            String src = entry.getKey().getAbsolutePath();
            String dst = entry.getValue().getAbsolutePath();
            try {
                m_channel.put(src, dst);
                if (m_log.isDebugEnabled()) {
                    m_log.debug("put " + src + " " + dst);
                }
            } catch (SftpException sfex) {
                throw new SFTPException("put " + src + " " + dst, sfex);
            }
        }
    }

    /**
     * if found, it deletes artifacts held in the directories that
     * contain the given list of files
     *
     * @param files a collection of files
     *
     * @throws SFTPException when an error occurs during SFTP operations performed
     *   by this method
     */
    public void deletePreviouslyInstalledArtifacts( Collection<File> files) {
        Preconditions.checkArgument(
                files != null, "null file collection"
                );
        Preconditions.checkState(
                m_channel != null, "stale session"
                );

        // dedup directories containing files
        TreeSet<File> directories = new TreeSet<File>();
        for (File f: files) {
            directories.add( f.getParentFile());
        }
        // look for file artifacts that end with .so, .jar, and .jnilib
        for (File d: directories) {
            final ArrayList<String> toBeDeleted = new ArrayList<String>();
            LsEntrySelector selector = new LsEntrySelector() {
                @Override
                public int select(LsEntry entry) {
                    Matcher mtc = ARTIFACT_REGEXP.matcher(entry.getFilename());
                    SftpATTRS attr = entry.getAttrs();
                    if (mtc.find() && !attr.isDir() && !attr.isLink()) {
                        toBeDeleted.add(entry.getFilename());
                    }
                    return CONTINUE;
                }
            };
            try {
                m_channel.ls( d.getAbsolutePath(), selector);
                if (m_log.isDebugEnabled()) {
                    m_log.debug("ls " + d.getAbsolutePath());
                }
            } catch (SftpException sfex) {
                throw new SFTPException("list directory " + d, sfex);
            }
            // delete found artifacts
            for (String f: toBeDeleted) {
                File artifact = new File( d, f);
                try {
                    m_channel.rm(artifact.getAbsolutePath());
                    if (m_log.isDebugEnabled()) {
                        m_log.debug("rm " + artifact.getAbsolutePath());
                    }
                } catch (SftpException sfex) {
                    throw new SFTPException("remove artifact " + artifact, sfex);
                }
            }
        }
    }

    /**
     * Akin to mkdir -p for all directories containing the given collection of
     * files.
     *
     * @param files a collection of files
     *
     * @throws SFTPException when an error occurs during SFTP operations performed
     *   by this method
     */
    public void ensureDirectoriesExistFor( Collection<File> files) {
        Preconditions.checkArgument(
                files != null, "null file collection"
                );
        Preconditions.checkState(
                m_channel != null, "stale session"
                );

        /*
         * directory entries are sorted first by their level (/l1 < /l1/l2 < /l1/l2/l3)
         * and then their name. This loop adds all the directories that are required
         * to ensure that given list of destination files can be copied over successfully
         */
        TreeSet<DirectoryEntry> directories = new TreeSet<DirectoryEntry>();
        for (File f: files) {
           addDirectoryAncestors(f.getParentFile(), directories);
        }
        /*
         * for each entry it tests whether or not it already exists, and if it
         * does not, it creates it (akin to mkdir -p)
         */
        for (DirectoryEntry entry: directories) {
            if (!directoryExists(entry.getDirectory())) {
                try {
                    m_channel.mkdir(entry.getDirectory().getAbsolutePath());
                    if (m_log.isDebugEnabled()) {
                        m_log.debug("mkdir " + entry.getDirectory().getAbsolutePath());
                    }
                } catch (SftpException sfex) {
                    throw new SFTPException("create directory " + entry, sfex);
                }
            }
        }
        directories.clear();
    }

    /**
     * Test whether or not the given directory exists on the remote host
     *
     * @param directory directory name
     *
     * @return true if does, false if it does not
     *
     * @throws SFTPException when an error occurs during SFTP operations performed
     *   by this method
     */
    public boolean directoryExists( final File directory) {
        Preconditions.checkArgument(
                directory != null, "null directory"
                );
        Preconditions.checkState(
                m_channel != null, "stale session"
                );

        DirectoryExistsSelector selector = new DirectoryExistsSelector(directory);
        try {
            m_channel.ls(directory.getParent(), selector);
            if (m_log.isDebugEnabled()) {
                m_log.debug("ls " + directory.getParent());
            }
        } catch (SftpException sfex) {
            throw new SFTPException("list directory " + directory.getParent(), sfex);
        }
        return selector.doesExist();
    }

    /**
     * traverses up all the given directory ancestors, and adds them as directory
     * entries, designated by their level, to the given set of directory entries
     *
     * @param directory directory name
     * @param directories set of directory entries
     * @return
     */
    private int addDirectoryAncestors(
            final File directory,
            final TreeSet<DirectoryEntry> directories)
    {
        // root folder
        if (directory == null) return 0;
        // if not root recurse and return this directory level
        int level = addDirectoryAncestors(directory.getParentFile(), directories);
        // add it to the set if it is not a root folder
        if( level > 0) {
            directories.add( new DirectoryEntry(level, directory));
        }
        return level + 1;
    }

    /**
     * Terminate the SFTP session associated with this instance
     */
    public void terminate() {
        try {
            if (m_channel == null) return;
            Session session = null;
            try { session = m_channel.getSession(); } catch (Exception ignoreIt) {}
            try { m_channel.disconnect(); } catch (Exception ignoreIt) {}
            if (session != null)
                try { session.disconnect(); } catch (Exception ignoreIt) {}
        } finally {
            m_channel = null;
        }
    }

    private final static class DirectoryExistsSelector implements LsEntrySelector {
        boolean m_exists = false;
        final File m_directory;

        private DirectoryExistsSelector(final File directory) {
            this.m_directory = directory;
        }

        @Override
        public int select(LsEntry entry) {
            m_exists = m_directory.getName().equals(entry.getFilename())
                    && entry.getAttrs().isDir();
            if (m_exists) return BREAK;
            else return CONTINUE;
        }

        private boolean doesExist() {
            return m_exists;
        }
    }

    private final static class DirectoryEntry implements Comparable<DirectoryEntry> {
        final int m_level;
        final File m_directory;

        DirectoryEntry( final int level, final File directory) {
            this.m_level = level;
            this.m_directory = directory;
        }

        @SuppressWarnings("unused")
        int getLevel() {
            return m_level;
        }

        File getDirectory() {
            return m_directory;
        }

        @Override
        public int compareTo(DirectoryEntry o) {
            int cmp = this.m_level - o.m_level;
            if ( cmp == 0) cmp = this.m_directory.compareTo(o.m_directory);
            return cmp;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result
                    + ((m_directory == null) ? 0 : m_directory.hashCode());
            result = prime * result + m_level;
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            DirectoryEntry other = (DirectoryEntry) obj;
            if (m_directory == null) {
                if (other.m_directory != null)
                    return false;
            } else if (!m_directory.equals(other.m_directory))
                return false;
            if (m_level != other.m_level)
                return false;
            return true;
        }

        @Override
        public String toString() {
            return "DirectoryEntry [level=" + m_level + ", directory="
                    + m_directory + "]";
        }
    }

    public class SFTPException extends RuntimeException {

        private static final long serialVersionUID = 9135753444480123857L;

        public SFTPException() {
            super("(Host: " + m_host + ")");
        }

        public SFTPException(String message, Throwable cause) {
            super("(Host: " + m_host + ") " + message, cause);
            m_log.error(this);
        }

        public SFTPException(String message) {
            super("(Host: " + m_host + ") " + message);
            m_log.error(this);
        }

        public SFTPException(Throwable cause) {
            super("(Host: " + m_host + ")",cause);
            m_log.error(this);
        }
    }

}
