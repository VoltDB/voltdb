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

public class SFTPSession {

    protected static final VoltLogger cmdLog = new VoltLogger("REMOTECMD");
    private final static Pattern ARTIFACT_REGEXP = Pattern.compile("\\.(?:jar|so|jnilib)\\Z");

    private ChannelSftp m_channel;
    private final String m_host;

    public SFTPSession( String user, String key, String host, int port) {
        Preconditions.checkArgument(
                user != null && !user.trim().isEmpty(),
                "specified empty or null user"
                );
        Preconditions.checkArgument(
                host != null && !host.trim().isEmpty(),
                "specified empty or null host"
                );

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
        this(user, key, host, 22);
    }

    public void install( Map<File, File> files) {
        Preconditions.checkArgument(
                files != null, "null file collection"
                );

        ensureDirectoriesExistFor(files.values());
        deletePreviouslyInstalledArtifacts(files.values());
        copyOverFiles(files);
    }

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
                if (cmdLog.isDebugEnabled()) {
                    cmdLog.debug("put " + src + " " + dst);
                }
            } catch (SftpException sfex) {
                throw new SFTPException("put " + src + " " + dst, sfex);
            }
        }
    }

    public void deletePreviouslyInstalledArtifacts( Collection<File> files) {
        Preconditions.checkArgument(
                files != null, "null file collection"
                );
        Preconditions.checkState(
                m_channel != null, "stale session"
                );

        TreeSet<File> directories = new TreeSet<File>();
        for (File f: files) {
            directories.add( f.getParentFile());
        }
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
                if (cmdLog.isDebugEnabled()) {
                    cmdLog.debug("ls " + d.getAbsolutePath());
                }
            } catch (SftpException sfex) {
                throw new SFTPException("list directory " + d, sfex);
            }

            for (String f: toBeDeleted) {
                File artifact = new File( d, f);
                try {
                    m_channel.rm(artifact.getAbsolutePath());
                    if (cmdLog.isDebugEnabled()) {
                        cmdLog.debug("rm " + artifact.getAbsolutePath());
                    }
                } catch (SftpException sfex) {
                    throw new SFTPException("remove artifact " + artifact, sfex);
                }
            }
        }
    }

    public void ensureDirectoriesExistFor( Collection<File> files) {
        Preconditions.checkArgument(
                files != null, "null file collection"
                );
        Preconditions.checkState(
                m_channel != null, "stale session"
                );

        TreeSet<DirectoryEntry> directories = new TreeSet<DirectoryEntry>();
        for (File f: files) {
           addDirectoryAncestors(f.getParentFile(), directories);
        }
        for (DirectoryEntry entry: directories) {
            if (!directoryExists(entry.getDirectory())) {
                try {
                    m_channel.mkdir(entry.getDirectory().getAbsolutePath());
                    if (cmdLog.isDebugEnabled()) {
                        cmdLog.debug("mkdir " + entry.getDirectory().getAbsolutePath());
                    }
                } catch (SftpException sfex) {
                    throw new SFTPException("create directory " + entry, sfex);
                }
            }
        }
        directories.clear();
    }

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
            if (cmdLog.isDebugEnabled()) {
                cmdLog.debug("ls " + directory.getParent());
            }
        } catch (SftpException sfex) {
            throw new SFTPException("list directory " + directory.getParent(), sfex);
        }
        return selector.doesExist();
    }

    private int addDirectoryAncestors( final File directory, final TreeSet<DirectoryEntry> directories) {
        if (directory == null) return 0;
        int level = addDirectoryAncestors(directory.getParentFile(), directories);
        if( level > 0) {
            directories.add( new DirectoryEntry(level, directory));
        }
        return level + 1;
    }

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
            super();
        }

        public SFTPException(String message, Throwable cause) {
            super("(Host: " + m_host + ") " + message, cause);
            cmdLog.error(this);
        }

        public SFTPException(String message) {
            super("(Host: " + m_host + ") " + message);
            cmdLog.error(this);
        }

        public SFTPException(Throwable cause) {
            super(cause);
            cmdLog.error(this);
        }
    }

}
