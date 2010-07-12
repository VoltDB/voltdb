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


package org.hsqldb_voltpatches.persist;

import java.io.RandomAccessFile;
import java.nio.channels.FileLock;

/**
 * LockFile variant that capitalizes upon the availability of
 * {@link java.nio.channels.FileLock FileLock}. <p>
 *
 * <b>Configuration</b>:<p>
 *
 * To enable POSIX mandatory file lock tagging, set the system property
 * {@link #POSIX_MANDITORY_FILELOCK_PROPERTY} true. <p>
 *
 * <hr>
 *
 * <b><em>Windows</em><sup>tm</sup> File Locking Notes</b>:<p>
 *
 * There are two major differences between Unix and Windows locking.<p>
 *
 * First, <em>Windows</em><sup>tm</sup> supports a share reservation programming
 * interface. Share reservations apply to the entire file and are specified at
 * the time a file is created or opened. A share reservation consists of a pair
 * of modes. The first is the access mode, which is how the application will
 * access the file: read, write, or read/write. The second is the access
 * that the application will deny to other applications: none, read, write,
 * or read/write. When the application attempts to open a file, the operating
 * system checks to see if there are any other open requests on the file. If so,
 * it first compares the application's access mode with the deny mode of the
 * other openers. If there is a match, then the open is denied. If not, then
 * the operating system compares the application's deny mode with the access
 * mode of the other openers. Again, if there is a match, the open is denied.<p>
 *
 * Second, there is no advisory locking under <em>Windows</em>. Whole file
 * locking, byte range locking, and share reservation locking are all
 * mandatory. <p>
 *
 * As a side note, research indicates that very few <em>Windows</em> programs
 * actually rely on byte range mandatory locking. <p>
 *
 * <b>POSIX File Locking Notes</b>:<p>
 *
 * There are three commonly found locking functions under POSIX - flock, lockf,
 * and fcntl.<p>
 *
 * These functions, while managed by the kernel, are advisory locking mechanisms
 * by default. That is, the kernel does not actually stop programs from reading,
 * writing or deleting locked files. Instead, each program must check to see if
 * a lock is in place and act accordingly (be cooperative).<p>
 *
 * This might be problematic when some programs are cooperative and others are
 * not, especially if you do not have the ability to recompile the uncooperative
 * code, yet it must be allowed to run.<p>
 *
 * As POSIX has evolved, provisions have been made to allow locks to be enforced
 * at the kernel level with mandatory locking semantics. For historical reasons
 * <sup><a href="#note1">[1]</a></sup> and for practical reasons <sup>
 * <a href="#note2">[2]</a></sup>, POSIX mandatory locking is generally
 * implemented such that it must be configured on a file-by-file basis. When a
 * program attempts to lock a file with lockf or fcntl, the kernel will prevent
 * other programs from accessing the file if and only if the file has mandatory
 * locking set (note: processes which use flock never trigger a mandatory
 * lock).<p>
 *
 * In addition, to enable mandatory locking under Linux, the target filesystem
 * device must be mounted with the <tt>mand</tt> option, for example:<p>
 *
 * <pre>
 * # mount -omand /dev/hdb3 /mnt/data
 *
 * # mount | grep /mnt/data
 * /dev/hdb3 on /mnt/data type ext3 (rw,mand)
 *</pre>
 *
 * To automate this across reboots, <tt>mand</tt> must be added to the
 * appropriate <tt>/etc/fstab</tt> entry. <p>
 *
 * Although Linux, Solaris and HP/UX are known to support kernel-enforced
 * mandatory locking semantics, some POSIX systems may provide no support at all,
 * while others may have system-specific configuration prerequisites. Some may
 * even enforce manditory locking semantics by default under certain conditions
 * while others may silently ignore mandatory locking directives or consistently
 * raise exception conditions in response to such directives. <p>
 *
 * Regardless of any wider scoped prerequisites, restrictions and behavioural
 * variations, it is generally true under POSIX that only specifically tagged
 * files will exhibit mandatory locks. <p>
 *
 * And it is generally accepted that the way to designate a file to be governed
 * by mandatory locking semantics is to set its sgid (setgroupid) bit and unset
 * its group execute bit.  This has become the commonly accepted practice
 * specifically because the combination is devoid of meaning in the regular
 * sense. <p>
 *
 * As a specific illustration, if mandatory locking is required on all
 * pre-existing files in a certain directory, then the corresponding
 * <tt>chmod</tt> invocation would be: <p>
 *
 * <pre>
 * $ chmod g+s,g-x /path/to/directory/*
 * </pre>
 *
 * <hr>
 *
 * <a name="note1"/>[1]</a>
 * The earliest versions of Unix had no way to lock files except to create
 * lock files. The idea is that two or more processes would more or less
 * simultaneously try to create a lock file in exclusive mode, via the
 * O_EXCL flag of the open( ) system call. The  operating system would
 * return success to the process that won the race, and a "file exists"
 * error to losing processes. One problem with this scheme is that it
 * relies on the winning process to remove the lock file before it exits.
 * If the process is running buggy software, this might not happen.
 * Some applications mitigate this problem by recording the process ID of
 * the winner into the contents of the lock file. A process that finds that
 * it gets a "file exists" error can then read the lock file to see if the
 * owning process is still running. <p>
 *
 * Still, lock files can be clumsy. In the 1980s, Unix versions were
 * released with file locking support built into the operating system.
 * The System V branch of Unix offered file locking via the fcntl( ) system
 * call, whereas the BSD branch provided the flock( ) system call. In both
 * cases, when the process that creates the lock dies, the lock will be
 * automatically released. <p>
 *
 * In a perfect world all processes would use and honour a cooperative, or
 * "advisory" locking scheme. However, the world isn't perfect, and there's
 * a lot of poorly written code out there. <p>
 *
 * In trying to address this problem, the designers of System V UNIX came up
 * with a "mandatory" locking scheme, whereby the operating system kernel
 * would block attempts by a process to write to a file upon which another
 * process holds a "read" -or- "shared" lock, and block attempts both to
 * read and to write a file upon which a process holds a "write " -or-
 * "exclusive" lock. <p>
 *
 * The System V mandatory locking scheme was intended to have as little
 * impact as possible on existing user code. The scheme is based on marking
 * individual files as candidates for mandatory locking, and using the
 * existing fcntl()/lockf() interface for applying locks, just as if they
 * were normal, advisory locks. <p>
 *
 * <a name="note2"/>[2]</a>
 * Even with mandatory locks, conflicts can occur. If program A reads in a
 * file, program B locks, edits, and unlocks the file, and program A then
 * writes out what it originally read, this may still be less than
 * desirable. <p>
 *
 * As well, it is generally true that nothing can override a mandatory
 * lock, not even root-owned processes.  In this situation, the best root
 * can do is kill the process that has the lock upon the file. <p>
 *
 * And this can be especially problematic if a file upon which a mandatory
 * lock exists is also available via NFS or some other remotely-accessible
 * filesystem, specifically because the entire fileserver process may be
 * forced to block until the lock is released. <p>
 *
 * Indeed, these effects of mandatory locking policy are commonly encountered in
 * the <em>Windows</em><sup>tm</sup> environment, where all locks are of the
 * mandatory style. <p>
 *
 * <hr>
 *
 * <b>Research Results</b>:<p>
 *
 * After some experimentation under JDK 1.5/6 and Linux (at least Fedora
 * Core 4), research results indicate that, after mounting the target file
 * system device using the described <tt>mand</tt> option and doing a
 * <tt>chmod g+s,g-x</tt> on the lock file before issuing a
 * <tt>FileChannel.tryLock(...)</tt>, it is still possible to delete the lock
 * file from another process while the resulting <tt>FileLock</tt> is held,
 * although mandatory locking does appear to be in effect for read/write
 * operations. <p>
 *
 * This result was actually the one anticipated, because deletion of open files
 * is generally possible under POSIX.  In turn, this is because POSIX file
 * deletion simply removes the iNode entry from the file's parent directory,
 * while any processes with open file handles continue to have access to the
 * deleted file.  Only when all file handles have been released does the space
 * occupied by the file become elligible for reuse.<p>
 *
 * In other words, under both <em>Windows</em><sup>tm</sup> and Linux (at lease
 * FC 4), it appears this class is a practically useless extension to the base.
 * Under Java for <em>Windows</em>, the act of holding a file handle open is
 * enough to produce a mandatory lock on the underlying file and also prevents
 * the file's deletion (i.e. the ultimately desired behavior occurs with or
 * without <tt>NIOLockFile</tt>), while under Java for Linux it appears
 * impossible to use NIO to produce a lock that prevents file deletion, yeilding
 * protection against inadvertent modification of the lock file practically
 * useless. <p>
 *
 * To put it another way, even after the much-heralded introduction of Java
 * NIO, without further resorting to platform-specific JNI alternatives, we
 * might as well still be back in the early 80's, because we cannot guarantee,
 * in a cross-platform manner, conditions better than offered by the earliest
 * known lock file approach, even on systems that do support mandatory file
 * locking semantics.  Instead, we must simply still trust that all software is
 * well written and cooperative in nature. <p>
 *
 * @author boucherb@users
 * @version 1.8.0.3
 * @since 1.7.2
 */
final class NIOLockFile extends LockFile {

    /** The largest lock region that can be specified with <tt>java.nio</tt>. */
    public static final long MAX_LOCK_REGION = Long.MAX_VALUE;

    /**
     * Generally, the largest lock region that can be specified for files on
     * network file systems. <p>
     *
     * From the java.nio.channels.FileLock API JavaDocs: <p>
     *
     * Some network filesystems do not implement file locks on regions
     * that extend past a certain position, often 2**30 or 2**31.
     * In general, great care should be taken when locking files that
     * reside on network filesystems.
     */
    public static final long MAX_NFS_LOCK_REGION = (1L << 30);

    /**
     * The smallest lock region that still protects the area actually used to
     * record lock information.
     */
    public static final long MIN_LOCK_REGION = LockFile.USED_REGION;

    /**
     * Whether POSIX mandatory file lock tagging is performed by default. <p>
     *
     * Under the default build, this is <tt>false</tt>, but custom
     * distributions are free to rebuild with a default <tt>true</tt>
     * value. <p>
     *
     * For <em>Windows</em> targets, distributions should build with this set
     * false.
     */
    public static final boolean POSIX_MANDITORY_FILELOCK_DEFAULT = false;

    /**
     * System property that can be used to control whether POSIX mandatory
     * file lock tagging is performed.
     */
    public static final String POSIX_MANDITORY_FILELOCK_PROPERTY =
        "hsqldb.lockfile.posix.manditory.filelock";

    /**
     * Represents an OS-enforced lock region upon this object's lock file.
     */
    private volatile FileLock fileLock;

    /**
     * Retrieves whether POSIX mandatory file lock tagging is performed. <p>
     *
     * The value is obtained in the following manner: <p>
     *
     * <ol>
     * <li>manditory is assigned <tt>POSIX_MANDITORY_FILELOCK_DEFAULT</tt>
     * <li>manditory is assigned <tt>"true".equalsIgnoreCase(
     *                       System.getProperty(POSIX_MANDITORY_FILELOCK_PROPERTY,
     *                                          manditory ? "true" : "false"));
     *    </tt>, inside a try-catch block, to silently ignore any security
     *    exception.
     * </ol>
     * @return <tt>true</tt> if POSIX mandatory file lock tagging is performed
     */
    public boolean isPosixManditoryFileLock() {

        boolean manditory = POSIX_MANDITORY_FILELOCK_DEFAULT;

        try {
            manditory = "true".equalsIgnoreCase(
                System.getProperty(
                    POSIX_MANDITORY_FILELOCK_PROPERTY, manditory ? "true"
                                                                 : "false"));
        } catch (Exception e) {}

        return manditory;
    }

    /**
     * Does work toward ensuring that a {@link #fileLock FileLock} exists upon
     * this object's lock file. <p>
     *
     * <b>POST:</b><p>
     *
     * Upon exit, if a valid <tt>fileLock</tt> could not be aquired for any
     * reason, then a best effort has also been expended toward releasing and
     * nullifying any resources obtained in the process.
     *
     * @return true if there is valid FileLock on exit, else false.
     */
    protected boolean doOptionalLockActions() {    // override
        return this.aquireFileLock();
    }

    /**
     * Peforms best effort work toward releasing this object's
     * {@link #fileLock FileLock}, nullifying it in the process.
     *
     * @return true if <tt>fileLock</tt> is released successfully, else false
     */
    protected boolean doOptionalReleaseActions() {    // override
        return this.releaseFileLock();
    }

    /**
     * Retrieves whether this object's {@link #fileLock FileLock}
     * represents a valid lock region upon this object's lock file.
     *
     * @return true if this object's {@link #fileLock FileLock} attribute is
     *      valid, else false
     * @throws SecurityException if a required system property value cannot
     *         be accessed, or if a security manager exists and its <tt>{@link
     *         java.lang.SecurityManager#checkRead}</tt> method denies
     *         read access to the lock file;
     */
    public boolean isValid() {    // override

        try {
            return super.isValid()
                   && (this.fileLock != null && this.fileLock.isValid());
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Retrieves the String value: "fileLock = " + {@link #fileLock FileLock}.<p>
     *
     * @return the String value: "fileLock = " + fileLock
     */
    protected String toStringImpl() {    // override
        return "fileLock = " + this.fileLock;
    }

    // ------------------------- Internal Implementation------------------------
    // does the real work of aquiring the FileLock
    private boolean aquireFileLock() {

        // PRE:
        //
        // raf is never null and is never closed upon entry.
        //
        // Rhetorical question to self: How does one tell if a RandomAccessFile
        // is closed, short of invoking an operation and getting an IOException
        // the says its closed (assuming you can control the Locale of the error
        // message)?
        //
        final RandomAccessFile lraf = super.raf;

        // In an ideal world, we would use a lock region back off approach,
        // starting with region MAX_LOCK_REGION, then MAX_NFS_LOCK_REGION,
        // then MIN_LOCK_REGION.
        //
        // In practice, however, it is just generally unwise to mount network
        // file system database instances.  Be warned.
        //
        // In general, it is probably also unwise to mount removable media
        // database instances that are not read-only.
        boolean success = false;

        try {
            if (this.fileLock != null) {

                // API says never throws exception, but I suspect
                // it's quite possible some research / FOSS JVMs might
                // still throw unsupported operation exceptions on certain
                // NIO classes...better to be safe than sorry.
                if (this.fileLock.isValid()) {
                    return true;
                } else {

                    // It's not valid, so releasing it is a no-op.
                    //
                    // However, we should still clean up the referenceand hope
                    // no previous complications exist (a hung FileLock in a
                    // flaky JVM) or that gc kicks in and saves the day...
                    // (unlikely, though).
                    this.releaseFileLock();
                }
            }

            if (isPosixManditoryFileLock()) {
                try {
                    Runtime.getRuntime().exec(new String[] {
                        "chmod", "g+s,g-x", file.getPath()
                    });
                } catch (Exception ex) {

                    //ex.printStackTrace();
                }
            }

            // Note: from FileChannel.tryLock(...) JavaDoc:
            //
            // @return  A lock object representing the newly-acquired lock,
            //          or <tt>null</tt> if the lock could not be acquired
            //          because another program holds an overlapping lock
            this.fileLock = lraf.getChannel().tryLock(0, MIN_LOCK_REGION,
                    false);

            // According to the API, if it's non-null, it must be valid.
            // This may not actually yet be the full truth of the matter under
            // all commonly available JVM implementations.
            // fileLock.isValid() API says it never throws, though, so
            // with fingers crossed...
            success = (this.fileLock != null && this.fileLock.isValid());
        } catch (Exception e) {}

        if (!success) {
            this.releaseFileLock();
        }

        return success;
    }

    // does the real work of releasing the FileLock
    private boolean releaseFileLock() {

        // Note:  Closing the super class RandomAccessFile has the
        //        side-effect of closing the file lock's FileChannel,
        //        so we do not deal with this here.
        boolean success = false;

        if (this.fileLock == null) {
            success = true;
        } else {
            try {
                this.fileLock.release();

                success = true;
            } catch (Exception e) {}
            finally {
                this.fileLock = null;
            }
        }

        return success;
    }
}
