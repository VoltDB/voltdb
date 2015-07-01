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

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.hsqldb_voltpatches.DatabaseManager;
import org.hsqldb_voltpatches.Error;
import org.hsqldb_voltpatches.ErrorCode;
import org.hsqldb_voltpatches.HsqlDateTime;
import org.hsqldb_voltpatches.HsqlException;
import org.hsqldb_voltpatches.lib.FileUtil;
import org.hsqldb_voltpatches.lib.HsqlTimer;
import org.hsqldb_voltpatches.lib.StringConverter;

/**
 * Base cooperative file locking implementation and <tt>LockFile</tt>
 * factory. <p>
 *
 * <hr>
 *
 * Provides a facility for cooperative file locking across process boundaries
 * and isolated in-process class loader contexts. <p>
 *
 * The need is obvious for inter-process cases, but it is no less essential for
 * in-process Java clients whose classes have been loaded by isolated class
 * loaders.  This is because static fields--the conventional<a href="#note1">
 * <sup>*</sup></a> means for supporting in-process global discovery--become
 * distinct and inaccessible across Java class loader context boundaries when
 * the contexts do not share a common parent class loader or do not implement
 * normal parent class loader delegation semantics. <p>
 *
 * <a name="note1"><sup>*</sup></a>
 * The only purely in-process global discovery alternative known to the author
 * is to reflect upon objects found while traversing up the Java runtime thread
 * hierarchy.  However, this method is often subject to Java security
 * restrictions whose collective purpose is essentially dissimilar to that of
 * restrictions in effect relative to cooperative file locking requirements,
 * making it a generally unacceptable option in this context. <p>
 *
 * <hr>
 *
 * Here is the way this class presently operates: <p>
 *
 * <ol style="list-style-type: upper-latin">
 *    <li>A file with a commonly agreed-upon path is used to implement
 *        cooperative locking semantics regarding another set of files with
 *        commonly agreed-upon paths.<p>
 *
 *    <li>In particular, a background thread periodically writes a timestamp
 *        value, which acts as a heartbeat that indicates to others whether a
 *        cooperative lock condition is currently held.<p>
 *
 *    <li>In addition, a magic value is written so that it is possible to
 *        distinguish with a reasonably high degree of accuracy between the
 *        existence of a lock file and some other type of file.<p>
 *
 *    <li>The generic rules used to aquire a cooperative lock condition are
 *        as follows:<p>
 *
 *    <ol>
 *        <li>If a lock condition is already held by this object, do nothing and
 *            signify that the lock attempt was successful, else...<p>
 *
 *        <li>Poll the underlying file, using a configured maximum number of
 *            retries and a configured interval between the end of a failed
 *            poll and the beginning of the next.<p>
 *
 *        <li>For each poll:<p>
 *
 *        <ol style="list-style-type: lower-roman">
 *
 *            <li>Attempt to atomically create the underlying file if and only
 *                if it does not yet exist, exit the polling loop immediately
 *                indicating success if the attempt succeeds, else fast fail
 *                the current poll if a security exeption is thrown in response
 *                to the attempt, else...<p>
 *
 *            <li>Test if the underlying file exists, fast failing the current
 *                poll if it is impossible to determine (i.e. if a security
 *                exeption is thrown).<p>
 *
 *            <li>If the file does not exist, exit the polling loop immediately
 *                indicating success.<p>
 *
 *                This can occur only under pre-JDK 1.2 runtimes; or when the
 *                underlying platform does not correctly support {@link
 *                java.io.File#createNewFile()}; or when the underlying file is
 *                deleted within a very short time after i.), above (typically
 *                on the order of microseconds). <p>
 *
 *                If the underlying platform employs a kernel-enforced mandatory
 *                file locking blanket policy for open files (e.g. <em>Windows
 *                </em><sup>tm</sup>), then this is likely a non-issue. And if
 *                this case makes possible a race condition with another
 *                <tt>LockFile</tt> object (because the test for existence and
 *                subsequent file creation is not atomic relative to all other
 *                file system actions), it is still <em>very</em> unlikely that
 *                so unfortunate a timing will occur as to allow simultaneous
 *                lock conditions to be established. Finally, if some
 *                non-<tt>LockFile</tt> entity deleted the file, then there are
 *                much worse things to worry about, in particular that the files
 *                this object is supposed to protect are in reality subject to
 *                arbitrary external modification and deletion.<p>
 *
 *            <li>Test the file's length, fast failing the current poll if the
 *                length cannot be determined or it is not the expected
 *                value.<p>
 *
 *            <li>Open a stream to read the file's <tt>MAGIC</tt> and heartbeat
 *                timestamp values, fast failing the current poll if the stream
 *                cannot be opened.<p>
 *
 *            <li>Test the file's <tt>MAGIC</tt> value, failing the current poll
 *                if the value cannot be read or it is not the expected
 *                value.<p>
 *
 *            <li>Test the file's heartbeat timestamp value, fast failing the
 *                current poll if it cannot be read or it is less than a
 *                commonly agreed-upon value into the past (or future, to
 *                overcome a caveat observed by a patch contributor).<p>
 *        </ol>
 *        <li>If the polling phase exits with a failure indication, then one or
 *            more of the following cases must have been true at every poll
 *            iteration: <p>
 *
 *            <ul>
 *               <li>The file had the wrong length or <tt>MAGIC</tt> value (was
 *                   not an HSQLDB lock file).
 *
 *               <li>The file was deleted externally after a poll's initial
 *                   test for existence and recreated at some point before
 *                   the next poll's initial test for existence.
 *
 *               <li>An incompatible OS-enforced security restriction was in
 *                   effect.
 *
 *               <li>An incompatible Java-enforced security restriction was
 *                   in effect.
 *
 *               <li>The target file system media was effectively inaccessible.

 *               <li>A cooperative lock condition was held by some other
 *                   <tt>LockFile</tt>.
 *
 *               <li>A kernel-enforced manditory or advisory file lock was held.
 *            </ul> <p>
 *
 *            In this case, signify failure indicating the last encountered
 *            reason, else...<p>
 *
 *        <li>Open the file for reading and writing, write the magic value and
 *            an initial heartbeat timestamp, schedule a periodic heartbeat
 *            timestamp writer task and signify success.<p>
 *    </ol>
 *    <li>The generic rules used to release a cooperative lock condition are:<p>
 *    <ol>
 *        <li>If a lock condition is not currently held, do nothing and signify
 *            success, else...<p>
 *
 *        <li>A lock condition is currently held by this object, so try to
 *            release it. <p>
 *
 *            By default, releasing the lock condition consists of closing and
 *            nullifying any objects that have a file descriptor open on the
 *            lock file, cancelling the periodic heartbeat timestamp writer
 *            task and deleting the lock file. If the release occurs without
 *            raising an exception, signify success, else signify that the
 *            release attempt <em>might</em> have failed. <p>
 *    </ol>
 * </ol> <p>
 *
 * <hr>
 *
 * Additionally, {@link #doOptionalLockActions() doOptionalLockActions()} and
 * {@link #doOptionalReleaseActions() doOptionalReleaseActions()} are invoked
 * during lock and release attempts, respectively.  This enables integration of
 * extended lock and release strategies based on subclassing. Subclass
 * availability is automatically detected and exposed by the factory method
 * {@link #newLockFile newLockFile()}.<p>
 *
 * In particular, if {@link #USE_NIO_FILELOCK_PROPERTY} is true and the required
 * classes are available at static initialization, then <tt>newLockFile()</tt>
 * produces {@link NIOLockFile NIOLockFile} instances.<p>
 *
 * When <tt>NIOLockFile</tt> instances are produced, then it is possible that
 * true kernel-enforced advisory or manditory file locking is used to protect
 * the underlying lock file from inadvertent modification (and possibly even
 * from deletion, including deletion by the system superuser).
 *
 * Otherwise, <tt>newLockFile()</tt> produces vanilla <tt>LockFile</tt>
 * instances, which exhibit just the elementary cooperative locking behavior on
 * platforms that do not, by default, implement kernel-enforced manditory
 * locking for open files. <p>
 *
 * At this point, it must be noted that not every target platform upon which
 * Java can run actually provides true kernel-enforced manditory (or even
 * advisory) file locking. Indeed, even when a target platform <em>does</em>
 * provide such locking guarantees for local file systems, it may not be able
 * to do so for network file systems, or it may only be able to do so safely
 * (or at all) with certain restrictions. Further, external system configuration
 * may be a prerequisite to enable manditory locking on systems that support it
 * but employ advisory locking by default. <p>
 *
 * In recognition of these facts, the official Java NIO package specification
 * explicitly states basically the same information. What is unfortunate,
 * however, is that no capabilities API is yet provided as part of the package.
 * What is even more unfortunate is that without something like a capabilities
 * API, it is impossible for an implementation to indicate or clients to
 * distiguish between simple lack of platform support and cases involving
 * immature Java runtimes that do not fully or correctly implement all NIO
 * features (and hence may throw exceptions at unexpected times or in places
 * where the API specification indicates none can be thrown).<p>
 *
 * It is for the preceding reasons that, as of HSQLDB 1.8.0.3,
 * <tt>FileLock</tt>'s use of Java NIO has been made a purely optional feature.
 * Previous to HSQLDB 1.8.0.3, if NIO was detected available, used to create a
 * <tt>FileLock</tt> and failed, then the enclosing cooperative lock attempt
 * failed also, despite the fact that a vanilla locking approach could
 * succeed. <p>
 *
 * <b>Polling Configuration</b>:<p>
 *
 * Although the {@link #HEARTBEAT_INTERVAL} and default polling values may
 * seem quite conservative, they are the result of ongoing research into
 * generally reasonable concerns regarding normal timing and resource
 * availability fluctuations experienced frequently under most, if not all
 * operating systems. <p>
 *
 * Regardless, flexibility is almost always a good thing, so this class is
 * designed to allow polling interval and retry count values to be configured
 * at run-time. <p>
 *
 * At present, this can be done at any time by setting the system properties
 * whose names are  {@link #POLL_RETRIES_PROPERTY} and {@link
 * #POLL_INTERVAL_PROPERTY}. <p>
 *
 * Some consideration has also been given to modifying the polling scheme so
 * that run-time configuration of the HEARTBEAT_INTERVAL is possible.  For now,
 * however, this option has been rejected due to the relative complexity of
 * guaranteeing acceptably safe, deterministic behaviour.  On the other hand,
 * if it can be guaranteed that certain site invariants hold (in particular,
 * that only one version of the hsqldb jar will ever be used to open database
 * instances at the site) and it is desirable or required to experiment with
 * a lower interval, then it is recommended for now simply to recompile the
 * jar using a different value in the static field assignment.  Note that great
 * care should be taken to avoid assigning too low a value, or else it may
 * become possible that even very short-lived timing and resource availability
 * fluctuations will cause incorrect operation of this class. <p>
 *
 * <b>NIO Configuration</b>:<p>
 *
 * Starting with 1.8.0.3, NIO-enhanced file lock attempts are turned off by
 * default. The general reasons for this are discussed above.  Anyone interested
 * in the reading the detailed research notes should refer to the overview of
 * {@link NIOLockFile}. If, after reviewing the notes and the capabilities of
 * the intended target platform, one should still wish to enable NIO-enhanced
 * file lock attempts, it can be done by setting the system property {@link
 * #USE_NIO_FILELOCK_PROPERTY} true at JVM startup (for example, by using a
 * command-line <tt>-D&lt;property-name&gt;=true</tt> directive). Be aware that
 * the system property value is read only once, in the static initializer block
 * for this class. <p>
 *
 * <b>Design Notes</b>:<p>
 *
 * First, it should be noted that no thread synchronization occurs in
 * this class.  Primarily, this is because the standard entry point,
 * {@link #newLockFileLock(String)}, is always called from within a block
 * synchronized upon an HSQLDB Database instance.  If this class is to be used
 * elsewhere and it could be accessed concurrently, then access should be
 * synchronized on an appropriate monitor.  That said, certain members of this
 * class have been declared volatile to minimize possibility of inconsistent
 * views under concurrent read-only access. <p>
 *
 * Second, to the limit of the author's present understanding, the
 * implementation details of this class represent a good comprimse under varying
 * and generally uncontrollable JVM, OS and hardware platform
 * limitations/capabilites, as well as under usability considerations and
 * external security or operating constraints that may need to be imposed.<p>
 *
 * Alternate approaches that have been considered and rejected for now
 * include: <p>
 *
 * <ul>
 *    <li>Socket-based locks (with/without broadcast protocol)
 *    <li>Pure NIO locking
 *    <li>Simple lock file (no heartbeat or polling)
 *    <li>JNI and native configuration alternatives
 * </ul>
 *
 * Of course, discussions involving and patches implementing improvements
 * or better alternatives are always welcome. <p>
 *
 * As a final note and sign post for developers starting to work with
 * Java NIO: <p>
 *
 * A separate <tt>NIOLockFile</tt> descendent exists specifically
 * because it was determined though experimenatation that
 * <tt>java.nio.channels.FileLock</tt> does not always exhibit the correct
 * or desired behaviour under reflective method invocation. That is, it was
 * discovered that under some operating system/JVM combinations, after calling
 * <tt>FileLock.release()</tt> via a reflective method invocation, the lock is
 * not released properly, deletion of the lock file is not possible even from
 * the owning object (this) and it is impossible for other <tt>LockFile</tt>
 * instances, other in-process objects or other processes to successfully obtain
 * a lock condition on the lock file, despite the fact that the
 * <tt>FileLock</tt> object reports that its lock is invalid (was released
 * successfully). Frustratingly, this condition appears to persist until full
 * exit of the process hosting the JVM in which the <tt>FileLock.tryLock()</tt>
 * method was reflectively invoked. <p>
 *
 * To solve this, the original <tt>LockFile</tt> class was split in two and
 * instead of reflective method invocation, subclass instantiation is now
 * performed at the level of the <tt>newLockFile()</tt> factory method.
 * Similarly, the HSQLDB ANT build script now detects the presence or abscence
 * of JDK 1.4+ features such as java.nio and only attempts to build and deploy
 * <tt>NIOLockFile</tt> to the hsqldb.jar if such features are reported
 * present. <p>
 *
 * @author Campbell Boucher-Burnett (boucherb@users dot sourceforge.net)
 * @version 1.8.0.3
 * @since 1.7.2
 */
public class LockFile {

    /**
     * Arbitary period, in milliseconds, at which heartbeat timestamps are
     * written to this object's lock file. <p>
     *
     * This value was selected to be very conservative, just in case timing
     * jitters are experienced on the order introduced by brief network
     * partitions, accidentally removed media and transient high load
     * CPU bursts.
     */
    public static final long HEARTBEAT_INTERVAL = 10000;

    /**
     * {@link #HEARTBEAT_INTERVAL} + 100. <p>
     *
     * Interval used by {@link #checkHeartbeat(boolean) checkHeartbeat} to
     * test whether the timestamp in the underlying lock file is live or stale.
     * Padding added in the hope of reducing potential timing jitter issues
     * under the polling scheme introduced in 1.8.0.3
     */
    public static final long HEARTBEAT_INTERVAL_PADDED = 10100;

    /**
     * Value written at the beginning of an HSQLDB lock file to distinguish it
     * from other file types. <p>
     *
     * The value is the octet sequence: {0x48, 0x53, 0x51, 0x4c, 0x4c, 0x4f,
     * 0x43, 0x4b}, which is the ASCII sequence {'H', 'S', 'Q', 'L', 'L', 'O',
     * 'C', 'K'}. <p>
     *
     * <b>Design Note</b>: <p>
     *
     * "HSQLLOCK".getBytes() is no longer used because it is dependent on the
     * underlying platform's default character set.
     */
    protected static final byte[] MAGIC = {
        0x48, 0x53, 0x51, 0x4c, 0x4c, 0x4f, 0x43, 0x4b
    };

    /**
     * Size, in bytes, of the region at the beginning of a lock file that is
     * actually used to record lock information. <p>
     *
     * Value is currently MAGIC.length + sizeof(long) = (8 + 8) = 16
     */
    public static final int USED_REGION = 16;

    /**
     * Number of retries used by default in {@link #pollHeartbeat()
     * pollHeartbeat}.
     */
    public static final int POLL_RETRIES_DEFAULT = 10;

    /**
     * System property that can be used to override the default number of
     * heartbeat poll retries.
     */
    public static final String POLL_RETRIES_PROPERTY =
        "hsqldb.lockfile.poll.retries";

    /**
     * System property that can be used to override the default number of
     * milliseconds between each heartbeat poll retry.
     */
    public static final String POLL_INTERVAL_PROPERTY =
        "hsqldb.lockfile.poll.interval";

    /** Whether <tt>java.nio</tt> file locking is attempted by default. */
    public static final boolean USE_NIO_FILELOCK_DEFAULT = false;

    /**
     * System property that can be used to control whether nio file locking is
     * attempted.
     */
    public static final String USE_NIO_FILELOCK_PROPERTY =
        "hsqldb.lockfile.nio.filelock";

    /**
     * Statically computed indication of <tt>java.nio.channels.FileLock</tt>
     * runtime availability. <p>
     *
     * <b>Design Note</b>:<p>
     *
     * Computed in a static initializer block.  Will be <tt>false</tt> if
     * <tt>USE_NIO_FILELOCK_PROPERTY</tt> is <tt>false</tt> at static
     * initialization, regardless of actual availability.
     */
    public static final boolean NIO_FILELOCK_AVAILABLE;

    /**
     * Statically computed reference to the <tt>NIOLockFile</tt> class. <p>
     *
     * <b>Design Note</b>:<p>
     *
     * Computed in a static initializer block.  Will be <tt>null</tt> if
     * <tt>USE_NIO_FILELOCK_PROPERTY</tt> is <tt>false</tt> at static
     * initialization, regardless of actual availability.
     */
    public static final Class NIO_LOCKFILE_CLASS;

    /**
     * The timed scheduler with which to register this object's
     * heartbeat task.
     */
    protected static final HsqlTimer timer = DatabaseManager.getTimer();

    // This static initializer comes last, since it references a subclass
    //
    // That is, it is best practice to ensure the static fields of this class
    // are all initialized before referecing a subclass whose static
    // field initializtion may in turn reference static fields in this class.
    static {
        synchronized (LockFile.class) {
            boolean use = USE_NIO_FILELOCK_DEFAULT;

            try {
                use = "true".equalsIgnoreCase(
                    System.getProperty(USE_NIO_FILELOCK_PROPERTY, use ? "true"
                                                                      : "false"));
            } catch (Exception e) {}

            boolean avail = false;
            Class   clazz = null;

            if (use) {
                try {
                    Class.forName("java.nio.channels.FileLock");

                    clazz = Class.forName("org.hsqldb_voltpatches.persist.NIOLockFile");
                    avail = true;
                } catch (Exception e) {}
            }

            NIO_FILELOCK_AVAILABLE = avail;
            NIO_LOCKFILE_CLASS     = clazz;
        }
    }

    /**
     * Canonical reference to this object's lock file. <p>
     *
     * <b>Design Note</b>:<p>
     *
     * Should really be final, but finality makes reflective construction
     * and adherence to desirable <tt>LockFile</tt> factory method event
     * sequence more complicated.
     */
    protected File file;

    /**
     * Cached value of the lock file's canonical path
     *
     * <b>Design Note</b>:<p>
     *
     * Should really be final, but finality makes reflective construction
     * and adherence to desirable <tt>LockFile</tt> factory method event
     * sequence much more complicated.
     */
    private String cpath;

    /**
     * A <tt>RandomAccessFile</tt> constructed from this object's canonical file
     * reference. <p>
     *
     * This <tt>RandomAccessFile</tt> is used to periodically write out the
     * heartbeat timestamp to this object's lock file.
     */
    protected volatile RandomAccessFile raf;

    /** Indicates presence or absence of the cooperative lock condition. */
    protected volatile boolean locked;

    /** Opaque reference to this object's heatbeat task. */
    private volatile Object timerTask;

    /**
     * Retrieves a new <tt>NIOLockFile</tt>, or <tt>null</tt> if not available
     * under the current runtime environment.
     *
     * @return a new <tt>NIOLockFile</tt>, or <tt>null</tt> if not available
     *      under the current runtime environment
     */
    private static final LockFile newNIOLockFile() {

        if (NIO_FILELOCK_AVAILABLE && NIO_LOCKFILE_CLASS != null) {
            try {
                return (LockFile) NIO_LOCKFILE_CLASS.newInstance();
            } catch (Exception e) {

                // e.printStackTrace()
            }
        }

        return null;
    }

    /**
     * To allow subclassing without exposing a public constructor.
     */
    protected LockFile() {}

    /**
     * Retrieves a <tt>LockFile</tt> instance, initialized with a <tt>File</tt>
     * object whose path is the canonical form of the one specified by the
     * given <tt>path</tt> argument. <p>
     *
     * The resulting <tt>LockFile</tt> instance does not yet hold a lock
     * condition on the file with the given path, nor does it guarantee that the
     * file pre-exists or is created.
     *
     * However, upon successful execution, it is guaranteed that all required
     * parent directories have been created and that the underlying platform has
     * verified the specified path is legal on the file system of the underlying
     * storage partition.
     *
     * @return a <tt>LockFile</tt> instance initialized with a <tt>File</tt>
     *         object whose path is the one specified by the given <tt>path</tt>
     *         argument.
     * @param path the path of the <tt>File</tt> object with which the retrieved
     *        <tt>LockFile</tt> object is to be initialized
     * @throws FileCanonicalizationException if an I/O error occurs upon
     *         canonicalization of the given path, which is possible because
     *         it may be illegal on the runtime file system or because
     *         construction of the canonical path name may require native file
     *         system queries
     * @throws FileSecurityException if a required system property value cannot
     *         be accessed, or if a security manager exists and its <tt>{@link
     *         java.lang.SecurityManager#checkRead}</tt> method denies read
     *         access to the file; or if its <tt>{@link
     *         java.lang.SecurityManager#checkRead(java.lang.String)}</tt>
     *         method does not permit verification of the existence of all
     *         necessary parent directories; or if the <tt>{@link
     *         java.lang.SecurityManager#checkWrite(java.lang.String)}</tt>
     *         method does not permit all necessary parent directories to be
     *         created
     */
    public static final LockFile newLockFile(final String path)
    throws FileCanonicalizationException, FileSecurityException {

        LockFile lockFile = newNIOLockFile();

        if (lockFile == null) {
            lockFile = new LockFile();
        }

        lockFile.setPath(path);

        return lockFile;
    }

    /**
     * {@link org.hsqldb_voltpatches.persist.Logger#acquireLock(java.lang.String)}
     * delegate.<p>
     *
     * Retrieves a new <tt>LockFile</tt> object holding a cooperative lock
     * condition upon the file with the given path, appended with the
     * extension '.lck'. <p>
     *
     * @param path of the lock file, to which will be appended '.lck'
     * @throws org.hsqldb_voltpatches.HsqlException if the lock condition cannot
     *      be obtained for any reason.
     * @return a new <tt>LockFile</tt> object holding a cooperative lock
     *      condition upon the file with the given path, appended with the
     *      extension '.lck'
     */
    public static final LockFile newLockFileLock(final String path)
    throws HsqlException {

        LockFile lockFile = null;

        try {
            lockFile = LockFile.newLockFile(path + ".lck");
        } catch (LockFile.BaseException e) {
            throw Error.error(ErrorCode.LOCK_FILE_ACQUISITION_FAILURE,
                              e.getMessage());
        }

        boolean locked = false;

        try {
            locked = lockFile.tryLock();
        } catch (LockFile.BaseException e) {
            throw Error.error(ErrorCode.LOCK_FILE_ACQUISITION_FAILURE,
                              e.getMessage());
        }

        // Paranoia mode: In theory, this case can't happen, given the way
        // tryLock now works; by all current understanding of the involved API
        // contracts, an exception will always be thrown instead by the code
        // above.
        if (!locked) {
            throw Error.error(ErrorCode.LOCK_FILE_ACQUISITION_FAILURE,
                              lockFile.toString());
        }

        return lockFile;
    }

    /**
     * Checks whether the underlying file is an HSQLDB lock file and, if so,
     * whether its heartbeat timestamp is live (is, as far as can be known,
     * presumably in use by another <tt>LockFile</tt> instance) or stale. <p>
     *
     * The check conforms to the following rules: <p>
     *
     * <ol>
     * <li>If the parameter <tt>withCreateNewFile</tt> is true, {@link
     *     java.io.File#createNewFile()} is available and its invocation
     *     upon this object's <tt>file</tt> object indicates the underlying
     *     file was atomically created if and only if it did not yet exist,
     *     then return immediately (we have won the <em>race</em> to establish
     *     a lock file). <p>
     *
     * <li>Test again if the file exists, returning immediately if it does not
     *     (there's no file and hence no heartbeat to check). <p>
     *
     *     An immediate return can occur here only under pre-JDK 1.2 runtimes;
     *     or when the underlying platform does not correctly support
     *     <tt>File.createNewFile()</tt>; or when the underlying file is deleted
     *     within a very short time after i.), above (typically on the order of
     *     microseconds). <p>
     *
     *     If the underlying platform employs a kernel-enforced mandatory file
     *     locking blanket policy for open files (e.g. <em>Windows</em><sup>tm
     *     </sup>), then this is likely a non-issue. And if this case makes
     *     possible a race condition with another <tt>LockFile</tt> object
     *     (because the test for existence yeilds false and subsequent file
     *     creation is not atomic relative to all other file system actions), it
     *     is still <em>very</em> unlikely that so unfortunate a timing will
     *     occur as to allow simultaneous lock conditions to be established.
     *     Finally, if some non-<tt>LockFile</tt> entity deleted the file, then
     *     there are much worse things to worry about, in particular that the
     *     files this object is supposed to protect are in reality subject to
     *     arbitrary external modification and deletion by some uncooperative
     *     process.<p>
     *
     * <li>If a Java security exception is thrown while testing for existence,
     *     it is rethrown as a <tt>FileSecurityException</tt>.
     *
     * <li>Read the file's length.
     *
     * <li>If a Java security exception is thrown reading length, it is rethrown
     *     as a <tt>FileSecurityException</tt> (it <em>is</em> possible somebody
     *     concurrently refreshed the system Policy in the interim).
     *
     * <li>If the file does not have the expected length, a
     *     <tt>WrongLengthException</tt> is thrown (we're trying to check
     *     something that is not an HSQLDB lock file).
     *
     * <li>Open an input steam to read the file's <tt>MAGIC</tt> and heartbeat
     *     timestamp values.
     *
     * <li>If a file not found exception is thrown above, it is rethrown as an
     *     <tt>UnexpectedFileNotFoundException</tt> (we've already tested for
     *     existence).
     *
     * <li>If a Java security exception is thrown above, it is rethrown as a
     *     <tt>FileSecurityException</tt> (it <em>is</em> possible somebody
     *     concurrently refreshed the system Policy in the interim).
     *
     * <li>Read the <tt>MAGIC</tt> value.
     *
     * <li>If an end of file exepction is thrown above, it is rethrown as an
     *     <tt>UnexpectedEndOfFileException</tt> (we've already tested the
     *     length... did someone truncate the file in the interim?).
     *
     * <li>If an I/O exception is thrown, it is rethrown as an
     *     <tt>UnexpectedFileIOException</tt> (we've already tested for
     *     existence, length and successfully opened a stream...did someone,
     *     for example, force unmount or physically remove the underlying device
     *     in the interim?)
     *
     * <li>If the value read in does not match the expected <tt>MAGIC</tt> value,
     *     a <tt>WrongMagicException</tt> is thrown (we're trying to check
     *     something that is not an HSQLDB lock file).
     *
     * <li>Read the heartbeat timestamp.
     *
     * <li>If a Java security exception is thrown above, it is rethrown as a
     *     <tt>FileSecurityException</tt> (it <em>is</em> possible somebody
     *     concurrently refreshed the system Policy in the interim).
     *
     * <li>If an end of file exection is thrown above, it is rethrown as an
     *     <tt>UnexpectedEndOfFileException</tt> (we've already tested the
     *     length... did someone truncate the file in the interim?).
     *
     * <li>If an I/O exception is thrown, it is rethrown as an
     *     <tt>UnexpectedFileIOException</tt> (we've already tested for
     *     existence, length and successfully opened a stream...did someone,
     *     for example, force unmount or physically remove the underlying device
     *     in the interim?)
     *
     * <li>If the timestamp read in is less than or equal to
     *     {@link #HEARTBEAT_INTERVAL_PADDED} milliseconds into the past or
     *     future, then a <tt>LockHeldExternallyException</tt> is thrown.
     *
     * <li>Otherwise, this method simply returns.
     * </ol>
     *
     * @param withCreateNewFile if <tt>true</tt>, attempt to employ
     *      <tt>File.createNewFile()</tt> as part of the check so as to
     *      eliminate potential race conditions when establising a new
     *      lock file
     * @throws FileSecurityException if the check fails due to a Java
     *      security permission check failure
     * @throws LockHeldExternallyException if it is determined that the
     *      file's heartbeat timestamp is less than
     *      <tt>HEARTBEAT_INTERVAL_PADDED</tt> into the past (or future)
     * @throws UnexpectedEndOfFileException if an <tt>EOFExceoption</tt> is
     *      thrown while reading either the magic or heartbeat timestamp values
     * @throws UnexpectedFileIOException if an <tt>IOException</tt> other than
     *      <tt>EOFException</tt> is thrown while reading either the magic or
     *      heartbeat timestamp values
     * @throws UnexpectedFileNotFoundException if a
     *      <tt>FileNotFoundException</tt> is thrown while attempting to open a
     *      stream to read the underlying file's magic and heartbeat timestamp
     *      values
     * @throws WrongLengthException if it is determined that the length
     *      of the file does not equal {@link #USED_REGION}
     * @throws WrongMagicException if it is determined that the file's
     *      content does not start with {@link #MAGIC}.
     */
    private final void checkHeartbeat(boolean withCreateNewFile)
    throws LockFile.FileSecurityException,
           LockFile.LockHeldExternallyException,
           LockFile.UnexpectedEndOfFileException,
           LockFile.UnexpectedFileIOException,
           LockFile.UnexpectedFileNotFoundException,
           LockFile.WrongLengthException, LockFile.WrongMagicException {

        long now;
        long lastHeartbeat;
        long length = 0;

        try {
            if (withCreateNewFile) {
                try {
                    file.createNewFile();

                    return;
                } catch (IOException ioe) {}
            }

            if (!file.exists()) {
                return;
            }

            length = file.length();
        } catch (SecurityException se) {
            throw new FileSecurityException(this, "checkHeartbeat", se);
        }

        if (length != USED_REGION) {
            throw new WrongLengthException(this, "checkHeartbeat", length);
        }

        // Compute the current wall clock time *first* to reduce possibility
        // of unwanted time dilation effects introduced, for example,
        // by intervening thread or process context switches under CPU
        // bursts.
        //
        // Example:
        //
        // Say currentTimeMillis is actually somewhere in (-0.5 and 0.5]
        // and another LockFile concurrently writes a 0-valued heartbeat
        // timestamp.
        //
        // Then, if readHeartbeat comes first here, happens to 'win the race
        // condition' (reads the previous heartbeat: -10,000) and an intervening
        // switch causes greater than ~0.5 millisecond elapsed time to
        // be experienced between readHeartbeat and currentTimeMillis, then
        // currentTimeMillis will be computed as n (n > 0), and (now -
        // lastHearbeat) will be HEARTBEAT_INTERVAL + n, instead of
        // HEARTBEAT_INTERVAL.
        //
        // Now, let n be greater than (HEARTBEAT_INTERVAL_PADDED -
        // HEARTBEAT_INTERVAL).
        //
        // Then the check will succeed, although it should fail.
        //
        // On the other hand, if currentTimeMillis is computed first, the
        // worst than can happen is a false positive indication that
        // the read heartbeat timestamp value was written by a live LockFile
        // instance.
        //
        now           = System.currentTimeMillis();
        lastHeartbeat = readHeartbeat();

        // Using padded interval to further reduce corner case effects,
        // now that heartbeat polling is in effect.
        //
        // Basically, it is absolutely essential to fail when a lock really is
        // still held elsewhere, so it is OK to fail on corner cases where
        // the last written heartbeat is very close to HEARTBEAT_INTERVAL
        // in the past and it is possible that timing jitters make it uncertain
        // whether the lock really is still held.
        if (Math.abs(now - lastHeartbeat) <= (HEARTBEAT_INTERVAL_PADDED)) {
            throw new LockHeldExternallyException(this, "checkHeartbeat", now,
                                                  lastHeartbeat);
        }
    }

    /**
     * Closes this object's {@link #raf RandomAccessFile}. <p>
     *
     * As a side-effect, the associated <tt>FileChannel</tt> object, if any,
     * is closed as well.
     *
     * @throws UnexpectedFileIOException if an <tt>IOException</tt> is thrown
     */
    private final void closeRAF() throws LockFile.UnexpectedFileIOException {

        if (raf != null) {
            try {
                raf.close();
            } catch (IOException ex) {
                throw new UnexpectedFileIOException(this, "closeRAF", ex);
            } finally {
                raf = null;
            }
        }
    }

    /**
     * Provides any optional locking actions for the {@link #tryLock()
     * tryLock()} template method. <p>
     *
     * Descendents are free to provide additional functionality here,
     * using the following rules: <p>
     *
     * <b>PRE:</b><p>
     *
     * This method is called only from <tt>tryLock()</tt> and it is called if
     * and only if <tt>tryLock()</tt> successfully invokes
     * <tt>pollHeartbeat()</tt> and <tt>openRAF()</tt> first. <p>
     *
     * From this, it can be inferred that upon entry: <p>
     *
     * <ol>
     * <li><tt>locked == false</tt>.
     * <li><tt>raf</tt> is a non-null instance that can be used to get a
     *     <tt>FileChannel</tt> instance, if desired.
     * <li>the underlying file either did not exist before invoking
     *     <tt>openRAF()</tt> or it was a valid but stale HSQLDB lock file
     *     because it:
     *
     *     <ol style="list-style-type: lower-roman">
     *     <li>did exist,
     *     <li>was readable on <tt>USED_REGION</tt>,
     *     <li>had the expected length and <tt>MAGIC</tt> value and
     *     <li>had a stale heartbeat timestamp value.
     *     </ol>
     * </ol> <p>
     *
     * Further, it can be assumed that this object's heatbeat task is definitely
     * cancelled and/or has never been scheduled at this point, so whatever
     * timestamp is recorded in the lock file, if it did pre-exist, was written
     * by a different <tt>LockFile</tt> instance or as the result of a previous,
     * successful <tt>tryLock()</tt> invocation upon this <tt>LockFile</tt>
     * instance. <p>
     *
     * Finally, it is important that this method does not rethrow any exceptions
     * it encounters as unchecked exceptions to the calling context. <p>
     *
     * <b>POST:</b><p>
     *
     * This method should return <tt>false</tt> if optional locking work is not
     * performed or if it fails, else <tt>true</tt>. <p>
     *
     * In general, if optional locking work fails, then any resources
     * acquired in the process should be freed before this method returns.
     * In this way, the surrounding implementation can take advantage of a
     * <tt>false</tt> return value to avoid calling {@link
     * #doOptionalReleaseActions() doOptionalReleaseActions()} as part of the
     * {@link #tryRelease() tryRelease()} method. <p>
     *
     * <b>Note:</b><p>
     *
     * The default implementation does nothing and always returns
     * <tt>false</tt>. <p>
     *
     * @return <tt>true</tt> if optional lock actions are performed and they
     *      succeed, else <tt>false</tt>
     */
    protected boolean doOptionalLockActions() {
        return false;
    }

    /**
     * Provides any optional release actions for the {@link #tryRelease()
     * tryRelease()} template method. <p>
     *
     * <b>PRE:</b> <p>
     *
     * It is important that this method does not rethrow any exceptions
     * it encounters as unchecked exceptions to the calling context. <p>
     *
     * <b>POST:</b> <p>
     *
     * In general, <tt>false</tt> should be returned if optional locking work
     * is not performed or if it fails, else <tt>true</tt>.  However, the return
     * value is currenly treated  as purely informative. <p>
     *
     * <b>Note:</b> <p>
     *
     * The default implementation does nothing and always returns false. <p>
     *
     * @return <tt>true</tt> if optional release actions are performed and they
     *      succeed, else <tt>false</tt>
     */
    protected boolean doOptionalReleaseActions() {
        return false;
    }

    /**
     * Initializes this object with a <tt>File</tt> object whose path has the
     * canonical form of the given <tt>path</tt> argument. <p>
     *
     *  <b>PRE</b>:<p>
     *
     * <ol>
     *    <li>This method is called once and <em>only</em> once per
     *        <tt>Lockfile</tt> instance.
     *
     *    <li>It is <em>always</em> the first method called after
     *        <tt>LockFile</tt> construction
     *
     *    <li>The supplied <tt>path</tt> argument is <em>never</em>
     *        <tt>null</tt>.
     * </ol>
     *
     * @param path the abstract path representing the file this object is to
     *        use as its lock file
     * @throws FileCanonicalizationException if an I/O error occurs upon
     *         canonicalization of the given path, which is possible because
     *         the given path may be illegal on the runtime file system or
     *         because construction of the canonical pathname may require
     *         native file system queries
     * @throws FileSecurityException if a required system property value cannot
     *         be accessed, or if a Java security manager exists and its
     *        <tt>{@link java.lang.SecurityManager#checkRead}</tt> method denies
     *         read access to the file; or if its <tt>{@link
     *         java.lang.SecurityManager#checkRead(java.lang.String)}</tt>
     *         method does not permit verification of the existence of
     *         all necessary parent directories; or if
     *         its <tt>{@link
     *         java.lang.SecurityManager#checkWrite(java.lang.String)}</tt>
     *         method does not permit all necessary parent directories to be
     *         created
     */
    private final void setPath(String path)
    throws LockFile.FileCanonicalizationException,
           LockFile.FileSecurityException {

        // Should at least be absolutized for reporting purposes, just in case
        // a security or canonicalization exception gets thrown.
        path = FileUtil.getDefaultInstance().canonicalOrAbsolutePath(path);
        this.file = new File(path);

        try {
            FileUtil.getDefaultInstance().makeParentDirectories(this.file);
        } catch (SecurityException ex) {
            throw new FileSecurityException(this, "setPath", ex);
        }

        try {
            this.file = FileUtil.getDefaultInstance().canonicalFile(path);
        } catch (SecurityException ex) {
            throw new FileSecurityException(this, "setPath", ex);
        } catch (IOException ex) {
            throw new FileCanonicalizationException(this, "setPath", ex);
        }

        this.cpath = this.file.getPath();
    }

    /**
     * Opens (constructs) this object's {@link #raf RandomAccessFile}. <p>
     *
     * @throws UnexpectedFileNotFoundException if a
     *         <tt>FileNotFoundException</tt> is thrown in reponse to
     *         constructing the <tt>RandomAccessFile</tt> object.
     * @throws FileSecurityException if a required system property value cannot
     *         be accessed, or if a Java security manager exists and its
     *         <tt>{@link java.lang.SecurityManager#checkRead}</tt> method
     *         denies read access to the file; or if its <tt>{@link
     *         java.lang.SecurityManager#checkWrite(java.lang.String)}</tt>
     *         method denies write access to the file
     */
    private final void openRAF()
    throws LockFile.UnexpectedFileNotFoundException,
           LockFile.FileSecurityException {

        try {
            raf = new RandomAccessFile(file, "rw");
        } catch (SecurityException ex) {
            throw new FileSecurityException(this, "openRAF", ex);
        } catch (FileNotFoundException ex) {
            throw new UnexpectedFileNotFoundException(this, "openRAF", ex);
        }
    }

    /**
     * Checks whether the given <tt>DataInputStream</tt> contains the
     * {@link #MAGIC} value.
     *
     * @param  dis the stream to check
     * @throws FileSecurityException if a required system property value cannot
     *         be accessed, or if a Java security manager exists and its
     *         <tt>{@link java.lang.SecurityManager#checkRead}</tt> method
     *         denies read access to the file
     * @throws UnexpectedEndOfFileException if an <tt>EOFException</tt> is
     *         thrown while reading the <tt>DataInputStream</tt>
     * @throws UnexpectedFileIOException if an <tt>IOException</tt> other than
     *         <tt>EOFException</tt> is thrown while reading the
     *         <tt>DataInputStream</tt>
     * @throws WrongMagicException if a value other than <tt>MAGIC</tt> is read
     *         from the <tt>DataInputStream</tt>
     */
    private final void checkMagic(final DataInputStream dis)
    throws LockFile.FileSecurityException,
           LockFile.UnexpectedEndOfFileException,
           LockFile.UnexpectedFileIOException, LockFile.WrongMagicException {

        boolean      success = true;
        final byte[] magic   = new byte[MAGIC.length];

        try {
            for (int i = 0; i < MAGIC.length; i++) {
                magic[i] = dis.readByte();

                if (MAGIC[i] != magic[i]) {
                    success = false;
                }
            }
        } catch (SecurityException ex) {
            throw new FileSecurityException(this, "checkMagic", ex);
        } catch (EOFException ex) {
            throw new UnexpectedEndOfFileException(this, "checkMagic", ex);
        } catch (IOException ex) {
            throw new UnexpectedFileIOException(this, "checkMagic", ex);
        }

        if (!success) {
            throw new WrongMagicException(this, "checkMagic", magic);
        }
    }

    /**
     * Retrieves the last written hearbeat timestamp from this object's lock
     * file.  If this object's lock file does not exist, then <tt>Long.MIN_VALUE
     * </tt> (the earliest time representable as a <tt>long</tt> in Java) is
     * returned immediately. <p>
     *
     * @return the hearbeat timestamp read from this object's lock file,
     *         as a <tt>long</tt> value or, if this object's lock
     *         file does not exist, <tt>Long.MIN_VALUE</tt>, the earliest time
     *         representable as a <tt>long</tt> in Java.
     * @throws FileSecurityException if a required system property value cannot
     *         be accessed, or if a Java security manager exists and its
     *         <tt>{@link java.lang.SecurityManager#checkRead}</tt> method
     *         denies read access to the file
     * @throws UnexpectedEndOfFileException if an <tt>EOFException</tt> is
     *         thrown while attempting to read the target file's <tt>MAGIC</tt>
     *         or heartbeat timestamp value
     * @throws UnexpectedFileNotFoundException if, after successfully testing
     *         for existence, the target file is not found a moment later while
     *         attempting to read its <tt>MAGIC</tt> and heartbeat timestamp
     *         values
     * @throws UnexpectedFileIOException if any other input stream error occurs
     * @throws WrongMagicException if the lock file does not start with the
     *         the {@link #MAGIC} value
     */
    private final long readHeartbeat()
    throws LockFile.FileSecurityException,
           LockFile.UnexpectedFileNotFoundException,
           LockFile.UnexpectedEndOfFileException,
           LockFile.UnexpectedFileIOException, LockFile.WrongMagicException {

        FileInputStream fis = null;
        DataInputStream dis = null;

        try {
            if (!file.exists()) {
                return Long.MIN_VALUE;
            }

            fis = new FileInputStream(file);
            dis = new DataInputStream(fis);

            checkMagic(dis);

            return dis.readLong();
        } catch (SecurityException ex) {
            throw new FileSecurityException(this, "readHeartbeat", ex);
        } catch (FileNotFoundException ex) {
            throw new UnexpectedFileNotFoundException(this, "readHeartbeat",
                    ex);
        } catch (EOFException ex) {
            throw new UnexpectedEndOfFileException(this, "readHeartbeat", ex);
        } catch (IOException ex) {
            throw new UnexpectedFileIOException(this, "readHeartbeat", ex);
        } finally {
            if (fis != null) {
                try {
                    fis.close();
                } catch (IOException ioe) {

                    // ioe.printStackTrace();
                }
            }
        }
    }

    /**
     * Schedules the lock heartbeat task.
     */
    private final void startHeartbeat() {

        if (timerTask == null || HsqlTimer.isCancelled(timerTask)) {
            Runnable runner = new HeartbeatRunner();

            timerTask = timer.schedulePeriodicallyAfter(0, HEARTBEAT_INTERVAL,
                    runner, true);
        }
    }

    /**
     * Cancels the lock heartbeat task.
     */
    private final void stopHeartbeat() {

        if (timerTask != null && !HsqlTimer.isCancelled(timerTask)) {
            HsqlTimer.cancel(timerTask);

            timerTask = null;
        }
    }

    /**
     * Writes the {@link #MAGIC} value to this object's lock file that
     * distiguishes it as an HSQLDB lock file. <p>
     *
     * @throws FileSecurityException possibly never (seek and write are native
     *      methods whose JavaDoc entries do not actually specify throwing
     *      <tt>SecurityException</tt>).  However, it is conceivable that these
     *      native methods may, in turn, access Java methods that do
     *      throw <tt>SecurityException</tt>. In this case, a
     *      <tt>SecurityException</tt> might be thrown if a required system
     *      property value cannot be accessed, or if a security manager exists
     *      and its <tt>{@link
     *      java.lang.SecurityManager#checkWrite(java.io.FileDescriptor)}</tt>
     *      method denies write access to the file
     * @throws UnexpectedEndOfFileException if an end of file exception is
     *      thrown while attempting to write the <tt>MAGIC</tt> value to the
     *      target file (typically, this cannot happen, but the case is
     *      included to distiguish it from the general <tt>IOException</tt>
     *      case).
     * @throws UnexpectedFileIOException if any other I/O error occurs while
     *      attepting to write the <tt>MAGIC</tt> value to the target file.
     */
    private final void writeMagic()
    throws LockFile.FileSecurityException,
           LockFile.UnexpectedEndOfFileException,
           LockFile.UnexpectedFileIOException {

        try {
            raf.seek(0);
            raf.write(MAGIC);
        } catch (SecurityException ex) {
            throw new FileSecurityException(this, "writeMagic", ex);
        } catch (EOFException ex) {
            throw new UnexpectedEndOfFileException(this, "writeMagic", ex);
        } catch (IOException ex) {
            throw new UnexpectedFileIOException(this, "writeMagic", ex);
        }
    }

    /**
     * Writes the current hearbeat timestamp value to this object's lock
     * file. <p>
     *
     * @throws FileSecurityException possibly never (seek and write are native
     *      methods whose JavaDoc entries do not actually specifiy throwing
     *      <tt>SecurityException</tt>).  However, it is conceivable that these
     *      native methods may, in turn, access Java methods that do throw
     *      <tt>SecurityException</tt>. In this case, a
     *      <tt>SecurityException</tt> might be thrown if a required system
     *      property value cannot be accessed, or if a security manager exists
     *      and its <tt>{@link
     *      java.lang.SecurityManager#checkWrite(java.io.FileDescriptor)}</tt>
     *      method denies write access to the file
     * @throws UnexpectedEndOfFileException if an end of file exception is
     *      thrown while attepting to write the heartbeat timestamp value to
     *      the target file (typically, this cannot happen, but the case is
     *      included to distiguish it from the general IOException case).
     * @throws UnexpectedFileIOException if the current heartbeat timestamp
     *      value cannot be written due to an underlying I/O error
     */
    private final void writeHeartbeat()
    throws LockFile.FileSecurityException,
           LockFile.UnexpectedEndOfFileException,
           LockFile.UnexpectedFileIOException {

        try {
            raf.seek(MAGIC.length);
            raf.writeLong(System.currentTimeMillis());
        } catch (SecurityException ex) {
            throw new FileSecurityException(this, "writeHeartbeat", ex);
        } catch (EOFException ex) {
            throw new UnexpectedEndOfFileException(this, "writeHeartbeat", ex);
        } catch (IOException ex) {
            throw new UnexpectedFileIOException(this, "writeHeartbeat", ex);
        }
    }

    /**
     * Tests whether some other object is "equal to" this one. <p>
     *
     * An object is considered equal to a <tt>LockFile</tt> object if and
     * only if it is not null, it is an instance of <tt>LockFile</tt> and
     * either it is the identical instance or it has the same lock file.  More
     * formally, is is considered equal if and only if it is not null, it is an
     * instance of <tt>LockFile</tt>, and the expression: <p>
     *
     * <pre>
     * this == other ||
     * this.file == null ? other.file == null : this.file.equals(other.file);
     * </pre>
     *
     * yeilds true. <p>
     *
     * Note that <tt>file</tt> must be a canonical reference to correctly
     * satisfy this contract. <p>
     *
     * @param obj the reference object with which to compare.
     * @return <tt>true</tt> if this object is equal to the <tt>obj</tt>
     *         argument; <tt>false</tt> otherwise.
     * @see #hashCode
     */
    public final boolean equals(final Object obj) {

        if (this == obj) {
            return true;
        } else if (obj instanceof LockFile) {
            LockFile other = (LockFile) obj;

            return (this.file == null) ? other.file == null
                                       : this.file.equals(other.file);
        }

        return false;
    }

    /**
     * Retrieves the canonical path of this object's lock file, as a
     * <tt>String</tt> object. <p>
     *
     * @return the canonical path of this object's lock file.
     */
    public final String getCanonicalPath() {
        return cpath;
    }

    /**
     * Retrieves the hash code value for this object. <p>
     *
     * The value is zero if <tt>file</tt> is <tt>null</tt>, else the
     * <tt>hashCode</tt> of <tt>file</tt>. That is, two <tt>LockFile</tt>
     * objects have the same <tt>hashCode</tt> value if they refer to the
     * same lock file. <p>
     *
     * Note that <tt>file</tt> must be a canonical reference to correctly
     * satisfy this contract. <p>
     *
     * @return a hash code value for this object.
     * @see #equals(java.lang.Object)
     */
    public final int hashCode() {
        return file == null ? 0
                            : file.hashCode();
    }

    /**
     * Retrieves whether this object has successfully obtained and is still
     * holding (has not yet released) a cooperative lock condition on its
     * lock file. <p>
     *
     * <b>Note:</b> <p>
     *
     * Due to platform-independence retrictions placed on a JVM, it is quite
     * possible to successfully acquire a lock condition and yet for the
     * condition to become invalid while still held. <p>
     *
     * For instance, under JVMs with no <tt>java.nio</tt> package or under
     * operating systems that do not apply mandatory file locking (espcially
     * mandatory locking that precludes deletion), it is quite possible for
     * another process or even an uncooperative bit of code running in the same
     * JVM to overwrite or delete the target lock file while this object holds
     * a lock condition. <p>
     *
     * Because of this, the <tt>isValid()</tt> method is provided in the public
     * interface in order to allow clients to detect at least a subset of such
     * situations. <p>
     *
     * @return <tt>true</tt> if this object has successfully obtained and is
     *        still holding (has not yet released) a lock condition, else
     *        <tt>false</tt>
     * @see #isValid
     */
    public final boolean isLocked() {
        return locked;
    }

    /**
     * Retrieves whether there is potentially already a cooperative lock,
     * operating system lock or some other situation preventing a cooperative
     * lock condition from being aquired using the specified path.
     *
     * @param path the path to test
     * @return <tt>true</tt> if there is currently something preventing the
     *      acquisition of a cooperative lock condition using the specified
     *      <tt>path</tt>, else <tt>false</tt>
     */
    public final static boolean isLocked(final String path) {

        boolean locked = true;

        try {
            LockFile lockFile = LockFile.newLockFile(path);

            lockFile.checkHeartbeat(false);

            locked = false;
        } catch (Exception e) {}

        return locked;
    }

    /**
     * Retrieves whether this object holds a valid lock condition on its
     * lock file. <p>
     *
     * More formally, this method retrieves true if and only if: <p>
     *
     * <pre>
     * isLocked() && file != null && file.exists() && raf != null
     * </pre>
     *
     * @return <tt>true</tt> if this object holds a valid lock condition on its
     *        lock file; else <tt>false</tt>
     * @throws SecurityException if a required system property value cannot
     *         be accessed, or if a Java security manager exists and its
     *         <tt>checkRead</tt> method denies read access to the lock file;
     */
    public boolean isValid() {
        return isLocked() && file != null && file.exists() && raf != null;
    }

    /**
     * Retrieves a String representation of this object. <p>
     *
     * The String is of the form: <p>
     *
     * <pre>
     * super.toString() +
     * "[file=" + getCanonicalPath() +
     * ", exists=" + file.exists() +
     * ", locked=" + isLocked() +
     * ", valid=" + isValid() +
     * ", " + toStringImpl() +
     * "]";
     * </pre>
     *
     *
     * @return a String representation of this object.
     * @see #toStringImpl
     * @throws SecurityException if a required system property value cannot
     *         be accessed, or if a security manager exists and its <tt>{@link
     *         java.lang.SecurityManager#checkRead}</tt> method denies
     *         read access to the lock file;
     */
    public String toString() {

        return new StringBuffer(super.toString()).append("[file =").append(
            cpath).append(", exists=").append(file.exists()).append(
            ", locked=").append(isLocked()).append(", valid=").append(
            isValid()).append(", ").append(toStringImpl()).append(
            "]").toString();
    }

    /**
     * Retrieves an implementation-specific tail value for the
     * <tt>toString()</tt> method. <p>
     *
     * The default implementation returns the empty string.
     *
     * @return an implementation-specific tail value for the <tt>toString()</tt>
     *      method
     * @see #toString
     */
    protected String toStringImpl() {
        return "";
    }

    /**
     * Retrieves the number of times <tt>checkHeartbeat</tt> may fail before
     * <tt>pollHeartbeat</tt> fails as a consequence. <p>
     *
     * The value is obtained in the following manner: <p>
     *
     * <ol>
     * <li>retries is assigned <tt>POLL_RETRIES_DEFAULT</tt>.
     *
     * <li>retries is assigned <tt>Integer.getInteger(POLL_RETRIES_PROPERTY,
     * retries)</tt> inside a try-catch block to silently ignore any security
     * exception.
     *
     * <li>If retries is less than one (1), retries is assigned one (1).
     * </ol>
     *
     * @return the number of times <tt>checkHeartbeat</tt> may fail before
     *      <tt>pollHeartbeat</tt> fails as a consequence.
     */
    public int getPollHeartbeatRetries() {

        int retries = POLL_RETRIES_DEFAULT;

        try {
            retries = Integer.getInteger(
                HsqlDatabaseProperties.system_lockfile_poll_retries_property,
                retries).intValue();
        } catch (Exception e) {}

        if (retries < 1) {
            retries = 1;
        }

        return retries;
    }

    /**
     * Retrieves the interval, in milliseconds, that <tt>pollHeartbeat</tt>
     * waits between failed invocations of <tt>checkHeartbeat</tt>.
     *
     * The value is obtained in the following manner: <p>
     *
     * <ol>
     * <li>interval is assigned <tt>10 + (HEARTBEAT_INTERVAL_PADDED
     * getPollHeartbeatRetries())</tt>
     *
     * <li>interval is assigned <tt>Long.getLong(POLL_INTERVAL_PROPERTY,
     * interval)</tt>, inside a try-catch block, to silently ignore any security
     * exception.
     *
     * <li>If interval is less than or equal to zero (0), interval is reassigned
     * <tt>10 + (HEARTBEAT_INTERVAL_PADDED / getPollHeartbeatRetries())</tt>
     * </ol>
     *
     * @return the interval, in milliseconds, that <tt>pollHeartbeat</tt>
     *      waits between failed invocations of <tt>checkHeartbeat</tt>
     */
    public long getPollHeartbeatInterval() {

        int  retries  = getPollHeartbeatRetries();
        long interval = 10 + (HEARTBEAT_INTERVAL_PADDED / retries);

        try {
            interval = Long.getLong(POLL_INTERVAL_PROPERTY,
                                    interval).longValue();
        } catch (Exception e) {}

        if (interval <= 0) {
            interval = 10 + (HEARTBEAT_INTERVAL_PADDED / retries);
        }

        return interval;
    }

    /**
     * Polls the underlying lock file to determine if a lock condition
     * exists. <p>
     *
     * Specifically, polls {@link #checkHeartbeat(boolean) checkHeartbeat} at
     * the configured interval until the check passes, the current poll interval
     * wait state is interrupted or the configured number of poll retries is
     * reached. <p>
     *
     * The last exception thrown by <tt>checkHeartbeat</tt> is re-thrown if no
     * check passes. <p>
     *
     * @throws FileSecurityException if the Java security system denied read
     *      to the target file
     * @throws LockHeldExternallyException if the target file's heartbeat
     *      timestamp indicated that a lock condition was held by another
     *      <tt>LockFile</tt>.
     * @throws UnexpectedFileNotFoundException if the target file became
     *      unavailable between a test for existence and an attempt to read
     *      the <tt>MAGIC</tt> or heartbeat timestamp value.
     * @throws UnexpectedEndOfFileException if an <tt>EOFException</tt> was
     *      raised while trying to read the <tt>MAGIC</tt> or heartbeat
     *      timestamp value of the target file
     * @throws UnexpectedFileIOException if an <tt>EOFException</tt> other than
     *      <tt>EOFException</tt> was raised while trying to read the
     *      <tt>MAGIC</tt> or heartbeat timestamp value of the target file
     * @throws WrongLengthException if the target file did not have the
     *      expected length
     * @throws WrongMagicException if the target file did not begin with the
     *      expected <tt>MAGIC</tt> value
     */
    private final void pollHeartbeat()
    throws LockFile.FileSecurityException,
           LockFile.LockHeldExternallyException,
           LockFile.UnexpectedFileNotFoundException,
           LockFile.UnexpectedEndOfFileException,
           LockFile.UnexpectedFileIOException, LockFile.WrongLengthException,
           LockFile.WrongMagicException {

        boolean                success  = false;
        int                    retries  = getPollHeartbeatRetries();
        long                   interval = getPollHeartbeatInterval();
        LockFile.BaseException reason   = null;

        for (int i = retries; i > 0; i--) {
            try {
                checkHeartbeat(true);    // withCreateNewFile == true

                success = true;

                break;
            } catch (LockFile.BaseException ex) {
                reason = ex;
            }

            // We get here if and only if success == false and reason != null,
            // so its OK to 'break'
            try {
                Thread.sleep(interval);
            } catch (InterruptedException ex) {
                break;
            }
        }

        /**
         * @todo:
         * Do not want to specify just BaseException in the throws clause.
         * Is this really the cleanest way?
         */
        if (!success) {
            if (reason instanceof FileSecurityException) {
                throw (FileSecurityException) reason;
            } else if (reason instanceof LockHeldExternallyException) {
                throw (LockHeldExternallyException) reason;
            } else if (reason instanceof UnexpectedFileNotFoundException) {
                throw (UnexpectedFileNotFoundException) reason;
            } else if (reason instanceof UnexpectedEndOfFileException) {
                throw (UnexpectedEndOfFileException) reason;
            } else if (reason instanceof UnexpectedFileIOException) {
                throw (UnexpectedFileIOException) reason;
            } else if (reason instanceof WrongLengthException) {
                throw (WrongLengthException) reason;
            } else if (reason instanceof WrongMagicException) {
                throw (WrongMagicException) reason;
            }
        }
    }

    /**
     * Attempts to obtain a cooperative lock condition upon this object's lock
     * file. <p>
     *
     * @return <tt>true</tt> if this object already holds a lock or the lock was
     *      obtained successfully, else <tt>false</tt>
     * @throws FileSecurityException if the lock condition could not be
     *      obtained due to a Java security permission violation
     * @throws LockHeldExternallyException if the lock condition could not
     *      be obtained because the target file's heartbeat timestamp indicated
     *      that a lock condition was held by another <tt>LockFile</tt>.
     * @throws UnexpectedFileNotFoundException if the lock condition could not
     *      be obtained because the target file became unavailable between a
     *      successful test for existence and an attempt to read its
     *      <tt>MAGIC</tt> or heartbeat timestamp value.
     * @throws UnexpectedEndOfFileException if the lock condition could not be
     *      obtained because <tt>EOFException</tt> was raised while trying to
     *      read the <tt>MAGIC</tt> or heartbeat timestamp value of the target
     *      file
     * @throws UnexpectedFileIOException if the lock condition could not be
     *      obtained due to an <tt>IOException</tt> other than
     *      <tt>EOFException</tt>
     * @throws WrongLengthException if the lock condition could not be obtained
     *      because the target file was the wrong length
     * @throws WrongMagicException if the lock condition could not be obtained
     *      because the target file had the wrong <tt>MAGIC</tt> value
     * @return <tt>true</tt> if and only if a lock condition is obtained;
     *      <tt>false</tt> otherwise.  In general, an exception will
     *      <em>always</em> be thrown if a lock condition cannot be obtained for
     *      any reason
     */
    public final boolean tryLock()
    throws LockFile.FileSecurityException,
           LockFile.LockHeldExternallyException,
           LockFile.UnexpectedFileNotFoundException,
           LockFile.UnexpectedEndOfFileException,
           LockFile.UnexpectedFileIOException, LockFile.WrongLengthException,
           LockFile.WrongMagicException {

        if (this.locked) {
            return true;
        }

        try {
            pollHeartbeat();
            openRAF();

            // Must come *after* openRAF to comply with the
            // doOptionalLockActions() PRE: assertion contract.
            //
            // <sigh> In an ideal world, it would be possible from Java to open
            // a file handle and obtain at least one associated NIO FileLock in
            // one kernel-enforced atomic operation.  However, we can't even
            // guarantee that NIO is available.
            //
            // Note:
            // The NIOLockFile version of this operation is 'self cleaning'...
            // if it fails for some reason, then it does a 'best effort' to
            // eagerly release and nullify its FileLock object before
            // returning.
            doOptionalLockActions();

            // Inlined the following to reduce potential for timing issues
            // such as initial timer thread startup induced delay of first
            // pulse.
            //
            // In general, what we'll get is two initial pulses in rapid
            // sucession: one here and one an instant later as a result of
            // startHeartbeat (which is OK... no harm, and it's one-shot
            // behaviour, not repeated on every writeHeartbeat)
            //
            // Unfortunately, we may occasionally encounter astronomic (at least
            // in computer time) delays between invocation of startHeartbeat
            // and the time at which effort is actually expended toward writing
            // the initial MAGIC and heartbeat timestamp values.
            //
            // Another good reason to inline the first writeHeartbeat is to
            // provide a last line of defence against inter-process as well
            // as inter-thread race conditions.  That is, exceptions thrown in
            // HeartbeatRunner.run() do yet get propagated anywhere useful.
            //
            // Of course, if we are operating under a fully-featured and correct
            // NIO implementation, the concerns described above are really
            // non-issues... at this point, we will have (at least in theory) a
            // valid OS-enforced file lock.
            //
            // But in an ideal world (with or without NIO), any pulse failure in
            // HeartbeatRunner.run() would flag the database Logger that a
            // database lock condition violation has occured, preventing further
            // ad-hoc operation of the database.
            //
            // The problem is, if a lock condition has been violated that is
            // being used by a database instance, what mechanism can be used to
            // safely checkpoint, backup and/or shut down that instance?  For
            // all we know, the violation indicates that another instance is now
            // happily writing to the other database files...
            //
            // A prudent course of action to take under detection of a
            // cooperative lock condition violation in the heartbeatRunner task
            // would be to perform a 'SCRIPT <file>' to some pre-ordained 'safe'
            // backup location using a globally unique file name and then do a
            // 'SHUTDOWN IMMEDIATELY' in one database-scope atomic context (e.g.
            // a single JDBC statement execution).
            //
            // However, by the time a lock condition violation has been detected,
            // the data cache file (and log/script) may already be quite
            // corrupted, meaning the resulting script may be totally inaccurate
            // or worse.
            //
            // Bottom line:
            //
            // Regardless of this inlining measure, if a lock violation occurs
            // after startHeartbeat, it's almost certain there's much worse in
            // store...
            writeMagic();
            writeHeartbeat();
            FileUtil.getDefaultInstance().deleteOnExit(file);

            this.locked = true;

            startHeartbeat();
        } finally {
            if (!locked) {

                // No harm in this...
                //
                // If this LockFile is an NIOLockFile instance and
                // doOptionalLockActions() failed above, then a 'best
                // effort' optional release was already perfomed and
                // this will be a no-op.
                //
                // On the other hand, if doOptionalLockActions() succeeded, best
                // to undo them here right away, since the core locking work
                // failed.
                //
                // In practice, however, it is very unlikely for the core
                // locking work to fail if this LockFile is an NIOLockFile
                // instance and doOptionalLockActions() succeeded, except
                // under JVM implementations whose NIO package is broken in
                // a very specific way.
                //
                // Other possibilities include unfortunate timing of events
                // under certain network file system or removable media
                // configurations, device umounts, physical removal of storage
                // media, Java security or file system security policy
                // updates, etc.
                doOptionalReleaseActions();

                try {
                    closeRAF();
                } catch (Exception ex) {

                    // It's too late to do anything useful with this exception.
                    //
                    // we've already/ failed and will let the caller know the
                    // reason via the exception thrown in the try block.
                    //
                    // ex.printStackTrace();
                }
            }
        }

        return this.locked;
    }

    /**
     * Attempts to release any cooperative lock condition this object
     * may hold upon its lock file. <p>
     *
     *
     * @return <tt>true</tt> if this object does not currently hold a
     *      lock condition or the lock is released completely (including
     *      successful file deletion), else <tt>false</tt>.
     * @throws FileSecurityException if a <tt>SecurityException</tt> is raised
     *      in the process of releasing the lock condition
     * @throws UnexpectedFileIOException if an IoException is raised in the
     *      process of releasing the lock condition
     */
    public final boolean tryRelease()
    throws LockFile.FileSecurityException, LockFile.UnexpectedFileIOException {

        boolean released = !locked;

        if (released) {
            return true;
        }

        stopHeartbeat();
        doOptionalReleaseActions();

        UnexpectedFileIOException closeRAFReason = null;
        FileSecurityException     securityReason = null;

        try {
            try {
                closeRAF();
            } catch (UnexpectedFileIOException ex) {
                closeRAFReason = ex;
            }

            try {

                // Hack Alert:
                //
                // Even without the presence of concurrent locking attempts,
                // the delete or exists invocations below occasionally return
                // false otherwise, perhaps due to a race condition with the
                // heartbeat timestamp writer task or some nio file lock release
                // timing issue?
                //
                // TODO:
                //
                // determine if this is an external constraint or if we can
                // solve it instead by waiting for any in-progress
                // writeHeartbeat operation to conclude.
                Thread.sleep(100);
            } catch (Exception ex) {

                // ex.printStackTrace();
            }

            try {
                released = file.delete();

                // Perhaps excessive...
                //
                // Another Lockfile may recreate the file an instant after it is
                // deleted above (if it it deleted successfully, that is)
                // released = !file.exists();
            } catch (SecurityException ex) {
                securityReason = new FileSecurityException(this, "tryRelease",
                        ex);
            }
        } finally {

            // Regardless of whether all release work succeeds, it is important
            // to indicate that, from the perspective of this instance, a lock
            // condition is no longer held.
            //
            // However, in a world of concurrent execution, we do not want to
            // to expose this fact extenally until *after* all release work has
            // been at least attempted.
            this.locked = false;
        }

        if (closeRAFReason != null) {
            throw closeRAFReason;
        } else if (securityReason != null) {
            throw securityReason;
        }

        return released;
    }

    /**
     * Attempts to release this object's cooperative lock condition. <p>
     *
     * @throws Throwable if this object encounters an unhandled exception
     *        while trying to release the cooperative lock condition
     */
    protected final void finalize() throws Throwable {
        this.tryRelease();
    }

    /**
     * For internal use only. <p>
     *
     * This Runnable class provides the implementation for the timed task
     * that periodically writes out a heartbeat timestamp to the lock file.<p>
     */
    private final class HeartbeatRunner implements Runnable {

        public void run() {

            try {
                LockFile.this.writeHeartbeat();
            } catch (Throwable t) {
                Error.printSystemOut(t.toString());
            }
        }
    }

    /**
     * Base exception class for lock condition specific exceptions. <p>
     *
     */
    public abstract static class BaseException extends Exception {

        private final LockFile lockFile;
        private final String   inMethod;

        /**
         * Constructs a new <tt>LockFile.BaseException</tt>. <p>
         *
         * @param lockFile the underlying <tt>LockFile</tt> object
         * @param inMethod the name of the method in which the exception
         *        was originally thrown (may be passed up serveral levels)
         */
        public BaseException(final LockFile lockFile, final String inMethod) {

            super();

            if (lockFile == null) {
                throw new NullPointerException("lockFile");
            }

            if (inMethod == null) {
                throw new NullPointerException("inMethod");
            }

            this.lockFile = lockFile;
            this.inMethod = inMethod;
        }

        /**
         * Subclass-specific override. <p>
         *
         * @return representation of <tt>lockFile</tt> and
         *      <tt>inMethod</tt>, as <tt>String</tt> object
         */
        public String getMessage() {    // override
            return "lockFile: " + lockFile + " method: " + inMethod;
        }

        /**
         * Getter for <tt>inMethod</tt> property. <p>
         *
         * @return name of method in which exception originally occured
         */
        public String getInMethod() {
            return this.inMethod;
        }

        /**
         * Getter for <tt>lockFile</tt> property. <p>
         *
         * @return the underlying <tt>LockFile</tt> object
         */
        public LockFile getLockFile() {
            return this.lockFile;
        }
    }

    /**
     * Thrown when canonicalization of a <tt>LockFile</tt> object's target
     * file path fails. <p>
     *
     * This is possible because the given path may be illegal on the runtime
     * file system or because construction of the canonical pathname may require
     * filesystem queries.
     */
    public static final class FileCanonicalizationException
    extends BaseException {

        private final IOException reason;

        /**
         * Constructs a new <tt>FileCanonicalizationException</tt>.<p>
         *
         * @param lockFile the underlying <tt>LockFile</tt> object
         * @param inMethod the name of the method in which the exception
         *        was originally thrown (may be passed up serveral levels)
         * @param reason the exception thrown during canonicalization
         */
        public FileCanonicalizationException(final LockFile lockFile,
                                             final String inMethod,
                                             final IOException reason) {

            super(lockFile, inMethod);

            this.reason = reason;
        }

        /**
         * Retrieves the underlying <tt>IOException</tt>. <p>
         *
         * @return Value of property reason.
         */
        public IOException getReason() {
            return this.reason;
        }

        /**
         * Subclass-specific override. <p>
         *
         * @return representation of <tt>lockFile</tt>, <tt>inMethod</tt> and
         *      <tt>reason</tt>, as a <tt>String</tt> object
         */
        public String getMessage() {    // override
            return super.getMessage() + " reason: " + reason;
        }
    }

    /**
     * Thrown when access to a <tt>LockFile</tt> object's target file raises a
     * Java <tt>SecurityEception</tt>. <p>
     *
     * This can occur if a required system property value cannot be accessed, or
     * if a security manager exists and its <tt>{@link
     * java.lang.SecurityManager#checkRead}</tt> method denies read access to a
     * file; or if its <tt>{@link
     * java.lang.SecurityManager#checkRead(java.lang.String)}</tt>
     * method does not permit verification of the existence of all necessary
     * parent directories; or if its <tt>{@link
     * java.lang.SecurityManager#checkWrite(java.lang.String)}</tt>
     * method does not permit all necessary parent directories to be
     * created. <p>
     *
     */
    public static final class FileSecurityException extends BaseException {

        private final SecurityException reason;

        /**
         * Constructs a new <tt>FileSecurityException</tt>. <p>
         *
         * @param lockFile the underlying LockFile object
         * @param inMethod the name of the method in which the exception
         *        was originally thrown (may be passed up serveral levels)
         * @param reason the underlying Java security exception
         */
        public FileSecurityException(final LockFile lockFile,
                                     final String inMethod,
                                     final SecurityException reason) {

            super(lockFile, inMethod);

            this.reason = reason;
        }

        /**
         * Retrieves the underlying <tt>SecurityException</tt>. <p>
         *
         * @return Value of property reason.
         */
        public SecurityException getReason() {
            return this.reason;
        }

        /**
         * Subclass-specific override.
         *
         * @return representation of lockFile, inMethod and reason, as
         *      a String object
         */
        public String getMessage() {    // override
            return super.getMessage() + " reason: " + reason;
        }
    }

    /**
     * Thrown when an externally held lock condition prevents lock
     * aquisition. <p>
     *
     * Specifically, this exception is thrown when polling fails because the
     * lock file's heartbeat timestamp value indicates that another LockFile
     * object still holds the lock condition. <p>
     *
     */
    public static final class LockHeldExternallyException
    extends BaseException {

        private final long read;
        private final long heartbeat;

        /**
         * Constructs a new <tt>LockHeldExternallyException</tt>. <p>
         *
         * @param lockFile the underlying <tt>LockFile</tt> object
         * @param inMethod the name of the method in which the exception
         *        was originally thrown (may be passed up serveral levels)
         * @param read the time, in milliseconds since 1970-01-01, at which
         *      the heartbeat timestamp value was read from the lock file
         * @param heartbeat the heartbeat timestamp value, in milliseconds
         *      since 1970-01-01, that was read from the lock file.
         */
        public LockHeldExternallyException(final LockFile lockFile,
                                           final String inMethod,
                                           final long read,
                                           final long heartbeat) {

            super(lockFile, inMethod);

            this.read      = read;
            this.heartbeat = heartbeat;
        }

        /**
         * Getter for the <tt>heartbeat</tt> attribute. <p>
         *
         * @return the heartbeat timestamp value, in milliseconds since
         *      1970-01-01, that was read from the lock file.
         */
        public long getHeartbeat() {
            return this.heartbeat;
        }

        /**
         * Getter for the <tt>read</tt> attribute. <p>
         *
         * @return the time, in milliseconds since 1970-01-01, that
         *      the heartbeat timestamp value was read from the lock file.
         */
        public long getRead() {
            return this.read;
        }

        /**
         * Subclass-specific override. <p>
         *
         * @return representation of <tt>lockFile</tt>, <tt>inMethod</tt>,
         *      <tt>read</tt> and <tt>heartbeat</tt>, as a <tt>String</tt>
         *      object
         */
        public String getMessage() {    // override

            return super.getMessage() + " read: "
                   + HsqlDateTime.getTimestampString(this.read)
                   + " heartbeat - read: " + (this.heartbeat - this.read)
                   + " ms.";
        }
    }

    /**
     * Thrown when access to a <tt>LockFile</tt> object's target file raises an
     * unexpected <tt>EOFException</tt>.
     */
    public static final class UnexpectedEndOfFileException
    extends BaseException {

        private final EOFException reason;

        /**
         * Constructs a new <tt>UnexpectedEndOfFileException</tt>. <p>
         *
         * @param lockFile the underlying <tt>LockFile</tt> object
         * @param inMethod the name of the method in which the exception
         *        was originally thrown (may be passed up serveral levels)
         * @param reason the underlying exception
         */
        public UnexpectedEndOfFileException(final LockFile lockFile,
                                            final String inMethod,
                                            final EOFException reason) {

            super(lockFile, inMethod);

            this.reason = reason;
        }

        /**
         * Retrieves the underlying <tt>EOFException<tt>. <p>
         *
         * @return Value of property reason.
         */
        public EOFException getReason() {
            return this.reason;
        }

        /**
         * Subclass-specific override. <p>
         *
         * @return representation of <tt>lockFile<tt>, <tt>inMethod</tt> and
         *      <tt>reason</tt>, as a <tt>String</tt> object
         */
        public String getMessage() {    // override
            return super.getMessage() + " reason: " + reason;
        }
    }

    /**
     * Thrown when access to a <tt>LockFile</tt> object's target file raises an
     * unexpected <tt>IOException</tt> other than <tt>EOFException</tt>.
     */
    public static final class UnexpectedFileIOException extends BaseException {

        private final IOException reason;

        /**
         * Constructs a new <tt>UnexpectedFileIOException<tt>.
         *
         * @param lockFile the underlying <tt>LockFile</tt> object
         * @param inMethod the name of the method in which the exception
         *        was originally thrown (may be passed up serveral levels)
         * @param reason the underlying exception
         */
        public UnexpectedFileIOException(final LockFile lockFile,
                                         final String inMethod,
                                         final IOException reason) {

            super(lockFile, inMethod);

            this.reason = reason;
        }

        /**
         * Retrieves the underlying <tt>IOException</tt>.
         *
         * @return Value of property reason.
         */
        public IOException getReason() {
            return this.reason;
        }

        /**
         * Subclass-specific override.
         *
         * @return representation of <tt>lockFile</tt>, <tt>inMethod</tt> and
         *      <tt>reason</tt>, as a <tt>String</tt> object
         */
        public String getMessage() {    // override
            return super.getMessage() + " reason: " + reason;
        }
    }

    /**
     * Thrown when access to a <tt>LockFile</tt> object's target file raises an
     * unexpected <tt>FileNotFoundException</tt>.
     */
    public static final class UnexpectedFileNotFoundException
    extends BaseException {

        private final FileNotFoundException reason;

        /**
         * Constructs a new <tt>UnexpectedFileNotFoundException<tt>. <p>
         *
         * @param lockFile the underlying <tt>LockFile</tt> object
         * @param inMethod the name of the method in which the exception
         *        was originally thrown (may be passed up serveral levels)
         * @param reason the underlying exception
         */
        public UnexpectedFileNotFoundException(
                final LockFile lockFile, final String inMethod,
                final FileNotFoundException reason) {

            super(lockFile, inMethod);

            this.reason = reason;
        }

        /**
         * Retrieves the underlying FileNotFoundException.
         *
         * @return Value of property reason.
         */
        public FileNotFoundException getReason() {
            return this.reason;
        }

        /**
         * Subclass-specific override.
         *
         * @return representation of lockFile, inMethod and reason, as
         *      a String object
         */
        public String getMessage() {    // override
            return super.getMessage() + " reason: " + reason;
        }
    }

    /**
     * Thrown when it is detected that a LockFile object's target file does not
     * have the expected length.
     */
    public static final class WrongLengthException extends BaseException {

        private final long length;

        /**
         * Constructs a new WrongLengthException.
         *
         * @param lockFile the underlying LockFile object
         * @param inMethod the name of the method in which the exception
         *        was originally thrown (may be passed up serveral levels)
         * @param length the actual length reported by the file system
         */
        public WrongLengthException(final LockFile lockFile,
                                    final String inMethod, final long length) {

            super(lockFile, inMethod);

            this.length = length;
        }

        /**
         * Retreives the actual length reported by the file system.
         *
         * @return the actual length reported by the file system
         */
        public long getLength() {
            return this.length;
        }

        /**
         * Subclass-specific override.
         *
         * @return representation of lockFile, inMethod and length, as
         *      a String object
         */
        public String getMessage() {    // override
            return super.getMessage() + " length: " + length;
        }
    }

    /**
     * Thrown when it is detected that a LockFile object's target file does not
     * start with the expected MAGIC value.
     */
    public static final class WrongMagicException extends BaseException {

        private final byte[] magic;

        /**
         * Constructs a new WrongMagicException.
         *
         * @param lockFile the underlying LockFile object
         * @param inMethod the name of the method in which the exception
         *        was originally thrown (may be passed up serveral levels)
         * @param magic the actual magic value read from the file
         */
        public WrongMagicException(final LockFile lockFile,
                                   final String inMethod, final byte[] magic) {

            super(lockFile, inMethod);

            this.magic = magic;
        }

        /**
         * Subclass-specific override.
         *
         * @return representation of inMethod, file and magic,
         *      as a String object
         */
        public String getMessage() {    // override

            String message = super.getMessage() + " magic: ";

            message = message + ((magic == null) ? "null"
                                                 : "'"
                                                   + StringConverter.byteArrayToHexString(magic)
                                                   + "'");

            return message;
        }

        /**
         * Retrieves a copy of the actual <tt>MAGIC</tt> value read from the
         * file. <p>
         *
         * @return a copy of the actual <tt>MAGIC</tt> value read from the file
         */
        public byte[] getMagic() {
            return (magic == null) ? null
                                   : (byte[]) this.magic.clone();
        }
    }
}
