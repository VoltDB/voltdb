/*
 * Copyright (C) 2007 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google_voltpatches.common.io;

import static com.google_voltpatches.common.base.Preconditions.checkNotNull;
import static com.google_voltpatches.common.base.Preconditions.checkPositionIndexes;

import com.google_voltpatches.common.annotations.Beta;
import com.google_voltpatches.common.annotations.GwtIncompatible;
import com.google_voltpatches.errorprone.annotations.CanIgnoreReturnValue;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Provides utility methods for working with character streams.
 *
 * <p>All method parameters must be non-null unless documented otherwise.
 *
 * <p>Some of the methods in this class take arguments with a generic type of
 * {@code Readable & Closeable}. A {@link java.io.Reader} implements both of those interfaces.
 * Similarly for {@code Appendable & Closeable} and {@link java.io.Writer}.
 *
 * @author Chris Nokleberg
 * @author Bin Zhu
 * @author Colin Decker
 * @since 1.0
 */
@Beta
@GwtIncompatible
public final class CharStreams {

  /**
   * Creates a new {@code CharBuffer} for buffering reads or writes.
   */
  static CharBuffer createBuffer() {
    return CharBuffer.allocate(0x800); // 2K chars (4K bytes)
  }

  private CharStreams() {}

  /**
   * Copies all characters between the {@link Readable} and {@link Appendable} objects. Does not
   * close or flush either object.
   *
   * @param from the object to read from
   * @param to the object to write to
   * @return the number of characters copied
   * @throws IOException if an I/O error occurs
   */
  @CanIgnoreReturnValue
  public static long copy(Readable from, Appendable to) throws IOException {
    checkNotNull(from);
    checkNotNull(to);
    CharBuffer buf = createBuffer();
    long total = 0;
    while (from.read(buf) != -1) {
      buf.flip();
      to.append(buf);
      total += buf.remaining();
      buf.clear();
    }
    return total;
  }

  /**
   * Reads all characters from a {@link Readable} object into a {@link String}. Does not close the
   * {@code Readable}.
   *
   * @param r the object to read from
   * @return a string containing all the characters
   * @throws IOException if an I/O error occurs
   */
  public static String toString(Readable r) throws IOException {
    return toStringBuilder(r).toString();
  }

  /**
   * Reads all characters from a {@link Readable} object into a new {@link StringBuilder} instance.
   * Does not close the {@code Readable}.
   *
   * @param r the object to read from
   * @return a {@link StringBuilder} containing all the characters
   * @throws IOException if an I/O error occurs
   */
  private static StringBuilder toStringBuilder(Readable r) throws IOException {
    StringBuilder sb = new StringBuilder();
    copy(r, sb);
    return sb;
  }

  /**
   * Reads all of the lines from a {@link Readable} object. The lines do not include
   * line-termination characters, but do include other leading and trailing whitespace.
   *
   * <p>Does not close the {@code Readable}. If reading files or resources you should use the
   * {@link Files#readLines} and {@link Resources#readLines} methods.
   *
   * @param r the object to read from
   * @return a mutable {@link List} containing all the lines
   * @throws IOException if an I/O error occurs
   */
  public static List<String> readLines(Readable r) throws IOException {
    List<String> result = new ArrayList<String>();
    LineReader lineReader = new LineReader(r);
    String line;
    while ((line = lineReader.readLine()) != null) {
      result.add(line);
    }
    return result;
  }

  /**
   * Streams lines from a {@link Readable} object, stopping when the processor returns {@code false}
   * or all lines have been read and returning the result produced by the processor. Does not close
   * {@code readable}. Note that this method may not fully consume the contents of {@code readable}
   * if the processor stops processing early.
   *
   * @throws IOException if an I/O error occurs
   * @since 14.0
   */
  @CanIgnoreReturnValue // some processors won't return a useful result
  public static <T> T readLines(Readable readable, LineProcessor<T> processor) throws IOException {
    checkNotNull(readable);
    checkNotNull(processor);

    LineReader lineReader = new LineReader(readable);
    String line;
    while ((line = lineReader.readLine()) != null) {
      if (!processor.processLine(line)) {
        break;
      }
    }
    return processor.getResult();
  }

  /**
   * Reads and discards data from the given {@code Readable} until the end of the stream is
   * reached. Returns the total number of chars read. Does not close the stream.
   *
   * @since 20.0
   */
  @CanIgnoreReturnValue
  public static long exhaust(Readable readable) throws IOException {
    long total = 0;
    long read;
    CharBuffer buf = createBuffer();
    while ((read = readable.read(buf)) != -1) {
      total += read;
      buf.clear();
    }
    return total;
  }

  /**
   * Discards {@code n} characters of data from the reader. This method will block until the full
   * amount has been skipped. Does not close the reader.
   *
   * @param reader the reader to read from
   * @param n the number of characters to skip
   * @throws EOFException if this stream reaches the end before skipping all the characters
   * @throws IOException if an I/O error occurs
   */
  public static void skipFully(Reader reader, long n) throws IOException {
    checkNotNull(reader);
    while (n > 0) {
      long amt = reader.skip(n);
      if (amt == 0) {
        throw new EOFException();
      }
      n -= amt;
    }
  }

  /**
   * Returns a {@link Writer} that simply discards written chars.
   *
   * @since 15.0
   */
  public static Writer nullWriter() {
    return NullWriter.INSTANCE;
  }

  private static final class NullWriter extends Writer {

    private static final NullWriter INSTANCE = new NullWriter();

    @Override
    public void write(int c) {}

    @Override
    public void write(char[] cbuf) {
      checkNotNull(cbuf);
    }

    @Override
    public void write(char[] cbuf, int off, int len) {
      checkPositionIndexes(off, off + len, cbuf.length);
    }

    @Override
    public void write(String str) {
      checkNotNull(str);
    }

    @Override
    public void write(String str, int off, int len) {
      checkPositionIndexes(off, off + len, str.length());
    }

    @Override
    public Writer append(CharSequence csq) {
      checkNotNull(csq);
      return this;
    }

    @Override
    public Writer append(CharSequence csq, int start, int end) {
      checkPositionIndexes(start, end, csq.length());
      return this;
    }

    @Override
    public Writer append(char c) {
      return this;
    }

    @Override
    public void flush() {}

    @Override
    public void close() {}

    @Override
    public String toString() {
      return "CharStreams.nullWriter()";
    }
  }

  /**
   * Returns a Writer that sends all output to the given {@link Appendable} target. Closing the
   * writer will close the target if it is {@link Closeable}, and flushing the writer will flush the
   * target if it is {@link java.io.Flushable}.
   *
   * @param target the object to which output will be sent
   * @return a new Writer object, unless target is a Writer, in which case the target is returned
   */
  public static Writer asWriter(Appendable target) {
    if (target instanceof Writer) {
      return (Writer) target;
    }
    return new AppendableWriter(target);
  }
}
