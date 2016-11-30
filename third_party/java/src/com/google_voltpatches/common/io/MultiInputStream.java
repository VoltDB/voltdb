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

import com.google_voltpatches.common.annotations.GwtIncompatible;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import javax.annotation_voltpatches.Nullable;

/**
 * An {@link InputStream} that concatenates multiple substreams. At most one stream will be open at
 * a time.
 *
 * @author Chris Nokleberg
 * @since 1.0
 */
@GwtIncompatible
final class MultiInputStream extends InputStream {

  private Iterator<? extends ByteSource> it;
  private InputStream in;

  /**
   * Creates a new instance.
   *
   * @param it an iterator of I/O suppliers that will provide each substream
   */
  public MultiInputStream(Iterator<? extends ByteSource> it) throws IOException {
    this.it = checkNotNull(it);
    advance();
  }

  @Override
  public void close() throws IOException {
    if (in != null) {
      try {
        in.close();
      } finally {
        in = null;
      }
    }
  }

  /**
   * Closes the current input stream and opens the next one, if any.
   */
  private void advance() throws IOException {
    close();
    if (it.hasNext()) {
      in = it.next().openStream();
    }
  }

  @Override
  public int available() throws IOException {
    if (in == null) {
      return 0;
    }
    return in.available();
  }

  @Override
  public boolean markSupported() {
    return false;
  }

  @Override
  public int read() throws IOException {
    if (in == null) {
      return -1;
    }
    int result = in.read();
    if (result == -1) {
      advance();
      return read();
    }
    return result;
  }

  @Override
  public int read(@Nullable byte[] b, int off, int len) throws IOException {
    if (in == null) {
      return -1;
    }
    int result = in.read(b, off, len);
    if (result == -1) {
      advance();
      return read(b, off, len);
    }
    return result;
  }

  @Override
  public long skip(long n) throws IOException {
    if (in == null || n <= 0) {
      return 0;
    }
    long result = in.skip(n);
    if (result != 0) {
      return result;
    }
    if (read() == -1) {
      return 0;
    }
    return 1 + in.skip(n - 1);
  }
}
