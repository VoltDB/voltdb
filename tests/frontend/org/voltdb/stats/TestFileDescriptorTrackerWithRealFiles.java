/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */
package org.voltdb.stats;

import com.google.common.io.Closeables;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class TestFileDescriptorTrackerWithRealFiles {

    @Test
    @SuppressWarnings("UnstableApiUsage")
    public void shouldUpdateFileDescriptorCount() {
        // Given
        FileDescriptorsTracker tracker = new FileDescriptorsTracker();
        int current = tracker.getOpenFileDescriptorCount();

        List<BufferedReader> openFiles = Stream.generate(this::createAndOpenTmpFile)
                .limit(100)
                .collect(Collectors.toList());

        // When
        tracker.update();

        // Then
        int actual = tracker.getOpenFileDescriptorCount();
        assertThat(actual).isGreaterThan(current);

        openFiles.forEach(Closeables::closeQuietly);
    }

    private BufferedReader createAndOpenTmpFile() {
        try {
            Path tempFile = Files.createTempFile(getClass().getName(), "-test");
            tempFile.toFile().deleteOnExit();

            return Files.newBufferedReader(
                    tempFile
            );
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
