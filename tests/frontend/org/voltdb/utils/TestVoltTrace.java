/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

package org.voltdb.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.zip.GZIPInputStream;

import com.google_voltpatches.common.collect.Sets;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestVoltTrace {

    private static final String FILE_NAME_PREFIX = "tracetest";

    private ObjectMapper m_mapper = new ObjectMapper();
    private File m_tempDir = null;

    @Before
    public void setUp() throws Exception {
        m_tempDir = File.createTempFile(FILE_NAME_PREFIX, null);
        assertTrue(m_tempDir.delete());
        assertTrue(m_tempDir.mkdir());
        closeFilterIfOn();
    }

    @After
    public void tearDown() throws Exception {
        closeFilterIfOn();
        VoltTrace.closeAllAndShutdown(null, 0);
        FileUtils.deleteDirectory(m_tempDir);
        m_tempDir = null;
    }

    @Test
    public void testFileWriter() throws IOException {
        final File f = File.createTempFile(FILE_NAME_PREFIX, "json");
        f.deleteOnExit();

        LinkedList<VoltTrace.TraceEventBatch> events = new LinkedList<>();
        LinkedList<VoltTrace.TraceEvent> flatEvents = new LinkedList<>();
        for (int i = 0; i < 10; i++) {
            final VoltTrace.TraceEventBatch batch = new VoltTrace.TraceEventBatch(VoltTrace.Category.CI);
            for (int j = 0; j < 10; j++) {
                final VoltTrace.TraceEvent e = randomEvent();
                batch.add(() -> e);
                flatEvents.add(e);
            }
            events.add(batch);
        }
        new TraceFileWriter(f, events).run();

        verifyFileContents(flatEvents, f.getAbsolutePath());
    }

    @Test
    public void testCategoryToggle() throws IOException {
        // Nothing is enabled by default
        for (VoltTrace.Category category : VoltTrace.Category.values()) {
            assertNull(VoltTrace.log(category));
        }

        // Enable two categories
        VoltTrace.enableCategories(VoltTrace.Category.CI, VoltTrace.Category.SPI);
        assertEquals(Sets.newHashSet(VoltTrace.Category.CI, VoltTrace.Category.SPI),
                     VoltTrace.enabledCategories());
        for (VoltTrace.Category category : VoltTrace.Category.values()) {
            if (category == VoltTrace.Category.CI || category == VoltTrace.Category.SPI) {
                assertNotNull(VoltTrace.log(category));
            } else {
                assertNull(VoltTrace.log(category));
            }
        }

        // Disable one of them
        VoltTrace.disableCategories(VoltTrace.Category.CI);
        assertEquals(Collections.singleton(VoltTrace.Category.SPI), VoltTrace.enabledCategories());
        for (VoltTrace.Category category : VoltTrace.Category.values()) {
            if (category == VoltTrace.Category.SPI) {
                assertNotNull(VoltTrace.log(category));
            } else {
                assertNull(VoltTrace.log(category));
            }
        }

        // Disable all and make sure the tracer is gone
        VoltTrace.disableCategories(VoltTrace.Category.values());
        assertTrue(VoltTrace.enabledCategories().isEmpty());
        assertNull(VoltTrace.dump(m_tempDir.getAbsolutePath()));
    }

    @Test
    public void testBasicTrace() throws Exception {
        VoltTrace.enableCategories(VoltTrace.Category.values());
        final SenderRunnable sender = new SenderRunnable();
        sender.run();
        final String path = VoltTrace.closeAllAndShutdown(m_tempDir.getAbsolutePath(), 0);

        verifyFileContents(sender.getSentList(), path);
    }

    @Test
    public void testTraceLimit() throws IOException {
        VoltTrace.enableCategories(VoltTrace.Category.values());

        // These events should be purged once the limit is reached
        for (int i = 0; i < 10; i++) {
            VoltTrace.log(VoltTrace.Category.CI).add(this::randomEvent);
        }

        // Fill the queue with exactly the limit number of events
        final SenderRunnable sender = new SenderRunnable(VoltTrace.QUEUE_SIZE);
        sender.run();

        verifyFileContents(sender.getSentList(), VoltTrace.closeAllAndShutdown(m_tempDir.getAbsolutePath(), 0));
    }

    @Test
    public void testTraceFilterState() throws Exception {
        // The filter is turned off by default
        assertTrue(!VoltTrace.isFilterOn());
        assertTrue(VoltTrace.getFilterTime() == 0);

        // Turn on the filter
        final double FILTER_TIME_1 = 678;
        VoltTrace.turnOnFilter(FILTER_TIME_1);
        assertTrue(VoltTrace.isFilterOn());
        assertTrue(VoltTrace.getFilterTime() == FILTER_TIME_1);

        // Reset the filter time
        final double FILTER_TIME_2 = 800;
        VoltTrace.turnOnFilter(FILTER_TIME_2);
        assertTrue(VoltTrace.isFilterOn());
        assertTrue(VoltTrace.getFilterTime() == FILTER_TIME_2);

        // Turn off the filter
        VoltTrace.turnOffFilter();
        assertTrue(!VoltTrace.isFilterOn());
        assertTrue(VoltTrace.getFilterTime() == 0);
    }

    private void closeFilterIfOn() {
        if (VoltTrace.isFilterOn()) {
            VoltTrace.turnOffFilter();
        }
    }

    private ArrayList<VoltTrace.TraceEventType> m_allEventTypes = new ArrayList<>(EnumSet.allOf(VoltTrace.TraceEventType.class));
    private Random m_random = new Random();
    private VoltTrace.TraceEvent randomEvent() {
        VoltTrace.TraceEvent event = null;
        while (event==null) {
            VoltTrace.TraceEventType type = m_allEventTypes.get(m_random.nextInt(m_allEventTypes.size()));
            switch(type) {
            case ASYNC_BEGIN:
                event = randomAsync(true);
                break;
            case ASYNC_END:
                event = randomAsync(false);
                break;
            case ASYNC_INSTANT:
                event = randomInstant(true);
                break;
            case DURATION_BEGIN:
                event = randomDurationBegin();
                break;
            case DURATION_END:
                event = randomDurationEnd();
                break;
            case INSTANT:
                event = randomInstant(false);
                break;
            case METADATA:
                event = randomMeta();
                break;
            default:
                break;
            }
        }

        event.setTid(Thread.currentThread().getId());
        event.setNanos(System.nanoTime());

        return event;
    }

    private VoltTrace.TraceEvent randomDurationBegin() {
        return new VoltTrace.TraceEvent(VoltTrace.TraceEventType.DURATION_BEGIN,
                                        "name"+m_random.nextInt(5), null, randomArgs());
    }

    private VoltTrace.TraceEvent randomDurationEnd() {
        return new VoltTrace.TraceEvent(VoltTrace.TraceEventType.DURATION_END,
                                        null, null);
    }

    private VoltTrace.TraceEvent randomAsync(boolean begin) {
        VoltTrace.TraceEventType type = (begin) ?
                VoltTrace.TraceEventType.ASYNC_BEGIN : VoltTrace.TraceEventType.ASYNC_END;
        return new VoltTrace.TraceEvent(type, "name"+m_random.nextInt(5),
                                        Long.toString(m_random.nextLong()), randomArgs());
    }

    private VoltTrace.TraceEvent randomInstant(boolean async) {
        VoltTrace.TraceEventType type = (async) ?
                VoltTrace.TraceEventType.ASYNC_INSTANT : VoltTrace.TraceEventType.INSTANT;
        String id = (async) ? Long.toString(m_random.nextLong()) : null;
        return new VoltTrace.TraceEvent(type,
                "name"+m_random.nextInt(5),
                                        id, randomArgs());
    }

    private static String[] s_metadataNames = { "process_name", "process_labels", "process_sort_index",
            "thread_name", "thread_sort_index"
    };
    private VoltTrace.TraceEvent randomMeta() {
        String name = s_metadataNames[m_random.nextInt(s_metadataNames.length)];
        return new VoltTrace.TraceEvent(VoltTrace.TraceEventType.METADATA, name, null,
                                        randomArgs());
    }

    private static String[] s_argKeys = { "name", "dest", "ciHandle", "txnid", "commit", "key1", "keyn" };
    private Object[] randomArgs() {
        int count = m_random.nextInt(4);
        String[] args = new String[count*2];
        for (int i=0; i<count; i++) {
            String key = s_argKeys[m_random.nextInt(s_argKeys.length)];
            args[i*2] = key;
            if (m_random.nextBoolean()) {
                args[i * 2 + 1] = key + "-val";
            } else {
                args[i * 2 + 1] = null;
            }
        }

        return args;
    }

    private void verifyFileContents(List<VoltTrace.TraceEvent> expectedList, String outfile)
        throws IOException {
        List<VoltTrace.TraceEvent> readEvents = new ArrayList<>();
        BufferedReader reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(outfile))));
        String line;
        while ((line=reader.readLine()) != null) {
            line = line.trim();
            if (line.equals("]") || line.equals("[")) {
                continue;
            }

            if (line.charAt(line.length()-1)==',') {
                line = line.substring(0, line.length()-1);
            }
            readEvents.add(m_mapper.readValue(line, VoltTrace.TraceEvent.class));
        }
        reader.close();
        assertEquals(expectedList.size(), readEvents.size());

        readEvents.sort(Comparator.comparingDouble(VoltTrace.TraceEvent::getTs));
        expectedList.sort(Comparator.comparingDouble(VoltTrace.TraceEvent::getNanos));
        System.out.println("Expected");
        expectedList.forEach(System.out::println);
        System.out.println("Read");
        readEvents.forEach(System.out::println);
        for (int i = 0; i < expectedList.size(); i++) {
            compare(expectedList.get(i), readEvents.get(i));
        }
    }

    private void compare(VoltTrace.TraceEvent expected, VoltTrace.TraceEvent actual) {
        assertEquals(expected.getCategory(), actual.getCategory());
        assertEquals(expected.getName(), actual.getName());
        assertEquals(expected.getPid(), actual.getPid());
        assertEquals(expected.getTid(), actual.getTid());
        assertEquals(expected.getTypeChar(), actual.getTypeChar());
        assertEquals(expected.getId(), actual.getId());
        assertEquals(expected.getType(), actual.getType());
        assertEquals(expected.getArgs(), actual.getArgs());
    }

    public class SenderRunnable implements Runnable {
        private final long m_count;
        private List<VoltTrace.TraceEvent> m_sentList = new ArrayList<>();

        public SenderRunnable() {
            this(10);
        }

        public SenderRunnable(long count) {
            m_count = count;
        }

        public void run() {
            try {
                for (int i = 0; i < m_count; i++) {
                    final VoltTrace.Category category = VoltTrace.Category.values()[m_random.nextInt(VoltTrace.Category.values().length)];
                    final VoltTrace.TraceEventBatch log = VoltTrace.log(category);
                    assert log != null;

                    VoltTrace.TraceEvent event = randomEvent();
                    event.setCategory(category);

                    String[] args = new String[event.getArgs().size()*2];
                    int j=0;
                    for (String key : event.getArgs().keySet()) {
                        args[j++] = key;
                        args[j++] = event.getArgs().get(key);
                    }
                    switch(event.getType()) {
                    case ASYNC_BEGIN:
                        log.add(() -> VoltTrace.beginAsync(event.getName(),
                                                           event.getId(),
                                                           (Object[]) args));
                        break;
                    case ASYNC_END:
                        log.add(() -> VoltTrace.endAsync(event.getName(),
                                                         event.getId(),
                                                         (Object[]) args));
                        break;
                    case ASYNC_INSTANT:
                        log.add(() -> VoltTrace.instantAsync(event.getName(),
                                                             event.getId(),
                                                             (Object[]) args));
                        break;
                    case DURATION_BEGIN:
                        log.add(() -> VoltTrace.beginDuration(event.getName(),
                                                              (Object[]) args));
                        break;
                    case DURATION_END:
                        log.add(VoltTrace::endDuration);
                        break;
                    case INSTANT:
                        log.add(() -> VoltTrace.instant(event.getName(),
                                                        (Object[]) args));
                        break;
                    case METADATA:
                        log.add(() -> VoltTrace.meta(event.getName(),
                                                     (Object[]) args));
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported event type: " + event.getType());
                    }
                    m_sentList.add(event);
                }
            } catch(Throwable t) {
                t.printStackTrace();
            }
        }

        public List<VoltTrace.TraceEvent> getSentList() {
            return m_sentList;
        }
    }
}
