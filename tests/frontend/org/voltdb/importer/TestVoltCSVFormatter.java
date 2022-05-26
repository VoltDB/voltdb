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

package org.voltdb.importer;

import static org.voltcore.common.Constants.VOLT_TMP_DIR;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.osgi.framework.Bundle;
import org.osgi.framework.Constants;
import org.osgi.framework.ServiceReference;
import org.osgi.framework.launch.Framework;
import org.osgi.framework.launch.FrameworkFactory;
import org.voltdb.importer.formatter.AbstractFormatterFactory;
import org.voltdb.importer.formatter.Formatter;
import org.voltdb.importer.formatter.FormatterBuilder;

import com.google_voltpatches.common.base.Function;
import com.google_voltpatches.common.base.Joiner;
import com.google_voltpatches.common.collect.FluentIterable;
import com.google_voltpatches.common.collect.ImmutableList;
import com.google_voltpatches.common.collect.ImmutableMap;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import junit.framework.TestCase;

public class TestVoltCSVFormatter extends TestCase {
    private Bundle m_bundle;
    private Framework m_framework;
    private final static Joiner COMMA_JOINER = Joiner.on(",").skipNulls();

    private final static Function<String, String> appendVersion = new Function<String, String>() {
        @Override
        public String apply(String input) {
            return input + ";version=1.0.0";
        }
    };

    @Override
    @Before
    public void setUp() throws Exception {
        List<String> packages = ImmutableList.<String> builder()
                .add("org.voltcore.network")
                .add("org.voltcore.logging")
                .add("org.voltdb.importer")
                .add("org.voltdb.importer.formatter")
                .add("org.apache.log4j")
                .add("org.voltdb.client")
                .add("org.slf4j")
                .add("org.voltcore.utils")
                .add("com.google_voltpatches.common.base")
                .add("com.google_voltpatches.common.collect")
                .add("com.google_voltpatches.common.net")
                .add("com.google_voltpatches.common.io")
                .add("com.google_voltpatches.common.util.concurrent")
                .build();
        String tmpFilePath = System.getProperty(VOLT_TMP_DIR, System.getProperty("java.io.tmpdir"));
        //Create a directory in temp + username
        File f = new File(tmpFilePath, System.getProperty("user.name"));
        String systemPackagesSpec = FluentIterable.from(packages).transform(appendVersion).join(COMMA_JOINER);
        Map<String, String> m_frameworkProps = ImmutableMap.<String, String> builder()
                .put(Constants.FRAMEWORK_SYSTEMPACKAGES_EXTRA, systemPackagesSpec)
                .put("org.osgi.framework.storage.clean", "onFirstInit").put("felix.cache.rootdir", f.getAbsolutePath())
                .put("felix.cache.locking", Boolean.FALSE.toString()).build();
        FrameworkFactory frameworkFactory = ServiceLoader.load(FrameworkFactory.class).iterator().next();
        m_framework = frameworkFactory.newFramework(m_frameworkProps);
        m_framework.start();
        m_bundle = m_framework.getBundleContext()
                .installBundle("file:" + System.getProperty("user.dir") + "/bundles/voltcsvformatter.jar");
        m_bundle.start();
    }

    @Test
    public void testCSVBundle() throws Exception {
        ServiceReference refs[] = m_bundle.getRegisteredServices();
        ServiceReference<AbstractFormatterFactory> reference = refs[0];
        AbstractFormatterFactory o = m_bundle.getBundleContext().getService(reference);
        Properties prop = new Properties();
        FormatterBuilder builder = new FormatterBuilder("csv", prop);
        builder.setFormatterFactory(o);
        Formatter formatter = builder.create();
        Object[] results = formatter.transform(ByteBuffer.wrap("12,10.05,test".getBytes(StandardCharsets.UTF_8)));
        assertEquals(results.length, 3);
        assertEquals(results[0], "12");
        assertEquals(results[1], "10.05");
        assertEquals(results[2], "test");
    }

    @Test
    public void testTSVBundle() throws Exception {
        ServiceReference refs[] = m_bundle.getRegisteredServices();
        ServiceReference<AbstractFormatterFactory> reference = refs[0];
        AbstractFormatterFactory o = m_bundle.getBundleContext().getService(reference);
        Properties prop = new Properties();
        FormatterBuilder builder = new FormatterBuilder("tsv", prop);
        builder.setFormatterFactory(o);
        Formatter formatter = builder.create();

        Object[] results = formatter.transform(ByteBuffer.wrap("12\t10.05\ttest".getBytes(StandardCharsets.UTF_8)));
        assertEquals(results.length, 3);
        assertEquals(results[0], "12");
        assertEquals(results[1], "10.05");
        assertEquals(results[2], "test");
    }

    @Test
    public void testBadFormat() throws Exception {
        ServiceReference refs[] = m_bundle.getRegisteredServices();
        ServiceReference<AbstractFormatterFactory> reference = refs[0];
        AbstractFormatterFactory o = m_bundle.getBundleContext().getService(reference);
        Properties prop = new Properties();
        FormatterBuilder builder = new FormatterBuilder("badformat", prop);
        builder.setFormatterFactory(o);
         try {
            builder.create();
            fail();
        } catch (RuntimeException e) {
        }
    }

    @Test
    public void testNullTransform() throws Exception {
        ServiceReference refs[] = m_bundle.getRegisteredServices();
        ServiceReference<AbstractFormatterFactory> reference = refs[0];
        AbstractFormatterFactory o = m_bundle.getBundleContext().getService(reference);
        Properties prop = new Properties();
        FormatterBuilder builder = new FormatterBuilder("csv", prop);
        builder.setFormatterFactory(o);
        Formatter formatter = builder.create();

        Object[] results = formatter.transform(null);
        assertNull(results);
    }

    //char separator, char quotechar, char escape, boolean strictQuotes, boolean ignoreLeadingWhiteSpace
    @Test
    public void testQuoteChar() throws Exception {
        ServiceReference refs[] = m_bundle.getRegisteredServices();
        ServiceReference<AbstractFormatterFactory> reference = refs[0];
        AbstractFormatterFactory o = m_bundle.getBundleContext().getService(reference);
        Properties prop = new Properties();
        prop.setProperty("quotechar", "'");
        FormatterBuilder builder = new FormatterBuilder("csv", prop);
        builder.setFormatterFactory(o);
        Formatter formatter = builder.create();

        Object[] results = formatter.transform(ByteBuffer.wrap("12,'10.05,test'".getBytes(StandardCharsets.UTF_8)));
        assertEquals(results.length, 2);
        assertEquals(results[0], "12");
        assertEquals(results[1], "10.05,test");
    }

    @Test
    public void testEscape() throws Exception {
        ServiceReference refs[] = m_bundle.getRegisteredServices();
        ServiceReference<AbstractFormatterFactory> reference = refs[0];
        AbstractFormatterFactory o = m_bundle.getBundleContext().getService(reference);
        Properties prop = new Properties();
        prop.setProperty("escape", "|");
        FormatterBuilder builder = new FormatterBuilder("csv", prop);
        builder.setFormatterFactory(o);
        Formatter formatter = builder.create();

        Object[] results = formatter.transform(ByteBuffer.wrap("12,\"10.05,|\"test|\"\"".getBytes(StandardCharsets.UTF_8)));
        assertEquals(results.length, 2);
        assertEquals(results[0], "12");
        assertEquals(results[1], "10.05,\"test\"");
    }

    @Test
    public void testStrictQuotes() throws Exception {
        ServiceReference refs[] = m_bundle.getRegisteredServices();
        ServiceReference<AbstractFormatterFactory> reference = refs[0];
        AbstractFormatterFactory o = m_bundle.getBundleContext().getService(reference);
        Properties prop = new Properties();
        prop.setProperty("strictquotes", "true");
        FormatterBuilder builder = new FormatterBuilder("csv", prop);
        builder.setFormatterFactory(o);
        Formatter formatter = builder.create();

        Object[] results = formatter.transform(ByteBuffer.wrap("\"12\",\"10.05\",\"es\"".getBytes(StandardCharsets.UTF_8)));
        assertEquals(results.length, 3);
        assertEquals(results[0], "12");
        assertEquals(results[1], "10.05");
        assertEquals(results[2], "es");
    }

    @Test
    public void testTrimunquoted() throws Exception {
        ServiceReference refs[] = m_bundle.getRegisteredServices();
        ServiceReference<AbstractFormatterFactory> reference = refs[0];
        AbstractFormatterFactory o = m_bundle.getBundleContext().getService(reference);
        Properties prop = new Properties();
        prop.setProperty("trimunquoted", "false");
        FormatterBuilder builder = new FormatterBuilder("csv", prop);
        builder.setFormatterFactory(o);
        Formatter formatter = builder.create();

        Object[] results = formatter.transform(ByteBuffer.wrap("12,10.05,  test".getBytes(StandardCharsets.UTF_8)));
        assertEquals(results.length, 3);
        assertEquals(results[0], "12");
        assertEquals(results[1], "10.05");
        assertEquals(results[2], "  test");
    }

    @Test
    public void testTrimunquotedTrim() throws Exception {
        ServiceReference refs[] = m_bundle.getRegisteredServices();
        ServiceReference<AbstractFormatterFactory> reference = refs[0];
        AbstractFormatterFactory o = m_bundle.getBundleContext().getService(reference);
        Properties prop = new Properties();
        prop.setProperty("trimunquoted", "true");
        FormatterBuilder builder = new FormatterBuilder("csv", prop);
        builder.setFormatterFactory(o);
        Formatter formatter = builder.create();

        Object[] results = formatter.transform(ByteBuffer.wrap("12,10.05,  test".getBytes(StandardCharsets.UTF_8)));
        assertEquals(results.length, 3);
        assertEquals(results[0], "12");
        assertEquals(results[1], "10.05");
        assertEquals(results[2], "test");
    }

    @Test
    public void testNowhitespace() throws Exception {
        ServiceReference refs[] = m_bundle.getRegisteredServices();
        ServiceReference<AbstractFormatterFactory> reference = refs[0];
        AbstractFormatterFactory o = m_bundle.getBundleContext().getService(reference);
        Properties prop = new Properties();
        prop.setProperty("nowhitespace", "true");
        FormatterBuilder builder = new FormatterBuilder("csv", prop);
        builder.setFormatterFactory(o);
        Formatter formatter = builder.create();

        try {
            Object[] results = formatter.transform(ByteBuffer.wrap("12,10.05,  test".getBytes(StandardCharsets.UTF_8)));
            fail();
        } catch (RuntimeException e) {
        }
    }

    @Test
    public void testJapaneseCharacterSeperator() throws Exception {
        ServiceReference refs[] = m_bundle.getRegisteredServices();
        ServiceReference<AbstractFormatterFactory> reference = refs[0];
        AbstractFormatterFactory o = m_bundle.getBundleContext().getService(reference);
        Properties prop = new Properties();
        prop.setProperty("separator", "の");
        FormatterBuilder builder = new FormatterBuilder("csv", prop);
        builder.setFormatterFactory(o);
        Formatter formatter = builder.create();

        Object[] results = formatter.transform(ByteBuffer.wrap("12の10.05のtest".getBytes(StandardCharsets.UTF_8)));
        assertEquals(results.length, 3);
        assertEquals(results[0], "12");
        assertEquals(results[1], "10.05");
        assertEquals(results[2], "test");
    }

    @Test
    public void testBlankError() throws Exception {
        ServiceReference refs[] = m_bundle.getRegisteredServices();
        ServiceReference<AbstractFormatterFactory> reference = refs[0];
        AbstractFormatterFactory o = m_bundle.getBundleContext().getService(reference);
        Properties prop = new Properties();
        prop.setProperty("separator", ",");
        prop.setProperty("blank", "error");
        FormatterBuilder builder = new FormatterBuilder("csv", prop);
        builder.setFormatterFactory(o);
        Formatter formatter = builder.create();

        try {
            Object[] results = formatter.transform(ByteBuffer.wrap("12,,test".getBytes(StandardCharsets.UTF_8)));
            fail();
        } catch (RuntimeException e) {
        }
    }

    @Test
    public void testBlankEmpty() throws Exception {
        ServiceReference refs[] = m_bundle.getRegisteredServices();
        ServiceReference<AbstractFormatterFactory> reference = refs[0];
        AbstractFormatterFactory o = m_bundle.getBundleContext().getService(reference);
        Properties prop = new Properties();
        prop.setProperty("separator", ",");
        prop.setProperty("blank", "empty");
        FormatterBuilder builder = new FormatterBuilder("csv", prop);
        builder.setFormatterFactory(o);
        Formatter formatter = builder.create();

        Object[] results = formatter.transform(ByteBuffer.wrap("12,,test".getBytes(StandardCharsets.UTF_8)));
        assertEquals(results.length, 3);
        assertEquals(results[0], "12");
        assertEquals(results[1], null);
        assertEquals(results[2], "test");
    }

    @Test
    public void testCustomNull() throws Exception {
        ServiceReference refs[] = m_bundle.getRegisteredServices();
        ServiceReference<AbstractFormatterFactory> reference = refs[0];
        AbstractFormatterFactory o = m_bundle.getBundleContext().getService(reference);
        Properties prop = new Properties();
        prop.setProperty("separator", ",");
        prop.setProperty("nullstring", "empty");
        FormatterBuilder builder = new FormatterBuilder("csv", prop);
        builder.setFormatterFactory(o);
        Formatter formatter = builder.create();

        Object[] results = formatter.transform(ByteBuffer.wrap("12,empty,test".getBytes(StandardCharsets.UTF_8)));
        assertEquals(results.length, 3);
        assertEquals(results[0], "12");
        assertEquals(results[1], null);
        assertEquals(results[2], "test");
    }

    @Override
    @After
    public void tearDown() throws Exception {
        m_bundle.stop();
        m_framework.stop();
    }
}
