/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.osgi.framework.Bundle;
import org.osgi.framework.Constants;
import org.osgi.framework.ServiceReference;
import org.osgi.framework.launch.Framework;
import org.osgi.framework.launch.FrameworkFactory;
import org.voltdb.importer.transformer.AbstractTransformer;

import com.google_voltpatches.common.base.Function;
import com.google_voltpatches.common.base.Joiner;
import com.google_voltpatches.common.collect.FluentIterable;
import com.google_voltpatches.common.collect.ImmutableList;
import com.google_voltpatches.common.collect.ImmutableMap;

public class TestBuiltinTransformer extends TestCase {
    private Bundle m_bundle;
    private Framework m_framework;
    private final static Joiner COMMA_JOINER = Joiner.on(",").skipNulls();

    private final static Function<String,String> appendVersion = new Function<String, String>() {
        @Override
        public String apply(String input) {
            return input + ";version=1.0.0";
        }
    };

    @Before
    public void setUp() throws Exception {
        List<String> packages = ImmutableList.<String>builder()
                .add("org.voltcore.network")
                .add("org.voltcore.logging")
                .add("org.voltdb.importer")
                .add("org.voltdb.importer.transformer")
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
        Map<String, String> m_frameworkProps = ImmutableMap.<String,String>builder()
        .put(Constants.FRAMEWORK_SYSTEMPACKAGES_EXTRA, systemPackagesSpec)
        .put("org.osgi.framework.storage.clean", "onFirstInit")
        .put("felix.cache.rootdir", f.getAbsolutePath())
        .put("felix.cache.locking", Boolean.FALSE.toString())
        .build();
        FrameworkFactory frameworkFactory = ServiceLoader.load(FrameworkFactory.class).iterator().next();
        m_framework = frameworkFactory.newFramework(m_frameworkProps);
        m_framework.start();
        m_bundle = m_framework.getBundleContext().installBundle("file:/home/dweiss/dev/voltdb/bundles/builtintransformer.jar");
        m_bundle.start();
    }

    @Test
    public void testCSVBundle() throws Exception {
        ServiceReference refs[] = m_bundle.getRegisteredServices();
        ServiceReference<AbstractTransformer> reference = refs[0];
        AbstractTransformer o = m_bundle.getBundleContext().getService(reference);
        Properties prop = new Properties();
        prop.setProperty("format", "csv");
        o.configure(prop);
        ByteBuffer input = ByteBuffer.wrap("12,10.05,test".getBytes());
        Object[] results = o.transform(input);
        assertEquals(results.length, 3);

    }

    @Test
    public void testTSVBundle() throws Exception {
        ServiceReference refs[] = m_bundle.getRegisteredServices();
        ServiceReference<AbstractTransformer> reference = refs[0];
        AbstractTransformer o = m_bundle.getBundleContext().getService(reference);
        Properties prop = new Properties();
        prop.setProperty("format", "tsv");
        o.configure(prop);
        ByteBuffer input = ByteBuffer.wrap("12\t10.05\ttest".getBytes());
        Object[] results = o.transform(input);
        assertEquals(results.length, 3);

    }

    @After
    public void tearDown() throws Exception {
        m_bundle.stop();
        m_framework.stop();
    }
}

