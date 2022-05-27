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
package org.voltdb.e3.topics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Map;

import org.junit.Rule;
import org.junit.Test;
import org.voltdb.test.utils.RandomTestRule;

import com.google_voltpatches.common.collect.ImmutableList;
import com.google_voltpatches.common.collect.ImmutableMap;

public class TestTopicProperties {
    @Rule
    public final RandomTestRule m_random = new RandomTestRule();

    @Test
    public void csvProperties() {
        // Test that the default properties are all the expected
        TopicProperties props = new TopicProperties(ImmutableMap.of());
        assertEquals('\\', props.get(TopicProperties.Key.CONFIG_CSV_ESCAPE).charValue());
        assertEquals(Boolean.TRUE, props.get(TopicProperties.Key.CONFIG_CSV_IGNORE_LEADING_WHITESPACE));
        assertEquals("\\N", props.get(TopicProperties.Key.CONFIG_CSV_NULL));
        assertEquals('"', props.get(TopicProperties.Key.CONFIG_CSV_QUOTE).charValue());
        assertEquals(Boolean.FALSE, props.get(TopicProperties.Key.CONFIG_CSV_QUOTE_ALL));
        assertEquals(',', props.get(TopicProperties.Key.CONFIG_CSV_SEPARATOR).charValue());
        assertEquals(Boolean.FALSE, props.get(TopicProperties.Key.CONFIG_CSV_STRICT_QUOTES));

        // Can create a properties with all valid csv values
        Map<String, String> rawProperties = ImmutableMap.<String, String>builder()
                .put(TopicProperties.Key.CONFIG_CSV_ESCAPE.name(), "\\")
                .put(TopicProperties.Key.CONFIG_CSV_IGNORE_LEADING_WHITESPACE.name(), "false")
                .put(TopicProperties.Key.CONFIG_CSV_NULL.name(), "NULL")
                .put(TopicProperties.Key.CONFIG_CSV_QUOTE.name(), "\'")
                .put(TopicProperties.Key.CONFIG_CSV_QUOTE_ALL.name(), "true")
                .put(TopicProperties.Key.CONFIG_CSV_SEPARATOR.name(), "-")
                .put(TopicProperties.Key.CONFIG_CSV_STRICT_QUOTES.name(), "true").build();
        props = new TopicProperties(rawProperties);

        assertEquals('\\', props.get(TopicProperties.Key.CONFIG_CSV_ESCAPE).charValue());
        assertEquals(Boolean.FALSE, props.get(TopicProperties.Key.CONFIG_CSV_IGNORE_LEADING_WHITESPACE));
        assertEquals("NULL", props.get(TopicProperties.Key.CONFIG_CSV_NULL));
        assertEquals('\'', props.get(TopicProperties.Key.CONFIG_CSV_QUOTE).charValue());
        assertEquals(Boolean.TRUE, props.get(TopicProperties.Key.CONFIG_CSV_QUOTE_ALL));
        assertEquals('-', props.get(TopicProperties.Key.CONFIG_CSV_SEPARATOR).charValue());
        assertEquals(Boolean.TRUE, props.get(TopicProperties.Key.CONFIG_CSV_STRICT_QUOTES));

        for (TopicProperties.Key<Character> charKey : ImmutableList.of(TopicProperties.Key.CONFIG_CSV_ESCAPE,
                TopicProperties.Key.CONFIG_CSV_QUOTE, TopicProperties.Key.CONFIG_CSV_SEPARATOR)) {
            // test boundaries
            testGoodProperty(charKey, '\t');
            testGoodProperty(charKey, ' ');
            testGoodProperty(charKey, '~');

            testBadProperty(charKey, (char) (' ' - 1), ".*Value must be a printable ascii character.*");
            testBadProperty(charKey, (char) ('~' + 1), ".*Value must be a printable ascii character.*");

            // One character is required
            testBadProperty(charKey, "", ".*Value must be a single character.*");
            testBadProperty(charKey, "ab", ".*Value must be a single character.*");

            // Test some random characters
            for (int i = 0; i < 10; ++i) {
                testGoodProperty(charKey, (char) m_random.nextInt(' ', '~'));

                testBadProperty(charKey, (char) (m_random.nextInt('~' + 1, Character.MAX_VALUE)),
                        ".*Value must be a printable ascii character.*");
            }
        }
    }

    private void testGoodProperty(TopicProperties.Key<Character> key, char value) {
        TopicProperties props = new TopicProperties(ImmutableMap.of(key.name(), Character.toString(value)));
        assertEquals(value, props.get(key).charValue());
    }

    private void testBadProperty(TopicProperties.Key<?> key, char value, String expectedError) {
        testBadProperty(key, Character.toString(value), expectedError);
    }

    private void testBadProperty(TopicProperties.Key<?> key, String value, String expectedError) {
        try {
            Map<String, String> rawProperties = ImmutableMap.of(key.name(), value);
            new TopicProperties(rawProperties);
            fail("Should have thrown an exception for: " + rawProperties);
        } catch (Exception e) {
            if (expectedError != null && !e.getMessage().matches(expectedError)) {
                fail("Message '" + e.getMessage() + "' does not match: '" + expectedError + '\'');
            }
        }
    }
}
