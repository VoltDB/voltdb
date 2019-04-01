/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

import static org.junit.Assert.assertEquals;
import static org.voltdb.utils.PbdSegmentName.PBD_SUFFIX;
import static org.voltdb.utils.PbdSegmentName.VERSION_STR;

import java.io.File;

import org.junit.Test;
import org.voltcore.logging.VoltLogger;

public class TestPbdSegmentName {
    private static final VoltLogger LOG = new VoltLogger("DUMMY");

    @Test
    public void testValidPbdName() {
        assertPbdSegmentNameDeserialize("abc_def_0000000123_0000000456_" + VERSION_STR + PBD_SUFFIX, "abc_def", 123,
                456, false);
        assertPbdSegmentNameDeserialize("abc_def_0000000123_0000000456_q_" + VERSION_STR + PBD_SUFFIX, "abc_def",
                123, 456, true);
    }

    @Test
    public void testCreateParseName() {
        long id = 987654156L;
        long prevId = 21654564L;
        assertPbdSegmentNameSerializeDeserialize("this_is_my_nonce", id, prevId, false);
        assertPbdSegmentNameSerializeDeserialize("this_is_my_nonce", id, prevId, true);
    }

    @Test
    public void testNotPbd() {
        for (String name : new String[] { "dkjashfdkjasfkldsjf;dsfja", "dkjashfdkjasf.pbdkldsjf;dsfja",
                "dkjashfdkjasfkldsjf;dsfja.pb", "dkjashfdkjasfkldsjf;dsfja.pbda", "", "dkjashfdkjasfkldsjf;dsfjapbd",
                "pbd" }) {
            assertResult(PbdSegmentName.Result.NOT_PBD, name);
        }
    }

    @Test
    public void testInvalidName() {
        for (String name : new String[] { PBD_SUFFIX, "abc" + PBD_SUFFIX,
                "abcdefghijklmnopqrstuvwxqz_0000000123_" + VERSION_STR + PBD_SUFFIX,
                "abc_abc_def_ghi_jkl_0000000123_" + VERSION_STR + PBD_SUFFIX,
                "nonce_0000001234_0000000456_a_" + VERSION_STR + PBD_SUFFIX,
                "_00000000123_00000000456_" + VERSION_STR + PBD_SUFFIX }) {
            assertResult(PbdSegmentName.Result.INVALID_NAME, name);
        }
    }

    @Test
    public void testAsQuarantinedFile() {
        assertEquals(new File("a/b/c/abc_def_0000000123_0000000456_q_" + VERSION_STR + PBD_SUFFIX),
                PbdSegmentName.asQuarantinedSegment(LOG,
                        new File("a/b/c/abc_def_0000000123_0000000456_" + VERSION_STR + PBD_SUFFIX)).m_file);
    }

    @Test
    public void testInvalidVersion() {
        assertResult(PbdSegmentName.Result.INVALID_VERSION, "abc_def_0000000123_0000000456_NARV" + PBD_SUFFIX);
        assertResult(PbdSegmentName.Result.INVALID_VERSION, "abc_def_0000000123_0000000456_q" + PBD_SUFFIX);
        assertResult(PbdSegmentName.Result.INVALID_VERSION, "abc_def_0000000123_0000000456" + PBD_SUFFIX);

    }

    @Test(expected = IllegalArgumentException.class)
    public void testQuarantineNonPbd() {
        PbdSegmentName.asQuarantinedSegment(LOG, new File("abc_def_123_456.exe"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testQuarantineQuarantinedSegment() {
        PbdSegmentName.asQuarantinedSegment(LOG, new File("abc_def_123_456_q" + PBD_SUFFIX));
    }

    private static void assertPbdSegmentNameSerializeDeserialize(String nonce, long id, long prevId,
            boolean quarantine) {
        assertPbdSegmentNameDeserialize(PbdSegmentName.createName(nonce, id, prevId, quarantine), nonce, id, prevId,
                quarantine);
    }

    private static void assertPbdSegmentNameDeserialize(String name, String nonce, long id, long prevId,
            boolean quarantine) {
        PbdSegmentName pbdSegmentName = assertResult(PbdSegmentName.Result.OK, name);
        assertEquals(nonce, pbdSegmentName.m_nonce);
        assertEquals(id, pbdSegmentName.m_id);
        assertEquals(prevId, pbdSegmentName.m_prevId);
        assertEquals(quarantine, pbdSegmentName.m_quarantined);
    }

    private static PbdSegmentName assertResult(PbdSegmentName.Result expected, String name) {
        PbdSegmentName pbdSegmentName = PbdSegmentName.parseFile(LOG, new File(name));
        assertEquals(name, expected, pbdSegmentName.m_result);
        return pbdSegmentName;
    }
}
