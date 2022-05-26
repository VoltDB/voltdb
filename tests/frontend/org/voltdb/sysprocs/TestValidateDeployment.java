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

package org.voltdb.sysprocs;

import junit.framework.TestCase;
import org.junit.Test;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;

public class TestValidateDeployment extends TestCase {

    @Test
    public void testValidateDeployment() {
        ValidateDeployment validator = new ValidateDeployment();
        // Good deployment
        String deployment = "<deployment> <cluster hostcount=\"1\" /> </deployment>";
        VoltTable res = validator.run(deployment);
        res.advanceRow();
        assertEquals("Valid deployment should return STATUS_OK (0)", 0L, res.get(ValidateDeployment.STATUS, VoltType.BIGINT));

        deployment = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
                "<deployment>\n" +
                "    <!-- <cluster hostcount=\"1\" sitesperhost=\"4\"/> -->\n" +
                "    <cluster hostcount=\"1\" sitesperhost=\"8\"/>\n" +
                "    <partition-detection/>\n" +
                "    <heartbeat/>\n" +
                "    <ssl/>\n" +
                "    <httpd enabled=\"true\">\n" +
                "        <jsonapi enabled=\"true\"/>\n" +
                "    </httpd>\n" +
                "    <snapshot enabled=\"true\"/>\n" +
                "    <commandlog enabled=\"true\">\n" +
                "        <frequency/>\n" +
                "    </commandlog>\n" +
                "    <systemsettings>\n" +
                "        <temptables/>\n" +
                "        <snapshot/>\n" +
                "        <elastic/>\n" +
                "        <query/>\n" +
                "        <procedure/>\n" +
                "        <resourcemonitor>\n" +
                "            <memorylimit/>\n" +
                "        </resourcemonitor>\n" +
                "<!--\n" +
                "        <flushinterval minimum=\"7000\">\n" +
                "            <export interval=\"8000\" />\n" +
                "            <dr interval=\"20000\" />\n" +
                "        </flushinterval>\n" +
                "-->\n" +
                "    </systemsettings>\n" +
                "    <security/>\n" +
                "    <dr id=\"2\" role=\"xdcr\" >\n" +
                "      <connection source=\"localhost:6555\" />\n" +
                "    </dr>\n" +
                "</deployment>\n";
        res = validator.run(deployment);
        res.advanceRow();
        assertEquals("Valid deployment should return STATUS_OK (0)", 0L, res.get(ValidateDeployment.STATUS, VoltType.BIGINT));

        // Bad deployments
        deployment = "<deployment> <cluster hostcount=\"x\" /> </deployment>";
        res = validator.run(deployment);
        res.advanceRow();
        assertEquals("Invalid deployment should return STATUS_FAILURE (1).", 1L, res.get(ValidateDeployment.STATUS, VoltType.BIGINT));

        deployment = "";
        res = validator.run(deployment);
        res.advanceRow();
        assertEquals("Invalid deployment should return STATUS_FAILURE (1).", 1L, res.get(ValidateDeployment.STATUS, VoltType.BIGINT));
    }
}