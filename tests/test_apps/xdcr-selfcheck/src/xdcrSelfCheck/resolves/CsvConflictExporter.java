/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

package xdcrSelfCheck.resolves;

import xdcrSelfCheck.resolves.XdcrConflict.CONFLICT_TYPE;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class CsvConflictExporter {

    private File xdcrConflictExportFile;
    private String tableName;
    private List<String> lines = new ArrayList<>();
    private static String FMT = "%d:%s:%s:%s";

    public CsvConflictExporter(String primaryvoltdbroot, String tableName) {
        this.tableName = tableName;
        File xdcrConflictFolder = new File(primaryvoltdbroot + "/xdcr_conflicts");
        if (! xdcrConflictFolder.exists()) {
            throw new IllegalArgumentException("XDCR conflict export folder does not exist: " + xdcrConflictFolder);
        }

        xdcrConflictExportFile = new File(xdcrConflictFolder, "csv_" + tableName);
    }

    public void export(XdcrConflict xdcrExpected, List<XdcrConflict> xdcrActuals, CONFLICT_TYPE conflictType) {
        if (! xdcrActuals.isEmpty()) {
            Optional<XdcrConflict> max = xdcrActuals.stream().max(
                    (xdcr1, xdcr2) -> xdcr1.getCurrentTimestamp().compareTo(xdcr2.getCurrentTimestamp())
            );
            String currentTS = max.get().getCurrentTimestamp();
            Optional<XdcrConflict> min = xdcrActuals.stream().min(
                    (xdcr1, xdcr2) -> xdcr1.getTimeStamp().compareTo(xdcr2.getTimeStamp())
            );
            String ts = min.get().getTimeStamp();
            lines.add(String.format(FMT, xdcrExpected.getCid(), ts, currentTS, conflictType.name()));
        }
    }

    public void flush() throws IOException {
        Files.write(xdcrConflictExportFile.toPath(), lines, Charset.forName("UTF-8"));
    }
}
