/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.processtools;

import java.io.BufferedInputStream;
import java.io.IOException;

import org.voltcore.logging.VoltLogger;


public abstract class ShellTools {
    private static VoltLogger log = new VoltLogger("HOST");

    private static Process createProcess(String command[]) {
        ProcessBuilder pb = new ProcessBuilder(command);
        pb.redirectErrorStream(true);
        Process p = null;
        try {
            p = pb.start();
        } catch (IOException e) {
            StringBuilder sb = new StringBuilder();
            if (command != null) {
                for (String c : command) {
                    sb.append(c).append(" ");
                }
            }
            log.error("Failed to run command " + sb.toString() + ". Error is " + e.getMessage());
            return null;
        }
        return p;
    }

    public static String local_cmd(String command) {
        return local_cmd(command.split(" "));
    }

    public static String local_cmd(String[] command) {
        StringBuilder retval = new StringBuilder();
        Process p = createProcess(command);
        if (p == null) {
            return null;
        }

        BufferedInputStream in = new BufferedInputStream(p.getInputStream());
        int c;
        try {
            while((c = in.read()) != -1) {
                retval.append((char)c);
            }
        }
        catch (Exception e) {
            p.destroy();
        }
        try {
            p.waitFor();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        p.destroy();
        return retval.toString();
    }
}