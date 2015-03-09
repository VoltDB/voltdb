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

package org.voltdb;

import java.io.File;

/**
 * This file isn't long for this world. It's just something I've been using
 * to debug multi-process rejoin stuff.
 *
 */
public class VLog {
    static File m_logfile = new File("vlog.txt");

    public synchronized static void setPortNo(int portNo) {
        m_logfile = new File(String.format("vlog-%d.txt", portNo));
    }

    public synchronized static void log(String str) {

        // turn off this stupid thing for now
        /*try {
            FileWriter log = new FileWriter(m_logfile, true);
            log.write(str + "\n");
            log.flush();
            log.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }*/
    }

    public static void log(String format, Object... args) {
        log(String.format(format, args));
    }
}
