/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

package org.voltdb.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.Serializable;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.processtools.ShellTools;


public class PlatformProperties implements Serializable {

    private static final VoltLogger hostLog = new VoltLogger("HOST");

    /////////////////////////////////
    // ACTUAL PLATFORM PROPERTIES
    /////////////////////////////////

    // hardware
    public final int ramInMegabytes;
    public final int hardwareThreads;
    public final int coreCount;
    public final int socketCount;
    public final String cpuDesc;
    public final boolean isCoreReportedByJava;

    // operating system
    public final String osArch;
    public final String osVersion;
    public final String osName;
    public final String locale;

    // java
    public final String javaVersion;
    public final String javaRuntime;
    public final String javaVMInfo;

    /////////////////////////////////
    // CONSTRUCTOR TO SET PROPERTIES
    /////////////////////////////////

    protected static class HardwareInfo {
        int ramInMegabytes = -1;
        int hardwareThreads = -1;
        int coreCount = -1;
        int socketCount = -1;
        String cpuDesc = "unknown";
    }

    protected HardwareInfo getMacHardwareInfo() {
        HardwareInfo hw = new HardwareInfo();

        hw.hardwareThreads = CoreUtils.availableProcessors();
        String result = ShellTools.local_cmd("/usr/sbin/system_profiler -detailLevel mini SPHardwareDataType").trim();
        String[] lines = result.split("\n");
        for (String line : lines) {
            line = line.trim();
            if (line.length() == 0) continue;
            String[] parts = line.split(":");
            if (parts.length < 2) continue;

            if (parts[0].compareToIgnoreCase("Processor Name") == 0) {
                hw.cpuDesc = parts[1].trim();
            }
            if (parts[0].compareToIgnoreCase("Processor Speed") == 0) {
                hw.cpuDesc += " " + parts[1].trim();
            }
            if (parts[0].compareToIgnoreCase("Number of Processors") == 0) {
                hw.socketCount = Integer.valueOf(parts[1].trim());
            }
            if (parts[0].compareToIgnoreCase("Total Number of Cores") == 0) {
                hw.coreCount = Integer.valueOf(parts[1].trim());
            }
            if (parts[0].compareToIgnoreCase("L2 Cache (per Core)") == 0) {
                hw.cpuDesc += " " + parts[1].trim() + " L2/core";
            }
            if (parts[0].compareToIgnoreCase("L3 Cache") == 0) {
                hw.cpuDesc += " " + parts[1].trim() + " L3";
            }
            if (parts[0].compareToIgnoreCase("Memory") == 0) {
                String[] subParts = parts[1].trim().split(" ");
                if (subParts[1].contains("GB")) {
                    hw.ramInMegabytes = Integer.parseInt(subParts[0]) * 1024;
                }
                else {
                    assert(subParts[1].contains("MB"));
                    hw.ramInMegabytes = Integer.parseInt(subParts[0]);
                }
            }
        }

        return hw;
    }

    protected HardwareInfo getLinuxHardwareInfo() {
        HardwareInfo hw = new HardwareInfo();

        // determine ram
        String meminfo = readLinuxStat("/proc/meminfo", true, 1);
        long memInKB = Long.parseLong(meminfo.split("\\s+")[1]);
        hw.ramInMegabytes = (int) (memInKB / 1024);

        // determine cpuinfo
        String cpuinfo = readLinuxStat("/proc/cpuinfo", true, Integer.MAX_VALUE);
        String[] cpus = cpuinfo.trim().split("\n\n");
        hw.hardwareThreads = cpus.length;

        // almost everything we need is in the first cpu's info section
        String[] lines = cpus[0].trim().split("\n");
        for (String line : lines) {
            String[] parts = line.trim().split(":");
            if (parts.length < 2) continue;

            if (parts[0].trim().compareToIgnoreCase("model name") == 0) {
                hw.cpuDesc = parts[1].trim();
            }
            if (parts[0].trim().compareToIgnoreCase("cache size") == 0) {
                hw.cpuDesc += " " + parts[1].trim() + " cache";
            }
            if (parts[0].trim().compareToIgnoreCase("siblings") == 0) {
                hw.socketCount = hw.hardwareThreads / Integer.parseInt(parts[1].trim());
            }
            if (parts[0].trim().compareToIgnoreCase("cpu cores") == 0) {
                hw.coreCount = Integer.parseInt(parts[1].trim()) * hw.socketCount;
            }
        }

        // Adjust ram based on any cgroup limit. Empirically, we get a value
        // of 7fff_ffff_ffff_f000 (max value minus 4k) for no limit. Here
        // we ignore anything larger than physical memory.
        String memlimit = readLinuxStat("/sys/fs/cgroup/memory/memory.limit_in_bytes", false, 1);
        if (memlimit != null) {
            try {
                long limitInMB = Long.parseLong(memlimit.trim()) / (1024 * 1024);
                if (limitInMB > 0 && limitInMB < hw.ramInMegabytes) {
                    hostLog.info(String.format("Physical memory is %d MB, cgroup limit is %d MB; using cgroup limit",
                                               hw.ramInMegabytes, limitInMB));
                    hw.ramInMegabytes = (int) limitInMB;
                }
            }
            catch (NumberFormatException ex) {
                // ignore
            }
        }

        return hw;
    }

    /*
     * Utility to collect Linux file-like config data, as for example
     * in the /proc filesystem.
     */
    private String readLinuxStat(String filename, boolean required, int lineLimit) {
        String out = null;
        try (BufferedReader r = new BufferedReader(new FileReader(filename))) {
            StringBuilder sb = new StringBuilder();
            String line = null; int count = 0;
            while (count < lineLimit && (line = r.readLine()) != null) {
                sb.append(line).append("\n");
                count++;
            }
            out = sb.toString();
        }
        catch (Exception ex) {
            if (required) {
                hostLog.fatal(String.format("Exiting because unable to read %s: %s", filename, ex));
                System.exit(-1);
            }
            else {
                hostLog.debug(String.format("Unable to read %s: %s", filename, ex));
            }
        }
        return out;
    }

    protected PlatformProperties() {
        HardwareInfo hw = null;
        if (System.getProperty("os.name").toLowerCase().contains("mac")) {
            hw = getMacHardwareInfo();
        }
        else if (System.getProperty("os.name").toLowerCase().contains("linux")) {
            hw = getLinuxHardwareInfo();
        }
        else {
            hostLog.warn("Unable to determine supported operating system. Hardware info such as Memory,CPU will be incorrectly reported.");
            hw = new HardwareInfo();
        }

        // hardware
        ramInMegabytes = hw.ramInMegabytes;
        hardwareThreads = hw.hardwareThreads;
        if (hw.coreCount == -1) {
            // some VMs don't provide cpu core count
            coreCount = Runtime.getRuntime().availableProcessors();
            isCoreReportedByJava = true;
        } else {
            coreCount = hw.coreCount;
            isCoreReportedByJava = false;
        }
        socketCount = hw.socketCount;
        cpuDesc = hw.cpuDesc;

        // operating system
        osArch = System.getProperty("os.arch");
        osVersion = System.getProperty("os.version");
        osName = System.getProperty("os.name");
        locale = System.getProperty("user.language") + "_" + System.getProperty("user.country");

        // java
        javaVersion = System.getProperty("java.version");
        javaRuntime = String.format("%s (%s)",
                System.getProperty("java.runtime.name"),
                System.getProperty("java.runtime.version"));
        javaVMInfo = String.format("%s (%s, %s)",
                System.getProperty("java.vm.name"),
                System.getProperty("java.vm.version"),
                System.getProperty("java.vm.info"));
    }

    /////////////////////////////////
    // OUTPUT AND FORMATTING
    /////////////////////////////////

    public String toLogLines(String dbVersion) {
        StringBuilder sb = new StringBuilder();

        sb.append(String.format("CPU INFO:         %d Cores%s, %d Sockets, %d Hardware Threads\n",
                                coreCount, isCoreReportedByJava ? " (Reported by Java)" : "",
                                socketCount, hardwareThreads));
        sb.append(String.format("CPU DESC:         %s\n", cpuDesc));
        sb.append(String.format("HOST MEMORY (MB): %d\n", ramInMegabytes));

        sb.append(String.format("OS PROFILE:       %s %s %s %s\n",
                                osName, osVersion, osArch, locale));
        sb.append(String.format("DB VERSION:       %s\n", dbVersion));
        sb.append(String.format("JAVA VERSION:     %s\n", javaVersion));
        sb.append(String.format("JAVA RUNTIME:     %s\n", javaRuntime));
        sb.append(String.format("JAVA VM:          %s\n", javaVMInfo));

        return sb.toString();
    }

    public String toHTML() {
        StringBuilder sb = new StringBuilder();
        sb.append("<tt>");

        sb.append(String.format("CPU INFO:         %d Cores%s, %d Sockets, %d Hardware Threads<br/>\n",
                                coreCount, isCoreReportedByJava ? " (Reported by Java)" : "",
                                socketCount, hardwareThreads));
        sb.append(String.format("CPU DESC:         %s<br/>\n", cpuDesc));
        sb.append(String.format("HOST MEMORY (MB): %d<br/>\n", ramInMegabytes));

        sb.append(String.format("OS PROFILE:       %s %s %s %s<br/>\n",
                                osName, osVersion, osArch, locale));
        sb.append(String.format("JAVA VERSION:     %s<br/>\n", javaVersion));
        sb.append(String.format("JAVA RUNTIME:     %s<br/>\n", javaRuntime));
        sb.append(String.format("JAVA VM:          %s<br/>\n", javaVMInfo));

        sb.append("</tt>");

        return sb.toString();
    }

    /////////////////////////////////
    // STATIC CODE
    /////////////////////////////////

    protected static PlatformProperties m_singletonProperties = null;
    private static final long serialVersionUID = 1841311119700809716L;

    public synchronized static PlatformProperties getPlatformProperties() {
        if (m_singletonProperties != null)
            return m_singletonProperties;
        m_singletonProperties = new PlatformProperties();
        assert(m_singletonProperties != null);
        return m_singletonProperties;
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        PlatformProperties pp = getPlatformProperties();
        System.out.println(pp.toLogLines(""));
    }
}
