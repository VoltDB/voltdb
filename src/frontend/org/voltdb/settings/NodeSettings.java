/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

package org.voltdb.settings;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.aeonbits.owner.Config.Sources;
import org.aeonbits.owner.ConfigFactory;
import org.voltdb.common.Constants;
import org.voltdb.utils.MiscUtils;
import org.voltdb.utils.VoltFile;

import com.google_voltpatches.common.collect.ImmutableList;
import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.collect.ImmutableSet;
import com.google_voltpatches.common.collect.ImmutableSortedMap;

@Sources({"file:${org.voltdb.config.dir}/path.properties", "file:${org.voltdb.config.dir}/../.paths"})
public interface NodeSettings extends Settings {

    public final static String CL_SNAPSHOT_PATH_KEY = "org.voltdb.path.command_log_snapshot";
    public final static String CL_PATH_KEY = "org.voltdb.path.command_log";
    public final static String SNAPTHOT_PATH_KEY = "org.voltdb.path.snapshots";
    public final static String VOLTDBROOT_PATH_KEY = "org.voltdb.path.voltdbroot";
    public final static String EXPORT_OVERFLOW_PATH_KEY = "org.voltdb.path.export_overflow";
    public final static String DR_OVERFLOW_PATH_KEY = "org.voltdb.path.dr_overflow";
    public final static String LOCAL_SITES_COUNT_KEY = "org.voltdb.local_sites_count";

    @Key(VOLTDBROOT_PATH_KEY)
    public VoltFile getVoltDBRoot();

    @Key(CL_PATH_KEY)
    public File getCommandLog();

    @Key(CL_SNAPSHOT_PATH_KEY)
    public File getCommandLogSnapshot();

    @Key(SNAPTHOT_PATH_KEY)
    public File getSnapshoth();

    @Key(EXPORT_OVERFLOW_PATH_KEY)
    public File getExportOverflow();

    @Key(DR_OVERFLOW_PATH_KEY)
    public File getDROverflow();

    @Key(LOCAL_SITES_COUNT_KEY)
    @DefaultValue("8")
    public int getLocalSitesCount();

    public static NodeSettings create(Map<?, ?>...imports) {
        return ConfigFactory.create(NodeSettings.class, imports);
    }

    default File resolve(File path) {
        return path.isAbsolute() ? path : new File(getVoltDBRoot(), path.getPath());
    }

    default NavigableMap<String, File> getManagedArtifactPaths() {
        return ImmutableSortedMap.<String, File>naturalOrder()
                .put(CL_PATH_KEY, resolve(getCommandLog()))
                .put(CL_SNAPSHOT_PATH_KEY, resolve(getCommandLogSnapshot()))
                .put(SNAPTHOT_PATH_KEY, resolve(getSnapshoth()))
                .put(EXPORT_OVERFLOW_PATH_KEY, resolve(getExportOverflow()))
                .put(DR_OVERFLOW_PATH_KEY, resolve(getDROverflow()))
                .build();
    }

    default boolean archiveSnapshotDirectory() {
        File snapshotDH = resolve(getSnapshoth());
        String [] snapshots = snapshotDH.list();
        if (snapshots == null || snapshots.length == 0) {
            return false;
        }

        Pattern archvRE = Pattern.compile(getSnapshoth().getName() + "\\.(\\d+)");
        final ImmutableSortedMap.Builder<Integer, File> mb = ImmutableSortedMap.naturalOrder();

        File parent = snapshotDH.getParentFile();
        parent.listFiles(new FileFilter() {
            @Override
            public boolean accept(File path) {
                Matcher mtc = archvRE.matcher(path.getName());
                if (path.isDirectory() && mtc.matches()) {
                    mb.put(Integer.parseInt(mtc.group(1)), path);
                    return true;
                }
                return false;
            }
        });
        NavigableMap<Integer, File> snapdirs = mb.build();
        for (Map.Entry<Integer, File> e: snapdirs.descendingMap().entrySet()) {
            File renameTo = new File(snapshotDH.getPath() + "." + (e.getKey() + 1));
            try {
                Files.move(e.getValue().toPath(), renameTo.toPath());
            } catch (IOException exc) {
                throw new SettingsException("failed to rename " + e.getValue()  + " to " + renameTo, exc);
            }
        }
        File renameTo = new File(snapshotDH.getPath() + ".1");
        try {
            Files.move(snapshotDH.toPath(), renameTo.toPath());
        } catch (IOException e) {
            throw new SettingsException("failed to rename " + snapshotDH  + " to " + renameTo, e);
        }
        if (!snapshotDH.mkdir()) {
            throw new SettingsException("failed to create snapshot directory " + snapshotDH);
        }
        return true;
    }

    default List<String> ensureDirectoriesExist() {
        ImmutableList.Builder<String> failed = ImmutableList.builder();
        Map<String, File> managedArtifactsPaths = getManagedArtifactPaths();
        File configDH = resolve(new File(Constants.CONFIG_DIR));
        File logDH = resolve(new File("log"));
        for (File path: managedArtifactsPaths.values()) {
            if (!path.exists() && !path.mkdirs()) {
                failed.add("Unable to create directory \"" + path + "\"");
            }
            if (!path.isDirectory()) {
                failed.add("Specified path \"" + path + "\" is not a directory");
            }
            if (   !path.canRead()
                || !path.canWrite()
                || !path.canExecute())
            {
                failed.add("Directory \"" + path + "\" is not read, write, execute (rwx) accessible");
            }
            if (logDH.equals(path) || configDH.equals(path)) {
                failed.add("\"" + path + "\" is a reserved directory name");
            }
        }
        Set<File> distinct = ImmutableSet.<File>builder()
                .addAll(managedArtifactsPaths.values())
                .add(getVoltDBRoot())
                .build();
        if (distinct.size() < (managedArtifactsPaths.size() + 1)) {
            failed.add("Managed path values \"" + managedArtifactsPaths + "\" are not distinct");
        }
        return failed.build();
    }

    default boolean clean() {
        boolean archivedSnapshots = archiveSnapshotDirectory();
        for (File path: getManagedArtifactPaths().values()) {
            File [] children = path.listFiles();
            if (children == null) continue;
            for (File child: children) {
                MiscUtils.deleteRecursively(child);
            }
        }
        return archivedSnapshots;
    }

    @Override
    default Properties asProperties() {
        ImmutableMap.Builder<String, String> mb = ImmutableMap.builder();
        try {
            mb.put(VOLTDBROOT_PATH_KEY, getVoltDBRoot().getCanonicalPath());
            for (Map.Entry<String, File> e: getManagedArtifactPaths().entrySet()) {
                mb.put(e.getKey(), e.getValue().getCanonicalPath());
            }
            mb.put(LOCAL_SITES_COUNT_KEY, Integer.toString(getLocalSitesCount()));
        } catch (IOException e) {
            throw new SettingsException("failed to canonicalize" + this);
        }
        Properties props = new Properties();
        props.putAll(mb.build());
        return props;
    }

    default void store() {
        File configFH = new File(Settings.getConfigDir(), "path.properties");
        store(configFH, "VoltDB path settings. DO NOT MODIFY THIS FILE!");

        File deprectedConfigFH = new File(getVoltDBRoot(),".paths");
        if (deprectedConfigFH.exists()) deprectedConfigFH.delete();
    }
}
