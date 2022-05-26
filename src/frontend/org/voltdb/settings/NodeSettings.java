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

import org.aeonbits.owner.Config.Sources;
import org.aeonbits.owner.ConfigFactory;
import org.voltdb.common.Constants;
import org.voltdb.utils.MiscUtils;

import com.google_voltpatches.common.collect.ImmutableList;
import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.collect.ImmutableSet;
import com.google_voltpatches.common.collect.ImmutableSortedMap;

@Sources({"file:${org.voltdb.config.dir}/path.properties", "file:${org.voltdb.config.dir}/../.paths"})
public interface NodeSettings extends Settings {

    public final static String CL_SNAPSHOT_PATH_KEY = "org.voltdb.path.command_log_snapshot";
    public final static String CL_PATH_KEY = "org.voltdb.path.command_log";
    public final static String SNAPSHOT_PATH_KEY = "org.voltdb.path.snapshots";
    public final static String VOLTDBROOT_PATH_KEY = "org.voltdb.path.voltdbroot";
    public final static String EXPORT_CURSOR_PATH_KEY = "org.voltdb.path.export_cursor";
    public final static String EXPORT_OVERFLOW_PATH_KEY = "org.voltdb.path.export_overflow";
    public final static String TOPICS_DATA_PATH_KEY = "org.voltdb.path.topics_data";
    public final static String DR_OVERFLOW_PATH_KEY = "org.voltdb.path.dr_overflow";
    public final static String LARGE_QUERY_SWAP_PATH_KEY = "org.voltdb.path.large_query_swap";
    public final static String LOCAL_SITES_COUNT_KEY = "org.voltdb.local_sites_count";
    public final static String LOCAL_ACTIVE_SITES_COUNT_KEY = "org.voltdb.local_active_sites_count";

    @Key(VOLTDBROOT_PATH_KEY)
    public File getVoltDBRoot();

    @Key(CL_PATH_KEY)
    public File getCommandLog();

    @Key(CL_SNAPSHOT_PATH_KEY)
    public File getCommandLogSnapshot();

    @Key(SNAPSHOT_PATH_KEY)
    public File getSnapshot();

    @Key(EXPORT_OVERFLOW_PATH_KEY)
    public File getExportOverflow();

    @Key(EXPORT_CURSOR_PATH_KEY)
    @DefaultValue("export_cursor") // must match value in voltdb/compiler/DeploymentFileSchema.xsd
    public File getExportCursor();

    @Key(TOPICS_DATA_PATH_KEY)
    @DefaultValue("topics_data")
    public File getTopicsData();

    @Key(DR_OVERFLOW_PATH_KEY)
    public File getDROverflow();

    @Key(LARGE_QUERY_SWAP_PATH_KEY)
    @DefaultValue("large_query_swap") // must match value in voltdb/compiler/DeploymentFileSchema.xsd
    public File getLargeQuerySwap();

    @Key(LOCAL_SITES_COUNT_KEY)
    @DefaultValue("8")
    public int getLocalSitesCount();

    @Key(LOCAL_ACTIVE_SITES_COUNT_KEY)
    @DefaultValue("8")
    public int getLocalActiveSitesCount();

    public static NodeSettings create(Map<?, ?>...imports) {
        return ConfigFactory.create(NodeSettings.class, imports);
    }

    // check properties that don't have a default value were set from path.properties
    default void checkKeys() {
        File f;
        String key = VOLTDBROOT_PATH_KEY;
        try {
            f = getVoltDBRoot();
            if (f == null)
                throw new SettingsException("Missing property " + key + " in path.properties");

            key = CL_PATH_KEY;
            f = getCommandLog();
            if (f == null)
                throw new SettingsException("Missing property " + key + " in path.properties");

            key = CL_SNAPSHOT_PATH_KEY;
            f = getCommandLogSnapshot();
            if (f == null)
                throw new SettingsException("Missing property " + key + " in path.properties");

            key = SNAPSHOT_PATH_KEY;
            f = getSnapshot();
            if (f == null)
                throw new SettingsException("Missing property " + key + " in path.properties");

            key = EXPORT_OVERFLOW_PATH_KEY;
            f = getExportOverflow();
            if (f == null)
                throw new SettingsException("Missing property " + key + " in path.properties");

            key = DR_OVERFLOW_PATH_KEY;
            f = getDROverflow();
            if (f == null)
                throw new SettingsException("Missing property " + key + " in path.properties");

        } catch (NullPointerException npe) {
            throw new SettingsException("Missing property " + key + " in path.properties");
        }
    }





    default File resolve(File path) {
        return path.isAbsolute() ? path : new File(getVoltDBRoot(), path.getPath());
    }

    default File resolveToAbsolutePath(File path) {
        try {
            return path.isAbsolute() ? path : new File(getVoltDBRoot(), path.getPath()).getCanonicalFile();
        } catch (IOException e) {
            throw new SettingsException(
                    "Failed to canonicalize: " +
                    path.toString() +
                    ". Reason: " +
                    e.getMessage());
        }
    }

    default NavigableMap<String, File> getManagedArtifactPaths() {
        return ImmutableSortedMap.<String, File>naturalOrder()
                .put(CL_PATH_KEY, resolve(getCommandLog()))
                .put(CL_SNAPSHOT_PATH_KEY, resolve(getCommandLogSnapshot()))
                .put(SNAPSHOT_PATH_KEY, resolve(getSnapshot()))
                .put(EXPORT_OVERFLOW_PATH_KEY, resolve(getExportOverflow()))
                .put(DR_OVERFLOW_PATH_KEY, resolve(getDROverflow()))
                .put(LARGE_QUERY_SWAP_PATH_KEY, resolve(getLargeQuerySwap()))
                .put(EXPORT_CURSOR_PATH_KEY, resolve(getExportCursor()))
                .put(TOPICS_DATA_PATH_KEY, resolve(getTopicsData()))
                .build();
    }

    default List<String> ensureDirectoriesExist() {
        ImmutableList.Builder<String> failed = ImmutableList.builder();
        Map<String, File> managedArtifactsPaths = getManagedArtifactPaths();
        File configDH = resolveToAbsolutePath(new File(Constants.CONFIG_DIR));
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

    default void clean() {
        for (File path: getManagedArtifactPaths().values()) {
            File [] children = path.listFiles();
            if (children == null) {
                continue;
            }
            for (File child: children) {
                MiscUtils.deleteRecursively(child);
            }
        }
    }

    @Override
    default Properties asProperties() {
        ImmutableMap.Builder<String, String> mb = ImmutableMap.builder();
        try {
            // Voltdbroot path is always absolute
            File voltdbroot = getVoltDBRoot().getCanonicalFile();
            mb.put(VOLTDBROOT_PATH_KEY, voltdbroot.getCanonicalPath());
            for (Map.Entry<String, File> e: getManagedArtifactPaths().entrySet()) {
                // For other paths (command log, command log snap shot etc.), we will translate their values
                // to be relative to the voltdbroot if they are not absolute.
                File path = e.getValue();
                if (path.isAbsolute()) {
                    mb.put(e.getKey(), path.getCanonicalPath());
                } else {
                    mb.put(e.getKey(), voltdbroot.toPath().relativize(path.getCanonicalFile().toPath()).toString());
                }
            }
            mb.put(LOCAL_SITES_COUNT_KEY, Integer.toString(getLocalSitesCount()));
            mb.put(LOCAL_ACTIVE_SITES_COUNT_KEY, Integer.toString(getLocalActiveSitesCount()));
        } catch (IOException e) {
            throw new SettingsException("Failed to canonicalize " + this);
        }
        Properties props = new Properties();
        props.putAll(mb.build());
        return props;
    }

    default void store() {

        // check required properties are set
        checkKeys();

        // delete stranded temp files if any exist
        for (File f : Settings.getConfigDir().listFiles()) {
            if (f.getName().endsWith(".tmp")) {
                f.delete();
            }
        }

        // Create a new temp file and write NodeSettings properties to file
        File tempFH = null;
        try {
            tempFH = File.createTempFile("path", ".tmp", Settings.getConfigDir());
        } catch (IOException e) {
            throw new SettingsException("Failed to create .tmp file for paths in config directory");
        }
        store(tempFH, "VoltDB path settings. DO NOT MODIFY THIS FILE!");

        // rename temp file to path.properties
        File pathFH = new File(Settings.getConfigDir(), "path.properties");
        if (!tempFH.renameTo(pathFH)) {
            throw new SettingsException("Unable to rename " + tempFH.getName() + " to path.properties");
        }

        // delete deprecated file
        File deprecatedPathFile = new File(getVoltDBRoot(),".paths");
        if (deprecatedPathFile.exists()) {
            deprecatedPathFile.delete();
        }
    }
}
