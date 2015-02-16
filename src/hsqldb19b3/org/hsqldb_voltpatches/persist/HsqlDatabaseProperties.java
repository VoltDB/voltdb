/* Copyright (c) 2001-2014, The HSQL Development Group
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of the HSQL Development Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL HSQL DEVELOPMENT GROUP, HSQLDB.ORG,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */


package org.hsqldb_voltpatches.persist;

import java.util.Enumeration;

import org.hsqldb_voltpatches.Database;
import org.hsqldb_voltpatches.DatabaseURL;
import org.hsqldb_voltpatches.error.Error;
import org.hsqldb_voltpatches.error.ErrorCode;
import org.hsqldb_voltpatches.lib.HashMap;
import org.hsqldb_voltpatches.lib.HashSet;
import org.hsqldb_voltpatches.lib.Iterator;
import org.hsqldb_voltpatches.lib.Set;
import org.hsqldb_voltpatches.lib.StringUtil;

/**
 * Manages a .properties file for a database.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.2
 * @since 1.7.0
 */
public class HsqlDatabaseProperties extends HsqlProperties {

    private static final String hsqldb_method_class_names =
        "hsqldb_voltpatches.method_class_names";
    public static final String textdb_allow_full_path =
        "textdb.allow_full_path";
    private static HashSet accessibleJavaMethodNames;
    private static boolean allowFullPath;

    static {
        try {
            String prop = System.getProperty(hsqldb_method_class_names);

            if (prop != null) {
                accessibleJavaMethodNames = new HashSet();

                String[] names = StringUtil.split(prop, ";");

                for (int i = 0; i < names.length; i++) {
                    accessibleJavaMethodNames.add(names[i]);
                }
            }

            prop = System.getProperty(textdb_allow_full_path);

            if (prop != null) {
                if (Boolean.valueOf(prop)) {
                    allowFullPath = true;
                }
            }
        } catch (Exception e) {}
    }

    /**
     * If the system property "hsqldb_voltpatches.method_class_names" is not set, then
     * static methods of all available Java classes can be accessed as functions
     * in HSQLDB. If the property is set, then only the list of semicolon
     * seperated method names becomes accessible. An empty property value means
     * no class is accessible.<p>
     *
     * A property value that ends with .* is treated as a wild card and allows
     * access to all classe or method names formed by substitution of the
     * asterisk.<p>
     *
     * All methods of java.lang.Math are always accessible.
     */
    public static boolean supportsJavaMethod(String name) {

        if (accessibleJavaMethodNames == null) {
            return true;
        }

        if (name.startsWith("java.lang.Math.")) {
            return true;
        }

        if (accessibleJavaMethodNames.contains(name)) {
            return true;
        }

        Iterator it = accessibleJavaMethodNames.iterator();

        while (it.hasNext()) {
            String className = (String) it.next();
            int    limit     = className.lastIndexOf(".*");

            if (limit < 1) {
                continue;
            }

            if (name.startsWith(className.substring(0, limit + 1))) {
                return true;
            }
        }

        return false;
    }

    // accessibility
    public static final int SYSTEM_PROPERTY = 0;
    public static final int FILE_PROPERTY   = 1;
    public static final int SQL_PROPERTY    = 2;

    // db files modified
    public static final int     FILES_NOT_MODIFIED = 0;
    public static final int     FILES_MODIFIED     = 1;
    public static final int     FILES_MODIFIED_NEW = 2;
    public static final int     FILES_NEW          = 3;
    private static final String MODIFIED_NO        = "no";
    private static final String MODIFIED_YES       = "yes";
    private static final String MODIFIED_YES_NEW   = "yes-new-files";
    private static final String MODIFIED_NO_NEW    = "no-new-files";

    // allowed property metadata
    private static final HashMap dbMeta   = new HashMap(67);
    private static final HashMap textMeta = new HashMap(17);

    // versions
    public static final String VERSION_STRING_1_8_0 = "1.8.0";
    public static final String THIS_VERSION         = "2.3.2";
    public static final String THIS_FULL_VERSION    = "2.3.2";
    public static final String THIS_CACHE_VERSION   = "2.0.0";
    public static final String PRODUCT_NAME         = "HSQL Database Engine";
    public static final int    MAJOR                = 2,
                               MINOR                = 3,
                               REVISION             = 2;

    /**
     * system properties supported by HSQLDB
     */
    public static final String system_lockfile_poll_retries_property =
        "hsqldb_voltpatches.lockfile_poll_retries";
    public static final String system_max_char_or_varchar_display_size =
        "hsqldb_voltpatches.max_char_or_varchar_display_size";

    //
    public static final String hsqldb_inc_backup = "hsqldb_voltpatches.inc_backup";

    //
    public static final String  hsqldb_version  = "version";
    public static final String  hsqldb_readonly = "readonly";
    private static final String hsqldb_modified = "modified";

    //
    public static final String hsqldb_cache_version = "hsqldb_voltpatches.cache_version";

    //
    public static final String runtime_gc_interval = "runtime.gc_interval";

    //
    public static final String url_ifexists        = "ifexists";
    public static final String url_create          = "create";
    public static final String url_default_schema  = "default_schema";
    public static final String url_check_props     = "check_props";
    public static final String url_get_column_name = "get_column_name";
    public static final String url_close_result    = "close_result";

    //
    public static final String url_storage_class_name = "storage_class_name";
    public static final String url_fileaccess_class_name =
        "fileaccess_class_name";
    public static final String url_storage_key = "storage_key";
    public static final String url_shutdown    = "shutdown";
    public static final String url_recover     = "recover";
    public static final String url_tls_wrapper = "tls_wrapper";

    //
    public static final String url_crypt_key      = "crypt_key";
    public static final String url_crypt_type     = "crypt_type";
    public static final String url_crypt_provider = "crypt_provider";
    public static final String url_crypt_lobs     = "crypt_lobs";

    //
    public static final String hsqldb_tx       = "hsqldb_voltpatches.tx";
    public static final String hsqldb_tx_level = "hsqldb_voltpatches.tx_level";
    public static final String hsqldb_tx_conflict_rollback =
        "hsqldb_voltpatches.tx_conflict_rollback";
    public static final String hsqldb_applog         = "hsqldb_voltpatches.applog";
    public static final String hsqldb_sqllog         = "hsqldb_voltpatches.sqllog";
    public static final String hsqldb_lob_file_scale = "hsqldb_voltpatches.lob_file_scale";
    public static final String hsqldb_lob_file_compressed =
        "hsqldb_voltpatches.lob_compressed";
    public static final String hsqldb_cache_file_scale =
        "hsqldb_voltpatches.cache_file_scale";
    public static final String hsqldb_cache_free_count =
        "hsqldb_voltpatches.cache_free_count";
    public static final String hsqldb_cache_rows = "hsqldb_voltpatches.cache_rows";
    public static final String hsqldb_cache_size = "hsqldb_voltpatches.cache_size";
    public static final String hsqldb_default_table_type =
        "hsqldb_voltpatches.default_table_type";
    public static final String hsqldb_defrag_limit   = "hsqldb_voltpatches.defrag_limit";
    public static final String hsqldb_files_readonly = "files_readonly";
    public static final String hsqldb_lock_file      = "hsqldb_voltpatches.lock_file";
    public static final String hsqldb_log_data       = "hsqldb_voltpatches.log_data";
    public static final String hsqldb_log_size       = "hsqldb_voltpatches.log_size";
    public static final String hsqldb_nio_data_file  = "hsqldb_voltpatches.nio_data_file";
    public static final String hsqldb_nio_max_size   = "hsqldb_voltpatches.nio_max_size";
    public static final String hsqldb_script_format  = "hsqldb_voltpatches.script_format";
    public static final String hsqldb_temp_directory = "hsqldb_voltpatches.temp_directory";
    public static final String hsqldb_result_max_memory_rows =
        "hsqldb_voltpatches.result_max_memory_rows";
    public static final String hsqldb_write_delay = "hsqldb_voltpatches.write_delay";
    public static final String hsqldb_write_delay_millis =
        "hsqldb_voltpatches.write_delay_millis";
    public static final String hsqldb_full_log_replay =
        "hsqldb_voltpatches.full_log_replay";
    public static final String hsqldb_large_data  = "hsqldb_voltpatches.large_data";
    public static final String hsqldb_files_space = "hsqldb_voltpatches.files_space";
    public static final String hsqldb_files_check = "hsqldb_voltpatches.files_check";
    public static final String hsqldb_digest      = "hsqldb_voltpatches.digest";

    //
    public static final String jdbc_translate_tti_types =
        "jdbc.translate_tti_types";

    //
    public static final String sql_ref_integrity       = "sql.ref_integrity";
    public static final String sql_compare_in_locale = "sql.compare_in_locale";
    public static final String sql_enforce_size        = "sql.enforce_size";
    public static final String sql_enforce_strict_size =
        "sql.enforce_strict_size";    // synonym for sql_enforce_size
    public static final String sql_enforce_refs   = "sql.enforce_refs";
    public static final String sql_enforce_names  = "sql.enforce_names";
    public static final String sql_regular_names  = "sql.regular_names";
    public static final String sql_enforce_types  = "sql.enforce_types";
    public static final String sql_enforce_tdcd   = "sql.enforce_tdc_delete";
    public static final String sql_enforce_tdcu   = "sql.enforce_tdc_update";
    public static final String sql_concat_nulls   = "sql.concat_nulls";
    public static final String sql_nulls_first    = "sql.nulls_first";
    public static final String sql_nulls_order    = "sql.nulls_order";
    public static final String sql_unique_nulls   = "sql.unique_nulls";
    public static final String sql_convert_trunc  = "sql.convert_trunc";
    public static final String sql_avg_scale      = "sql.avg_scale";
    public static final String sql_double_nan     = "sql.double_nan";
    public static final String sql_syntax_db2     = "sql.syntax_db2";
    public static final String sql_syntax_mss     = "sql.syntax_mss";
    public static final String sql_syntax_mys     = "sql.syntax_mys";
    public static final String sql_syntax_ora     = "sql.syntax_ora";
    public static final String sql_syntax_pgs     = "sql.syntax_pgs";
    public static final String sql_longvar_is_lob = "sql.longvar_is_lob";
    public static final String sql_pad_space      = "sql.pad_space";
    public static final String sql_ignore_case    = "sql.ignore_case";

    //
    public static final String textdb_cache_scale = "textdb.cache_scale";
    public static final String textdb_cache_size_scale =
        "textdb.cache_size_scale";
    public static final String textdb_cache_rows   = "textdb.cache_rows";
    public static final String textdb_cache_size   = "textdb.cache_size";
    public static final String textdb_all_quoted   = "textdb.all_quoted";
    public static final String textdb_encoding     = "textdb.encoding";
    public static final String textdb_ignore_first = "textdb.ignore_first";
    public static final String textdb_quoted       = "textdb.quoted";
    public static final String textdb_fs           = "textdb.fs";
    public static final String textdb_vs           = "textdb.vs";
    public static final String textdb_lvs          = "textdb.lvs";

    static {

        // text table defaults
        textMeta.put(textdb_allow_full_path,
                     HsqlProperties.getMeta(textdb_allow_full_path,
                                            SYSTEM_PROPERTY, allowFullPath));
        textMeta.put(textdb_quoted,
                     HsqlProperties.getMeta(textdb_quoted, SQL_PROPERTY,
                                            true));
        textMeta.put(textdb_all_quoted,
                     HsqlProperties.getMeta(textdb_all_quoted, SQL_PROPERTY,
                                            false));
        textMeta.put(textdb_ignore_first,
                     HsqlProperties.getMeta(textdb_ignore_first, SQL_PROPERTY,
                                            false));
        textMeta.put(textdb_fs,
                     HsqlProperties.getMeta(textdb_fs, SQL_PROPERTY, ","));
        textMeta.put(textdb_vs,
                     HsqlProperties.getMeta(textdb_vs, SQL_PROPERTY, null));
        textMeta.put(textdb_lvs,
                     HsqlProperties.getMeta(textdb_lvs, SQL_PROPERTY, null));
        textMeta.put(textdb_encoding,
                     HsqlProperties.getMeta(textdb_encoding, SQL_PROPERTY,
                                            "ISO-8859-1"));
        textMeta.put(textdb_cache_scale,
                     HsqlProperties.getMeta(textdb_cache_scale, SQL_PROPERTY,
                                            10, 8, 16));
        textMeta.put(textdb_cache_size_scale,
                     HsqlProperties.getMeta(textdb_cache_size_scale,
                                            SQL_PROPERTY, 10, 6, 20));
        textMeta.put(textdb_cache_rows,
                     HsqlProperties.getMeta(textdb_cache_rows, SQL_PROPERTY,
                                            1000, 100, 1000000));
        textMeta.put(textdb_cache_size,
                     HsqlProperties.getMeta(textdb_cache_size, SQL_PROPERTY,
                                            100, 10, 1000000));
        dbMeta.putAll(textMeta);

        // string defaults for protected props
        dbMeta.put(hsqldb_version,
                   HsqlProperties.getMeta(hsqldb_version, FILE_PROPERTY,
                                          null));
        dbMeta.put(hsqldb_modified,
                   HsqlProperties.getMeta(hsqldb_modified, FILE_PROPERTY,
                                          null));
        dbMeta.put(hsqldb_cache_version,
                   HsqlProperties.getMeta(hsqldb_cache_version, FILE_PROPERTY,
                                          null));

        // boolean defaults for protected props
        dbMeta.put(hsqldb_readonly,
                   HsqlProperties.getMeta(hsqldb_readonly, FILE_PROPERTY,
                                          false));
        dbMeta.put(hsqldb_files_readonly,
                   HsqlProperties.getMeta(hsqldb_files_readonly,
                                          FILE_PROPERTY, false));

        // string defaults for user defined props
        dbMeta.put(hsqldb_tx,
                   HsqlProperties.getMeta(hsqldb_tx, SQL_PROPERTY, "LOCKS"));
        dbMeta.put(hsqldb_tx_level,
                   HsqlProperties.getMeta(hsqldb_tx_level, SQL_PROPERTY,
                                          "READ_COMMITTED"));
        dbMeta.put(hsqldb_temp_directory,
                   HsqlProperties.getMeta(hsqldb_temp_directory, SQL_PROPERTY,
                                          null));
        dbMeta.put(hsqldb_default_table_type,
                   HsqlProperties.getMeta(hsqldb_default_table_type,
                                          SQL_PROPERTY, "MEMORY"));
        dbMeta.put(hsqldb_digest,
                   HsqlProperties.getMeta(hsqldb_digest, SQL_PROPERTY, "MD5"));

        // boolean defaults for user defined props
        dbMeta.put(hsqldb_tx_conflict_rollback,
                   HsqlProperties.getMeta(hsqldb_tx_conflict_rollback,
                                          SQL_PROPERTY, true));
        dbMeta.put(jdbc_translate_tti_types,
                   HsqlProperties.getMeta(jdbc_translate_tti_types,
                                          SQL_PROPERTY, true));
        dbMeta.put(hsqldb_inc_backup,
                   HsqlProperties.getMeta(hsqldb_inc_backup, SQL_PROPERTY,
                                          true));
        dbMeta.put(hsqldb_lock_file,
                   HsqlProperties.getMeta(hsqldb_lock_file, SQL_PROPERTY,
                                          true));
        dbMeta.put(hsqldb_log_data,
                   HsqlProperties.getMeta(hsqldb_log_data, SQL_PROPERTY,
                                          true));
        dbMeta.put(hsqldb_nio_data_file,
                   HsqlProperties.getMeta(hsqldb_nio_data_file, SQL_PROPERTY,
                                          true));
        dbMeta.put(hsqldb_full_log_replay,
                   HsqlProperties.getMeta(hsqldb_full_log_replay,
                                          SQL_PROPERTY, false));
        dbMeta.put(sql_ref_integrity,
                   HsqlProperties.getMeta(sql_ref_integrity, SQL_PROPERTY,
                                          true));

        // SQL reserved words not allowed as some identifiers
        dbMeta.put(sql_enforce_names,
                   HsqlProperties.getMeta(sql_enforce_names, SQL_PROPERTY,
                                          false));
        dbMeta.put(sql_regular_names,
                   HsqlProperties.getMeta(sql_regular_names, SQL_PROPERTY,
                                          true));
        dbMeta.put(sql_enforce_refs,
                   HsqlProperties.getMeta(sql_enforce_refs, SQL_PROPERTY,
                                          false));

        // char padding to size and exception if data is too long
        dbMeta.put(sql_enforce_size,
                   HsqlProperties.getMeta(sql_enforce_size, SQL_PROPERTY,
                                          true));
        dbMeta.put(sql_enforce_types,
                   HsqlProperties.getMeta(sql_enforce_types, SQL_PROPERTY,
                                          false));
        dbMeta.put(sql_enforce_tdcd,
                   HsqlProperties.getMeta(sql_enforce_tdcd, SQL_PROPERTY,
                                          true));
        dbMeta.put(sql_enforce_tdcu,
                   HsqlProperties.getMeta(sql_enforce_tdcu, SQL_PROPERTY,
                                          true));
        dbMeta.put(sql_concat_nulls,
                   HsqlProperties.getMeta(sql_concat_nulls, SQL_PROPERTY,
                                          true));
        dbMeta.put(sql_nulls_first,
                   HsqlProperties.getMeta(sql_nulls_first, SQL_PROPERTY,
                                          true));
        dbMeta.put(sql_nulls_order,
                   HsqlProperties.getMeta(sql_nulls_order, SQL_PROPERTY,
                                          true));
        dbMeta.put(sql_unique_nulls,
                   HsqlProperties.getMeta(sql_unique_nulls, SQL_PROPERTY,
                                          true));
        dbMeta.put(sql_convert_trunc,
                   HsqlProperties.getMeta(sql_convert_trunc, SQL_PROPERTY,
                                          true));
        dbMeta.put(sql_avg_scale,
                   HsqlProperties.getMeta(sql_avg_scale, SQL_PROPERTY, 0, 0,
                                          10));
        dbMeta.put(sql_double_nan,
                   HsqlProperties.getMeta(sql_double_nan, SQL_PROPERTY, true));
        dbMeta.put(sql_syntax_db2,
                   HsqlProperties.getMeta(sql_syntax_db2, SQL_PROPERTY,
                                          false));
        dbMeta.put(sql_syntax_mss,
                   HsqlProperties.getMeta(sql_syntax_mss, SQL_PROPERTY,
                                          false));
        dbMeta.put(sql_syntax_mys,
                   HsqlProperties.getMeta(sql_syntax_mys, SQL_PROPERTY,
                                          false));
        dbMeta.put(sql_syntax_ora,
                   HsqlProperties.getMeta(sql_syntax_ora, SQL_PROPERTY,
                                          false));
        dbMeta.put(sql_syntax_pgs,
                   HsqlProperties.getMeta(sql_syntax_pgs, SQL_PROPERTY,
                                          false));
        dbMeta.put(sql_compare_in_locale,
                   HsqlProperties.getMeta(sql_compare_in_locale, SQL_PROPERTY,
                                          false));
        dbMeta.put(sql_longvar_is_lob,
                   HsqlProperties.getMeta(sql_longvar_is_lob, SQL_PROPERTY,
                                          false));
        dbMeta.put(sql_pad_space,
                   HsqlProperties.getMeta(sql_pad_space, SQL_PROPERTY, true));
        dbMeta.put(sql_ignore_case,
                   HsqlProperties.getMeta(sql_ignore_case, SQL_PROPERTY,
                                          false));
        dbMeta.put(hsqldb_write_delay,
                   HsqlProperties.getMeta(hsqldb_write_delay, SQL_PROPERTY,
                                          true));
        dbMeta.put(hsqldb_large_data,
                   HsqlProperties.getMeta(hsqldb_large_data, SQL_PROPERTY,
                                          false));
        dbMeta.put(hsqldb_files_space,
                   HsqlProperties.getMeta(hsqldb_files_space, SQL_PROPERTY, 0,
                                          new int[] {
            0, 1, 2, 4, 8, 16, 32, 64
        }));
        dbMeta.put(hsqldb_files_check,
                   HsqlProperties.getMeta(hsqldb_files_check, SQL_PROPERTY, 0,
                                          new int[] {
            0, 1
        }));

        // integral defaults for user-defined props - sets
        dbMeta.put(hsqldb_write_delay_millis,
                   HsqlProperties.getMeta(hsqldb_write_delay_millis,
                                          SQL_PROPERTY, 500, 0, 10000));
        dbMeta.put(hsqldb_applog,
                   HsqlProperties.getMeta(hsqldb_applog, SQL_PROPERTY, 0, 0,
                                          3));
        dbMeta.put(hsqldb_sqllog,
                   HsqlProperties.getMeta(hsqldb_sqllog, SQL_PROPERTY, 0, 0,
                                          4));
        dbMeta.put(hsqldb_script_format,
                   HsqlProperties.getMeta(hsqldb_script_format, SQL_PROPERTY,
                                          0, new int[] {
            0, 1, 3
        }));
        dbMeta.put(hsqldb_lob_file_scale,
                   HsqlProperties.getMeta(hsqldb_lob_file_scale, SQL_PROPERTY,
                                          32, new int[] {
            1, 2, 4, 8, 16, 32
        }));
        dbMeta.put(hsqldb_lob_file_compressed,
                   HsqlProperties.getMeta(hsqldb_lob_file_compressed,
                                          SQL_PROPERTY, false));

        // this property is normally 8 - or 1 for old databases from early versions
        dbMeta.put(hsqldb_cache_file_scale,
                   HsqlProperties.getMeta(hsqldb_cache_file_scale,
                                          SQL_PROPERTY, 32, new int[] {
            1, 8, 16, 32, 64, 128, 256, 512, 1024
        }));

        // integral defaults for user defined props - ranges
        dbMeta.put(hsqldb_log_size,
                   HsqlProperties.getMeta(hsqldb_log_size, SQL_PROPERTY, 50,
                                          0, 4 * 1024));
        dbMeta.put(hsqldb_defrag_limit,
                   HsqlProperties.getMeta(hsqldb_defrag_limit, SQL_PROPERTY,
                                          0, 0, 100));
        dbMeta.put(runtime_gc_interval,
                   HsqlProperties.getMeta(runtime_gc_interval, SQL_PROPERTY,
                                          0, 0, 1000000));
        dbMeta.put(hsqldb_cache_size,
                   HsqlProperties.getMeta(hsqldb_cache_size, SQL_PROPERTY,
                                          10000, 100, 4 * 1024 * 1024));
        dbMeta.put(hsqldb_cache_rows,
                   HsqlProperties.getMeta(hsqldb_cache_rows, SQL_PROPERTY,
                                          50000, 100, 4 * 1024 * 1024));
        dbMeta.put(hsqldb_cache_free_count,
                   HsqlProperties.getMeta(hsqldb_cache_free_count,
                                          SQL_PROPERTY, 512, 0, 4096));
        dbMeta.put(hsqldb_result_max_memory_rows,
                   HsqlProperties.getMeta(hsqldb_result_max_memory_rows,
                                          SQL_PROPERTY, 0, 0,
                                          4 * 1024 * 1024));
        dbMeta.put(hsqldb_nio_max_size,
                   HsqlProperties.getMeta(hsqldb_nio_max_size, SQL_PROPERTY,
                                          256, 64, 262144));
    }

    private Database database;

    public HsqlDatabaseProperties(Database db) {

        super(dbMeta, db.getPath(), db.logger.getFileAccess(),
              db.isFilesInJar());

        database = db;

        setNewDatabaseProperties();
    }

    void setNewDatabaseProperties() {

        // version of a new database
        setProperty(hsqldb_version, THIS_VERSION);
        setProperty(hsqldb_modified, MODIFIED_NO_NEW);

        // OOo related code
        if (database.logger.isStoredFileAccess()) {
            setProperty(hsqldb_cache_rows, 25000);
            setProperty(hsqldb_cache_size, 6000);
            setProperty(hsqldb_log_size, 10);
            setProperty(sql_enforce_size, true);
            setProperty(hsqldb_nio_data_file, false);
            setProperty(hsqldb_lock_file, true);
            setProperty(hsqldb_default_table_type, "cached");
            setProperty(jdbc_translate_tti_types, true);
        }

        // OOo end
    }

    /**
     * Creates file with defaults if it didn't exist.
     * Returns false if file already existed.
     */
    public boolean load() {

        boolean exists;

        if (!DatabaseURL.isFileBasedDatabaseType(database.getType())) {
            return true;
        }

        try {
            exists = super.load();
        } catch (Throwable t) {
            throw Error.error(t, ErrorCode.FILE_IO_ERROR,
                              ErrorCode.M_LOAD_SAVE_PROPERTIES, new Object[] {
                t.toString(), fileName
            });
        }

        if (!exists) {
            return false;
        }

        filterLoadedProperties();

        String version = getStringProperty(hsqldb_version);
        int    check = version.substring(0, 5).compareTo(VERSION_STRING_1_8_0);

        // do not open early version databases
        if (check < 0) {
            throw Error.error(ErrorCode.WRONG_DATABASE_FILE_VERSION);
        }

        // do not open databases of 1.8 versions if script format is not compatible
        if (check == 0) {
            if (getIntegerProperty(hsqldb_script_format) != 0) {
                throw Error.error(ErrorCode.WRONG_DATABASE_FILE_VERSION);
            }
        }

        check = version.substring(0, 2).compareTo(THIS_VERSION);

        // do not open if the database belongs to a later (future) version (3.x)
        if (check > 0) {
            throw Error.error(ErrorCode.WRONG_DATABASE_FILE_VERSION);
        }

        return true;
    }

    public void save() {

        if (!DatabaseURL.isFileBasedDatabaseType(database.getType())
                || database.isFilesReadOnly() || database.isFilesInJar()) {
            return;
        }

        try {
            HsqlProperties props = new HsqlProperties(dbMeta,
                database.getPath(), database.logger.getFileAccess(), false);

            if (getIntegerProperty(hsqldb_script_format) == 3) {
                props.setProperty(hsqldb_script_format, 3);
            }

            props.setProperty(hsqldb_version, THIS_VERSION);

            if (database.logger.isStoredFileAccess()) {
                if (!database.logger.isNewStoredFileAccess()) {

// when jar is used with embedded databases in AOO 3.4 and recent(2012) LO this
// line can be uncommented to circumvent hard-coded check in OOo code in
// drivers/hsqldb_voltpatches/HDriver.cxx
//                    props.setProperty(hsqldb_version, VERSION_STRING_1_8_0);
                }
            }

            props.setProperty(hsqldb_modified, getProperty(hsqldb_modified));
            props.save(fileName + ".properties" + ".new");
            fa.renameElement(fileName + ".properties" + ".new",
                             fileName + ".properties");
        } catch (Throwable t) {
            database.logger.logSevereEvent("save failed", t);

            throw Error.error(t, ErrorCode.FILE_IO_ERROR,
                              ErrorCode.M_LOAD_SAVE_PROPERTIES, new Object[] {
                t.toString(), fileName
            });
        }
    }

    void filterLoadedProperties() {

        String val = stringProps.getProperty(sql_enforce_strict_size);

        if (val != null) {
            stringProps.setProperty(sql_enforce_size, val);
        }

        Enumeration en = stringProps.propertyNames();

        while (en.hasMoreElements()) {
            String  key    = (String) en.nextElement();
            boolean accept = dbMeta.containsKey(key);

            if (!accept) {
                stringProps.remove(key);
            }
        }
    }

    /**
     *  overload file database properties with any passed on URL line
     *  do not store password etc
     */
    public void setURLProperties(HsqlProperties p) {

        boolean strict = false;

        if (p == null) {
            return;
        }

        String val = p.getProperty(sql_enforce_strict_size);

        if (val != null) {
            p.setProperty(sql_enforce_size, val);
            p.removeProperty(sql_enforce_strict_size);
        }

        strict = p.isPropertyTrue(url_check_props, false);

        for (Enumeration e = p.propertyNames(); e.hasMoreElements(); ) {
            String   propertyName  = (String) e.nextElement();
            String   propertyValue = p.getProperty(propertyName);
            boolean  valid         = false;
            boolean  validVal      = false;
            String   error         = null;
            Object[] meta          = (Object[]) dbMeta.get(propertyName);

            if (meta != null
                    && ((Integer) meta[HsqlProperties.indexType]).intValue()
                       == SQL_PROPERTY) {
                valid = true;
                error = HsqlProperties.validateProperty(propertyName,
                        propertyValue, meta);
                validVal = error == null;
            }

            if (propertyName.startsWith("sql.")
                    || propertyName.startsWith("hsqldb_voltpatches.")
                    || propertyName.startsWith("textdb.")) {
                if (strict && !valid) {
                    throw Error.error(ErrorCode.X_42555, propertyName);
                }

                if (strict && !validVal) {
                    throw Error.error(ErrorCode.X_42556, propertyName);
                }
            }
        }

        for (Enumeration e = p.propertyNames(); e.hasMoreElements(); ) {
            String   propertyName = (String) e.nextElement();
            Object[] meta         = (Object[]) dbMeta.get(propertyName);

            if (meta != null
                    && ((Integer) meta[HsqlProperties.indexType]).intValue()
                       == SQL_PROPERTY) {
                setDatabaseProperty(propertyName, p.getProperty(propertyName));
            }
        }
    }

    public Set getUserDefinedPropertyData() {

        Set      set = new HashSet();
        Iterator it  = dbMeta.values().iterator();

        while (it.hasNext()) {
            Object[] row = (Object[]) it.next();

            if (((Integer) row[HsqlProperties.indexType]).intValue()
                    == SQL_PROPERTY) {
                set.add(row);
            }
        }

        return set;
    }

    public boolean isUserDefinedProperty(String key) {

        Object[] row = (Object[]) dbMeta.get(key);

        return row != null
               && ((Integer) row[HsqlProperties.indexType]).intValue()
                  == SQL_PROPERTY;
    }

    public boolean isBoolean(String key) {

        Object[] row = (Object[]) dbMeta.get(key);

        return row != null && row[HsqlProperties.indexClass].equals("Boolean")
               && ((Integer) row[HsqlProperties.indexType]).intValue()
                  == SQL_PROPERTY;
    }

    public boolean isIntegral(String key) {

        Object[] row = (Object[]) dbMeta.get(key);

        return row != null && row[HsqlProperties.indexClass].equals("Integer")
               && ((Integer) row[HsqlProperties.indexType]).intValue()
                  == SQL_PROPERTY;
    }

    public boolean isString(String key) {

        Object[] row = (Object[]) dbMeta.get(key);

        return row != null && row[HsqlProperties.indexClass].equals("String")
               && ((Integer) row[HsqlProperties.indexType]).intValue()
                  == SQL_PROPERTY;
    }

    public boolean setDatabaseProperty(String key, String value) {

        Object[] meta  = (Object[]) dbMeta.get(key);
        String   error = HsqlProperties.validateProperty(key, value, meta);

        if (error != null) {
            return false;
        }

        stringProps.put(key, value);

        return true;
    }

    public int getDefaultWriteDelay() {

        // OOo related code
        if (database.logger.isStoredFileAccess()) {
            return 2000;
        }

        // OOo end
        return 500;
    }

//---------------------
// new properties to review / persist
    public static final int NO_MESSAGE = 1;

    public int getErrorLevel() {
        return NO_MESSAGE;
    }

    public boolean divisionByZero() {
        return false;
    }

//------------------------
    public void setDBModified(int mode) {

        String value;

        switch (mode) {

            case FILES_NOT_MODIFIED :
                value = MODIFIED_NO;
                break;

            case FILES_MODIFIED :
                value = MODIFIED_YES;
                break;

            case FILES_MODIFIED_NEW :
                value = MODIFIED_YES_NEW;
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500,
                                         "HsqlDatabaseProperties");
        }

        stringProps.put(hsqldb_modified, value);
        save();
    }

    public int getDBModified() {

        String value = getStringProperty(hsqldb_modified);

        if (MODIFIED_YES.equals(value)) {
            return FILES_MODIFIED;
        } else if (MODIFIED_YES_NEW.equals(value)) {
            return FILES_MODIFIED_NEW;
        } else if (MODIFIED_NO_NEW.equals(value)) {
            return FILES_NEW;
        }

        return FILES_NOT_MODIFIED;
    }

//-----------------------
    public String getProperty(String key) {

        Object[] metaData = (Object[]) dbMeta.get(key);

        if (metaData == null) {
            throw Error.error(ErrorCode.X_42555, key);
        }

        return stringProps.getProperty(key);
    }

    /** for all types of property apart from system props */
    public String getPropertyString(String key) {

        Object[] metaData = (Object[]) dbMeta.get(key);

        if (metaData == null) {
            throw Error.error(ErrorCode.X_42555, key);
        }

        String prop = stringProps.getProperty(key);
        boolean isSystem =
            ((Integer) metaData[HsqlProperties.indexType]).intValue()
            == SYSTEM_PROPERTY;

        if (prop == null && isSystem) {
            try {
                prop = System.getProperty(key);
            } catch (SecurityException e) {}
        }

        if (prop == null) {
            Object value = metaData[HsqlProperties.indexDefaultValue];

            if (value == null) {
                return null;
            }

            return String.valueOf(value);
        }

        return prop;
    }

    public boolean isPropertyTrue(String key) {

        Boolean  value;
        Object[] metaData = (Object[]) dbMeta.get(key);

        if (metaData == null) {
            throw Error.error(ErrorCode.X_42555, key);
        }

        value = (Boolean) metaData[HsqlProperties.indexDefaultValue];

        String prop = null;
        boolean isSystem =
            ((Integer) metaData[HsqlProperties.indexType]).intValue()
            == SYSTEM_PROPERTY;

        if (isSystem) {
            try {
                prop = System.getProperty(key);
            } catch (SecurityException e) {}
        } else {
            prop = stringProps.getProperty(key);
        }

        if (prop != null) {
            value = Boolean.valueOf(prop);
        }

        return value.booleanValue();
    }

    public String getStringPropertyDefault(String key) {

        Object[] metaData = (Object[]) dbMeta.get(key);

        if (metaData == null) {
            throw Error.error(ErrorCode.X_42555, key);
        }

        return (String) metaData[HsqlProperties.indexDefaultValue];
    }

    public String getStringProperty(String key) {

        String   value;
        Object[] metaData = (Object[]) dbMeta.get(key);

        if (metaData == null) {
            throw Error.error(ErrorCode.X_42555, key);
        }

        value = (String) metaData[HsqlProperties.indexDefaultValue];

        String prop = stringProps.getProperty(key);

        if (prop != null) {
            value = prop;
        }

        return value;
    }

    public int getIntegerProperty(String key) {

        int      value;
        Object[] metaData = (Object[]) dbMeta.get(key);

        if (metaData == null) {
            throw Error.error(ErrorCode.X_42555, key);
        }

        value =
            ((Integer) metaData[HsqlProperties.indexDefaultValue]).intValue();

        String prop = stringProps.getProperty(key);

        if (prop != null) {
            try {
                value = Integer.parseInt(prop);
            } catch (NumberFormatException e) {}
        }

        return value;
    }

    public static Iterator getPropertiesMetaIterator() {
        return dbMeta.values().iterator();
    }

    public String getClientPropertiesAsString() {

        if (isPropertyTrue(jdbc_translate_tti_types)) {
            StringBuffer sb = new StringBuffer(jdbc_translate_tti_types);

            sb.append('=').append(true);

            return sb.toString();
        }

        return "";
    }

    public boolean isVersion18() {

        String version =
            getProperty(HsqlDatabaseProperties.hsqldb_cache_version,
                        THIS_VERSION);

        return version.substring(0, 4).equals("1.7.");
    }
}
