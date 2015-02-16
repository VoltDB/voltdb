/* Copyright (c) 2001-2011, The HSQL Development Group
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


package org.hsqldb_voltpatches.lib.tar;

import org.hsqldb_voltpatches.lib.ValidatingResourceBundle;
import org.hsqldb_voltpatches.lib.RefCapableRBInterface;

/* $Id: RB.java 4141 2011-03-14 01:35:49Z fredt $ */

/**
 * Resource Bundle for Tar classes
 * <P>
 * Purpose of this class is to wrap a RefCapablePropertyResourceBundle to
 *  reliably detect any possible use of a missing property key as soon as
 *  this class is clinitted.
 * The reason for this is to allow us developers to detect all such errors
 *  before end-users ever use this class.
 * </P> <P>
 * IMPORTANT:  To add a new ResourceBundle element, add two new lines, one
 * like <PRE>
 *    public static final int NEWKEYID = keyCounter++;
 * </PRE> and one line <PRE>
 *      new Integer(KEY2), "key2",
 * </PRE>
 * Both should be inserted right after all of the other lines of the same type.
 * NEWKEYID is obviously a new constant which you will use in calling code
 * like RB.NEWKEYID.
 * </P>
 */
public enum RB implements RefCapableRBInterface {
    DbBackup_syntax,
    DbBackup_syntaxerr,
    TarGenerator_syntax,
    pad_block_write,
    cleanup_rmfail,
    TarReader_syntax,
    unsupported_entry_present,
    bpr_write,
    stream_buffer_report,
    write_queue_report,
    file_missing,
    modified_property,
    file_disappeared,
    file_changed,
    file_appeared,
    pif_malformat,
    pif_malformat_size,
    zero_write,
    pif_toobig,
    read_denied,
    compression_unknown,
    insufficient_read,
    decompression_ranout,
    move_work_file,
    cant_overwrite,
    cant_write_dir,
    no_parent_dir,
    bad_block_write_len,
    illegal_block_boundary,
    workfile_delete_fail,
    unsupported_ext,
    dest_exists,
    parent_not_dir,
    cant_write_parent,
    parent_create_fail,
    tar_field_toobig,
    missing_supp_path,
    nonfile_entry,
    read_lt_1,
    data_changed,
    unexpected_header_key,
    tarreader_syntaxerr,
    unsupported_mode,
    dir_x_conflict,
    pif_unknown_datasize,
    pif_data_toobig,
    data_size_unknown,
    extraction_exists,
    extraction_exists_notfile,
    extraction_parent_not_dir,
    extraction_parent_not_writable,
    extraction_parent_mkfail,
    write_count_mismatch,
    header_field_missing,
    checksum_mismatch,
    create_only_normal,
    bad_header_value,
    bad_numeric_header_value,
    listing_format,
    ;

    private static ValidatingResourceBundle vrb =
            new ValidatingResourceBundle(
                    RB.class.getPackage().getName() + ".rb", RB.class);
    static {
        vrb.setMissingPosValueBehavior(
                ValidatingResourceBundle.NOOP_BEHAVIOR);
        vrb.setMissingPropertyBehavior(
                ValidatingResourceBundle.NOOP_BEHAVIOR);
    }

    public String getString() {
        return vrb.getString(this);
    }
    public String toString() {
        return ValidatingResourceBundle.resourceKeyFor(this);
    }
    public String getExpandedString() {
        return vrb.getExpandedString(this);
    }
    public String getExpandedString(String... strings) {
        return vrb.getExpandedString(this, strings);
    }
    public String getString(String... strings) {
        return vrb.getString(this, strings);
    }
    public String getString(int i1) {
        return vrb.getString(this, i1);
    }
    public String getString(int i1, int i2) {
        return vrb.getString(this, i1, i2);
    }
    public String getString(int i1, int i2, int i3) {
        return vrb.getString(this, i1, i2, i3);
    }
    public String getString(int i1, String s2) {
        return vrb.getString(this, i1, s2);
    }
    public String getString(String s1, int i2) {
        return vrb.getString(this, s1, i2);
    }
    public String getString(int i1, int i2, String s3) {
        return vrb.getString(this, i1, i2, s3);
    }
    public String getString(int i1, String s2, int i3) {
        return vrb.getString(this, i1, s2, i3);
    }
    public String getString(String s1, int i2, int i3) {
        return vrb.getString(this, s1, i2, i3);
    }
    public String getString(int i1, String s2, String s3) {
        return vrb.getString(this, i1, s3, s3);
    }
    public String getString(String s1, String s2, int i3) {
        return vrb.getString(this, s1, s2, i3);
    }
    public String getString(String s1, int i2, String s3) {
        return vrb.getString(this, s1, i2, s3);
    }
}
