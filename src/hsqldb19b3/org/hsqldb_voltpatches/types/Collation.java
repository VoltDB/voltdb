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


package org.hsqldb_voltpatches.types;

import java.text.Collator;
import java.util.Locale;

import org.hsqldb_voltpatches.HsqlException;
import org.hsqldb_voltpatches.HsqlNameManager;
import org.hsqldb_voltpatches.HsqlNameManager.HsqlName;
import org.hsqldb_voltpatches.SchemaObject;
import org.hsqldb_voltpatches.Session;
import org.hsqldb_voltpatches.SqlInvariants;
import org.hsqldb_voltpatches.Tokens;
import org.hsqldb_voltpatches.TypeInvariants;
import org.hsqldb_voltpatches.error.Error;
import org.hsqldb_voltpatches.error.ErrorCode;
import org.hsqldb_voltpatches.lib.HashMap;
import org.hsqldb_voltpatches.lib.Iterator;
import org.hsqldb_voltpatches.lib.OrderedHashSet;
import org.hsqldb_voltpatches.lib.StringUtil;
import org.hsqldb_voltpatches.rights.Grantee;

/**
 * Implementation of collation support for CHAR and VARCHAR data.
 *
 * @author Frand Schoenheit (frank.schoenheit@sun dot com)
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.0
 * @since 1.8.0
 */
public class Collation implements SchemaObject {

    static String               defaultCollationName = "SQL_TEXT";
    static String defaultIgnoreCaseCollationName     = "SQL_TEXT_UCC";
    public static final HashMap nameToJavaName       = new HashMap(101);
    public static final HashMap dbNameToJavaName     = new HashMap(101);
    public static final HashMap dbNameToCollation    = new HashMap(11);

    static {
        nameToJavaName.put("Afrikaans", "af-ZA");
        nameToJavaName.put("Amharic", "am-ET");
        nameToJavaName.put("Arabic", "ar");
        nameToJavaName.put("Assamese", "as-IN");
        nameToJavaName.put("Azerbaijani_Latin", "az-AZ");
        nameToJavaName.put("Azerbaijani_Cyrillic", "az-cyrillic");
        nameToJavaName.put("Belarusian", "be-BY");
        nameToJavaName.put("Bulgarian", "bg-BG");
        nameToJavaName.put("Bengali", "bn-IN");
        nameToJavaName.put("Tibetan", "bo-CN");
        nameToJavaName.put("Bosnian", "bs-BA");
        nameToJavaName.put("Catalan", "ca-ES");
        nameToJavaName.put("Czech", "cs-CZ");
        nameToJavaName.put("Welsh", "cy-GB");
        nameToJavaName.put("Danish", "da-DK");
        nameToJavaName.put("German", "de-DE");
        nameToJavaName.put("Greek", "el-GR");
        nameToJavaName.put("Latin1_General", "en-US");
        nameToJavaName.put("English", "en-US");
        nameToJavaName.put("Spanish", "es-ES");
        nameToJavaName.put("Estonian", "et-EE");
        nameToJavaName.put("Basque", "eu");
        nameToJavaName.put("Finnish", "fi-FI");
        nameToJavaName.put("French", "fr-FR");
        nameToJavaName.put("Guarani", "gn-PY");
        nameToJavaName.put("Gujarati", "gu-IN");
        nameToJavaName.put("Hausa", "ha-NG");
        nameToJavaName.put("Hebrew", "he-IL");
        nameToJavaName.put("Hindi", "hi-IN");
        nameToJavaName.put("Croatian", "hr-HR");
        nameToJavaName.put("Hungarian", "hu-HU");
        nameToJavaName.put("Armenian", "hy-AM");
        nameToJavaName.put("Indonesian", "id-ID");
        nameToJavaName.put("Igbo", "ig-NG");
        nameToJavaName.put("Icelandic", "is-IS");
        nameToJavaName.put("Italian", "it-IT");
        nameToJavaName.put("Inuktitut", "iu-CA");
        nameToJavaName.put("Japanese", "ja-JP");
        nameToJavaName.put("Georgian", "ka-GE");
        nameToJavaName.put("Kazakh", "kk-KZ");
        nameToJavaName.put("Khmer", "km-KH");
        nameToJavaName.put("Kannada", "kn-IN");
        nameToJavaName.put("Korean", "ko-KR");
        nameToJavaName.put("Konkani", "kok-IN");
        nameToJavaName.put("Kashmiri", "ks");
        nameToJavaName.put("Kirghiz", "ky-KG");
        nameToJavaName.put("Lao", "lo-LA");
        nameToJavaName.put("Lithuanian", "lt-LT");
        nameToJavaName.put("Latvian", "lv-LV");
        nameToJavaName.put("Maori", "mi-NZ");
        nameToJavaName.put("Macedonian", "mk-MK");
        nameToJavaName.put("Malayalam", "ml-IN");
        nameToJavaName.put("Mongolian", "mn-MN");
        nameToJavaName.put("Manipuri", "mni-IN");
        nameToJavaName.put("Marathi", "mr-IN");
        nameToJavaName.put("Malay", "ms-MY");
        nameToJavaName.put("Maltese", "mt-MT");
        nameToJavaName.put("Burmese", "my-MM");
        nameToJavaName.put("Danish_Norwegian", "nb-NO");
        nameToJavaName.put("Nepali", "ne-NP");
        nameToJavaName.put("Dutch", "nl-NL");
        nameToJavaName.put("Norwegian", "nn-NO");
        nameToJavaName.put("Oriya", "or-IN");
        nameToJavaName.put("Punjabi", "pa-IN");
        nameToJavaName.put("Polish", "pl-PL");
        nameToJavaName.put("Pashto", "ps-AF");
        nameToJavaName.put("Portuguese", "pt-PT");
        nameToJavaName.put("Romanian", "ro-RO");
        nameToJavaName.put("Russian", "ru-RU");
        nameToJavaName.put("Sanskrit", "sa-IN");
        nameToJavaName.put("Sindhi", "sd-IN");
        nameToJavaName.put("Slovak", "sk-SK");
        nameToJavaName.put("Slovenian", "sl-SI");
        nameToJavaName.put("Somali", "so-SO");
        nameToJavaName.put("Albanian", "sq-AL");
        nameToJavaName.put("Serbian_Cyrillic", "sr-YU");
        nameToJavaName.put("Serbian_Latin", "sh-BA");
        nameToJavaName.put("Swedish", "sv-SE");
        nameToJavaName.put("Swahili", "sw-KE");
        nameToJavaName.put("Tamil", "ta-IN");
        nameToJavaName.put("Telugu", "te-IN");
        nameToJavaName.put("Tajik", "tg-TJ");
        nameToJavaName.put("Thai", "th-TH");
        nameToJavaName.put("Turkmen", "tk-TM");
        nameToJavaName.put("Tswana", "tn-BW");
        nameToJavaName.put("Turkish", "tr-TR");
        nameToJavaName.put("Tatar", "tt-RU");
        nameToJavaName.put("Ukrainian", "uk-UA");
        nameToJavaName.put("Urdu", "ur-PK");
        nameToJavaName.put("Uzbek_Latin", "uz-UZ");
        nameToJavaName.put("Venda", "ven-ZA");
        nameToJavaName.put("Vietnamese", "vi-VN");
        nameToJavaName.put("Yoruba", "yo-NG");
        nameToJavaName.put("Chinese", "zh-CN");
        nameToJavaName.put("Zulu", "zu-ZA");

        //
        Iterator it = nameToJavaName.values().iterator();

        while (it.hasNext()) {
            String javaName = (String) it.next();
            String dbName = javaName.replace('-',
                                             '_').toUpperCase(Locale.ENGLISH);

            dbNameToJavaName.put(dbName, javaName);
        }
    }

    static final Collation defaultCollation           = new Collation(true);
    static final Collation defaultIgnoreCaseCollation = new Collation(false);

    static {
        defaultCollation.charset           = Charset.SQL_TEXT;
        defaultIgnoreCaseCollation.charset = Charset.SQL_TEXT;

        dbNameToCollation.put(defaultCollationName, defaultCollation);
        dbNameToCollation.put(defaultIgnoreCaseCollationName,
                              defaultIgnoreCaseCollation);
    }

    final HsqlName   name;
    private Collator collator;
    private Locale   locale;
    private boolean  isUnicodeSimple;
    private boolean  isUpperCaseCompare;
    private boolean  isFinal;
    private boolean  padSpace = true;

    //
    private Charset  charset;
    private HsqlName sourceName;

    private Collation(boolean simple) {

        String nameString = simple ? defaultCollationName
                                   : defaultIgnoreCaseCollationName;

        locale = Locale.ENGLISH;
        name = HsqlNameManager.newInfoSchemaObjectName(nameString, false,
                SchemaObject.COLLATION);
        isUnicodeSimple = simple;
        isFinal         = true;
    }

    private Collation(String name, String language, String country,
                      int strength, int decomposition, boolean ucc) {

        locale   = new Locale(language, country);
        collator = Collator.getInstance(locale);

        if (strength >= 0) {
            collator.setStrength(strength);
        }

        if (decomposition >= 0) {
            collator.setDecomposition(decomposition);
        }

        strength        = collator.getStrength();
        isUnicodeSimple = false;
        this.name = HsqlNameManager.newInfoSchemaObjectName(name, true,
                SchemaObject.COLLATION);
        charset            = Charset.SQL_TEXT;
        isUpperCaseCompare = ucc;
        isFinal            = true;
    }

    public Collation(HsqlName name, Collation source, Charset charset,
                     Boolean padSpace) {

        this.name            = name;
        this.locale          = source.locale;
        this.collator        = source.collator;
        this.isUnicodeSimple = source.isUnicodeSimple;
        this.isFinal         = true;

        //
        this.charset    = charset;
        this.sourceName = source.name;

        if (padSpace != null) {
            this.padSpace = padSpace.booleanValue();
        }
    }

    public static Collation getDefaultInstance() {
        return defaultCollation;
    }

    public static Collation getDefaultIgnoreCaseInstance() {
        return defaultIgnoreCaseCollation;
    }

    public static Collation newDatabaseInstance() {

        Collation collation = new Collation(true);

        collation.isFinal = false;

        return collation;
    }

    public static Iterator getCollationsIterator() {
        return nameToJavaName.keySet().iterator();
    }

    public static Iterator getLocalesIterator() {
        return nameToJavaName.values().iterator();
    }

    public synchronized static Collation getCollation(String name) {

        Collation collation = (Collation) dbNameToCollation.get(name);

        if (collation != null) {
            return collation;
        }

        collation = getNewCollation(name);

        dbNameToCollation.put(name, collation);

        return collation;
    }

    public synchronized static Collation getUpperCaseCompareCollation(
            Collation source) {

        if (defaultCollationName.equals(source.name.name)
                || defaultIgnoreCaseCollationName.equals(source.name.name)) {
            return defaultIgnoreCaseCollation;
        }

        if (source.isUpperCaseCompare) {
            return source;
        }

        String name = source.getName().name;

        if (name.contains(" UCC")) {
            return source;
        }

        name = name + " UCC";

        return getCollation(name);
    }

    private static Collation getNewCollation(String name) {

        int      strength      = -1;
        int      decomposition = -1;
        boolean  ucc           = false;
        String[] parts         = StringUtil.split(name, " ");
        String   dbName        = parts[0];
        int      index         = 1;
        int      limit         = parts.length;

        if (parts.length > index && "UCC".equals(parts[limit - 1])) {
            ucc = true;

            limit--;
        }

        if (index < limit) {
            strength = Integer.parseInt(parts[index]);

            index++;
        }

        if (index < limit) {
            decomposition = Integer.parseInt(parts[index]);

            index++;
        }

        if (index < limit) {
            throw Error.error(ErrorCode.X_42501, name);
        }

        String javaName = (String) dbNameToJavaName.get(dbName);

        if (javaName == null) {
            javaName = (String) nameToJavaName.get(dbName);

            if (javaName == null) {
                throw Error.error(ErrorCode.X_42501, dbName);
            }
        }

        parts = StringUtil.split(javaName, "-");

        String language = parts[0];
        String country  = parts.length == 2 ? parts[1]
                                            : "";

        return new Collation(name, language, country, strength, decomposition,
                             ucc);
    }

    public void setPadding(boolean padSpace) {

        if (isFinal) {
            throw Error.error(ErrorCode.X_42503);
        }

        this.padSpace = padSpace;
    }

    public void setCollationAsLocale() {

        Locale locale   = Locale.getDefault();
        String language = locale.getDisplayLanguage(Locale.ENGLISH);

        try {
            setCollation(language, false);
        } catch (HsqlException e) {}
    }

    public void setCollation(String newName, Boolean padSpace) {

        if (isFinal) {
            throw Error.error(ErrorCode.X_42503, newName);
        }

        Collation newCollation = Collation.getCollation(newName);

        this.name.rename(newCollation.name.name, true);

        this.locale          = newCollation.locale;
        this.collator        = newCollation.collator;
        this.isUnicodeSimple = newCollation.isUnicodeSimple;
        this.padSpace        = padSpace;
    }

    public boolean isPadSpace() {
        return padSpace;
    }

    public boolean isUnicodeSimple() {
        return isUnicodeSimple;
    }

    public boolean isUpperCaseCompare() {
        return isUpperCaseCompare;
    }

    public boolean isCaseSensitive() {

        // add support for case-sensitive language collations
        if (collator == null) {
            return isUnicodeSimple;
        } else {
            return !isUpperCaseCompare;
        }
    }

    /**
     * returns -1, 0 or +1
     */
    public int compare(String a, String b) {

        int i;

        if (collator == null) {
            if (isUnicodeSimple) {
                i = a.compareTo(b);
            } else {
                i = a.compareToIgnoreCase(b);
            }
        } else {
            if (isUpperCaseCompare) {
                i = collator.compare(toUpperCase(a), toUpperCase(b));
            } else {
                i = collator.compare(a, b);
            }
        }

        return (i == 0) ? 0
                        : (i < 0 ? -1
                                 : 1);
    }

    public String toUpperCase(String s) {
        return s.toUpperCase(locale);
    }

    public String toLowerCase(String s) {
        return s.toLowerCase(locale);
    }

    /**
     * the SQL_TEXT collation
     */
    public boolean isDefaultCollation() {
        return collator == null && isUnicodeSimple & padSpace;
    }

    /**
     * collation for individual object
     */
    public boolean isObjectCollation() {
        return isFinal;
    }

    public HsqlName getName() {
        return name;
    }

    public int getType() {
        return SchemaObject.COLLATION;
    }

    public HsqlName getSchemaName() {
        return name.schema;
    }

    public HsqlName getCatalogName() {
        return name.schema.schema;
    }

    public Grantee getOwner() {
        return name.schema.owner;
    }

    public OrderedHashSet getReferences() {
        return new OrderedHashSet();
    }

    public OrderedHashSet getComponents() {
        return null;
    }

    public void compile(Session session, SchemaObject parentObject) {}

    public String getSQL() {

        StringBuffer sb = new StringBuffer();

        sb.append(Tokens.T_CREATE).append(' ');
        sb.append(Tokens.T_COLLATION).append(' ');

        if (SqlInvariants.INFORMATION_SCHEMA.equals(name.schema.name)) {
            sb.append(name.getStatementName());
        } else {
            sb.append(name.getSchemaQualifiedStatementName());
        }

        sb.append(' ').append(Tokens.T_FOR).append(' ');

        if (SqlInvariants.INFORMATION_SCHEMA.equals(
                charset.name.schema.name)) {
            sb.append(charset.name.getStatementName());
        } else {
            sb.append(charset.name.getSchemaQualifiedStatementName());
        }

        sb.append(' ').append(Tokens.T_FROM).append(' ');
        sb.append(sourceName.statementName);
        sb.append(' ');

        if (!padSpace) {
            sb.append(Tokens.T_NO).append(' ').append(Tokens.T_PAD);
        }

        return sb.toString();
    }

    public long getChangeTimestamp() {
        return 0;
    }

    public String getCollateSQL() {

        StringBuffer sb = new StringBuffer();

        sb.append(Tokens.T_COLLATE).append(' ');
        sb.append(getName().statementName);

        return sb.toString();
    }

    public String getDatabaseCollationSQL() {

        StringBuffer sb = new StringBuffer();

        sb.append(Tokens.T_SET).append(' ');
        sb.append(Tokens.T_DATABASE).append(' ');
        sb.append(Tokens.T_COLLATION).append(' ');
        sb.append(getName().statementName);
        sb.append(' ');

        if (!padSpace) {
            sb.append(Tokens.T_NO).append(' ').append(Tokens.T_PAD);
        }

        return sb.toString();
    }
}
