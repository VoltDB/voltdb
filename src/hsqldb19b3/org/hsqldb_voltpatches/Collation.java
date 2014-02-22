/* Copyright (c) 2001-2009, The HSQL Development Group
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


package org.hsqldb_voltpatches;

import java.text.Collator;
import java.util.Locale;

import org.hsqldb_voltpatches.HsqlNameManager.HsqlName;
import org.hsqldb_voltpatches.lib.HashMap;
import org.hsqldb_voltpatches.lib.StringUtil;
import org.hsqldb_voltpatches.lib.java.JavaSystem;
import org.hsqldb_voltpatches.rights.Grantee;
import org.hsqldb_voltpatches.lib.OrderedHashSet;

/**
 * Implementation of collation support for CHAR and VARCHAR data.
 *
 * @author Frand Schoenheit frank.schoenheit@sun.com
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.8.0
 * @since 1.8.0
 */
public class Collation implements SchemaObject {

    public static final HashMap nameToJavaName = new HashMap(101);

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
    }

    final static Collation defaultCollation = new Collation();
    final HsqlName         name;
    Collator               collator;
    Locale                 locale;
    boolean                equalIsIdentical = true;

    public Collation() {

        locale = Locale.ENGLISH;

        String language = locale.getDisplayLanguage(Locale.ENGLISH);

        name = HsqlNameManager.newInfoSchemaObjectName(language, true,
                SchemaObject.COLLATION);
    }

    public HsqlName getName() {
        return name;
    }

    public static Collation getDefaultInstance() {
        return defaultCollation;
    }

    public static org.hsqldb_voltpatches.lib.Iterator getCollationsIterator() {
        return nameToJavaName.keySet().iterator();
    }

    public static org.hsqldb_voltpatches.lib.Iterator getLocalesIterator() {
        return nameToJavaName.values().iterator();
    }

    public void setCollationAsLocale() {

        Locale locale   = Locale.getDefault();
        String language = locale.getDisplayLanguage(Locale.ENGLISH);

        try {
            setCollation(language);
        } catch (HsqlException e) {}
    }

    void setCollation(String newName) {

        String jname = (String) Collation.nameToJavaName.get(newName);

        if (jname == null) {
            throw Error.error(ErrorCode.X_42501, jname);
        }

        name.rename(newName, true);

        String[] parts    = StringUtil.split(jname, "-");
        String   language = parts[0];
        String   country  = parts.length == 2 ? parts[1]
                                              : "";

        locale           = new Locale(language, country);
        collator         = Collator.getInstance(locale);
        equalIsIdentical = false;
    }

    /**
     * Returns true if two equal strings always contain identical sequence of
     * characters for the current collation, e.g. English language.
     */
    public boolean isEqualAlwaysIdentical() {
        return false;
    }

    /**
     * returns -1, 0 or +1
     */
    public int compare(String a, String b) {

        int i;

        if (collator == null) {
            i = a.compareTo(b);
        } else {
            i = collator.compare(a, b);
        }

        return (i == 0) ? 0
                        : (i < 0 ? -1
                                 : 1);
    }

    public int compareIgnoreCase(String a, String b) {

        int i;

        if (collator == null) {
            i = JavaSystem.CompareIngnoreCase(a, b);
        } else {
            i = collator.compare(toUpperCase(a), toUpperCase(b));
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

    public int getType() {
        return SchemaObject.COLLATION;
    }

    public HsqlName getSchemaName() {
        return SqlInvariants.INFORMATION_SCHEMA_HSQLNAME;
    }

    public HsqlName getCatalogName() {
        return null;
    }

    public Grantee getOwner() {
        return null;
    }

    public OrderedHashSet getReferences() {
        return new OrderedHashSet();
    }

    public OrderedHashSet getComponents() {
        return null;
    }

    public void compile(Session session) {}

    public String getSQL() {
        return "";
    }
}
