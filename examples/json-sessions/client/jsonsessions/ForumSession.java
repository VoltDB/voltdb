/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

package jsonsessions;

import java.util.Random;

public class ForumSession extends SessionBase {
    static final Random random = new Random();
    static final String version_list[] = {"v3.0", "v2.8.4.1", "v2.8.4", "v2.7", "v2.6", "v2.5"};
    static final String language_lists[][] = {{"Java"}, {"node.js"}, {"PHP"}, {"erlang"}, {"C++"}, {"C#"}, {"Ruby"}, {"C++", "Java"}};
    static final int two_language_list_index = 7;
    boolean moderator = false;
    long download_count = 0;

    public ForumSession()
    {
        super("VoltDB Forum");

        if (random.nextInt(10000) == 0) {
            moderator = true;  // make 1 out of 10,000 moderators.
        }

        // Set versions for 1/3rd of the logins
        int version_idx = random.nextInt(version_list.length*3);
        if (version_idx < version_list.length) {
            setDownloadVersion(version_list[version_idx]);
            // For one in 3,000 of those, add a second download
            version_idx = random.nextInt(version_list.length*3000);
            if (version_idx < version_list.length) {
                setDownloadVersion(version_list[version_idx]);
            }
        }

        // Set TWO programming languages, for 1 out of 5,000
        int language_idx = random.nextInt(5000);
        if (language_idx == 0) {
            setLanguages(language_lists[two_language_list_index]);
        // Set one programming language for 1/10th of the logins
        } else if (language_idx < 500) {
            setLanguages(language_lists[language_idx % two_language_list_index]);
        }
    }

    public void setDownloadVersion(String version)
    {
        addProperty("download_version", version);
        download_count++;
    }

    public void setLanguages(String[] languages)
    {
        addProperty("client_languages", languages);
    }

    public void setAvatar(String avatar)
    {
        addProperty("avatar", avatar);
    }

    public void setInProduction(boolean val)
    {
        addProperty("in_production", new Boolean(val).toString());
    }

}
