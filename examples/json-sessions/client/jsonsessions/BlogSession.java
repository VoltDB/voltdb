/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

public class BlogSession extends SessionBase {
    static final Random random = new Random();
    static final String role_lists[][] = {{"reader"}, {"author"}, {"author", "reader"}};
    String roles[];

    public BlogSession(String... roles)
    {
        super("VoltDB Blog");
        this.roles = roles;
    }

    public BlogSession()
    {
        super("VoltDB Blog");
        int randInt = random.nextInt(5000);
        if (randInt == 0) {
            roles = role_lists[2];  // only 1 in five thousand are both author and reader
        } else if (randInt < 50) {
            roles = role_lists[1];  // favor readers: only 1 out of a hundred are authors
        } else {
            roles = role_lists[0];  // the rest (99%) are readers (only)
        }
    }
}

