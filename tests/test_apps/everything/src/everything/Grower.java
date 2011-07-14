/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

package everything;

public class Grower extends ClientRunner.SubClient {

    @Override
    public void run() {
        if (getMBRss() > 2048) {
            try { Thread.sleep(5000); } catch (InterruptedException e) {}
            return;
        }

        for (int i = 0; i < 1000; i++) {
            // add 100 rows to a random partition
            try {
                byte pval = (byte) (m_rand.nextInt() % 127);
                m_client.callProcedure(new Callback(), "Insert你好", pval, 100, "你好");
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                System.exit(-1);
            }
        }
    }

}
