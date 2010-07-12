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


package org.hsqldb_voltpatches.util;

import java.util.Vector;
import java.awt.Choice;
import java.awt.Color;

/**
 * Class declaration
 *
 *
 * @author ulrivo@users
 * @version 1.0.0
 */

// a Choice for the GUI which implements ZaurusComponent
//
// in addition to a Choice, a ZaurusChoice saves a vector for values of the items
// for instance: in the choice list, there are the labels A,B,C
// the corresponding values are 100,200,300
// during the defintion process, the values are provided by a second argument to add
// getContent() answers the value !!
public class ZaurusChoice extends Choice implements ZaurusComponent {

    private static final int MaxLenInZChoice = 15;
    Vector                   values;
    int                      countChanges;

    public ZaurusChoice() {

        super();

        values       = new Vector(20);
        countChanges = 0;
    }

    // restrict strings for the choice to MaxLenInZChoice characters
    public void add(String item, String value) {

        int maxChar = MaxLenInZChoice;

        if (item.length() < MaxLenInZChoice) {
            maxChar = item.length();
        }

        super.add(item.substring(0, maxChar));
        values.addElement(value);
    }

    public void clearChanges() {
        countChanges = 0;
    }

    public void clearContent() {
        super.select(0);
    }

    public String getContent() {
        return (String) values.elementAt(super.getSelectedIndex());
    }

    public boolean hasChanged() {
        return countChanges > 0;
    }

    public void requestFocus() {
        super.requestFocus();
    }

    public void setChanged() {
        countChanges++;
    }

    // set the choice to the element in choice of the corresponding value
    public void setContent(String s) {
        super.select(this.findValue(s));
    }

    public void setEditable(boolean b) {

        super.setEnabled(b);

        if (b) {
            super.setBackground(Color.white);
        } else {
            super.setBackground(Color.lightGray);
        }    // end of if (b)else
    }

    // find for a given value the index in values
    private int findValue(String s) {

        for (int i = 0; i < values.size(); i++) {
            if (s.equals(values.elementAt(i))) {
                return i;
            }    // end of if (s.equals(values.elementAt(i)))
        }        // end of for (int i=0; i<values.size(); i++)

        return -1;
    }
}
