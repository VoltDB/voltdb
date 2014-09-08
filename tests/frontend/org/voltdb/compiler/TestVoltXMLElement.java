/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

package org.voltdb.compiler;

import java.util.List;

import org.hsqldb_voltpatches.VoltXMLElement;
import org.hsqldb_voltpatches.VoltXMLElement.VoltXMLDiff;

import junit.framework.TestCase;

public class TestVoltXMLElement extends TestCase {

    VoltXMLElement findNamedNode(List<VoltXMLElement> list, String name)
    {
        for (VoltXMLElement e : list) {
            if (name.equals(e.name)) {
                return e;
            }
        }
        return null;
    }

    boolean hasAttribute(List<String> list, String name)
    {
        for (String e : list) {
            if (name.equals(e)) {
                return true;
            }
        }
        return false;
    }

    public void testDiff() {
        VoltXMLElement first = new VoltXMLElement("element");
        first.attributes.put("deleted", "doesntmatter");
        first.attributes.put("remains", "doesntmatter");
        first.attributes.put("changes", "oldvalue");
        first.children.add(new VoltXMLElement("deletedchild"));
        first.children.add(new VoltXMLElement("unchangedchild"));
        VoltXMLElement changedChild1 = new VoltXMLElement("changedchild1");
        first.children.add(changedChild1);
        changedChild1.attributes.put("deleteme", "noreally");
        VoltXMLElement changedChild2 = new VoltXMLElement("changedchild2");
        first.children.add(changedChild2);
        VoltXMLElement changedGrandchild = new VoltXMLElement("changedgrandchild");
        changedChild2.children.add(changedGrandchild);
        changedGrandchild.children.add(new VoltXMLElement("doomeddescendent"));

        VoltXMLElement second = first.duplicate();
        second.attributes.remove("deleted");
        second.attributes.put("added", "doesntmatter");
        second.attributes.put("changes", "newvalue");
        second.children.add(new VoltXMLElement("addedchild"));
        second.children.remove(second.findChild("deletedchild"));
        second.findChild("changedchild1").attributes.remove("deleteme");
        VoltXMLElement temp = second.findChild("changedchild2").findChild("changedgrandchild");
        temp.children.remove(temp.findChild("doomeddescendent"));

        VoltXMLDiff diff = VoltXMLElement.computeDiff(first, second);

        List<String> addedAtt = diff.getAddedAttributes();
        assertEquals(1, addedAtt.size());
        assertTrue(hasAttribute(addedAtt, "added"));

        List<String> changedAtt = diff.getChangedAttributes();
        assertEquals(1, changedAtt.size());
        assertTrue(hasAttribute(changedAtt, "changes"));

        List<String> removedAtt = diff.getRemovedAttributes();
        assertEquals(1, removedAtt.size());
        assertTrue(hasAttribute(removedAtt, "deleted"));

        List<VoltXMLElement> added = diff.getAddedNodes();
        assertTrue(findNamedNode(added, "addedchild") != null);

        List<VoltXMLElement> removed = diff.getRemovedNodes();
        assertTrue(findNamedNode(removed, "deletedchild") != null);

        List<VoltXMLElement> changed = diff.getChangedNodes();
        assertTrue(findNamedNode(changed, "changedchild1") != null);
        assertTrue(findNamedNode(changed, "changedchild2") != null);

    }
}
