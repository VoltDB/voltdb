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
import java.util.Map;
import java.util.Set;

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

    public void testDiff() {
        VoltXMLElement first = new VoltXMLElement("element");
        VoltXMLElement changedChild1 = new VoltXMLElement("changedchild1");
        first.children.add(changedChild1);
        changedChild1.attributes.put("deleteme", "noreally");
        VoltXMLElement changedChild2 = new VoltXMLElement("changedchild2");
        first.children.add(changedChild2);
        VoltXMLElement changedGrandchild = new VoltXMLElement("changedgrandchild");
        changedChild2.children.add(changedGrandchild);
        changedGrandchild.children.add(new VoltXMLElement("doomeddescendent"));
        first.attributes.put("deleted", "doesntmatter");
        first.attributes.put("remains", "doesntmatter");
        first.attributes.put("changes", "oldvalue");
        first.children.add(new VoltXMLElement("deletedchild"));
        first.children.add(new VoltXMLElement("unchangedchild"));

        VoltXMLElement second = first.duplicate();
        second.attributes.remove("deleted");
        second.attributes.put("added", "addedval");
        second.attributes.put("changes", "newvalue");
        second.children.add(new VoltXMLElement("addedchild"));
        second.children.remove(second.findChild("deletedchilddefault"));
        second.findChild("changedchild1default").attributes.remove("deleteme");
        VoltXMLElement temp = second.findChild("changedchild2default").findChild("changedgrandchilddefault");
        temp.children.remove(temp.findChild("doomeddescendentdefault"));

        VoltXMLDiff diff = VoltXMLElement.computeDiff(first, second);

        Map<String, String> addedAtt = diff.getAddedAttributes();
        assertEquals(1, addedAtt.size());
        assertTrue(addedAtt.keySet().contains("added"));
        assertEquals("addedval", addedAtt.get("added"));

        Map<String, String> changedAtt = diff.getChangedAttributes();
        assertEquals(1, changedAtt.size());
        assertTrue(changedAtt.keySet().contains("changes"));
        assertEquals("newvalue", changedAtt.get("changes"));

        Set<String> removedAtt = diff.getRemovedAttributes();
        assertEquals(1, removedAtt.size());
        assertTrue(removedAtt.contains("deleted"));

        List<VoltXMLElement> added = diff.getAddedNodes();
        assertEquals(1, added.size());
        assertTrue(findNamedNode(added, "addedchild") != null);

        List<VoltXMLElement> removed = diff.getRemovedNodes();
        assertEquals(1, removed.size());
        assertTrue(findNamedNode(removed, "deletedchild") != null);

        Map<String, VoltXMLDiff> changed = diff.getChangedNodes();
        assertEquals(2, changed.size());
        assertTrue(changed.containsKey("changedchild1default"));
        VoltXMLDiff child1 = changed.get("changedchild1default");
        assertTrue(child1.getRemovedAttributes().contains("deleteme"));
        assertTrue(changed.containsKey("changedchild2default"));
        VoltXMLDiff child2 = changed.get("changedchild2default");
        assertTrue(child2.getChangedNodes().containsKey("changedgrandchilddefault"));
        VoltXMLDiff grandchild = child2.getChangedNodes().get("changedgrandchilddefault");
        assertTrue(findNamedNode(grandchild.getRemovedNodes(), "doomeddescendent") != null);

        VoltXMLElement third = first.duplicate();
        third.applyDiff(diff);
        System.out.println(first.toMinString());
        System.out.println(second.toMinString());
        System.out.println(third.toMinString());
        assertEquals(second.toMinString(), third.toMinString());
    }

    public void testDupeChild()
    {
        VoltXMLElement first = new VoltXMLElement("element");
        VoltXMLElement child1 = new VoltXMLElement("child");
        child1.attributes.put("value", "3");
        first.children.add(child1);
        // Same element name, no attribute "name"
        VoltXMLElement child2 = new VoltXMLElement("child");
        child2.attributes.put("value", "4");
        first.children.add(child2);

        VoltXMLElement second = new VoltXMLElement("element");
        VoltXMLElement child1s = new VoltXMLElement("child");
        child1s.attributes.put("value", "5");
        second.children.add(child1s);
        // Same element name, no attribute "name"
        VoltXMLElement child2s = new VoltXMLElement("child");
        child2s.attributes.put("value", "6");
        second.children.add(child2s);

        VoltXMLDiff diff = VoltXMLElement.computeDiff(first, second);
        System.out.println("diff: " + diff.toString());

        VoltXMLElement third = first.duplicate();
        third.applyDiff(diff);
        System.out.println(first.toMinString());
        System.out.println(second.toMinString());
        System.out.println(third.toMinString());
        assertEquals(second.toMinString(), third.toMinString());
    }

    public void testOrderFail()
    {
        VoltXMLElement first = new VoltXMLElement("element");
        first.children.add(new VoltXMLElement("first"));
        first.children.add(new VoltXMLElement("third"));
        first.children.add(new VoltXMLElement("fourth"));

        VoltXMLElement second = new VoltXMLElement("element");
        second.children.add(new VoltXMLElement("first"));
        second.children.add(new VoltXMLElement("second"));
        second.children.add(new VoltXMLElement("third"));
        second.children.add(new VoltXMLElement("fourth"));

        VoltXMLDiff diff = VoltXMLElement.computeDiff(first, second);
        System.out.println("diff: " + diff.toString());

        VoltXMLElement third = first.duplicate();
        third.applyDiff(diff);
        System.out.println(first.toMinString());
        System.out.println(second.toMinString());
        System.out.println(third.toMinString());
        assertEquals(second.toMinString(), third.toMinString());
    }
}
