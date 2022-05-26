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

package org.voltdb.compiler;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.hsqldb_voltpatches.VoltXMLElement;
import org.hsqldb_voltpatches.VoltXMLElement.VoltXMLDiff;

import junit.framework.TestCase;

public class TestVoltXMLElement extends TestCase {

    VoltXMLElement makeNamedElement(String elementName, String attName)
    {
        VoltXMLElement e = new VoltXMLElement(elementName);
        e.attributes.put("name", attName);
        return e;
    }

    VoltXMLElement findNamedNode(List<VoltXMLElement> list, String name)
    {
        for (VoltXMLElement e : list) {
            if (name.equals(e.attributes.get("name"))) {
                return e;
            }
        }
        return null;
    }

    public void testDiff() {
        VoltXMLElement first = makeNamedElement("element", "element");
        VoltXMLElement changedChild1 = makeNamedElement("child", "changedchild1");
        first.children.add(changedChild1);
        changedChild1.attributes.put("deleteme", "noreally");
        VoltXMLElement changedChild2 = makeNamedElement("child", "changedchild2");
        first.children.add(changedChild2);
        VoltXMLElement changedGrandchild = makeNamedElement("child", "changedgrandchild");
        changedChild2.children.add(changedGrandchild);
        changedGrandchild.children.add(makeNamedElement("child", "doomeddescendent"));
        first.attributes.put("deleted", "doesntmatter");
        first.attributes.put("remains", "doesntmatter");
        first.attributes.put("changes", "oldvalue");
        first.children.add(makeNamedElement("child", "deletedchild"));
        first.children.add(makeNamedElement("child", "unchangedchild"));

        VoltXMLElement second = first.duplicate();
        second.attributes.remove("deleted");
        second.attributes.put("added", "addedval");
        second.attributes.put("changes", "newvalue");
        second.children.add(makeNamedElement("child", "addedchild"));
        second.children.remove(second.findChild("child", "deletedchild"));
        second.findChild("child", "changedchild1").attributes.remove("deleteme");
        VoltXMLElement temp = second.findChild("child", "changedchild2").findChild("child", "changedgrandchild");
        temp.children.remove(temp.findChild("child", "doomeddescendent"));


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
        assertTrue(changed.containsKey("childchangedchild1"));
        VoltXMLDiff child1 = changed.get("childchangedchild1");
        assertTrue(child1.getRemovedAttributes().contains("deleteme"));
        assertTrue(changed.containsKey("childchangedchild2"));
        VoltXMLDiff child2 = changed.get("childchangedchild2");
        assertTrue(child2.getChangedNodes().containsKey("childchangedgrandchild"));
        VoltXMLDiff grandchild = child2.getChangedNodes().get("childchangedgrandchild");
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
        VoltXMLElement first = makeNamedElement("element", "element");
        VoltXMLElement child1 = new VoltXMLElement("child");
        child1.attributes.put("value", "3");
        first.children.add(child1);
        // Same element name, no attribute "name"
        VoltXMLElement child2 = new VoltXMLElement("child");
        child2.attributes.put("value", "4");
        first.children.add(child2);

        VoltXMLElement second = makeNamedElement("element", "element");
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
        VoltXMLElement first = makeNamedElement("element", "element");
        first.children.add(makeNamedElement("first", "first"));
        first.children.add(makeNamedElement("third", "third"));
        first.children.add(makeNamedElement("fourth", "fourth"));

        VoltXMLElement second = makeNamedElement("element", "element");
        second.children.add(makeNamedElement("first", "first"));
        second.children.add(makeNamedElement("second", "second"));
        second.children.add(makeNamedElement("third", "third"));
        second.children.add(makeNamedElement("fourth", "fourth"));

        VoltXMLDiff diff = VoltXMLElement.computeDiff(first, second);
        System.out.println("diff: " + diff.toString());

        VoltXMLElement third = first.duplicate();
        third.applyDiff(diff);
        System.out.println(first.toMinString());
        System.out.println(second.toMinString());
        System.out.println(third.toMinString());
        assertEquals(second.toMinString(), third.toMinString());
    }

    public void testNoDiff()
    {
        VoltXMLElement first = makeNamedElement("element", "element");
        VoltXMLElement changedChild1 = makeNamedElement("child", "changedchild1");
        first.children.add(changedChild1);
        changedChild1.attributes.put("deleteme", "noreally");
        VoltXMLElement changedChild2 = makeNamedElement("child", "changedchild2");
        first.children.add(changedChild2);
        VoltXMLElement changedGrandchild = makeNamedElement("child", "changedgrandchild");
        changedChild2.children.add(changedGrandchild);
        changedGrandchild.children.add(makeNamedElement("child", "doomeddescendent"));
        first.attributes.put("deleted", "doesntmatter");
        first.attributes.put("remains", "doesntmatter");
        first.attributes.put("changes", "oldvalue");
        first.children.add(makeNamedElement("child", "deletedchild"));
        first.children.add(makeNamedElement("child", "unchangedchild"));

        VoltXMLElement second = first.duplicate();

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
