/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.catalog.gui;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Enumeration;
import javax.swing.tree.TreeNode;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.CatalogType;

public class CatalogTypeTreeNode implements TreeNode {

    String m_name;
    CatalogType m_type;
    CatalogTypeTreeNode m_parent;
    ArrayList<TreeNode> m_children = new ArrayList<TreeNode>();
    boolean m_isLeaf = true;

    CatalogTypeTreeNode(CatalogType type, CatalogTypeTreeNode parent) {
        m_name = type.getTypeName();
        m_type = type;
        m_parent = parent;

        Class<?> cls = type.getClass();
        for (Method m : cls.getMethods()) {
            if (m.getReturnType() == CatalogMap.class) {
                CatalogMap<? extends CatalogType> map = null;
                try {
                    map = (CatalogMap<?>)m.invoke(type);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                String methodName = m.getName();
                methodName = methodName.substring(3);
                for (Object obj : map) {
                    CatalogType childType = (CatalogType)obj;
                    CatalogTypeTreeNode node = new CatalogTypeTreeNode(childType, this);
                    m_children.add(node);
                    m_isLeaf = false;
                }
            }
        }
    }

    public CatalogType getCatalogType() {
        return m_type;
    }

    @Override
    public Enumeration<TreeNode> children() {
        return new MapEnumerator(m_children.iterator());
    }

    @Override
    public boolean getAllowsChildren() {
        return true;
    }

    @Override
    public TreeNode getChildAt(int childIndex) {
        return m_children.get(childIndex);
    }

    @Override
    public int getChildCount() {
        return m_children.size();
    }

    @Override
    public int getIndex(TreeNode node) {
        for (int i = 0; i < m_children.size(); i++)
            if (node.equals(m_children.get(i)))
                return i;
        return -1;
    }

    @Override
    public TreeNode getParent() {
        return m_parent;
    }

    @Override
    public boolean isLeaf() {
        return m_isLeaf;
    }

    @Override
    public String toString() {
        return m_type.getClass().getSimpleName() + " : " + m_type.getTypeName();
    }

}
