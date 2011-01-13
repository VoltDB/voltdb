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

import java.awt.BorderLayout;
//import java.io.IOException;
import javax.swing.*;
import javax.swing.event.TreeSelectionEvent;
import javax.swing.event.TreeSelectionListener;
import javax.swing.tree.*;
//import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogType;

public class CatalogView extends JFrame implements TreeSelectionListener {
    private static final long serialVersionUID = 1L;

    static final int INITIAL_WIDTH = 600;
    static final int INITIAL_HEIGHT = 400;

    JTree m_tree;
    JTable m_table;
    JSplitPane m_splitPane;
    String[] m_columnNames = { "field", "value" };
    FieldViewerModel m_model = new FieldViewerModel();

    Catalog m_catalog;

    CatalogView() {
        setSize(INITIAL_WIDTH, INITIAL_HEIGHT);

        /*try {
            m_catalog = TPCCProjectBuilder.getTPCCSchemaCatalog();
        } catch (IOException e) {
            e.printStackTrace();
        }*/
        m_catalog = null;
        CatalogTypeTreeNode top = new CatalogTypeTreeNode(m_catalog, null);
        m_tree = new JTree(top);
        m_tree.getSelectionModel().setSelectionMode(TreeSelectionModel.SINGLE_TREE_SELECTION);
        m_tree.addTreeSelectionListener(this);

        m_table = new JTable(m_model);

        JScrollPane scroller1 = new JScrollPane(m_tree);
        JScrollPane scroller2 = new JScrollPane(m_table);

        m_splitPane = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT,
                scroller1, scroller2);

        setLayout(new BorderLayout());
        add(m_splitPane, BorderLayout.CENTER);
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        javax.swing.SwingUtilities.invokeLater(new Runnable() {

            @Override
            public void run() {
                CatalogView view = new CatalogView();
                view.setVisible(true);
            }

        });
    }

    @Override
    public void valueChanged(TreeSelectionEvent e) {
        CatalogTypeTreeNode selection = (CatalogTypeTreeNode)m_tree.getLastSelectedPathComponent();
        if (selection == null) {
            m_model.setCatalogType(null);
        }
        else {
            CatalogType type = selection.getCatalogType();
            m_model.setCatalogType(type);
        }
        m_model.fireTableDataChanged();
    }

}
