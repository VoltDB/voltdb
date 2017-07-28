/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.importclient.lifecycletest;

import org.voltdb.importer.AbstractImporter;

import java.net.URI;

public class LifecycleTestImporter extends AbstractImporter {

    @Override
    public URI getResourceID() {
        return null;
    }

    @Override
    protected void accept() {
        m_file.write();
        try {
            synchronized (this) {
                wait();
            }
        } catch (InterruptedException unexpected) {
            warn(unexpected, "Unexpected and most likely spurious interrupt resulted in %s shutting down", getName());
        }
        m_file.delete();
    }

    @Override
    public synchronized void stop() {
        notify();
    }

    @Override
    public String getName() {
        return "LifecycleTestImporter";
    }
}
