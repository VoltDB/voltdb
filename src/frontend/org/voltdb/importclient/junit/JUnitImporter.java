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

package org.voltdb.importclient.junit;

import com.google_voltpatches.common.base.Preconditions;
import org.voltdb.importer.AbstractImporter;

import java.net.URI;

import static org.voltdb.importclient.junit.JUnitImporter.Event.*;

/**
 * Created by bshaw on 5/31/17.
 */
public class JUnitImporter extends AbstractImporter {

    public static final int DEFAULT_IMPORTER_SLEEP_DURATION_MS = 250;

    public enum Event {
        CONSTRUCTED,
        ACCEPT_CALLED,
        STOP_CALLED,
        NO_LONGER_RUNNING
    }

    private JUnitImporterConfig m_config;

    private JUnitImporterMessenger getMessenger() {
        JUnitImporterMessenger messenger = JUnitImporterMessenger.instance();
        Preconditions.checkState(messenger != null);
        return messenger;
    }

    public JUnitImporter(JUnitImporterConfig config) {
        m_config = config;
        getMessenger().produceEvent(this, CONSTRUCTED);
    }

    @Override
    public String getName() {
        return "JUnitImporter";
    }

    @Override
    public URI getResourceID() {
        return m_config.getResourceID();
    }

    @Override
    protected void accept() {
        getMessenger().produceEvent(this, ACCEPT_CALLED);
        while (shouldRun()) {
            try {
                Thread.sleep(m_config.getSleepDurationMs());
            } catch (InterruptedException ignoreMe){
            }
        }
        getMessenger().produceEvent(this, NO_LONGER_RUNNING);
    }

    @Override
    protected void stop() {
        getMessenger().produceEvent(this, STOP_CALLED);
    }
}
