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
package org.voltdb.importclient.kafka.util;

import java.util.regex.Pattern;

public class BaseKafkaImporterConfig {

    public static final String CLIENT_ID = "voltdb-importer";
    public static final String GROUP_ID = "voltdb";
    public static final int KAFKA_DEFAULT_BROKER_PORT = 9092;

    // We don't allow period in topic names because we construct URIs using it
    public static final Pattern TOPIC_LEGAL_NAMES_PATTERN = Pattern.compile("[a-zA-Z0-9\\_-]+");
    public static final int TOPIC_MAX_NAME_LENGTH = 255;

}
