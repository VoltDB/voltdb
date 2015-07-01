/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

package org.voltdb.jdbc;

import java.text.MessageFormat;
import java.util.Locale;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

public class Resources {

    private static final String BUNDLE_NAME = "org.voltdb.jdbc.LocalizedResources";
    private static final ResourceBundle RESOURCE_BUNDLE;

    private Resources() {}
    static
    {
        ResourceBundle temp = null;
        try
        {
            temp = ResourceBundle.getBundle(BUNDLE_NAME, Locale.getDefault(), Resources.class.getClassLoader());
        }
        catch (Throwable t)
        {
            try
            {
                temp = ResourceBundle.getBundle(BUNDLE_NAME);
            }
            catch (Throwable t2)
            {
                RuntimeException rt = new RuntimeException("Can't load resource bundle due to underlying exception " + t.toString());
                rt.initCause(t2);
                throw rt;
            }
        }
        finally
        {
            RESOURCE_BUNDLE = temp;
        }
    }

    /**
     * Returns the localized message for the given message key
     *
     * @param key
     *            the message key
     * @return The localized message for the key
     */
    public static String getString(String key)
    {
        if (RESOURCE_BUNDLE == null)
            throw new RuntimeException("Localized messages from resource bundle '" + BUNDLE_NAME + "' not loaded during initialization of driver.");

        try
        {
            if (key == null)
                throw new IllegalArgumentException("Message key can not be null");

            String message = RESOURCE_BUNDLE.getString(key);
            if (message == null)
                message = "Missing error message for key '" + key + "'";

            return message;
        }
        catch (MissingResourceException e)
        {
            return '!' + key + '!';
        }
    }

    public static String getString(String key, Object... args)
    {
        return MessageFormat.format(getString(key), args);
    }
}
