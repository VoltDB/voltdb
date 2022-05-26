/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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

package org.voltdb.utils;

import java.util.ArrayList;
import java.util.List;

import org.voltdb.task.Initializable;

import com.google_voltpatches.common.base.Joiner;

/**
 * Helper that can be used to collect multiple errors that are encountered, e.g. while validating the parameters of any
 * {@link Initializable} class. This enables implementing a validation logic that will not stop at the first error, and
 * return a message compounding multiple error messages.
 */
public final class CompoundErrors {
    private List<String> m_errors = null;

    /**
     * Add a new error message to the set of errors being returned from this validation
     *
     * @param error message to add. If {@code null} the message will be ignored
     */
    public void addErrorMessage(String error) {
        if (error == null) {
            return;
        }
        if (m_errors == null) {
            m_errors = new ArrayList<>();
        }
        m_errors.add(error);
    }

    /**
     * @return {@code true} if errors have been added to this instance
     */
    public boolean hasErrors() {
        return m_errors != null;
    }

    /**
     * Creates an error message comprised of all of the error messages added to this instance by calling
     * {@link #addErrorMessage(String)}. If no error messages were added then {@code null} is returned.
     * <p>
     * This method uses a newline ('\n') character to separate the different error messages.
     *
     * @return An error message or {@code null} if {@link #hasErrors()} returns {@code false}
     */
    public String getErrorMessage() {
        return getErrorMessage("\n");
    }

    /**
     * A specialized version of {@link #getErrorMessage()}, using a specific separator.
     *
     * @param separator String separating individual error messages in the final string.
     * @return An error message or {@code null} if {@link #hasErrors()} returns {@code false}
     */
    public String getErrorMessage(String separator) {
        if (m_errors == null) {
            return null;
        }
        return Joiner.on(separator).join(m_errors);
    }
}
