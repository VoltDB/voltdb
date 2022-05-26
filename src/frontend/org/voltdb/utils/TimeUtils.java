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

import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Time-related utility routines.
 */
public class TimeUtils {

    private static final Pattern intervalPattern = Pattern.compile("\\s*(\\d+)\\s*([a-zA-Z]*)\\s*");

    /**
     * Exception class used for errors in data being converted.
     */
    public static class InvalidTimeException extends IllegalArgumentException {
        private InvalidTimeException(String msg, Object value) {
            super(String.format("Invalid time specification '%s' - %s", value, msg));
        }
        private InvalidTimeException(String msg, Object value, TimeUnit asUnit) {
            super(String.format("Invalid time specification '%s' - %s [%s]", value, msg, validFlags(asUnit)));
        }
    }

    /**
     * Structure used to hold result of parsing the usual "integer followed by
     * a unit indicator" string representation of a duration.
     */
    public static class TimeAndUnit {
        final public long number;
        final public char flag;
        final public TimeUnit unit;
        private TimeAndUnit(long n, char f, TimeUnit u) {
            number = n;
            flag = f;
            unit = u;
        }
    }

    /**
     * Converts a text string of the form "integer followed by a unit indicator"
     * to a long integer value expressed in some specified time unit.
     *
     * Supported unit indicators are s (seconds), m (minutes), h (hours),
     * d (days). Two-character forms are also permitted: ss, mn, hr, dy.
     *
     * Conversion to the specified return unit may lose precision, for example
     * if the user specifies a value in seconds, and 'asUnit' wants the return
     * value in minutes. This is potentially confusing, so units of higher
     * precision than 'asUnit' are disallowed.
     *
     * Neither 'asUnit' nor 'defaultUnit' is required to be one of the units
     * represented by s m h d. One interesting case might be to set both of
     * those arguments to 'milliseconds', in which case a plain number would
     * be interpreted in ms, but a unit could also be specified.
     *
     * @param value text string to convert
     * @param asUnit TimeUnit for return value (required)
     * @param defaultUnit default if 'value' specifies no unit (null: no default)
     * @return input value converted from specified units to 'asUnit'
     */
    public static long convertTimeAndUnit(String value, TimeUnit asUnit, TimeUnit defaultUnit) {
        TimeAndUnit tu = parseTimeAndUnit(value, asUnit, defaultUnit);
        return asUnit.convert(tu.number, tu.unit);
    }

    /**
     * Variant of convertTimeAndUnit() for returning a value
     * that is represented as a 32-bit integer.
     *
     * The range this represents depends on the specified return
     * type 'asUnit'. Some examples:
     *
     *   -- asUnit -- : -- limit --
     *   Microseconds : 35 seconds
     *   Milliseconds : 24 days
     *   Seconds      : 68 years
     *   Minutes      : 4085 years
     *
     * On overflow, returns an error message giving the limit in
     * terms of the unit used (seconds, minutes, hours, days).
     *
     * @param value text string to convert
     * @param asUnit TimeUnit for return value (required)
     * @param defaultUnit default if 'value' specifies no unit (null: no default)
     * @return input value converted from specified units to 'asUnit'
     */
    public static int convertIntTimeAndUnit(String value, TimeUnit asUnit, TimeUnit defaultUnit) {
        TimeAndUnit tu = parseTimeAndUnit(value, asUnit, defaultUnit);
        long result = asUnit.convert(tu.number, tu.unit);
        try {
            return Math.toIntExact(result);
        }
        catch (ArithmeticException ex) {
            long limit = tu.unit.convert(Integer.MAX_VALUE, asUnit);
            String info = String.format("limit is %d %s", limit, tu.unit.name().toLowerCase());
            throw new InvalidTimeException(info, value);
        }
    }

    /**
     * Parses a text string of the form "integer followed by a unit indicator"
     * to a long integer value and a standard Java TimeUnit constant.
     * No conversion is done between units.
     *
     * Supported unit indicators are s (seconds), m (minutes), h (hours),
     * d (days). Two-character forms are also permitted: ss, mn, hr, dy.
     *
     * At the caller's option, the unit indicator can be omitted, and a
     * default unit provided.
     *
     * The normalized single-character indicator is returned as 'flag'
     * in the result where possible. No flag is possible in the case where
     * a default is applied, and the precision of the default is better
     * than seconds.
     *
     * The caller must specify his intent for the return value. This
     * protects against possible loss of precision. For example, if the
     * user specifies a value in seconds, and the caller intends to convert
     * the value to minutes, this could be confusing (45 seconds turns into
     * 0 minutes). Units of higher precision than the given 'asUnit' are
     * therefore disallowed.
     *
     * @param value text string to convert
     * @param asUnit intended limit on precision (required)
     * @param defaultUnit default if 'value' specifies no unit (null: no default)
     * @return a TimeAndUnit structure holding the parsed result
     */
    public static TimeAndUnit parseTimeAndUnit(String value, TimeUnit asUnit, TimeUnit defaultUnit) {
        if (value == null) {
            throw new NullPointerException("'value' required");
        }
        if (asUnit == null) {
            throw new NullPointerException("'asUnit' required");
        }
        if (defaultUnit != null && precision(defaultUnit) > precision(asUnit)) {
            throw new IllegalArgumentException("defaultUnit precision exceeds requested return precision");
        }

        Matcher m = intervalPattern.matcher(value);
        if (!m.matches()) {
            throw new InvalidTimeException("should be integer followed by unit indicator", value, asUnit);
        }
        String numberStr = m.group(1), unitStr = m.group(2);

        long number = -1;
        try {
            number = Long.parseLong(numberStr);
        }
        catch (NumberFormatException ex) {
            throw new InvalidTimeException("invalid integer", value);
        }

        TimeUnit unit = defaultUnit;
        if (unitStr.isEmpty()) {
            if (unit == null) { // no default
                throw new InvalidTimeException("unit indicator required", value, asUnit);
            }
        }
        else {
            unit = convertTimeUnit(unitStr); // null on error
            if (unit == null || precision(unit) > precision(asUnit)) {
                throw new InvalidTimeException("invalid unit indicator", value, asUnit);
            }
        }

        char flag = 0; // no flag defined for milliseconds, etc.
        if (precision(unit) <= precision(TimeUnit.SECONDS)) {
            flag = Character.toLowerCase(unit.name().charAt(0));
        }

        return new TimeAndUnit(number, flag, unit);
    }

    /**
     * Converts a pair of text strings, an integer value and a unit
     * indicator, to a long value expressed in some specified time unit.
     *
     * Supported unit indicators are s (seconds), m (minutes), h (hours),
     * d (days). Two-character forms are also permitted: ss, mn, hr, dy.
     *
     * Conversion to the specified return unit may lose precision, for example
     * if the user specifies a value in seconds, and 'asUnit' wants the return
     * value in minutes. This is potentially confusing, so units of higher
     * precision than 'asUnit' are disallowed.
     *
     * @param numberStr text string to convert as integer
     * @param unitStr string giving units to be used
     * @param asUnit TimeUnit for return value (required)
     * @return input value converted from specified units to 'asUnit'
     */
    public static long convertSeparateTimeAndUnit(String numberStr, String unitStr, TimeUnit asUnit) {
        long number = -1;
        try {
            number = Long.parseLong(numberStr.trim());
        }
        catch (NumberFormatException ex) {
            throw new InvalidTimeException("invalid integer", numberStr);
        }

        TimeUnit unit = convertTimeUnit(unitStr.trim()); // null on error
        if (unit == null || precision(unit) > precision(asUnit)) {
            throw new InvalidTimeException("invalid unit indicator", unitStr, asUnit);
        }

        return asUnit.convert(number, unit);
    }

    /*
     * Returns a value that indicates the relative precision of a specified
     * time unit. The value will be larger for higher-precision units; thus
     * precision(SECONDS) > precision(MINUTES).
     *
     * Other than that, the value is arbitrary, and no assumption should be
     * made about the actual value.
     */
    private static long precision(TimeUnit unit) {
        return unit.convert(1, TimeUnit.DAYS);
    }

    /**
     * Given a short string representing a time unit, returns
     * the corresponding TimeUnit value.
     *
     * There are two conventions in current use in VoltDB: the
     * one-character convention (s,m,h,d) and the two-character
     * convention introduced for topics support (ss,mn,hr,dy).
     * This routine accepts both.
     *
     * It is limited to time units that have corresponding
     * units in the Java TimeUnit class; nothing larger than
     * day. Further, a day is assumed to be exactly 24 hours.
     *
     * @param flag unit indicator, one or two characters
     * @return a TimeUnit, or null if unknown flag
     */
    public static TimeUnit convertTimeUnit(String flag) {
        switch (flag.toLowerCase()) {
        case "s":
        case "ss":
            return TimeUnit.SECONDS;
        case "m":
        case "mn":
            return TimeUnit.MINUTES;
        case "h":
        case "hr":
            return TimeUnit.HOURS;
        case "d":
        case "dy":
            return TimeUnit.DAYS;
        default:
            return null;
        }
    }

    /*
     * Given a time unit indicating the target precision, returns
     * a string listing the unit indications ('flags') that are
     * valid with that precision.  For example, 's' is not valid
     * if the calling code wants a result expressed in minutes.
     *
     * This is only used in constructing error messages that
     * want to give advice to users about what is valid.
     */
    private static String validFlags(TimeUnit unit) {
        switch (unit) {
        case DAYS:
            return "d";
        case HOURS:
            return "h,d";
        case MINUTES:
            return "m,h,d";
        default:
            return "s,m,h,d";
        }
    }
}
