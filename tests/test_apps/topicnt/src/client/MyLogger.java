/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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
package client;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.voltcore.logging.Level;

public class MyLogger {
    static final SimpleDateFormat LOG_DF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");

    // Ad-hoc rate limiting
    private ConcurrentHashMap<Level, Long> m_lastLogs = new ConcurrentHashMap<>();

    void error(String msg) {
        log(Level.ERROR, msg);
    }
    void errorFmt(String format, Object... args) {
        log(Level.ERROR, format, args);
    }
    void warn(String msg) {
        log(Level.WARN, msg);
    }
    void warnFmt(String format, Object... args) {
        log(Level.WARN, format, args);
    }
    void info(String msg) {
        log(Level.INFO, msg);
    }
    void infoFmt(String format, Object... args) {
        log(Level.INFO, format, args);
    }

    void rateLimitedLog(long suppressInterval, Level level, String format, Object... args) {
        long now = System.nanoTime();
        long last = m_lastLogs.getOrDefault(level, 0L);
        if (TimeUnit.NANOSECONDS.toSeconds(now - last) > suppressInterval) {
            m_lastLogs.put(level, now);
            log(level, format, args);
        }
    }

    private void log(Level level, String format, Object... args) {
        log(level, String.format(format, args));
    }
    private void log(Level level, String msg) {
        System.out.print(LOG_DF.format(new Date()));
        System.out.println(String.format(" %s: %s", level, msg));
    }
}
