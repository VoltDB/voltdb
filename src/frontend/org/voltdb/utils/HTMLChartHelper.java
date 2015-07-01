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

package org.voltdb.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.TreeMap;

/**
 * Builds a chart URL using the Google Charts API. This code
 * is very specific to our memory monitoring charts for now.
 *
 */
public class HTMLChartHelper {

    public int width = 320;
    public int height = 240;
    public String title = null;
    public final List<DataSet> data = new ArrayList<DataSet>();
    public String timeLabel = "Start";
    public int megsMax = 256;

    final Map<String, String> params = new TreeMap<String, String>();

    public static class DataSet {

        public String title = "";
        public String belowcolor = "ffffff";
        public int thickness = 1;
        public int dashlength = 1;
        public int spacelength = 1;

        final Map<Long, Double> values = new TreeMap<Long, Double>();

        // returns min timestamp used
        public long[] getScaledOutput(int historyMinutes, long now, int megabytes, StringBuilder sb) {
            long cropts = now - (historyMinutes * 60 * 1000);
            double tsscale = 999.9 / (historyMinutes * 60 * 1000);

            long[] retval = { 999, 0 };

            double memscale = 999.9 / megabytes;

            for (long ts : values.keySet()) {
                if (ts < cropts) continue;
                short value = (short) ((ts - cropts) * tsscale);
                if (value < retval[0]) retval[0] = value;
                if (value > retval[1]) retval[1] = value;
                assert (value >= 0);
                assert (value < 1000);
                sb.append(value).append(",");
            }
            sb.setLength(sb.length() - 1);
            sb.append("|");

            for (Entry<Long, Double> e : values.entrySet()) {
                if (e.getKey() < cropts) continue;
                short value = (short) (e.getValue() * memscale);
                assert (value >= 0);
                assert (value < 1000);
                //encode(sb, value);
                sb.append(value).append(",");
            }
            sb.setLength(sb.length() - 1);

            return retval;
        }

        public void append(long ts, double y) {
            assert(ts > 0);
            assert(y > 0);

            values.put(ts, y);
        }
    }

    void compile(int historyMinutes, long now) {
        params.put("chs", String.format("%dx%d", width, height));
        if (title != null)
            params.put("chtt", title);
        params.put("cht", "lxy");
        params.put("chds", "0,999");
        params.put("chxt", "x,y");
        params.put("chxl", String.format("0:|%s|Now|1:||%dmb", timeLabel, megsMax));
        params.put("chg", "10,25");

        StringBuilder sb = new StringBuilder();
        sb.append("t:");

        long mints = 999;
        long maxts = 0;

        StringBuilder belowColors = new StringBuilder();
        StringBuilder lineStyles = new StringBuilder();

        sb.append("#MINTS#,#MAXTS#|0,0");
        lineStyles.append("0,1,0");
        for (int i = 0; i < data.size(); i++) {
            // do X values first
            DataSet ds = data.get(i);
            sb.append("|");
            long[] range = ds.getScaledOutput(historyMinutes, now, megsMax, sb);
            if (range[0] < mints) mints = range[0];
            if (range[1] > maxts) maxts = range[1];
            if (ds.belowcolor.compareToIgnoreCase("ffffff") != 0)
                belowColors.append(String.format("b,%s,%d,%d,0|", ds.belowcolor, i, i+1));
            lineStyles.append(String.format("|%d,%d,%d", ds.thickness, ds.dashlength, ds.spacelength));
        }

        String chd = sb.toString();
        chd = chd.replace("#MINTS#", String.valueOf(mints));
        chd = chd.replace("#MAXTS#", String.valueOf(maxts));

        params.put("chd", chd);
        belowColors.setLength(belowColors.length() - 1);
        params.put("chm", belowColors.toString());
        params.put("chls", lineStyles.toString());
    }

    public String getURL(int historyMinutes) {
        compile(historyMinutes, System.currentTimeMillis());
        StringBuilder sb = new StringBuilder();
        sb.append("http://chart.apis.google.com/chart?");
        for (Entry<String, String> e : params.entrySet()) {
            sb.append(e.getKey()).append("=");
            sb.append(e.getValue()).append("&");
            /*try { sb.append(URLEncoder.encode(e.getValue(), "UTF-8")).append("&"); }
            catch (UnsupportedEncodingException e1) {
                e1.printStackTrace();
            }*/
        }
        sb.setLength(sb.length() - 1);
        return sb.toString();
    }

    public static void main(String[] args) {
        Random r = new Random();
        HTMLChartHelper chart = new HTMLChartHelper();

        HTMLChartHelper.DataSet ds1 = new HTMLChartHelper.DataSet();
        chart.data.add(ds1);

        long now = System.currentTimeMillis();
        long then = now - 35 * 60 * 1000;

        long groupsize = 60 * 1000;

        // every five seconds for 35 minutes
        for (long i = then; i < now; i+= 5000) {
            long ts = (i / groupsize) * groupsize;
            ds1.append(ts , r.nextInt(4096));
        }

        chart.megsMax = 4096;
        String url = chart.getURL(30);
        System.out.println(url);
        System.out.printf("URL Length: %d\n", url.length());
    }
}
