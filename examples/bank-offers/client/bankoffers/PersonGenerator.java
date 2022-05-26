/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

package bankoffers;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Random;



public class PersonGenerator {


    public class Person {
        String firstname;
        String lastname;
        String sex;
        String state;
        String phonenumber;
        Date dob;
    }


    private static Random rand = new Random();
    private String[] firstnames;
    private String[] lastnames;
    private static final String[] sexes = {"M","F"};

    // for generating DOB
    private static final long startDate = Timestamp.valueOf("1910-01-01 00:00:00").getTime();
    private static final long endDate = System.currentTimeMillis();
    private static final long diff = endDate - startDate + 1;
    private Calendar cal = Calendar.getInstance();

    public static final short[] areaCodes = new short[]{
        907,205,256,334,251,870,501,479,480,602,623,928,520,341,764,628,831,925,
        909,562,661,510,650,949,760,415,951,209,669,408,559,626,442,530,916,627,
        714,707,310,323,213,424,747,818,858,935,619,805,369,720,303,970,719,860,
        203,959,475,202,302,689,407,239,850,727,321,754,954,927,352,863,386,904,
        561,772,786,305,941,813,478,770,470,404,762,706,678,912,229,808,515,319,
        563,641,712,208,217,872,312,773,464,708,224,847,779,815,618,309,331,630,
        317,765,574,260,219,812,913,785,316,620,606,859,502,270,504,985,225,318,
        337,774,508,339,781,857,617,978,351,413,443,410,301,240,207,517,810,278,
        679,313,586,947,248,734,269,989,906,616,231,612,320,651,763,952,218,507,
        636,660,975,816,573,314,557,417,769,601,662,228,406,336,252,984,919,980,
        910,828,704,701,402,308,603,908,848,732,551,201,862,973,609,856,575,957,
        505,775,702,315,518,646,347,212,718,516,917,845,631,716,585,607,914,216,
        330,234,567,419,440,380,740,614,283,513,937,918,580,405,503,541,971,814,
        717,570,878,835,484,610,267,215,724,412,401,843,864,803,605,423,865,931,
        615,901,731,254,325,713,940,817,430,903,806,737,512,361,210,979,936,409,
        972,469,214,682,832,281,830,956,432,915,435,801,385,434,804,757,703,571,
        276,236,540,802,509,360,564,206,425,253,715,920,262,414,608,304,307};

    public static final String[] states = new String[] {
        "AK","AL","AL","AL","AL","AR","AR","AR","AZ","AZ","AZ","AZ","AZ","CA","CA",
        "CA","CA","CA","CA","CA","CA","CA","CA","CA","CA","CA","CA","CA","CA","CA",
        "CA","CA","CA","CA","CA","CA","CA","CA","CA","CA","CA","CA","CA","CA","CA",
        "CA","CA","CA","CA","CO","CO","CO","CO","CT","CT","CT","CT","DC","DE","FL",
        "FL","FL","FL","FL","FL","FL","FL","FL","FL","FL","FL","FL","FL","FL","FL",
        "FL","FL","FL","GA","GA","GA","GA","GA","GA","GA","GA","GA","HI","IA","IA",
        "IA","IA","IA","ID","IL","IL","IL","IL","IL","IL","IL","IL","IL","IL","IL",
        "IL","IL","IL","IN","IN","IN","IN","IN","IN","KS","KS","KS","KS","KY","KY",
        "KY","KY","LA","LA","LA","LA","LA","MA","MA","MA","MA","MA","MA","MA","MA",
        "MA","MD","MD","MD","MD","ME","MI","MI","MI","MI","MI","MI","MI","MI","MI",
        "MI","MI","MI","MI","MI","MN","MN","MN","MN","MN","MN","MN","MO","MO","MO",
        "MO","MO","MO","MO","MO","MS","MS","MS","MS","MT","NC","NC","NC","NC","NC",
        "NC","NC","NC","ND","NE","NE","NH","NJ","NJ","NJ","NJ","NJ","NJ","NJ","NJ",
        "NJ","NM","NM","NM","NV","NV","NY","NY","NY","NY","NY","NY","NY","NY","NY",
        "NY","NY","NY","NY","NY","OH","OH","OH","OH","OH","OH","OH","OH","OH","OH",
        "OH","OH","OK","OK","OK","OR","OR","OR","PA","PA","PA","PA","PA","PA","PA",
        "PA","PA","PA","PA","RI","SC","SC","SC","SD","TN","TN","TN","TN","TN","TN",
        "TX","TX","TX","TX","TX","TX","TX","TX","TX","TX","TX","TX","TX","TX","TX",
        "TX","TX","TX","TX","TX","TX","TX","TX","TX","TX","UT","UT","UT","VA","VA",
        "VA","VA","VA","VA","VA","VA","VT","WA","WA","WA","WA","WA","WA","WI","WI",
        "WI","WI","WI","WV","WY"};

    // constructors
    public PersonGenerator() {
        this("data/firstnames.csv","data/lastnames.csv");
    }

    public PersonGenerator(String firstnamesFile, String lastnamesFile) {
        firstnames = fileToStringArray(firstnamesFile);
        lastnames = fileToStringArray(lastnamesFile);
    }

    // generate a random person
    public Person newPerson() {
        Person p = new Person();
        p.firstname = firstnames[rand.nextInt(firstnames.length)];
        p.lastname = lastnames[rand.nextInt(lastnames.length)];
        p.sex = sexes[rand.nextInt(2)];
        p.dob = randomDOB();

        // state and area code match
        int i = rand.nextInt(areaCodes.length);
        p.phonenumber = randomPhone(areaCodes[i]);
        p.state = states[i];

        return p;
    }

    public Date randomDOB() {
        long someTime = startDate + (long)(Math.random() * diff);
        cal.setTimeInMillis(someTime);
        cal.set(Calendar.HOUR_OF_DAY,0);
        cal.set(Calendar.MINUTE,0);
        cal.set(Calendar.SECOND,0);
        cal.set(Calendar.MILLISECOND,0);
        return cal.getTime();
    }

    public static String randomPhone(int areaCode) {
        int num2 = rand.nextInt(743);
        int num3 = rand.nextInt(10000);
        DecimalFormat df3 = new DecimalFormat("000"); // 3 digits with leading zeros
        DecimalFormat df4 = new DecimalFormat("0000"); // 4 digits with leading zeros
        return df3.format(areaCode) + "-" + df3.format(num2) + "-" + df4.format(num3);
    }

    public static String randomState() {
        return states[rand.nextInt(states.length)];
    }

    public static String[] fileToStringArray(String filename) {
        List<String> lines = new ArrayList<String>();
        try {
            FileReader fileReader = new FileReader(filename);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String line = null;
            while ((line = bufferedReader.readLine()) != null) {
                lines.add(line);
            }
            bufferedReader.close();
            System.out.println("loaded " + lines.size() + " lines from " + filename);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return lines.toArray(new String[lines.size()]);
    }

}
