/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

//
// Initializes the database, pushing the list of contestants and documenting domain data (Area codes and States).
//

package voter.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;

@ProcInfo (
    singlePartition = false
)
public class Initialize extends VoltProcedure
{
    // Check if the database has already been initialized
    public final SQLStmt checkStmt = new SQLStmt("SELECT COUNT(*) FROM contestants;");

    // Inserts an area code/state mapping
    public final SQLStmt insertACSStmt = new SQLStmt("INSERT INTO area_code_state VALUES (?,?);");

    // Inserts a contestant
    public final SQLStmt insertContestantStmt = new SQLStmt("INSERT INTO contestants (contestant_name, contestant_number) VALUES (?, ?);");

    // Domain data: matching lists of Area codes and States
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

    public long run(int maxContestants, String contestants) {

        String[] contestantArray = contestants.split(",");

        voltQueueSQL(checkStmt, EXPECT_SCALAR_LONG);
        long existingContestantCount = voltExecuteSQL()[0].asScalarLong();

        // if the data is initialized, return the contestant count
        if (existingContestantCount != 0)
            return existingContestantCount;

        // initialize the data

        for (int i=0; i < maxContestants; i++)
            voltQueueSQL(insertContestantStmt, EXPECT_SCALAR_MATCH(1), contestantArray[i], i+1);
        voltExecuteSQL();

        for(int i=0;i<areaCodes.length;i++)
            voltQueueSQL(insertACSStmt, EXPECT_SCALAR_MATCH(1), areaCodes[i], states[i]);
        voltExecuteSQL();

        return maxContestants;
    }
}
