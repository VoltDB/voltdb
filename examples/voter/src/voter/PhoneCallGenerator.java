/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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
package voter;

import java.util.Random;

public class PhoneCallGenerator
{
    // Initialize some common constants and variables
    private static final String[] AreaCodes = "907,205,256,334,251,870,501,479,480,602,623,928,520,341,764,628,831,925,909,562,661,510,650,949,760,415,951,209,669,408,559,626,442,530,916,627,714,707,310,323,213,424,747,818,858,935,619,805,369,720,303,970,719,860,203,959,475,202,302,689,407,239,850,727,321,754,954,927,352,863,386,904,561,772,786,305,941,813,478,770,470,404,762,706,678,912,229,808,515,319,563,641,712,208,217,872,312,773,464,708,224,847,779,815,618,309,331,630,317,765,574,260,219,812,913,785,316,620,606,859,502,270,504,985,225,318,337,774,508,339,781,857,617,978,351,413,443,410,301,240,207,517,810,278,679,313,586,947,248,734,269,989,906,616,231,612,320,651,763,952,218,507,636,660,975,816,573,314,557,417,769,601,662,228,406,336,252,984,919,980,910,828,704,701,402,308,603,908,848,732,551,201,862,973,609,856,575,957,505,775,702,315,518,646,347,212,718,516,917,845,631,716,585,607,914,216,330,234,567,419,440,380,740,614,283,513,937,918,580,405,503,541,971,814,717,570,878,835,484,610,267,215,724,412,401,843,864,803,605,423,865,931,615,901,731,254,325,713,940,817,430,903,806,737,512,361,210,979,936,409,972,469,214,682,832,281,830,956,432,915,435,801,385,434,804,757,703,571,276,236,540,802,509,360,564,206,425,253,715,920,262,414,608,304,307".split(",");

    public static class PhoneCall
    {
        public final int ContestantNumber;
        public final long PhoneNumber;
        protected PhoneCall(int contestantNumber, long phoneNumber)
        {
            this.ContestantNumber = contestantNumber;
            this.PhoneNumber = phoneNumber;
        }
    }

    private final int ContestantCount;
    private final Random Rand = new Random();
    private final int[] VotingMap = new int[AreaCodes.length];
    public PhoneCallGenerator(int contestantCount)
    {
        this.ContestantCount = contestantCount;

        // This is a just a small fudge to make the geographical voting map more interesting for the benchmark!
        for(int i=0;i<VotingMap.length;i++)
            VotingMap[i] = this.Rand.nextInt(100) < 30 ? 1 : (int)(Math.abs(Math.sin(i)*this.ContestantCount) % this.ContestantCount) + 1;

    }

    /**
     * Receives/generates a simulated voting call
     * @return Call details (calling number and contestant to whom the vote is given)
     */
    public PhoneCall receive()
    {
        // For the purpose of a benchmark, issue random voting activity (including invalid votes to demonstrate transaction validationg in the database)

        // Pick a random area code for the originating phone call
        final int areaCodeIndex = (this.Rand.nextInt(AreaCodes.length) * this.Rand.nextInt(AreaCodes.length)) % AreaCodes.length;

        // Pick a contestant number - introduce an invalid contestant every 100 call or so to simulate fraud / invalid entries (something the transaction validates against)
        final int contestantNumber =   ((this.Rand.nextInt(100) % 100) == 0) ? 999 : (this.Rand.nextInt(100) > 50 ? VotingMap[areaCodeIndex] : ((this.Rand.nextInt(this.ContestantCount) % this.ContestantCount) + 1));

        // Build the phone number
        final long phoneNumber = Long.parseLong(AreaCodes[areaCodeIndex])*10000000l + (Math.abs(this.Rand.nextLong()) % 9999999l);

        // Return the generated phone number
        return new PhoneCall(contestantNumber, phoneNumber);
    }
}
