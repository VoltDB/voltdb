-- contestants table holds the contestants numbers (for voting) and names
CREATE TABLE contestants
(
  contestant_number integer     NOT NULL
, contestant_name   varchar(50) NOT NULL
, CONSTRAINT PK_contestants PRIMARY KEY
  (
    contestant_number
  )
);

-- votes table holds every valid vote.
--   voters are not allowed to submit more than <x> votes, x is passed to client application
CREATE TABLE votes
(
  phone_number       bigint     NOT NULL
, state              varchar(2) NOT NULL
, contestant_number  integer    NOT NULL
);

PARTITION TABLE votes ON COLUMN phone_number;

-- track how many rejected votes each phone number has done
CREATE TABLE rejected_votes_by_phone_number
(
  phone_number   bigint NOT NULL
, rejected_votes bigint NOT NULL
, CONSTRAINT PK_rejected_votes_by_phone_number PRIMARY KEY
  (
    phone_number
  )
);

PARTITION TABLE rejected_votes_by_phone_number ON COLUMN phone_number;

-- Map of Area Codes and States for geolocation classification of incoming calls
CREATE TABLE area_code_state
(
  area_code smallint   NOT NULL
, state     varchar(2) NOT NULL
, CONSTRAINT PK_area_code_state PRIMARY KEY
  (
    area_code
  )
);

-- rollup of votes by phone number, used to reject excessive voting
CREATE VIEW v_votes_by_phone_number
(
  phone_number
, num_votes
)
AS
   SELECT phone_number
        , COUNT(*)
     FROM votes
 GROUP BY phone_number
;

-- rollup of votes by contestant and state for the heat map and results
CREATE VIEW v_votes_by_contestant_number_state
(
  contestant_number
, state
, num_votes
)
AS
   SELECT contestant_number
        , state
        , COUNT(*)
     FROM votes
 GROUP BY contestant_number
        , state
;

CREATE ROLE dbuser WITH adhoc, defaultproc;
CREATE ROLE adminuser WITH sysproc,adhoc;
CREATE ROLE hockey WITH adhoc;


-- Groovy stored procedures

CREATE PROCEDURE voter.procedures.Initialize ALLOW dbuser AS ###
    // Check if the database has already been initialized
    checkStmt = new SQLStmt("SELECT COUNT(*) FROM contestants;")

    // Inserts an area code/state mapping
    insertACSStmt = new SQLStmt("INSERT INTO area_code_state VALUES (?,?);")

    // Inserts a contestant
    insertContestantStmt = new SQLStmt("INSERT INTO contestants (contestant_name, contestant_number) VALUES (?, ?);")

    // Domain data: matching lists of Area codes and States
    areaCodes = [
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
        276,236,540,802,509,360,564,206,425,253,715,920,262,414,608,304,307]

    states = [
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
        "WI","WI","WI","WV","WY"]

    transactOn = { int maxContestants, String contestants ->
        voltQueueSQL(checkStmt, EXPECT_SCALAR_LONG)
        
        long existingContestantCount = voltExecuteSQL()[0].asScalarLong()
        if (existingContestantCount != 0) return existingContestantCount

        contestants.split(",").eachWithIndex { contestant, i -> 
            voltQueueSQL(insertContestantStmt, EXPECT_SCALAR_MATCH(1), contestant, i+1)
        }
        voltExecuteSQL()

        areaCodes.eachWithIndex { areaCode, i ->
            voltQueueSQL(insertACSStmt, EXPECT_SCALAR_MATCH(1), areaCode, states[i]);
        }
        voltExecuteSQL()

        maxContestants as long
    }
### LANGUAGE GROOVY;

CREATE PROCEDURE voter.procedures.Results ALLOW dbuser AS ###
    resultStmt = new SQLStmt('''
       SELECT a.contestant_name   AS contestant_name
            , a.contestant_number AS contestant_number
            , SUM(b.num_votes)    AS total_votes
        FROM v_votes_by_contestant_number_state AS b
           , contestants AS a
       WHERE a.contestant_number = b.contestant_number
       GROUP BY a.contestant_name
              , a.contestant_number
       ORDER BY total_votes DESC
              , contestant_number ASC
              , contestant_name ASC;
    ''')

    transactOn = { 
        voltQueueSQL(resultStmt)
        voltExecuteSQL(true)
    }
### LANGUAGE GROOVY;

CREATE PROCEDURE voter.procedures.Vote ALLOW dbuser AS ###
    // potential return codes
    long VOTE_SUCCESSFUL = 0
    long ERR_INVALID_CONTESTANT = 1
    long ERR_VOTER_OVER_VOTE_LIMIT = 2

    // Checks if the vote is for a valid contestant
    checkContestantStmt = new SQLStmt(
            'SELECT contestant_number FROM contestants WHERE contestant_number = ?;')

    // Checks if the voter has exceeded their allowed number of votes
    checkVoterStmt = new SQLStmt(
            'SELECT num_votes FROM v_votes_by_phone_number WHERE phone_number = ?;')

    // Checks an area code to retrieve the corresponding state
    checkStateStmt = new SQLStmt(
            'SELECT state FROM area_code_state WHERE area_code = ?;')

    // Records a vote
    insertVoteStmt = new SQLStmt(
            'INSERT INTO votes (phone_number, state, contestant_number) VALUES (?, ?, ?);')

    // Get rejected votes count
    checkRejectedVotesStmt = new SQLStmt(
            'SELECT rejected_votes FROM rejected_votes_by_phone_number WHERE phone_number = ?;')

    // Increment the rejected votes count
    insertRejectedVotesStmt = new SQLStmt('''
            INSERT INTO rejected_votes_by_phone_number (phone_number, rejected_votes)
            VALUES (?, ?);
        ''')

    // Increment the rejected votes count
    incrementRejectedVotesStmt = new SQLStmt('''
            UPDATE rejected_votes_by_phone_number
            SET rejected_votes = rejected_votes + 1
            WHERE phone_number = ?;
        ''')

    transactOn = { long phoneNumber, int contestantNumber, long maxVotesPerPhoneNumber ->
        result = new TableBuilder(STATUS:BIGINT,REJECTED:BIGINT).table 
        // Queue up validation statements
        voltQueueSQL(checkContestantStmt, EXPECT_ZERO_OR_ONE_ROW, contestantNumber)
        voltQueueSQL(checkVoterStmt, EXPECT_ZERO_OR_ONE_ROW, phoneNumber)
        voltQueueSQL(checkStateStmt, EXPECT_ZERO_OR_ONE_ROW, (short)(phoneNumber / 10000000l))
        voltQueueSQL(checkRejectedVotesStmt, EXPECT_ZERO_OR_ONE_ROW, phoneNumber)
        VoltTable [] validation = voltExecuteSQL();

        if (validation[0].rowCount == 0) {
            result.addRow(ERR_INVALID_CONTESTANT, -1)
            return [result] as VoltTable[]
        }

        // Get rejected votes
        long rejectedVotes = 1
        if (validation[3].rowCount == 1) {
            rejectedVotes = validation[3].asScalarLong() + 1
        }

        if ((validation[1].rowCount == 1) &&
                (validation[1].asScalarLong() >= maxVotesPerPhoneNumber)) {
            if (validation[3].rowCount == 1) {
                // Increment count
                voltQueueSQL(incrementRejectedVotesStmt, phoneNumber)
            } else {
                // insert
                voltQueueSQL(insertRejectedVotesStmt, phoneNumber, 1)
            }
            voltExecuteSQL();

            result.addRow(ERR_VOTER_OVER_VOTE_LIMIT, rejectedVotes);
            return [result] as VoltTable[]
        }

        String state = (validation[2].rowCount > 0) ? validation[2].fetchRow(0).getString(0) : "XX";

        // Post the vote
        voltQueueSQL(insertVoteStmt, EXPECT_SCALAR_MATCH(1), phoneNumber, state, contestantNumber);
        voltExecuteSQL(true);

        // Set the return value to 0: successful vote
        result.addRow(VOTE_SUCCESSFUL, rejectedVotes)
        return [result] as VoltTable[]
    }
### LANGUAGE GROOVY;
PARTITION PROCEDURE Vote ON TABLE votes COLUMN phone_number;

CREATE PROCEDURE voter.procedures.ContestantWinningStates ALLOW dbuser AS ###
    resultStmt = new SQLStmt('''
        SELECT contestant_number, state, SUM(num_votes) AS num_votes
        FROM v_votes_by_contestant_number_state
        GROUP BY contestant_number, state
        ORDER BY 2 ASC, 3 DESC, 1 ASC;
    ''')

    transactOn = { int contestantNumber, int max ->
        voltQueueSQL(resultStmt)

        results = []
        state = ""

        tuplerator(voltExecuteSQL()[0]).eachRow {
            isWinning = state != it[1]
            state = it[1]

            if (isWinning && it[0] == contestantNumber) {
                results << [state: state, votes: it[2]]
            }
        }
        if (max > results.size) max = results.size
        buildTable(state:STRING, num_votes:BIGINT) {
            results.sort { a,b -> b.votes - a.votes }[0..<max].each {
                row it.state, it.votes
            }
        }
    }
### LANGUAGE GROOVY;

CREATE PROCEDURE voter.procedures.GetStateHeatmap ALLOW dbuser AS ###
    resultStmt = new SQLStmt('''
        SELECT contestant_number, state, SUM(num_votes) AS num_votes
        FROM v_votes_by_contestant_number_state
        GROUP BY contestant_number, state
        ORDER BY 2 ASC, 3 DESC, 1 ASC;
    ''')

    transactOn = {
        voltQueueSQL(resultStmt)

        state = ""

        buildTable(state:STRING, contestant_number:INTEGER, num_votes:BIGINT, is_winning:TINYINT) {
            tuplerator(voltExecuteSQL()[0]).eachRow { 
                byte isWinning = state != it.state ? (byte)1 : (byte)0
                state = it.state
                
                row state, it.contestantNumber, it.numVotes, isWinning
            }
        }
    }
### LANGUAGE GROOVY;
