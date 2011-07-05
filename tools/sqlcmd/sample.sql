-- Include definitions for the Voter catalog
FILE 'voter.definitions.sql';

-- Execute the Results proc
EXEC Results;

-- Select the contents of the Contestants table
SELECT *
  FROM contestants;
  
  