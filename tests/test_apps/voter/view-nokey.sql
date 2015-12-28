CREATE VIEW v_votes_winner2
(
  contestant_number
, num_votes
, phone_number
)
AS
   SELECT contestant_number
        , COUNT(*)
     FROM votes
 GROUP BY contestant_number
;
