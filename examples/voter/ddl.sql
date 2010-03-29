-- contestants table holds the contestants numbers (for voting) and names
create table contestants (
  contestant_number  tinyint not null,
  contestant_name    varchar(50) not null,
  primary key (contestant_number));


-- votes table holds every valid vote.
--   voters are not allowed to submit more than <x> votes, x is passed to client application
create table votes (
  phone_number       bigint not null,
  contestant_number  tinyint not null);

create index idx_votes on votes (phone_number);


-- rollup of votes by phone number, used to reject excessive voting
create view v_votes_by_phone_number
  (phone_number,
   num_votes)
as select phone_number,
          count(*)
from votes
group by phone_number;

-- rollup of votes by contestant, used to determine winner
create view v_votes_by_contestant_number
  (contestant_number,
   num_votes)
as select contestant_number,
          count(*)
from votes
group by contestant_number;


