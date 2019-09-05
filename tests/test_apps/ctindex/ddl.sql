file -inlinebatch END_OF_BATCH

CREATE TABLE GAME (
   id BIGINT NOT NULL,
   game_id BIGINT NOT NULL,
   score BIGINT NOT NULL,
   PRIMARY KEY (id, game_id)
);
PARTITION TABLE GAME ON COLUMN game_id;

create unique index idx_1_COUNTER on GAME (game_id,score);

END_OF_BATCH

LOAD CLASSES ctindex-procs.jar;

-- The following CREATE PROCEDURE statements can all be batched.
file -inlinebatch END_OF_2ND_BATCH

CREATE PROCEDURE PARTITION ON TABLE GAME COLUMN game_id PARAMETER 1 FROM CLASS ctindex.Insert;
CREATE PROCEDURE PARTITION ON TABLE GAME COLUMN game_id PARAMETER 0 FROM CLASS ctindex.CountStarSmaller;
CREATE PROCEDURE PARTITION ON TABLE GAME COLUMN game_id PARAMETER 0 FROM CLASS ctindex.CountStarRange;

END_OF_2ND_BATCH
