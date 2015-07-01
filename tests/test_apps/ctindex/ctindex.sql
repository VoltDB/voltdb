CREATE TABLE GAME (
   id BIGINT NOT NULL,
   game_id BIGINT NOT NULL,
   score BIGINT NOT NULL,
   PRIMARY KEY (id)
);

create unique index idx_1_COUNTER on GAME (game_id,score);
