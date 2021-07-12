-- PLAYER
create table tp_player
(
  id BIGSERIAL PRIMARY KEY,
  firstname character varying(50),
  lastname character varying(100),
  birthdate date,
  nationality character varying(3),
  lastmatchplayed date,
  markedforupdate boolean,
  linktoprofile character varying(150) UNIQUE NOT NULL
);


-- TOURNAMENT
create table tp_tournament
(
  id BIGSERIAL PRIMARY KEY,
  rank character varying(25) NOT NULL,
  startdate date NOT NULL,
  enddate date NOT NULL,
  name character varying(100) NOT NULL,
  season bigint,
  location character varying(100) NOT NULL,
  indoor boolean NOT NULL,
  surface character varying(10)
);


-- MATCH
create table tp_match
(
  id BIGSERIAL PRIMARY KEY,
  tournament_id bigint NOT NULL references tp_tournament(id),
  round character varying(50) NOT NULL,
  wonplayerrank bigint,
  lostplayerrank bigint,
  wonplayer_id bigint NOT NULL references tp_player(id),
  lostplayer_id bigint NOT NULL references tp_player(id),
  score character varying(50) NOT NULL
);
