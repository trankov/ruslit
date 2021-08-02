CREATE TABLE IF NOT EXISTS writers
    (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        writer_qid      INTEGER NOT NULL,
        writer          TEXT NOT NULL,
        birthdate       TEXT,
        deathdate       TEXT,
        birthplace_qid  INTEGER NOT NULL,
        birthplace      TEXT NOT NULL,
        geo_lat         REAL NOT NULL,
        geo_lon         REAL NOT NULL,
        ethnicity_qid   INTEGER NOT NULL,
        ethnicity       TEXT NOT NULL,
        language_qid    INTEGER NOT NULL,
        language        TEXT NOT NULL,
        article         TEXT NOT NULL

    )
;

CREATE TABLE IF NOT EXISTS visits
    (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        writer_qid      INTEGER NOT NULL,
        yearmonth       REAL NOT NULL,
        visits          INTEGER NOT NULL
    )
;

CREATE TABLE IF NOT EXISTS ratings
    (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        writer_id       INTEGER NOT NULL,
        writer_qid      INTEGER NOT NULL,
        rating          REAL NOT NULL
    )
