CREATE TABLE evas (
    id INTEGER PRIMARY KEY,
    countryId INTEGER NOT NULL,
    vehicleId INTEGER NOT NULL,
    date DATE NOT NULL,
    duration INTEGER
);

CREATE TABLE astronautMissions (
    astronautId INTEGER,
    evaId INTEGER
);

CREATE TABLE astronauts (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE vehicles (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE countries (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);
