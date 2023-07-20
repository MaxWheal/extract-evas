import fs from 'fs';
import csv from 'csv-parser';
import sql from 'sqlite3';

const csvPath = 'Extra-vehicular_Activity__EVA__-_US_and_Russia.csv';
const dbPath = 'nasaevas.sqlite';

function* getAstronauts(inputString) {
    const regex = /([A-Za-z]+ [A-Za-z]+)/g;
    const matches = inputString.match(regex);

    if (matches) {
        for (const person of matches) {
            yield person.trim();
        }
    }
}

function importEVAs() {
    const countryColumn = 'Country';
    const vehicleColumn = 'Vehicle';
    const crewColumn = 'Crew';
    const durationColumn = 'Duration';

    let vehicles = {};
    let countries = {};
    let astronauts = {};
    let austronautMissions = [];
    let evas = [];

    fs.createReadStream(csvPath)
        .pipe(csv())
        .on('data', (row) => {
            if (!(row[countryColumn] in countries)) {
                countries[row[countryColumn]] = Object.keys(countries).length;
            }

            if (!(row[vehicleColumn] in vehicles)) {
                vehicles[row[vehicleColumn]] = Object.keys(vehicles).length;
            }

            let evaId = evas.length;

            for (const astronaut of getAstronauts(row[crewColumn])) {
                if (!(astronaut in astronauts)) {
                    astronauts[astronaut] = Object.keys(astronauts).length;
                }

                austronautMissions.push({ AstronautId: astronauts[astronaut], EVA: evaId });
            }

            let splitDuration = row[durationColumn].split(':');

            evas.push({ Id: evaId, Date: Date.parse(row['Date']), Duration: parseInt(splitDuration[0]) * 60 + parseInt(splitDuration[1]), CountryId: countries[row[countryColumn]], VehicleId: vehicles[row[vehicleColumn]] });
        })
        .on('end', () => {
            console.log('CSV file successfully processed');

            const db = new sql.Database(dbPath)

            const countryInsert = db.prepare('INSERT INTO countries (id, name) VALUES (?, ?)');
            const astronautInsert = db.prepare('INSERT INTO astronauts (id, name) VALUES (?, ?)');
            const vehicleInsert = db.prepare('INSERT INTO vehicles (id, name) VALUES (?, ?)');
            const evaInsert = db.prepare('INSERT INTO evas (id, date, duration, countryId, vehicleId) VALUES (?, ?, ?, ?, ?)');
            const astronautMissionInsert = db.prepare('INSERT INTO astronautMissions (astronautId, evaId) VALUES (?, ?)');

            for (const [key, value] of Object.entries(countries)) {
                countryInsert.run(value, key);
            }

            for (const [key, value] of Object.entries(astronauts)) {
                astronautInsert.run(value, key);
            }

            for (const [key, value] of Object.entries(vehicles)) {
                vehicleInsert.run(value, key);
            }

            evas.forEach(entry => {
                evaInsert.run(entry.Id, entry.Date, entry.Duration, entry.CountryId, entry.VehicleId);
            });

            austronautMissions.forEach(entry => {
                astronautMissionInsert.run(entry.AstronautId, entry.EVAId);
            });

            countryInsert.finalize(err => {
                if (err) {
                    console.error('Error finalizing the "countries" statement:', err.message);
                } else {
                    console.log('Countries entries inserted successfully.');
                }
                astronautInsert.finalize(err => {
                    if (err) {
                        console.error('Error finalizing the "astronauts" statement:', err.message);
                    } else {
                        console.log('Astronauts entries inserted successfully.');
                    }
                    vehicleInsert.finalize(err => {
                        if (err) {
                            console.error('Error finalizing the "vehicles" statement:', err.message);
                        } else {
                            console.log('Vehicles entries inserted successfully.');
                        }
                        evaInsert.finalize(err => {
                            if (err) {
                                console.error('Error finalizing the "evas" statement:', err.message);
                            } else {
                                console.log('Evas entries inserted successfully.');
                            }
                            astronautMissionInsert.finalize(err => {
                                if (err) {
                                    console.error('Error finalizing the "astronautMissions" statement:', err.message);
                                } else {
                                    console.log('AstronautMissions entries inserted successfully.');
                                }
                                db.close(err => {
                                    if (err) {
                                        console.error('Error closing the database:', err.message);
                                    }
                                });
                            });
                        });
                    });
                });
            });
        });
}

importEVAs();