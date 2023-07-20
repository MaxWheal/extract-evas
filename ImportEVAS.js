import csv from 'csv-parser';
import fs from 'fs';

const csvPath = 'Extra-vehicular_Activity__EVA__-_US_and_Russia.csv';
const dbPath = 'nasaevas.db';

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
            
            let evaId = parseInt(row['EVA #']);

            for (const astronaut of getAstronauts(row[crewColumn])) {
                if (!(astronaut in astronauts)) {
                    astronauts[astronaut] = Object.keys(astronauts).length;
                }

                austronautMissions.push({Astronaut: astronauts[astronaut], EVA: evaId});
            }

            let splitDuration = row[durationColumn].split(':');

            evas.push({Date: Date.parse(row['Date']), Duration: parseInt(splitDuration[0]) * 60 + parseInt(splitDuration[1]), Country: countries[row[countryColumn]], Vehicle: vehicles[row[vehicleColumn]]});
        })
        .on('end', () => {
            console.log('CSV file successfully processed');
            return evas;
        });
}

importEVAs();