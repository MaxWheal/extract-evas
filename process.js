const Db = require('mysql2-async').default
const fs = require("fs");
const { parse } = require("csv-parse");
const crypto = require('crypto');

// data set file name and location
let csv_file = '../data/Extra-vehicular_Activity__EVA__-_US_and_Russia.csv';

const db = new Db({
    multipleStatements: true,
    // host: 'localhost',
    socketPath: '/var/run/mysqld/mysqld.sock',
    user: 'root',
    password: '',
    database: 'nasaevas',
});

async function main() {
    let countries = {};
    let astronauts = {};
    let vehicles = {};

    let records = [];
    // read CSV file, start from line 2 and process each row
    const promise_csv = new Promise((resolve, reject) => { fs.createReadStream(csv_file)
        .pipe(parse({ delimiter: ',', from_line: 2 }))
        .on('data', (row) => { records.push(row); })
        .on('end', resolve) 
    });

    Promise.all([promise_csv])
    .then( async () => {
        // console.log(records);

        /* 0 : id           : can be ignored, incremental, db takes care of that
           1 : country      : save to country db, get id and save as object for further lookup
           2 : astronauts   : split on 3+ whitespace, trim, replace multiwhitespace, split again in first last
                            : store in astronaut table, get id and save in object for further lookup
           3 : vehicle.     : like country
           4 : date         : used directlly?
           5 : duration     : used directly?
           6 : purpose      : use directly -> maybe post process later
         */

        let imports = 0;
        for (const r of records) {
        // for (let m = 0; m < 1; m++) {
            // const r = records[m];
            console.log(r);

            // deal with the country
            let country_id;
            let country = r[1].trim();

            if (!(country in countries)) {
                const sqlquery = 'INSERT INTO country (name) VALUES (?)';
                const result = await db.query(sqlquery, [country])
                countries[country] = result.insertId;
            }
            country_id = countries[country];

            // deal with astronauts
            let crew = r[2];
            let crew_ids = [];
            console.log(crew);
            // split if there are multiple names
            // standardidize the space between names to three
            let names = crew.replace(/\s{3,}/g, '  ').trim().split('  ');
            for (const n of names) {
                // split first and last name
                let firstlast = n.trim().split(' ');
                console.log(firstlast);

                let oname = firstlast.join('').toLowerCase();
                if (!(oname in astronauts)) {
                    const sqlquery = 'INSERT INTO astronaut (first, last) VALUES (?, ?)';
                    const result = await db.query(sqlquery, [firstlast[0], firstlast[1]])
                    astronauts[oname] = result.insertId;
                }
                crew_ids.push(astronauts[oname]);
            }

            // vehicle
            let vehicle_id;
            let vehicle = r[3];
            vehicle = vehicle.replace(/\s{2,}/g, ' ').trim();
            if (!(vehicle in vehicles)) {
                const sqlquery = 'INSERT INTO vehicle (name) VALUES (?)';
                const result = await db.query(sqlquery, [vehicle])
                vehicles[vehicle] = result.insertId;
            }
            vehicle_id = vehicles[vehicle];

            // date
            let date = r[4];
            if (date.length == 0) {
                date = null;
            } else {
                const us_format = date.split('/');
                date = us_format[2] + '-' + us_format[0] + '-' + us_format[1];
            }

            // duration
            let duration = r[5];
            if (duration.length == 0) duration = null;

            // purpose
            let purpose = r[6];

            // insert eva into db
            let sqlquery = 'INSERT INTO eva (date, duration, purpose, country_id, vehicle_id) VALUES (?, ?, ?, ?, ?)';
            const result = await db.query(sqlquery, [date, duration, purpose, country_id, vehicle_id]);
            const eva_id = result.insertId;

            // insert crew
            for (const i of crew_ids) {
                sqlquery = 'INSERT INTO crew (eva_id, astronaut_id) VALUES(?, ?)';
                const result = await db.query(sqlquery, [eva_id, i])
            }

            imports++;
            await new Promise(r => setTimeout(r, 1500));
        }

        console.log(imports, 'imports done');
    });
}

main().catch(e => console.error(e));