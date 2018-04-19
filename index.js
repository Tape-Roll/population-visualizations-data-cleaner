"use strict";

const mongoose = require("mongoose");
const csv = require("csvtojson");

mongoose.Promise = global.Promise;
mongoose.connect("mongodb://127.0.0.1/population").catch(err => {
    console.log(err);
});

const stateSchema = mongoose.Schema({
    name: { type: String, unique: true },
    state_id: Number,
    years: [
        {
            year: Number,
            statisticsTable: {}
        }
    ],
    counties: [
        {
            name: String,
            county_id: Number,
            years: [
                {
                    year: Number,
                    statisticsTable: {}
                }
            ]
        }
    ]
});

const State = mongoose.model("State", stateSchema);

let mappings = [];
let data = {};

let state = process.argv[2];
let year = process.argv[3];
let mappingFile = "";
let locality = "";

if (state === "-s") {
    state = true;
    console.log("Parsing state data for " + year);
    mappingFile = "./data/state_mappings.csv";
    locality = "state";
} else {
    state = false;
    console.log("Parsing county data for " + year);
    mappingFile = "./data/county_mappings.csv";
    locality = "county";
}

csv({ noheader: true })
    .fromFile(mappingFile)
    .on("csv", csvRow => {})
    .on("json", json => {
        // console.log(json);
        if (!json.field1.includes("MOE")) {
            mappings[json.field1] = json.field2;
        }
    })
    .on("done", () => {
        //parsing finished
    });

csv({ noheader: false })
    .fromFile(`./data/${year}_${locality}_data.csv`)
    .on("csv", csvRow => {})
    .on("json", json => {
        if (json.GEO["display-label"] === "Geography") {
            return;
        }

        let obj = {};
        obj[locality + "_id"] = json.GEO.id2;

        if (state) {
            obj.name = json.GEO["display-label"];
        } else {
            let name = json.GEO["display-label"];
            obj.stateName = name.split(", ")[1];
            obj.name = name.split(", ")[0];
        }

        Object.keys(json).forEach(key => {
            if (mappings[key] !== undefined) {
                let categoryString = mappings[key];
                let categories = categoryString.split("; ");
                let movedFrom = "";
                let index = 2;
                if (categories[0] === "Moved") {
                    index++;
                    movedFrom = categories[1].trim();
                    switch (movedFrom) {
                        case "within same county":
                            movedFrom = "same_county";
                            break;
                        case "from different county, same state":
                            movedFrom = "moved_county";
                            break;
                        case "from different  state":
                            movedFrom = "moved_state";
                            break;
                        case "from abroad":
                            movedFrom = "abroad";
                            break;
                    }
                }
                let category = categories[index];
                if (category === "Population 1 year and over") {
                    // Population
                    obj.total_pop = json[key];
                } else if (category.includes("AGE")) {
                    // Age
                    splitAndAssign(json, key, obj, category, "age", movedFrom);
                } else if (category.includes("Median age (years)")) {
                    // Median age
                    obj.median_age = json[key];
                } else if (category.includes("SEX")) {
                    // Gender
                    splitAndAssign(json, key, obj, category, "gender", movedFrom);
                } else if (category.includes("RACE AND HISPANIC OR LATINO ORIGIN")) {
                    // Race
                    splitAndAssign(json, key, obj, category, "race", movedFrom, ["One race"]);
                } else if (category.includes("NATIVITY AND CITIZENSHIP STATUS")) {
                    // Nativity
                    splitAndAssign(json, key, obj, category, "nativity", movedFrom, [
                        "Foreign born"
                    ]);
                } else if (category.includes("MARITAL STATUS")) {
                    // Marital status
                    splitAndAssign(json, key, obj, category, "marital_status", movedFrom, [
                        "Population 15 years and over"
                    ]);
                } else if (category.includes("EDUCATIONAL ATTAINMENT")) {
                    //
                    splitAndAssign(json, key, obj, category, "education", movedFrom, [
                        "Population 25 years and over"
                    ]);
                }
            }
        });

        data[json.GEO["display-label"]] = obj;
    })
    .on("done", () => {
        //parsing finished
        Object.keys(data).forEach(key => {
            console.log("Processing " + key);
            let obj = data[key];
            let update = {};
            let query = {};

            if (state) {
                update = {
                    $push: {
                        years: { year: year, statisticsTable: obj.year }
                    }
                };

                query = {
                    name: obj.name
                };
            } else {
                update = {
                    $addToSet: {
                        "counties.$.years": {
                            year: year,
                            statisticsTable: obj.year
                        }
                    }
                };
                query.name = obj.stateName;
                query["counties.name"] = obj.name;
            }

            // Find the document
            State.findOneAndUpdate(query, update)
                .then(function(result) {
                    // If the document doesn't exist
                    if (result == null) {
                        // Create it
                        if (state) {
                            result = new State({
                                name: obj.name,
                                state_id: obj.state_id,
                                years: [
                                    {
                                        year: year,
                                        statisticsTable: obj.year
                                    }
                                ],
                                counties: []
                            });
                            console.log("Adding " + key);
                            result.save().catch(e => {
                                console.log(e);
                            });
                        } else {
                            console.log("Adding county");
                            State.findOneAndUpdate(
                                { name: obj.stateName },
                                {
                                    $addToSet: {
                                        counties: {
                                            name: obj.name,
                                            county_id: obj.county_id,
                                            years: [
                                                {
                                                    year: year,
                                                    statisticsTable: obj.year
                                                }
                                            ]
                                        }
                                    }
                                }
                            ).catch(err => {
                                console.log(err);
                            });
                        }
                    }
                })
                .catch(error => {
                    console.log(error);
                });
        });
    });

function splitAndAssign(json, key, obj, category, categoryName, moved, multi_level = []) {
    if (obj.year === undefined) {
        obj.year = {};
    }
    obj = obj.year;

    let cats = category.split(" - ");

    let index = 1;

    if (multi_level.includes(cats[1])) {
        index++;
    }

    if (moved !== "") {
        if (obj[moved] === undefined) {
            obj[moved] = {};
        }

        obj = obj[moved];
    } else {
        categoryName = "total_" + categoryName;
    }

    if (obj[categoryName] === undefined) {
        obj[categoryName] = {};
    }

    if (cats[index] !== undefined) {
        obj[categoryName][cats[index]] = json[key];
    }
}
