const fs = require("fs");
const zlib = require("zlib");
const sqlite = require("sqlite");

const dbPath = "./db/main.db";
const templatePath = "./db/schema.sql";
const channelsPath = "./db/channels.json.gz";

(async function() {
	await new Promise(resolve => fs.unlink(dbPath, resolve)); // delete existing database
	let db = await sqlite.open(dbPath); // open database with sqlite
	let template = fs.readFileSync(templatePath, "utf8"); // load template
	let lines = template.split(";"); // split into commands
	for (let line of lines) { // run commands in turn
		line = line.replace(/\n/gm, " "); // single line
		if (line.match(/\w/)) { // if there's actually text,
			await db.run(line.replace(/\n/gm, " ")); // execute line.
		}
	}
	let gzData = fs.readFileSync(channelsPath); // load gzipped channel data
	let json = JSON.parse(zlib.gunzipSync(gzData).toString("utf8")); // unzip and parse
	await db.run("BEGIN TRANSACTION");
	await Promise.all(json.map(channel => db.run("INSERT INTO Channels VALUES (?)", channel))); // write channel data
	await db.run("END TRANSACTION");
	console.log("All done!");
})();