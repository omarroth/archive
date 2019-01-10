const rp = require("request-promise-native");
const fetch = require("node-fetch");
const dnscache = require("dnscache")({
	enable: true,
	ttl: 600,
	cachesize: 100
});

const configPath = "./config-crawler.json";
const config = require(configPath);

const invidious = config.invidious;

Array.prototype.random = function() {
	return this[Math.floor(Math.random()*this.length)];
}

function progressBar(progress, max, length) {
	let bars = Math.floor(progress/max*length);
	return progress.toString().padStart(max.toString().length, " ")+"/"+max+" ["+"=".repeat(bars)+" ".repeat(length-bars)+"]";
}

function sp(object, path, def) {
	for (let entry of path.split(".")) {
		if (object != undefined) {
			object = object[entry];
		} else {
			object = def;
			break;
		}
	}
	return object;
}

Array.prototype.unique = function() {
	return this.filter((item, index, array) => !array.slice(0, index).includes(item));
}

class LockManager {
	constructor(limit, debug) {
		this.limit = limit;
		this.debug = debug;
		this.locked = 0;
		this.queue = [];
	}
	log(message) {
		if (this.debug) console.log(message);
	}
	waitForUnlock(callback) {
		this.log("WAIT FOR UNLOCK CALLED");
		if (this.locked < this.limit) {
			this.log("PROCEEDING");
			this.lock();
			callback();
		} else {
			this.log("WAITING");
			this.queue.push(() => {
				this.log("WAIT OVER, RETRYING");
				this.waitForUnlock(callback);
			});
		}
	}
	lock() {
		this.log("LOCKED");
		this.locked++;
	}
	unlock() {
		this.log("UNLOCKED");
		this.locked--;
		if (this.queue.length) {
			this.log("STARTING QUEUE");
			setImmediate(() => this.queue.shift()());
		}
	}
	promise() {
		return new Promise(resolve => this.waitForUnlock(resolve));
	}
}
let invidiousLocker = new LockManager(config.invidiousGlobalConcurrentLimit);

function untilItWorks(code) {
	return new Promise(resolve => {
		code().then(resolve).catch(err => {
			console.log("Something didn't work, but hopefully will next time.");
			console.log(err);
			resolve(untilItWorks(code));
		});
	});
}

const methods = [
	{
		name: "No Views",
		blacklisted: false,
		code: async () => {
			console.log("Using No Views for starting ID");
			let data = {channels: [], videos: []};
			let body = await rp("https://www.randomlyinspired.com/noviews");
			let match = body.match(/embed\/([\w-]+)/);
			if (!match) throw new Error("No Views page scrape failed");
			data.videos.push(match[1]);
			console.log("No Views gave us these IDs:", data.videos);
			return data;
		}
	},{
		name: "Random word",
		blacklisted: false,
		code: async () => {
			console.log("Using random word for starting ID");
			let data = {channels: [], videos: []};
			let words = await rp({
				url: "https://random-word.ryanrk.com/api/en/word/random",
				json: true
			});
			let sort = ["relevance", "rating", "upload_date", "view_count"].random();
			await invidiousLocker.promise();
			let results = await rp({
				url: invidious+"/api/v1/search?q="+words[0]+"&sort_by="+sort,
				json: true
			});
			invidiousLocker.unlock();
			for (let result of results) {
				data.videos.push(result.videoId);
				data.channels.push(result.authorId);
			}
			console.log("Search gave "+data.videos.length+" results");
			return data;
		}
	}
];

const crawlers = {
	videos: ids => {
		if (!ids || !ids.length) throw new Error("Video crawler was given nothing to crawl");
		console.log("Crawling "+ids.length+" videos for recommendations");
		let progress = 0;
		function writeProgress(done) {
			process.stdout.write("\r"+progressBar(progress, ids.length, 50));
			if (done) process.stdout.write("\n");
		}
		writeProgress();
		return Promise.all(
			ids.map(async id => {
				await invidiousLocker.promise();
				let result = await untilItWorks(() => rp({
					url: invidious+"/api/v1/videos/"+id,
					json: true,
					timeout: 20000
				}));
				invidiousLocker.unlock();
				progress++;
				writeProgress();
				return result;
			})
		).then(results => {
			writeProgress(true);
			let data = {videos: [], channels: []};
			for (let result of results) {
				data.videos = data.videos.concat(result.recommendedVideos.map(v => v.videoId));
				data.channels.push(result.authorId);
			}
			console.log(`Gathered ${data.videos.length}/${data.channels.length} recommendations`);
			return submitAndCrawl(data);
		});
	}
}

function selectBestMethod() {
	let method;
	// Filter out blacklisted methods
	let pool = methods.filter(m => !m.blacklisted);
	// If pool is empty, remove blacklist and retry
	if (!pool.length) {
		console.log("Resetting blacklist");
		for (let m of methods) m.blacklisted = false;
		return this();
	}
	// Check priority
	let prioritied = pool.filter(m => m.priority);
	if (prioritied.length) {
		console.log("Using priority overrides");
		method = prioritied.sort((a, b) => (b.priority - a.priority))[0];
	}
	// Pick randomly
	else {
		method = pool.random();
	}
	// Attempt method, and retry if failed
	return new Promise(resolve => {
		console.log("Using method "+method.name);
		method.code().then(result => {
			resolve(result);
		}).catch(err => {
			console.log(err);
			console.log("Method failed, adding to blacklist and retrying");
			method.blacklisted = true;
			resolve(this());
		});
	});
}

function submitAndCrawl(data) {
	let keys = Object.keys(data).filter(key => data[key] && data[key].length);
	return Promise.all(
		keys.map(k => {
			if (data[k].length) {
				let toSubmit = {};
				toSubmit[k] = data[k].unique();
				return fetch(config.master+"/api/"+k+"/submit", {
					method: "POST",
					body: JSON.stringify(toSubmit),
					headers: {
						"Content-Type": "application/json"
					}
				}).then(res => res.json());
			} else {
				return Promise.resolve({inserted: [], def: true});
			}
		})
	).then(results => {
		let keyedResults = {};
		for (let i = 0; i < keys.length; i++) {
			keyedResults[keys[i]] = results[i];
		}
		console.log(
			`Submitted ${data.videos.length}/${data.channels.length}, `+
			`inserted ${sp(keyedResults, "videos.inserted.length", 0)}/${sp(keyedResults, "channels.inserted.length", 0)}`
		);
		if (sp(keyedResults, "videos.inserted.length", 0)) return crawlers.videos(keyedResults.videos.inserted);
	});
}

async function run() {
	while (true) {
		let data = await selectBestMethod();
		await submitAndCrawl(data);
	}
}
run();