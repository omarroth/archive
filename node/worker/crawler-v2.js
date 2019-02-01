const rp = require("request-promise-native");
const fetch = require("node-fetch");
const dnscache = require("dnscache")({
	enable: true,
	ttl: 600,
	cachesize: 100
});
const Deque = require("double-ended-queue");

const configPath = "./config-crawler.json";
const config = require(configPath);

// generates a random number in range [min, max)
// that is, including min, and excluding max
function randint(min, max) {
	return Math.floor(Math.random() * (max - min) + min);
}

Array.prototype.random = function() {
	return this[randint(0, this.length)];
}

Array.prototype.shuffle = function() {
	for (let i = 0; i < this.length - 1; i++) {
		let j = randint(i, this.length);
		if (i == j) continue;
		let tmp = this[i];
		this[i] = this[j];
		this[j] = tmp;
	}
	return this;
}

let reportStr = "";
function log(...args) {
	if (reportStr.length) {
		process.stdout.write("\r" + " ".repeat(reportStr.length) + "\r");
	}
	console.log(...args);
	if (reportStr.length) {
		process.stdout.write(reportStr);
	}
}
function report(str) {
	if (str.length < reportStr.length) {
		process.stdout.write("\r" + " ".repeat(reportStr.length));
	};
	reportStr = str;
	process.stdout.write("\r"+str);
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

// https://github.com/nodejs/help/issues/711
let flatstr = function flatstr(s) {
	return (" " + s).slice(1);
}

/*
let flatstr = function flatstr(s) {
	Number(s);
	return s;

try {
	flatstr = Function("s", "return typeof s === \"string\" ? %FlattenString(s) : s");
} catch (e) {
	console.log("Native function call syntax unavailable, using Number(s) for string flattening");
}
*/ // XXX: ALL OF THIS LEAKS MEMORY WTF

class LockManager {
	constructor(limit, debug) {
		this.limit = limit;
		this.debug = debug;
		this.locked = 0;
		this.queue = [];
	}
	log(message) {
		if (this.debug) log(message);
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

function delay(time) {
	return new Promise(resolve => setTimeout(() => resolve(), time));
}

async function untilItWorks(code, options = {}) {
	let maxRetries = options.maxRetries;
	let timeout = options.timeout || 1000;
	let maxTimeout = options.maxTimeout || 5000;

	let tries = 0;
	while (true) {
		tries++;
		try {
			return await code();
		} catch (err) {
			if (maxRetries && tries > maxRetries) throw err;
			if (!options.silent) {
				log("Something didn't work, but hopefully will next time. [Attempt " + tries + "]");
				log(err);
			}
			await delay((Math.random() * 0.4 + 0.6) * timeout);
			timeout = Math.min(maxTimeout, timeout * 1.3);
		}
	}
}

const methods = [
	{
		name: "No Views",
		blacklisted: false,
		code: async () => {
			let data = {channels: [], videos: [], playlists: []};
			let body = await rp("https://www.randomlyinspired.com/noviews");
			let match = body.match(/embed\/([\w-]+)/);
			if (!match) throw new Error("No Views page scrape failed");
			data.videos.push(match[1]);
			log("No Views gave us these IDs:", data.videos);
			return data;
		}
	},{
		name: "HookTube random",
		blacklisted: false,
		code: async () => {
			let data = {channels: [], videos: [], playlists: []};
			let redirect = await rp({
				url: "https://hooktube.com/random",
				simple: false,
				resolveWithFullResponse: true,
				followRedirect: false
			});
			let match = redirect.headers.location.match(/watch\?v=([\w-]+)/);
			if (!match) throw "HookTube random failed, couldn't match location: "+redirect.headers.location;
			data.videos.push(match[1]);
			log("HookTube gave us these IDs:", data.videos);
			return data;
		}
	}
];

let submitLocker = new LockManager(10);

async function crawler() {
	let stop = false;

	const limits = {
		video: Math.floor(config.crawlLimit * 0.5),
		channel: Math.floor(config.crawlLimit * 0.3),
		playlist: Math.floor(config.crawlLimit * 0.2),
		continuation: config.crawlLimit, // essentially infinite
	}

	let work = new Deque();
	let workStats = { video: 0, playlist: 0, channel: 0, continuation: 0 };

	let submitQueue = { videos: new Set(), channels: new Set(), playlists: new Set() };

	const reporter = (async function reporter() {
		while (!stop) {
			report(`work: ${work.length} [${workStats.video}/${workStats.channel}/${workStats.playlist}]+${workStats.continuation}, submit queue: ${submitQueue.videos.size}/${submitQueue.channels.size}/${submitQueue.playlists.size}`);
			await delay(500);
		}
	})().catch(e => { log("Reporter crashed!"); stop = true; throw e; });

	function enqueue(url, type = "continuation", front = false, json = false) {
		if (!url) throw new Error("Bad URL");
		if (workStats[type] > limits[type]) return;
		workStats[type]++;
		if (front) {
			work.unshift({type: type, id: url, json: json});
		} else {
			work.push({type: type, id: url, json: json});
		}
	}
	function dequeue() {
		if (work.length == 0) {
			log("Dequeue on empty queue!");
			return;
		}
		let unit = work.shift();
		workStats[unit.type]--;
		return unit;
	}

	const workSeeder = (async function seeder() {
		while (!stop) {
			let data = await selectBestMethod();
			data.videos.forEach(id => enqueue(id, "video"));
			data.channels.forEach(id => enqueue(id, "channel", true));
			data.playlists.forEach(id => enqueue(id, "playlist", true));

			do {
				await delay(1000);
			} while (work.length > 100);
		}
	})().catch(e => { log("Seeder crashed!"); stop = true; throw e; });

	const submitter = (async function submitter() {

		function submit(target, data) {
			let chunks = [];
			for (var i = 0; i < data.length; i += 25000) {
				chunks.push(data.slice(i, i + 25000));
			}
			let completedChunks = 0;
			return Promise.all(chunks.map(async function sendChunk(chunk) {
				await submitLocker.promise();
				let res = await untilItWorks(() => fetch(config.master+"/api/"+target+"/submit", {
					method: "POST",
					body: JSON.stringify({ [target]: chunk }),
					headers: {"Content-Type": "application/json"}
				}).then(res => res.json()).then(results => {
					submitLocker.unlock();
					if (!results.inserted) {
						return Promise.reject("Bad submit result: " + JSON.stringify(results));
					}
					let ins = results.inserted || [];
					log(`[${++completedChunks}/${chunks.length}] Submitted ${target}: ${chunk.length}, inserted ${ins.length} - [${ins.slice(0,target=="videos"?10:3).join(", ")}${ins.length>(target=="videos"?10:3) ? "..." : ""}]`);
					return ins;
				}));
				for (let item in res) {
					if (!item) {
						log(results);
						throw new Error("Bad item in inserted");
					}
				}
				return res;
			})).then(results => results.reduce((acc, v) => { return acc.concat(v) }, []));
		}

		while (!stop) {
			await delay(10000);

			let videos = [...submitQueue.videos].filter(id => !idCache.seen(id));
			let channels = [...submitQueue.channels].filter(id => !chanCache.seen(id));
			let playlists = [...submitQueue.playlists].filter(id => !listCache.seen(id));
			submitQueue = { videos: new Set(), channels: new Set(), playlists: new Set() };

			log(`Submitting ${videos.length}/${channels.length}/${playlists.length}`);

			await Promise.all([submit("channels", channels), submit("videos", videos), submit("playlists", playlists)]).then(([chan, vids, lists]) => {
				let crawlVids = vids;
				let crawlChans = chan;
				let crawlLists = lists;
				log(`Submitted ${vids.length}/${chan.length}/${lists.length}`);
				if (work.length < config.crawlThreshold) {
					log(`Crawling rest anyway!`);
					crawlVids = vids.concat(videos);
					crawlChans = chan.concat(channels);
					crawlLists = lists.concat(playlists);
				}
				try {
					for (let chan of crawlChans) {
						enqueue(chan, "channel", true);
					}
					for (let list of crawlLists) {
						enqueue(list, "playlist", true);
					}
					for (let vid of crawlVids) {
						enqueue(vid, "video");
					}
				} catch (e) {
					log(crawlLists, lists, playlists);
					log(crawlChans, chan, channels);
					throw e;
				}
			});
		}
	})().catch(e => { log("Submitter crashed!"); stop = true; throw e; });

	const crawler = (async function crawler() {
		let ongoing = 0;
		let data = {videos: new Set(), channels: new Set(), playlists: new Set()};
		while (!stop) {
			while (ongoing < config.processConcurrentLimit && work.length) {
				ongoing++;
				startNew();
			}
			await delay(100);
			if (data.videos.size) {
				for (let vid of data.videos)
					submitQueue.videos.add(vid);
				data.videos = new Set();
			}
			if (data.playlists.size) {
				for (let list of data.playlists)
					submitQueue.playlists.add(list);
				data.playlists = new Set();
			}
			if (data.channels.size) {
				for (let chan of data.channels)
					submitQueue.channels.add(chan);
				data.channels = new Set();
			}
		}
		function callback() {
			if (work.length) {
				startNew();
			} else {
				ongoing--;
			}
		}
		function processBody(body) {
			if (typeof body === "object") {
				body = Object.values(body).join(" ");
			}
			for (let match of body.match(/(?:\bv=|youtu\.be\/)([\w-]{11})(?!\w)/g) || [])
				data.videos.add(flatstr(match.slice(-11)));
			for (let match of body.match(/\b(UC[\w-]{22})(?!\w)/g) || [])
				data.channels.add(flatstr(match));
			for (let match of body.match(/\b(PL(?:[0-9A-F]{16}|[\w-]{32})|[LF]L[\w-]{22})(?!\w)/) || [])
				data.playlists.add(flatstr(match));
			let cont = body.match(/"(\/browse_ajax?[^"]*)"/);
			if (cont != null) enqueue(flatstr(cont[1].replace(/&amp;/g, "&")), "continuation", true, true);
			callback();
		}
		function startNew() {
			let {type, id, json} = dequeue();
			if (!id) {
				log(unit);
			}
			let url;
			switch (type) {
			case "video":
				url = `https://www.youtube.com/watch?v=${id}&list=RD${id}&disable_polymer=1`;
				break;
			case "channel":
				url = `https://www.youtube.com/channel/${id}/playlists?disable_polymer=1`;
				enqueue("UU" + id.slice(2), "playlist", true);
				enqueue("/channel/" + id, "continuation", true); // has "Related channels"
				break;
			case "playlist":
				url = `https://www.youtube.com/playlist?list=${id}&disable_polymer=1`;
				break;
			case "continuation":
				url = "https://www.youtube.com" + id;
				break;
			}
			untilItWorks(() => rp({
				url: url,
				forever: true,
				json: json
			}), { silent: true, maxRetries: 10 }).then(processBody).catch(callback);
		}
	})().catch(e => { log("Crawler crashed!"); stop = true; throw e; });

	return await Promise.all([workSeeder, crawler, submitter, reporter]);
}

function selectBestMethod() {
	let method;
	// Filter out blacklisted methods
	let pool = methods.filter(m => !m.blacklisted);
	// If pool is empty, remove blacklist and retry
	if (!pool.length) {
		log("Resetting blacklist");
		for (let m of methods) m.blacklisted = false;
		return selectBestMethod();
	}
	// Check priority
	let prioritied = pool.filter(m => m.priority);
	if (prioritied.length) {
		log("Using priority overrides");
		method = prioritied.sort((a, b) => (b.priority - a.priority))[0];
	}
	// Pick randomly
	else {
		method = pool.random();
	}
	// Attempt method, and retry if failed
	return new Promise(resolve => {
		log("Using method "+method.name);
		method.code().then(result => {
			resolve(result);
		}).catch(err => {
			log(err);
			log("Method failed, adding to blacklist and retrying");
			method.blacklisted = true;
			resolve(selectBestMethod());
		});
	});
}

class Cache {
	constructor(limit) {
		this.limit = limit;
		this.queue = new Deque(limit + 1);
		this.set = new Set();
	}
	seen(elem) {
		if (this.set.has(elem)) return true;

		this.set.add(elem);
		this.queue.push(elem);

		if (this.queue.length > this.limit) {
			let rem = this.queue.shift();
			this.set.delete(rem);
		}
		return false;
	}
}

let idCache = new Cache(config.idCacheSize);
let chanCache = new Cache(config.channelCacheSize);
let listCache = new Cache(config.playlistCacheSize);

async function run() {
	try {
		await crawler();
	} catch (e) {
		log("Crawler crashed!");
		console.log(e);
		return;
	}

	log("ERROR: Crawler exited, somehow!");
	return;
}
run();
