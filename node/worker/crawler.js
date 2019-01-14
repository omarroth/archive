const rp = require("request-promise-native");
const fetch = require("node-fetch");
const dnscache = require("dnscache")({
	enable: true,
	ttl: 600,
	cachesize: 100
});
const http = require("http");
const Deque = require("double-ended-queue");

let port = process.env.PORT || 3001;
console.log("Will listen on "+port);
http.createServer((req, res) => {
	res.writeHead(200, {"Content-Type": "text/plain"});
	res.end("It works!");
}).listen(port);

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

// https://github.com/nodejs/help/issues/711
let flatstr = function flatstr(s) {
	return (" " + s).slice(1);
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

function untilItWorks(code, silence, timeout = 1000) {
	return new Promise(resolve => {
		code().then(resolve).catch(err => {
			if (!silence) {
				console.log("Something didn't work, but hopefully will next time.");
				console.log(err);
			}
			setTimeout(() => resolve(untilItWorks(code)), timeout);
		});
	});
}

const methods = [
	{
		name: "No Views",
		blacklisted: false,
		code: async () => {
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
			if (!config.useInvidious) throw "Invidious is disabled, cannot search YouTube.";
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
	},{
		name: "HookTube random",
		blacklisted: false,
		code: async () => {
			let data = {channels: [], videos: []};
			let redirect = await rp({
				url: "https://hooktube.com/random",
				simple: false,
				resolveWithFullResponse: true,
				followRedirect: false
			});
			let match = redirect.headers.location.match(/watch\?v=([\w-]+)/);
			if (!match) throw "HookTube random failed, couldn't match location: "+redirect.headers.location;
			data.videos.push(match[1]);
			console.log("HookTube gave us these IDs:", data.videos);
			return data;
		}
	}
];

function doCrawl(data) {
	let work = [];
	data.videos.forEach(id => work.push({type: "video", id: id}));
	data.channels.forEach(id => work.push({type: "channel", id: id}));
	data.playlists.forEach(id => work.push({type: "playlist", id: id}));
	if (!work.length) throw new Error("Video crawler was given nothing to crawl");
	console.log("Crawling "+work.length+" ids for recommendations");
	let progress = 0;
	let max = work.length;
	let extraWork = 0;
	function writeProgress(done) {
		if (config.progressBarMethod == 1) console.log(progressBar(progress, max + extraWork, 50));
		if (config.progressBarMethod == 2) process.stdout.write("\r"+progressBar(progress, max + extraWork, 50));
		if (done) process.stdout.write("\n");
	}
	function enqueue(url, type = "continuation") {
		work.push({type: type, id: url});
		extraWork++;
	}
	if (config.progressBarMethod) writeProgress();
	let promise;
	if (config.useInvidious) promise = Promise.all( // FIXME: This is probably definitely broken after changing the way crawling works
		vidIds.map(async id => {
			let result;
			await invidiousLocker.promise();
			result = await untilItWorks(() => new Promise((resolve, reject) => {
				rp({
					url: invidious+"/api/v1/videos/"+id,
					json: true,
					timeout: 20000
				}).then(resolve).catch(err => {
					if (err.name == "RequestError") {
						reject(err);
					} else {
						console.log(err);
						console.log("=== NAME: "+err.name);
						resolve(undefined);
					}
				});
			}), true);
			invidiousLocker.unlock();
			progress++;
			if (config.progressBarMethod && (progress % config.progressFrequency == 0)) writeProgress();
			return result;
		})
	).then(results => {
		if (config.progressBarMethod) writeProgress(true);
		let data = {videos: [], channels: []};
		for (let result of results) {
			if (result) {
				data.videos = data.videos.concat(result.recommendedVideos.map(v => v.videoId));
				data.channels.push(result.authorId);
			}
		}
		return data;
	});
	else promise = new Promise(resolve => {
		let data = {videos: new Set(), channels: new Set(), playlists: new Set()};
		let ongoing = 0;
		while (ongoing < config.processConcurrentLimit && work.length) {
			ongoing++;
			startNew();
		}
		function callback() {
			progress++;
			if (config.progressBarMethod && (progress % config.progressFrequency == 0)) writeProgress();
			if (work.length) {
				startNew();
			} else {
				if (!--ongoing) {
					if (config.progressBarMethod) writeProgress(true);
					resolve({videos: [...data.videos], channels: [...data.channels], playlists: [...data.playlists]});
				}
			}
		}
		function processBody(body) {
			for (let match of body.match(/(?:\bv=|youtu\.be\/)([\w-]{11})(?!\w)/g) || [])
				data.videos.add(flatstr(match.slice(-11)));
			for (let match of body.match(/\b(UC[\w-]{22})(?!\w)/g) || [])
				data.channels.add(flatstr(match));
			for (let match of body.match(/\b(PL(?:[0-9A-F]{16}|[\w-]{32})|LL[\w-]{22})(?!\w)/) || [])
				data.playlists.add(flatstr(match));
			let cont = body.match(/"(\/browse_ajax?[^"]*)"/);
			if (cont != null) enqueue(flatstr(cont[1].replace(/&amp;/g, "&")));
			callback();
		}
		function startNew() {
			let {type, id} = work.pop();
			let url;
			switch (type) {
			case "video":
				url = `https://www.youtube.com/watch?v=${id}&list=RD${id}&disable_polymer=1`;
				break;
			case "channel":
				url = `https://www.youtube.com/channel/${id}/playlists?disable_polymer=1`;
				enqueue("UU" + id.slice(2), "playlist");
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
				forever: true
			})).then(processBody);
		}
	});
	return promise.then(data => {
		console.log(`Gathered ${data.videos.length}/${data.channels.length}/${data.playlists.length} recommendations`);
		return submitAndCrawl(data);
	});
}

function selectBestMethod() {
	let method;
	// Filter out blacklisted methods
	let pool = methods.filter(m => !m.blacklisted);
	// If pool is empty, remove blacklist and retry
	if (!pool.length) {
		console.log("Resetting blacklist");
		for (let m of methods) m.blacklisted = false;
		return selectBestMethod();
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

let tries = 0;

function submitAndCrawl(data) {
	tries++;
	if (!(data.videos && data.videos.length)) return;
	let videos = data.videos.filter(id => !idCache.seen(id));
	let channels = (data.channels || []).filter(c => !chanCache.seen(c));
	let playlists = (data.playlists || []).filter(p => !listCache.seen(p));

	function submit(target, data) {
		let chunks = [];
		for (var i = 0; i < data.length; i += 25000) {
			chunks.push(data.slice(i, i + 25000));
		}
		let completedChunks = 0;
		return Promise.all(chunks.map(chunk =>
			untilItWorks(() => fetch(config.master+"/api/"+target+"/submit", {
				method: "POST",
				body: JSON.stringify({ [target]: chunk }),
				headers: {"Content-Type": "application/json"}
			}), false, 4000).then(res => res.json()).then(results => {
				let ins = results.inserted || [];
				console.log(`[${++completedChunks}/${chunks.length}] Submitted ${target}: ${chunk.length}, inserted ${ins.length}`);
				return ins;
			})
		)).then(results => results.reduce((acc, v) => acc.concat(v), []));
	}

	return Promise.all([submit("channels", channels), submit("videos", videos), submit("playlists", playlists)]).then(([chan, vids, lists]) => {
		let len = vids.length + chan.length + lists.length;
		let crawlVids = vids;
		let crawlChans = chan;
		let crawlLists = lists;
		if (len < config.crawlThreshold && tries <= 10) {
			console.log(`Crawling rest anyway! [${tries}/10]`);
			crawlVids = vids.concat(videos);
			crawlChans = chan.concat(channels);
			crawlLists = lists.concat(playlists);
		}
		crawlVids = crawlVids.slice(0, Math.floor(config.crawlLimit * 0.6));
		crawlChans = crawlChans.slice(0, Math.floor(config.crawlLimit * 0.25));
		crawlLists = crawlLists.slice(0, Math.floor(config.crawlLimit * 0.15));
		return doCrawl({videos: crawlVids, channels: crawlChans, playlists: crawlLists});
	});
}

async function run() {
	while (true) {
		let data = await selectBestMethod();
		tries = 0;
		await submitAndCrawl(data);
	}
}
run();
