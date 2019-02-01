const rp = require("request-promise-native");
const request = require("request");
const fs = require("fs");
const zlib = require("zlib");
const fetch = require("node-fetch");
const http = require("http");
const dnscache = require("dnscache")({
	enable: true,
	ttl: 600,
	cachesize: 100
});
const child = require("child_process");

const config = "./node/worker/config.json";

let port = process.env.PORT || 3000;
console.log("Will listen on "+port);
http.createServer((req, res) => {
	res.writeHead(200, {"Content-Type": "text/plain"});
	res.end("It works!");
}).listen(port);

function progressBar(progress, max, length) {
	let bars = Math.floor(progress/max*length);
	return progress.toString().padStart(max.toString().length, " ")+"/"+max+" ["+"=".repeat(bars)+" ".repeat(length-bars)+"]";
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
				console.log("Something didn't work, but hopefully will next time. [Attempt " + tries + "]");
				console.log(err);
			}
			await delay((Math.random() * 0.4 + 0.6) * timeout);
			timeout = Math.min(maxTimeout, timeout * 1.3);
		}
	}
}

function delay(time) {
	return new Promise(resolve => setTimeout(() => resolve(), time));
}

function spacesUpload(url, stream, length, lastDelay = 0) {
	return new Promise(resolve => {
		stream.pipe(request({
			url: url,
			method: "PUT",
			headers: {
				"Content-Type": "application/gzip",
				"Content-Length": length
			}
		})).on("response", response => {
			if (!response.statusCode.toString().startsWith("2")) {
				console.log("received status code "+response.statusCode);
				console.log(response.body);
				lastDelay += 4000;
				resolve(delay(lastDelay).then(() => spacesUpload(url, stream, length, lastDelay)));
			} else {
				resolve();
			}
		});
	});
}

class LockManager {
	constructor(debug) {
		this.debug = debug;
		this.locked = false;
		this.queue = [];
	}
	log(message) {
		if (this.debug) console.log(message);
	}
	waitForUnlock(callback) {
		this.log("WAIT FOR UNLOCK CALLED");
		if (!this.locked) {
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
		this.locked = true;
	}
	unlock() {
		this.log("UNLOCKED");
		this.locked = false;
		if (this.queue.length) {
			this.log("STARTING QUEUE");
			setImmediate(() => this.queue.shift()());
		}
	}
	promise() {
		return new Promise(resolve => this.waitForUnlock(resolve));
	}
}

class SendableObject {
	constructor(url, object) {
		this._cso = true;
		if (typeof(url) == "string") {
			this.object = object || {};
			this.object.headers = {};
			this.object.url = url;
		} else {
			this.object = url;
		}
		if (this.object.body && !this.object.method) this.object.method = "POST";
	}
	addQuery(query) {
		let additional = query.split("=").map(p => encodeURIComponent(p)).join("=");
		if (this.object.url.includes("?")) this.object.url += "&"+additional;
		else this.object.url += "?"+additional;
	}
	addHeaders(headers) {
		Object.assign(this.object.headers, headers);
	}
}

class Worker {
	constructor(configPath) {
		return new Promise(async resolve => {
			this.configPath = configPath;
			this.config = JSON.parse(fs.readFileSync(this.configPath, "utf8"));
			if (this.config.workers.length) {
				this._worker = this.config.workers[0];
				console.log("Using worker from config file: "+this.config.workers[0]);
			} else {
				await this.getWorker();
			}
			this.ready = false;
			this.batchProcesses = [];
			this.run();
			resolve();
		});
	}
	writeConfig() {
		return new Promise((resolve, reject) => {
			fs.writeFile(this.configPath, JSON.stringify(this.config, null, 3), "utf8", err => {
				if (err) reject(err);
				else resolve();
			});
		});
	}
	run() {
		this.batchProcesses.push(new BatchProcess(this));
	}
	request(url, object) {
		let so = new SendableObject(url, object);
		so.object.json = true;
		so.object.url = (this.config.master+so.object.url).replace(/([^:])\/{2,}/g, "$1/");
		//console.log("» "+so.object.url);
		if (typeof(so.object.body) == "object") {
			so.object.body = JSON.stringify(so.object.body);
			so.addHeaders({"Content-Type": "application/json"});
		}
		return untilItWorks(() => fetch(so.object.url, so.object), {timeout: 3000, maxTimeout: 10000})
		.then(res => {
			if (res.status == 204) return Promise.resolve("");
			else return res.json();
		});
	}
	async workerRequest(url, object) {
		let workerID = await this.getWorker();
		let so = new SendableObject(url, object);
		if (!so.object.body) so.object.body = {};
		so.object.body.worker_id = workerID;
		so.object.method = "POST";
		return this.request(so.object);
	}
	async getWorker(refetch) {
		if (this._worker && !refetch) return this._worker;
		else {
			console.log("Requesting a new worker ID");
			return this._worker = new Promise(async resolve => {
				let data = await this.request("/api/workers/create", {method: "POST", body: ""});
				this.config.s3_url = data.s3_url;
				this._worker = data.worker_id;
				this.config.workers.push(this._worker);
				this.writeConfig();
				console.log("New worker created: "+this._worker+"\n");
				resolve(this._worker);
			});
		}
	}
}

class BatchProcess {
	constructor(worker) {
		this.worker = worker;
		this.run();
	}
	async run() {
		while (true) {
			let batch = await this.getBatch();
			{
				fetch("https://cadence.moe/api/ytaa/"+this.worker._worker+"/"+this.batchID).catch(new Function());
				console.log("Batch "+this.batchID+" contains "+batch.length+" items");
				let total = batch.length;
				let results = {};
				console.log("Cache functionality removed.");
				console.log("Downloading annotation data...");
				let completed = Object.keys(results).length;
				const drawProgress = override => {
					if (override || completed % this.worker.config.progressFrequency == 0) {
						console.log(progressBar(completed, total, 50));
					}
				}
				drawProgress(true);
				let remainingProcesses = 0;
				await new Promise(resolve => {
					const callback = (id, response) => {
						results[id] = response;
						completed++;
						drawProgress();
						if (batch.length) new AnnotationProcess(this, batch.pop(), callback);
						else if (--remainingProcesses == 0) resolve();
					}
					for (let i = 0; i < this.worker.config.annotationConcurrentLimit; i++) {
						if (batch.length) {
							remainingProcesses++;
							new AnnotationProcess(this, batch.pop(), callback);
						}
					}
					if (remainingProcesses == 0) resolve();
				});
				drawProgress(true);
				process.stdout.write("\n");
				console.log("All annotations fetched, dumping to disk");
				let writeStream = fs.createWriteStream("dump.json", {encoding: "utf8"});
				writeStream.write("{");
				let keys = Object.keys(results);
				let i = 0;
				await new Promise(resolve => {
					(function write() {
						let ok = true;
						while (i < keys.length && ok) {
							let key = keys[i];
							writeStream.write('"'+key+'":');
							let string = results[key];
							string = string.replace(/("|\\)/g, "\\$1").replace(/\n/msg, "\\n");
							ok = writeStream.write('"'+string+'"');
							if (i != keys.length-1) writeStream.write(",");
							i++;
							if (ok) process.stdout.write(" ");
						}
						if (!ok) {
							writeStream.once("drain", write);
						} else {
							writeStream.write("}");
							writeStream.end();
							writeStream.on("finish", resolve);
						}
					})();
				});
			}
			let oldSize = fs.statSync("dump.json").size;
			console.log("Dumped "+(oldSize/1e6).toFixed(1)+" MB, gzipping...");
			await new Promise(resolve => child.exec("gzip dump.json", resolve));
			let newSize = fs.statSync("dump.json.gz").size;
			console.log("Done, new size is "+(newSize/1e6).toFixed(1)+" MB\n");
			console.log("Committing... ");
			let commitResponse = await this.worker.workerRequest("/api/commit", {body: {batch_id: this.batchID, content_size: newSize}});
			if (commitResponse.error_code) {
				if (commitResponse.error_code == 8) {
					console.log("\nDumping failed batch to disk: dump.json.gz");
				}
				throw new Error(commitResponse.error);
			}
			if (commitResponse.upload_url) {
				console.log("uploading... ");
				let stream = fs.createReadStream("dump.json.gz", {encoding: null});
				await spacesUpload(commitResponse.upload_url, stream, newSize);
				console.log("finalising... ");
				await this.worker.workerRequest("/api/finalize", {body: {batch_id: this.batchID}});
				console.log("done.\n");
			} else {
				console.log("done, no upload required.\n");
			}
			if (this.worker.config.eraseDB) {
				fs.unlinkSync("dump.json.gz");
			}
			void 0;
		}
	}
	getBatch() {
		process.stdout.write("Fetching batch data... ");
		return this.worker.workerRequest("/api/batches", {simple: false}).then(response => {
			if (response.objects) {
				process.stdout.write("done, starting new batch\n");
				this.batchID = response.batch_id;
				return response.objects;
			} else if (response.error_code == 4) {
				this.batchID = response.batch_id;
				return this.worker.workerRequest("/api/batches/"+this.batchID).then(response => {
					process.stdout.write("done, resuming previous batch\n");
					return response.objects;
				});
			} else if (response.error_code) {
				/* 1 : Too many workers for IP
				 * 2 : Worker does not exist
				 * 3 : Worker is disabled
				 * 4 : Worker must commit #{batch_id}
				 * 5 : Worker isn't allowed access to #{batch_id}
				 * 6 : Cannot commit with empty batch_id
				 * 7 : Batch #{batch_id} does not exist
				 * 8 : Invalid size for #{batch_id}
				 */
				throw new Error("Batch request returned API error "+response.error_code);
			}
		});
	}
}

class AnnotationProcess {
	constructor(parent, id, callback) {
		this.parent = parent;
		this.id = id;
		this.callback = callback;
		this.errorCount = 0;
		this.run();
		this.errorCount = 0;
	}
	run() {
		let backend = this.parent.worker.config.annotationFetchBackend;
		let url = "https://www.youtube.com/watch?gl=US&hl=en&disable_polymer=1&v="+this.id;
		if (backend == "fetch") {
			fetch(url).then(response => {
				response.text().then(text => {
					if (response.status == 200) this.done(text);
					else if (response.status == 400) this.done("");
					else throw response;
				});
			}).catch(err => {
				if (err.constructor.name == "FetchError" && (err.message.includes("EAI_AGAIN") || err.message.includes("getaddrinfo ENOTFOUND"))) {
					this.errorLog("DNS error. Will retry in a second...");
					setTimeout(() => this.run(), 1000);
				} else {
					if (this.parent.worker.config.forceRetryAllErrors && ++this.errorCount < 10) {
						this.errorLog("Error, will retry in a moment.\nIf you get lots of these errors, try turning down annotationConcurrentLimit in config.json.");
						setTimeout(() => this.run(), 8000);
					} else {
						throw err;
					}
				}
			});
		} else if (backend == "request") {
			rp({url, forever: true}).then(response => {
				this.done(response);
			}).catch(err => {
				if (err.constructor.name == "StatusCodeError") {
					this.done("");
				} else if (err.constructor.name == "RequestError" && (err.message.includes("EAI_AGAIN") || err.message.includes("getaddrinfo ENOTFOUND"))) {
					this.errorLog("\nDNS error. Will retry in a second...");
					setTimeout(() => this.run(), 1000);
				} else {
					if (this.parent.worker.config.forceRetryAllErrors && ++this.errorCount < 10) {
						this.errorLog("Error, will retry in a moment.\nIf you get lots of these errors, try turning down annotationConcurrentLimit in config.json.");
						setTimeout(() => this.run(), 8000);
					} else {
						throw err;
					}
				}
			});
		} else {
			throw new Error("Please specify a valid option for config.annotationFetchBackend");
		}
	}
	done(response) {
		process.nextTick(() => this.callback(this.id, response));
	}
	errorLog(message) {
		if (!this.parent.worker.config.silenceErrors) console.log("\n"+message);
	}
}

let worker = new Worker(config);
