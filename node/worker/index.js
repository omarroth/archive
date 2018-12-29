const rp = require("request-promise-native");
const fs = require("fs");
const zlib = require("zlib");
const sqlite = require("sqlite");
const fetch = require("node-fetch");

const config = "./config.json";

function progressBar(progress, max, length) {
	let bars = Math.floor(progress/max*length);
	return progress.toString().padStart(max.toString().length, " ")+"/"+max+" ["+"=".repeat(bars)+" ".repeat(length-bars)+"]";
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
			this.db = await sqlite.open(this.config.db);
			await this.db.run("CREATE TABLE IF NOT EXISTS Cache (videoID TEXT, data TEXT, PRIMARY KEY (videoID))");
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
		return fetch(so.object.url, so.object)
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
			console.log("Batch "+this.batchID+" contains "+batch.length+" items");
			let total = batch.length;
			let results = {};
			{
				process.stdout.write("Checking cache... ");
				let paramLimit = this.worker.config.sqliteHostParamLimit;
				let promises = [];
				let collected = 0;
				for (let i = 0; i < batch.length; i += paramLimit) {
					let batchFragment = batch.slice(i, i+paramLimit);
					let statement = "SELECT * FROM Cache WHERE videoID IN ("+",?".repeat(batchFragment.length).slice(1)+")";
					promises.push(this.worker.db.all(statement, batchFragment).then(rows => {
						collected += rows.length;
						for (let row of rows) {
							results[row.videoID] = row.data;
							let index = batch.indexOf(row.videoID);
							if (index != -1) batch.splice(index, 1);
						}
					}));
				}
				await Promise.all(promises);
				process.stdout.write(`done, retrieved ${collected} items\n`);
			}
			console.log("Downloading annotation data...");
			let completed = Object.keys(results).length;
			const drawProgress = override => {
				if (override || completed % this.worker.config.progressFrequency == 0) {
					process.stdout.write("\r"+progressBar(completed, total, 50));
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
			console.log("All annotations fetched");
			let toCompress = JSON.stringify(results);
			process.stdout.write("Compressing "+(toCompress.length/1e6).toFixed(1)+"MB of data... ");
			let gzipData = await new Promise(resolve => zlib.gzip(toCompress, (err, buf) => {
				if (err) throw err;
				resolve(buf);
			}));
			process.stdout.write("done, new size is "+(gzipData.length/1e6).toFixed(1)+" MB\n");
			process.stdout.write("Committing... ");
			let commitResponse = await this.worker.workerRequest("/api/commit", {body: {batch_id: this.batchID, content_size: gzipData.length}});
			if (commitResponse.error_code) throw commitResponse;
			if (commitResponse.upload_url) {
				process.stdout.write("uploading... ");
				await rp({
					url: commitResponse.upload_url,
					method: "PUT",
					body: gzipData,
					headers: {
						"Content-Type": "application/gzip"
					}
				});
				process.stdout.write("finalising... ");
				await this.worker.workerRequest("/api/finalize", {body: {batch_id: this.batchID}});
				process.stdout.write("done.\n");
			} else {
				process.stdout.write("done, no upload required.\n");
			}
			if (this.worker.config.eraseDB) {
				process.stdout.write("Wiping cache database... ");
				await this.worker.db.run("DELETE FROM Cache");
				process.stdout.write("done.\n");
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
				throw response;
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
	}
	run() {
		let backend = this.parent.worker.config.annotationFetchBackend;
		let url = "https://www.youtube.com/annotations_invideo?video_id="+this.id;
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
			rp(url).then(response => {
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
		this.parent.worker.db.run("INSERT INTO Cache VALUES (?, ?)", [this.id, response]).catch(err => {
			console.log(err);
			throw err;
		});
	}
	errorLog(message) {
		if (!this.parent.worker.config.silenceErrors) console.log("\n"+message);
	}
}

let worker = new Worker(config);