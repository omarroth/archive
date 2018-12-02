const rp = require("request-promise-native");
const events = require("events");

const config = require("./config.json");

let _nextID = 0;
let nextID = () => ++_nextID;

class SendableObject {
	constructor(url, object) {
		this._cso = true;
		if (typeof(url) == "string") {
			this.object = object || {};
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
}

class Worker {
	constructor(config) {
		this.config = config;
		if (this.config.worker) this._worker = this.config.worker;
		else this.getWorker();
		this.ready = false;
		this.pendingChannels = [];
		this.channelProcesses = [];
	}
	run() {
		while (this.channelProcesses.length < this.config.channelJobs) this.channelProcesses.push(new ChannelProcess(this));
	}
	request(url, object) {
		return new Promise(resolve => {
			let so = new SendableObject(url, object);
			so.object.json = true;
			so.object.url = (this.config.master+so.object.url).replace(/[^:]\/{2,}/g, "/");
			rp(so.object).then(data => {
				if (data && typeof(data) == "object" && data.status != "success") throw JSON.stringify(data);
				console.log("» "+so.object.url);
				console.log("« "+JSON.stringify(data));
				resolve(data);
			});
		});
	}
	async workerRequest(url, object) {
		let worker = await this.getWorker();
		let so = new SendableObject(url, object);
		so.addQuery("worker="+worker);
		return this.request(so.object);
	}
	async getWorker(refetch) {
		if (this._worker && !refetch) return this._worker;
		else {
			return this._worker = new Promise(async resolve => {
				let data = await this.request("/api/worker");
				this._worker = data.workerID;
				resolve(this._worker);
			});
		}
	}
	recommendedChannels(limit) {
		console.log("Recommended channels request");
		if (!this._recommendedChannelsProcess) this._recommendedChannelsProcess = new Promise(resolve => {
			console.log("Actually performing request");
			this.workerRequest("/api/channels?limit="+(limit || this.config.channelFetchLimit || 1)).then(response => {
				this.pendingChannels = this.pendingChannels.concat(response.channels);
				this._recommendedChannelsProcess = null;
				console.log("Request complete");
				resolve();
			});
		});
		return this._recommendedChannelsProcess;
	}
}

class ChannelProcess {
	constructor(worker) {
		this.worker = worker;
		this.events = new events.EventEmitter();
		this.id = nextID();
		this.run();
	}
	async run() {
		while (true) {
			console.log("STARTING! ID: "+this.id);
			if (!this.worker.pendingChannels.length) await this.worker.recommendedChannels();
			let channel = this.worker.pendingChannels.shift();
			console.log("CHANNELS! ID: "+this.id+" CHANNEL: "+channel);
			await this.fetchChannel(channel);
		}
	}
	fetchChannel(channel) {
		return new Promise(resolve => {
			let videoIDs = [];
			let pageNumber = 0;
			rp({
				url: "https://www.youtube.com/channel/"+channel+"/videos?view=0&flow=grid&pbj=1",
				json: true,
				headers: {
					"Referer": "https://www.youtube.com/channel/"+channel,
					"X-YouTube-Client-Name": "1",
					"X-YouTube-Client-Version": "2.20181129",
					"X-YouTube-Page-Label": "youtube.ytfe.desktop_20181128_6_RC1",
					"X-SPF-Referer": "https://www.youtube.com/channel/"+channel,
					"X-SPF-Previous": "https://www.youtube.com/channel/"+channel,
					"X-YouTube-Variants-Checksum": "f0395f44f780d2c34d3b3ee6d1e492d2"
				}
			}).then(body => {
				let managePage = (page) => {
					pageNumber++;
					let token = page[1].xsrf_token;
					let continuation = undefined;
					if (page[1].response.contents) {
						if (page[1].response.contents.twoColumnBrowseResultsRenderer.tabs[1].tabRenderer.content.sectionListRenderer.contents[0].itemSectionRenderer.contents[0].gridRenderer.continuations) continuation = page[1].response.contents.twoColumnBrowseResultsRenderer.tabs[1].tabRenderer.content.sectionListRenderer.contents[0].itemSectionRenderer.contents[0].gridRenderer.continuations[0].nextContinuationData.continuation;
						let videos = page[1].response.contents.twoColumnBrowseResultsRenderer.tabs[1].tabRenderer.content.sectionListRenderer.contents[0].itemSectionRenderer.contents[0].gridRenderer.items;
						for (let video of videos) {
							videoIDs.push(video.gridVideoRenderer.videoId);
						}
						console.log("Page: "+pageNumber+", video count: "+videos.length+" → "+videoIDs.length+", continuation token: "+String(continuation).slice(0, 14));
					} else if (page[1].response.continuationContents) {
						if (page[1].response.continuationContents.gridContinuation.continuations) continuation = page[1].response.continuationContents.gridContinuation.continuations[0].nextContinuationData.continuation;
						let videos = page[1].response.continuationContents.gridContinuation.items;
						for (let video of videos) {
							videoIDs.push(video.gridVideoRenderer.videoId);
						}
						console.log("Page: "+pageNumber+", video count: "+videos.length+" → "+videoIDs.length+", continuation token: "+String(continuation).slice(0, 14));
					}
					if (continuation) {
						let so = new SendableObject({
							url: "https://www.youtube.com/browse_ajax",
							json: true,
							headers: {
								"Referer": "https://www.youtube.com/channel"+channel,
								"X-YouTube-Client-Name": "1",
								"X-YouTube-Client-Version": "2.20181129",
								"X-YouTube-Page-Label": "youtube.ytfe.desktop_20181128_6_RC1",
								"X-SPF-Referer": "https://www.youtube.com/channel/"+channel,
								"X-SPF-Previous": "https://www.youtube.com/channel/"+channel,
								"X-YouTube-Variants-Checksum": "f0395f44f780d2c34d3b3ee6d1e492d2"
							}
						});
						so.addQuery("ctoken="+encodeURIComponent(token));
						so.addQuery("continuation="+continuation);
						rp(so.object).then(body => {
							managePage(body);
						}).catch(err => {
							throw err;
						});
					} else if (pageNumber >= 100) {
						rp("https://www.youtube.com/channel/UC--i2rV5NCxiEIPefr3l-zQ/playlists/NC").then(playlistBody => {
							let lines = playlistBody.split("\n");
							let uploadsIndex = lines.findIndex(l => l.includes(`<span class="" >Uploads</span>`));
							let playlistID = lines[uploadsIndex+4].match(/list=(.*?)"/)[1];
							let page = 0;
							let managePlaylistPage = () => {
								page++;
								rp(this.worker.config.inv+"/api/v1/playlists/"+playlistID+"?page="+page, {json: true}).then(data => {
									if (data.videos.length == 0) {
										resolve(videoIDs);
									} else {
										for (let video of data.videos) {
											videoIDs.push(video.videoId);
										}
										console.log("Invidious page: "+page+", video count: "+data.videos.length+" → "+videoIDs.length);
										managePlaylistPage();
									}
								});
							}
							managePlaylistPage();
						}).catch(err => {
							throw err;
						});
					} else {
						resolve(videoIDs);
					}
				}
				managePage(body);
			});
		}).then(async videoIDs => {
			videoIDs = videoIDs.filter((v, i) => (!videoIDs.slice(0, i).includes(v)));
			let body = {};
			body[channel] = videoIDs;
			await this.worker.workerRequest("/api/channels", {body});
		}).catch(err => {
			console.log(err);
			console.log(channel);
			process.exit();
		});
	}
}

let worker = new Worker(config);
worker.run();