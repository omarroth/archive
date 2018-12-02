const rp = require("request-promise-native");
const events = require("events");

const config = require("./config.json");

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
		this.pendingVideos = [];
		this.videoProcesses = [];
	}
	run() {
		while (this.channelProcesses.length < this.config.channelJobs) this.channelProcesses.push(new ChannelProcess(this));
		while (this.videoProcesses.length < this.config.videoJobs) this.videoProcesses.push(new VideoProcess(this));
	}
	request(url, object) {
		return new Promise(resolve => {
			let so = new SendableObject(url, object);
			so.object.json = true;
			so.object.url = (this.config.master+so.object.url).replace(/[^:]\/{2,}/g, "/");
			//console.log("» "+so.object.url);
			rp(so.object).then(data => {
				if (data && typeof(data) == "object" && data.status != "success") throw JSON.stringify(data);
				//console.log("« "+JSON.stringify(data));
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
		console.log("Fetching new channels");
		//console.log("Recommended channels request");
		if (!this._recommendedChannelsProcess) this._recommendedChannelsProcess = new Promise(resolve => {
			//console.log("Actually performing request");
			this.workerRequest("/api/channels?limit="+(limit || this.config.channelFetchLimit || 1)).then(response => {
				this.pendingChannels = this.pendingChannels.concat(response.channels);
				this._recommendedChannelsProcess = null;
				//console.log("Request complete");
				resolve();
			});
		});
		return this._recommendedChannelsProcess;
	}
	recommendedVideos(limit) {
		console.log("Fetching new videos");
		return this.workerRequest("/api/videos?limit="+(limit || this.config.videoFetchLimit || 50)).then(response => {
			this.pendingVideos = this.pendingVideos.concat(response.videos);
		});
	}
}

class ChannelProcess {
	constructor(worker) {
		this.worker = worker;
		this.run();
	}
	async run() {
		while (true) {
			if (!this.worker.pendingChannels.length) await this.worker.recommendedChannels();
			let channel = this.worker.pendingChannels.shift();
			await this.fetchChannel(channel);
		}
	}
	abort(channel) {
		console.log("Aborting channel "+channel);
		return this.worker.workerRequest("/api/channels/abort", {body: [channel]});
	}
	ping(channel) {
		console.log("Pinging channel "+channel);
		return this.worker.workerRequest("/api/channels/ping", {body: [channel]});
	}
	fetchChannel(channel) {
		console.log("Fetching channel "+channel);
		let startedAt = Date.now();
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
					if (Date.now()-startedAt > this.worker.config.keepAliveInterval) this.ping(channel);
					pageNumber++;
					let token = page[1].xsrf_token;
					let continuation = undefined;
					if (page[1].response.contents) {
						if (page[1].response.contents.twoColumnBrowseResultsRenderer.tabs[1].tabRenderer.content.sectionListRenderer.contents[0].itemSectionRenderer.contents[0].messageRenderer) {
							// ...messageRenderer.text.simpleText = "This channel has no videos."
							resolve(videoIDs);
							return;
						} else if (page[1].response.contents.twoColumnBrowseResultsRenderer.tabs[1].tabRenderer.content.sectionListRenderer.contents[0].itemSectionRenderer.contents[0].gridRenderer.continuations) continuation = page[1].response.contents.twoColumnBrowseResultsRenderer.tabs[1].tabRenderer.content.sectionListRenderer.contents[0].itemSectionRenderer.contents[0].gridRenderer.continuations[0].nextContinuationData.continuation;
						let videos = page[1].response.contents.twoColumnBrowseResultsRenderer.tabs[1].tabRenderer.content.sectionListRenderer.contents[0].itemSectionRenderer.contents[0].gridRenderer.items;
						for (let video of videos) {
							videoIDs.push(video.gridVideoRenderer.videoId);
						}
						//console.log("Page: "+pageNumber+", video count: "+videos.length+" → "+videoIDs.length+", continuation token: "+String(continuation).slice(0, 14));
					} else if (page[1].response.continuationContents) {
						if (page[1].response.continuationContents.gridContinuation.continuations) continuation = page[1].response.continuationContents.gridContinuation.continuations[0].nextContinuationData.continuation;
						let videos = page[1].response.continuationContents.gridContinuation.items;
						for (let video of videos) {
							videoIDs.push(video.gridVideoRenderer.videoId);
						}
						//console.log("Page: "+pageNumber+", video count: "+videos.length+" → "+videoIDs.length+", continuation token: "+String(continuation).slice(0, 14));
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
							this.abort(channel);
						});
					} else if (pageNumber >= 100) {
						rp("https://www.youtube.com/channel/UC--i2rV5NCxiEIPefr3l-zQ/playlists/NC").then(playlistBody => {
							let lines = playlistBody.split("\n");
							let uploadsIndex = lines.findIndex(l => l.includes(`<span class="" >Uploads</span>`));
							let playlistID = lines[uploadsIndex+4].match(/list=(.*?)"/)[1];
							let page = 0;
							let managePlaylistPage = () => {
								if (Date.now()-startedAt > this.worker.config.keepAliveInterval) this.ping(channel);
								page++;
								rp(this.worker.config.inv+"/api/v1/playlists/"+playlistID+"?page="+page, {json: true}).then(data => {
									if (data.videos.length == 0) {
										resolve(videoIDs);
									} else {
										for (let video of data.videos) {
											videoIDs.push(video.videoId);
										}
										//console.log("Invidious page: "+page+", video count: "+data.videos.length+" → "+videoIDs.length);
										managePlaylistPage();
									}
								});
							}
							managePlaylistPage();
						}).catch(err => {
							this.abort(channel);
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
			console.log("Submitting channel "+channel);
			await this.worker.workerRequest("/api/channels", {body});
		}).catch(err => {
			this.abort(channel);
		});
	}
}

class VideoProcess {
	constructor(worker) {
		this.worker = worker;
		this.pendingAnnotations = [];
		this.run();
	}
	async run() {
		while (true) {
			if (!this.worker.pendingVideos.length) await this.worker.recommendedVideos();
			if (this.worker.pendingVideos.length) {
				let video = this.worker.pendingVideos.shift();
				let result = await this.fetchAnnotations(video);
				this.pendingAnnotations.push([video, result]);
				//console.log("New annotation count: "+this.pendingAnnotations.length);
				if (this.pendingAnnotations.length >= this.worker.config.annotationSubmissionThreshold) {
					let toSubmit = this.pendingAnnotations;
					this.pendingAnnotations = [];
					let body = {};
					for (let item of toSubmit) {
						body[item[0]] = item[1];
					}
					console.log("Submitting videos");
					this.worker.workerRequest("/api/videos", {body});
				}
			} else {
				await new Promise(resolve => setTimeout(resolve, 6000));
			}
		}
	}
	fetchAnnotations(video) {
		return rp("https://www.youtube.com/annotations_invideo?video_id="+video);
	}
}

let worker = new Worker(config);
worker.run();