module.exports = function({db, extra}) {
	const videoLockManager = new extra.LockManager();

	return [
		{
			route: "/api/videos", methods: ["GET"], worker: true, code: async ({params, worker}) => {
				let limit = parseInt(params.limit) || 50;
				limit = Math.min(Math.max(limit, 1), 250);
				await videoLockManager.promise();
				let videos = (await db.all(
					"SELECT Videos.videoID FROM Videos LEFT JOIN WorkerVideos ON Videos.videoID = WorkerVideos.videoID WHERE WorkerVideos.workerID IS NULL LIMIT ?",
					limit
				)).map(row => row.videoID);
				await Promise.all(videos.map(v => db.run(
					"INSERT INTO WorkerVideos VALUES (NULL, ?, ?, ?, ?, NULL)",
					[worker, v, Date.now(), Date.now()]
				)));
				videoLockManager.unlock();
				return [200, {status: "success", videos}];
				// ["videoID"]
			}
		},
		{
			route: "/api/videos", methods: ["POST"], worker: true, code: async ({params, worker, data}) => {
				// {"videoID": "XML data"}
				if (typeof(data) != "object" || data.constructor.name != "Object") return [400, {status: "error", code: 8}];
				let videoIDs = Object.keys(data);
				if (videoIDs.some(v => !v.match(/^[A-Za-z0-9_-]{11}$/))) return [400, {status: "error", code: 6}];
				let {workerRequestedCount} = await db.get(
					"SELECT count(distinct videoID) AS workerRequestedCount FROM WorkerVideos WHERE workerID = ? AND videoID IN ("+"?".repeat(videoIDs.length).split("").join(", ")+")",
					[worker].concat(videoIDs)
				);
				if (workerRequestedCount != videoIDs.length) return [400, {status: "error", code: 3}];
				let {videoExistsCount} = await db.get(
					"SELECT count(distinct videoID) AS videoExistsCount FROM Videos WHERE videoID IN ("+"?".repeat(videoIDs.length).split("").join(", ")+")",
					videoIDs
				);
				if (videoExistsCount != videoIDs.length) return [400, {status: "error", code: 4}];
				await Promise.all([].concat(videoIDs.map(v => {
					return [
						db.run(
							"INSERT INTO VideoAnnotations VALUES (NULL, ?, ?, ?)",
							[v, worker, data[v]]
						),
						db.run(
							"UPDATE WorkerVideos SET completedAt = ? WHERE videoID = ?",
							[Date.now(), v]
						)
					]
				})));
				return [204, ""];
			}
		},
		{
			route: "/api/videos/abort", methods: ["POST"], worker: true, code: async ({params, worker, data}) => {
				// ["videoID"]
				if (typeof(data) != "object" || data.constructor.name != "Array") return [400, {status: "error", code: 7}];
				if (data.some(c => typeof(c) != "string" || !c.match(/^[A-Za-z0-9_-]{11}$/))) return [400, {status: "error", code: 6}];
				let {workerRequestedCount} = await db.get(
					"SELECT count(distinct videoID) AS workerRequestedCount FROM WorkerVideos WHERE workerID = ? AND videoID IN ("+"?".repeat(data.length).split("").join(", ")+")",
					[worker].concat(data)
				);
				if (workerRequestedCount != data.length) return [400, {status: "error", code: 3}];
				await db.run(
					"DELETE FROM WorkerVideos WHERE completedAt IS NULL AND workerID = ? AND videoID IN ("+"?".repeat(data.length).split("").join(", ")+")",
					[worker].concat(data)
				);
				return [204, ""];
			}
		}
	];
}