module.exports = function({db, extra}) {
	const channelLockManager = new extra.LockManager();

	return [
		{
			route: "/api/channels", methods: ["GET"], worker: true, code: async ({params, worker}) => {
				let limit = parseInt(params.limit) || 1;
				limit = Math.min(Math.max(limit, 1), 50);
				await channelLockManager.promise();
				let channels = (await db.all(
					"SELECT Channels.channelID FROM Channels LEFT JOIN WorkerChannels ON Channels.channelID = WorkerChannels.channelID WHERE workerID IS NULL LIMIT ?",
					limit
				)).map(row => row.channelID);
				await Promise.all(channels.map(c => db.run(
					"INSERT INTO WorkerChannels VALUES (NULL, ?, ?, ?, ?, NULL)",
					[worker, c, Date.now(), Date.now()]
				)));
				channelLockManager.unlock();
				return [200, {status: "success", channels}];
				// ["channelID"]
			}
		},
		{
			route: "/api/channels", methods: ["POST"], worker: true, code: async ({params, worker, data}) => {
				// {"channelID": ["videoID"]}
				if (typeof(data) != "object" || data.constructor.name != "Object") return [400, {status: "error", code: 8}];
				let channelIDs = Object.keys(data);
				if (channelIDs.some(c => !c.match(/^UC[A-Za-z0-9_-]{22}$/))) return [400, {status: "error", code: 5}];
				if (Object.values(data).some(a => !a.every(v => typeof(v) == "string" && v.match(/^[A-Za-z0-9_-]{11}$/)))) return [400, {status: "error", code: 6}];
				let {workerRequestedCount} = await db.get(
					"SELECT count(distinct channelID) AS workerRequestedCount FROM WorkerChannels WHERE workerID = ? AND channelID IN ("+"?".repeat(channelIDs.length).split("").join(", ")+")",
					[worker].concat(channelIDs)
				);
				if (workerRequestedCount != channelIDs.length) return [400, {status: "error", code: 3}];
				let {channelExistsCount} = await db.get(
					"SELECT count(distinct channelID) AS channelExistsCount FROM Channels WHERE channelID IN ("+"?".repeat(channelIDs.length).split("").join(", ")+")",
					channelIDs
				);
				if (channelExistsCount != channelIDs.length) return [400, {status: "error", code: 4}];
				await Promise.all([].concat(channelIDs.map(c => {
					return data[c].map(v => db.run(
						"INSERT INTO Videos VALUES (NULL, ?, ?, ?)",
						[v, worker, c]
					)).concat(db.run(
						"UPDATE WorkerChannels SET completedAt = ? WHERE channelID = ?",
						[Date.now(), c]
					));
				})));
				return [204, ""];
			}
		},
		{
			route: "/api/channels/abort", methods: ["POST"], worker: true, code: async ({params, worker, data}) => {
				// ["channelID"]
				if (typeof(data) != "object" || data.constructor.name != "Array") return [400, {status: "error", code: 7}];
				if (data.some(c => typeof(c) != "string" || !c.match(/^UC[A-Za-z0-9_-]{22}$/))) return [400, {status: "error", code: 5}];
				let {workerRequestedCount} = await db.get(
					"SELECT count(distinct channelID) AS workerRequestedCount FROM WorkerChannels WHERE workerID = ? AND channelID IN ("+"?".repeat(data.length).split("").join(", ")+")",
					[worker].concat(data)
				);
				if (workerRequestedCount != data.length) return [400, {status: "error", code: 3}];
				await db.run(
					"DELETE FROM WorkerChannels WHERE completedAt IS NULL AND workerID = ? AND channelID IN ("+"?".repeat(data.length).split("").join(", ")+")",
					[worker].concat(data)
				);
				return [204, ""];
			}
		},
		{
			route: "/api/channels/ping", methods: ["POST"], worker: true, code: async ({params, worker, data}) => {
				// ["channelID"]
				if (typeof(data) != "object" || data.constructor.name != "Array") return [400, {status: "error", code: 7}];
				if (data.some(c => typeof(c) != "string" || !c.match(/^UC[A-Za-z0-9_-]{22}$/))) return [400, {status: "error", code: 5}];
				await db.run(
					"UPDATE WorkerChannels SET pingedAt = ? WHERE workerID = ? AND completedAt IS NULL AND channelID IN ("+"?".repeat(data.length).split("").join(", ")+")",
					[Date.now(), worker].concat(data)
				);
				return [204, ""];
			}
		}
	];
}