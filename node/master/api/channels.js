const events = require("events");

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
					"INSERT INTO WorkerChannels VALUES (?, ?, ?, NULL)",
					[worker, c, Date.now()]
				)));
				channelLockManager.unlock();
				return [200, {status: "success", channels}];
				// ["channelID"]
			}
		},
		{
			route: "/api/channels", methods: ["POST"], worker: true, code: async ({params, worker, data}) => {
				let channelIDs = Object.keys(data);
				if (channelIDs.some(c => !c.match(/^UC[A-Za-z0-9_-]{22}$/))) return [400, {status: "error", code: 5}];
				if (Object.values(data).some(a => !a.every(v => typeof(v) == "string" && v.match(/^[A-Za-z0-9_-]{11}$/)))) return [400, {status: "error", code: 6}];
				let {workerRequestedCount} = await db.get(
					"SELECT count(*) AS workerRequestedCount FROM WorkerChannels WHERE workerID = ? AND channelID IN ("+"?".repeat(channelIDs.length)+")",
					[worker].concat(channelIDs)
				);
				if (workerRequestedCount != channelIDs.length) return [400, {status: "error", code: 3}];
				let {channelExistsCount} = await db.get(
					"SELECT count(*) AS channelExistsCount FROM Channels WHERE channelID IN ("+"?".repeat(channelIDs.length)+")",
					channelIDs
				);
				if (channelExistsCount != channelIDs.length) return [400, {status: "error", code: 4}];
				await Promise.all([].concat(channelIDs.map(c => {
					return data[c].map(v => db.run(
						"INSERT INTO Videos VALUES (?, ?, ?)",
						[v, worker, c]
					)).concat(db.run(
						"UPDATE WorkerChannels SET completedAt = ? WHERE channelID = ?",
						[Date.now(), c]
					));
				})));
				return [204, ""];
			}
		}
	];
}