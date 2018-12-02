const timeout = 5*60*1000;
const interval = 60*1000;

module.exports = function({db, extra}) {
	setInterval(async () => {
		let {channelCount} = await db.get(
			"SELECT count(*) AS channelCount FROM WorkerChannels WHERE completedAt IS NULL AND pingedAt < ?",
			Date.now()-timeout
		);
		let {videoCount} = await db.get(
			"SELECT count(*) AS videoCount FROM WorkerVideos WHERE completedAt IS NULL AND pingedAt < ?",
			Date.now()-timeout
		);
		console.log("Will clean "+channelCount+" channels and "+videoCount+" videos");
		db.run(
			"DELETE FROM WorkerChannels WHERE completedAt IS NULL AND pingedAt < ?",
			Date.now()-timeout
		);
		db.run(
			"DELETE FROM WorkerVideos WHERE completedAt IS NULL AND pingedAt < ?",
			Date.now()-timeout
		);
	}, interval);

	return [];
}