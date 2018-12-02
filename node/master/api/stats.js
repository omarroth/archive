module.exports = function({db, extra}) {
	function getStats() {
		return Promise.all([
			db.get("SELECT count(*) AS knownChannels FROM Channels"),
			db.get("SELECT count(*) AS completedChannels FROM WorkerChannels WHERE completedAt IS NOT NULL"),
			db.get("SELECT count(*) AS knownVideos FROM Videos"),
			db.get("SELECT count(*) AS completedVideos FROM WorkerVideos WHERE completedAt IS NOT NULL"),
			db.get("SELECT count(*) AS completedAnnotations FROM VideoAnnotations")
		]);
	}

	setInterval(() => {
		getStats().then(console.log);
	}, 30*1000);

	return [
		{
			route: "/api/stats", methods: ["GET"], code: () => {
				return getStats();
			}
		}
	];
}