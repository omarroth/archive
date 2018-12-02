const crypto = require("crypto");

module.exports = function({db, extra}) {
	return [
		{
			route: "/api/worker", methods: ["GET"], code: async () => {
				let hash = crypto.createHash("sha256").update(""+Math.random()).digest("hex");
				await db.run("INSERT INTO Workers VALUES (?)", hash);
				return [200, {status: "success", workerID: hash}];
			}
		}
	]
}