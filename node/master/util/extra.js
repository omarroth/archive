module.exports = function({db}) {
	return {
		getWorker: async function(params) {
			if (!params.worker) return [400, {status: "error", code: 1}];
			let exists = await db.get("SELECT workerID FROM Workers WHERE workerID = ?", params.worker);
			if (exists) return params.worker;
			else return [400, {status: "error", code: 2}];
		},
		LockManager: class LockManager {
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
	}
}