CREATE TABLE IF NOT EXISTS `Workers` (
	`workerID`	TEXT,
	PRIMARY KEY(`workerID`)
);
CREATE TABLE IF NOT EXISTS `WorkerVideos` (
	`id`	INTEGER PRIMARY KEY AUTOINCREMENT,
	`workerID`	TEXT,
	`videoID`	TEXT,
	`requestedAt`	INTEGER,
	`pingedAt`	INTEGER
	`completedAt`	INTEGER
);
CREATE TABLE IF NOT EXISTS `WorkerChannels` (
	`id`	INTEGER PRIMARY KEY AUTOINCREMENT,
	`workerID`	TEXT,
	`channelID`	TEXT,
	`requestedAt`	INTEGER,
	`pingedAt`	INTEGER,
	`completedAt`	INTEGER
);
CREATE TABLE IF NOT EXISTS `Videos` (
	`id`	INTEGER PRIMARY KEY AUTOINCREMENT,
	`videoID`	TEXT,
	`workerID`	TEXT,
	`channelID`	TEXT
);
CREATE TABLE IF NOT EXISTS `VideoAnnotations` (
	`id`	INTEGER PRIMARY KEY AUTOINCREMENT,
	`videoID`	TEXT,
	`workerID`	TEXT,
	`annotationData`	TEXT
);
CREATE TABLE IF NOT EXISTS `Channels` (
	`channelID`	TEXT,
	PRIMARY KEY(`channelID`)
);