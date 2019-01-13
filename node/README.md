## Running the project

1. Install node.js
1. `git clone https://github.com/omarroth/archive`
1. `cd archive/node/worker`
1. `npm install`

To run a worker server, `node index.js`
To run a crawler, `node crawler.js`

You can also do this easily on Heroku: just point it at either the "heroku" branch for a worker or the "heroku-crawler" branch for the crawler. Don't forget to periodically send HTTP requests to the Heroku server to keep it alive.

You can also do this easily on Docker: see README.md in the root of this repo for details.

If you need help, either ask in [the Discord server](https://discord.gg/dP4Pu6d) or [contact me personally](https://cadence.moe/about/contact).

## About this folder

Part of the YouTube annotations archival project.

Annotations on every YouTube video will be deleted forever on the 15th of January. The purpose of this project is to archive as much annotation data as possible before that happens.

The current process is to scrape as many channel IDs as possible, then to scrape video IDs from those channels, then to download annotation data for those videos.

Having one machine download data about millions of channels and hundreds of millions would videos is a ridiculous idea. It is presumably possible, but it's probably not possible to be done before the deadline.

What I'm working on is a distributed system: one master server ("master") hands out work to be done, and worker servers ("workers") perform the work and return it to master. Only master has a copy of the database.

The entire system operates on an HTTP API, so it's possible to write custom versions of the worker servers if you think you can get more performance compared to the worker code provided here.

## Current state of code

The worker is functional and is the recommended way of downloading annotations. The node.js port of the master server is abandoned, since there is already a functional master server written in Crystal.

[Check here to see the archive progress.](https://archive.omar.yt/api/stats)

## Contributing

Before you write any code, please [contact me](https://cadence.moe/about/contact) so we can talk about what you want to do and how you want to do it. Time is precious — let's not waste it on duplicate or unnecessary work!

Once that's sorted out, fork this repo, write your code, open a pull request, the usual.

## Files overview

## `/worker`

Code for the worker server.

`/index.js`
Sole file controlling the worker script. Intialises a worker and starts downloading video annotations according to the master server's directions.

`/config.json`
Easily editable config parameters. Check the code to find out what they do. Similarly named parameters may not do the same thing!

`/crawler.js`
Crawles extra channels and videos and submits their IDs to the master server. The master server will then process the IDs, put them into batches, and hand them out to workers.

`/config-crawler.json`
Configuration parameters for the crawler. Check the code to find out what they do.

## `/master`

Remember: this node.js port of the master server is incomplete and abandoned. Don't use it.

`/setup.js`
Erase and initialise `/db/main.db` for use by the master server. More specifically, create the schema from `/db/schema.sql`, then unzip `/db/channels.json.gz` (a list of 100k channels) and import it into Channels. This is never invoked automatically — run it manually whenever you need a fresh database file.

`/index.js`
The entry point for the master server. Code is based on https://github.com/cloudrac3r/cadencegq.

`/util`
Various server utility files.

`/db`
Database related files. You shouldn't need to edit anything here manually, but feel free to peek in.

`/api`
API endpoints. Each file in here is named according to what it manages.