## About this folder

*by cloudrac3r*

---

Part of the YouTube annotations archival project.

Annotations on every YouTube video will be deleted forever on the 15th of January. The purpose of this project is to archive as much annotation data as possible before that happens.

The current process is to scrape as many channel IDs as possible, then to scrape video IDs from those channels, then to download annotation data for those videos.

Having one machine download data about millions of channels and hundreds of millions would videos is a ridiculous idea. It is presumably possible, but it's probably not possible to be done before the deadline.

What I'm working on is a distributed system: one master server ("master") hands out work to be done, and worker servers ("workers") perform the work and return it to master. Only master has a copy of the database.

The entire system operates on an HTTP API, so it's possible to write custom versions of the worker servers, either for completeness or more performance compared to the worker code provided here.

---

## Current state of code

Everything is complete to the bare minimum. Both the master and the worker are functional, but require observation in case error arise.

## Final goal

A single hosted master server that knows about every channel ID with enough storage capacity to store hundreds of millions of blobs of annotation data, that can be left alone for extended periods of time without dying.

Many worker servers run by people with lots of bandwidth communicating with that master server.

## To do

- Lots and lots more error checking
- Master server optimisation
- Dynamic load balancing for workers
- Descriptive config parameter names
- Lots of testing (not too much, we don't have time)
- Customisable logging
- Migrate to Postgres
- Pipe in omarroth's completed channel list
- Host the master server somewhere
- Spread the word about the project and ask people to run the worker server

---

## Running

1. Install node.js
1. `git clone`
1. `cd repo/node`
1. `npm install`

`cd` to the folder containing the script before executing it.

If running the master server, don't forget to run `setup.js` (just once) to build the database.

If running the worker server:

- make sure you're running a master server for it to connect to
- if possible, run your own [Invidious](https://github.com/omarroth/invidious) server for faster responses and decreased load (contact omarroth for instructions)
- edit `config.json` to point to the correct master and Invidious servers

---

## Contributing

You don't *have* to do this, but before you write any code, *please* [contact me](https://cadence.moe/about/contact) so we can talk about what you want to do and how you want to do it. Time is precious — let's not waste it on duplicate or unnecessary work!

Once that's sorted out, fork this repo, write your code, open a pull request, the usual.

---

## Files overview

## `/master`

Code for the master server.

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

## `/worker`

Code for the worker server.

`/config.json`
Easily editable config parameters. Check the code to find out what they do. Similarly named parameters may not do the same thing!

`/index.js`
Sole file controlling the worker script. Intialises a worker and starts downloading channels and videos according to the master server's directions.