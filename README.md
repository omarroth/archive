**For cloudrac3r's work, see [README.md](https://github.com/omarroth/archive/blob/master/node/README.md) in the `node` folder.**

---

# Youtube Annotation Archive

Provides scripts for archiving YouTube Annotations. See the [wiki](https://github.com/omarroth/archive/wiki) for information about how it works.

Annotations on every YouTube video will be deleted forever on the 15th of January. The purpose of this project is to archive as much annotation data as possible before that happens.

The current process is to scrape as many channel IDs as possible, then to scrape video IDs from those channels, then to download annotation data for those videos.

## Usage

### Installing and running a worker (Node.js):

#### On Ubuntu:

```bash
# Install dependencies
$ sudo apt-get install curl python-software-properties
$ curl -sL https://deb.nodesource.com/setup_10.x | sudo -E bash -
$ sudo apt-get install nodejs gcc g++ make

$ git clone https://github.com/omarroth/archive
$ cd archive/node
$ npm install
$ cd worker
$ node index.js
```

### Installing and running a worker (Crystal):

#### On Ubuntu:

```bash
# Install dependencies
$ curl -sSL https://dist.crystal-lang.org/apt/setup.sh | sudo bash
$ sudo apt-get update
$ sudo apt-get install crystal libssl-dev libxml2-dev libyaml-dev libgmp-dev libreadline-dev librsvg2-dev

$ git clone https://github.com/omarroth/archive
$ cd archive
$ shards
$ crystal build src/worker.cr --release
$ ./worker -u https://archive.omar.yt -t 20
```

```bash
$ ./worker -h
    -u URL, --batch-url=URL          Master server URL
    -t THREADS, --max-threads=THREADS
                                     Number of threads for downloading annotations
    -h, --help                       Show this help
```

## Contributors

- [Omar Roth](https://github.com/omarroth) - creator and maintainer
- [cloudrac3r](https://github.com/cloudrac3r) - JavaScript developer
