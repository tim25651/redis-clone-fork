# redis-clone

A small Redis clone built with Python.

This module provides a basic implementation of a Redis server, supporting
core functionalities like:

-   String storage (SET, GET, MSET, MGET, DEL)
-   Key patterns (KEYS)
-   Database selection (SELECT)
-   Basic commands (ECHO, PING)
-   Data persistence to JSON file

It uses a custom RESP (Redis Serialization Protocol) decoder and encoder.

## Usage

The `redis_clone.py` script can be executed from the command line with several
options:

        python redis_clone.py [-h] [-n N_DBS] [-i INTERVAL] [-f FILE] [-p PORT]

*   `-h, --help`: Show the help message and exit.
*   `-n N_DBS, --n-dbs N_DBS`: Number of databases to initialize (default: 8).
*   `-i INTERVAL, --interval INTERVAL`: Dump interval in seconds (default: 300).
*   `-f FILE, --file FILE`: File to dump and load the databases from (default: databases.json).
*   `-p PORT, --port PORT`: Port number for the server to listen on (default: 6379).

## Example

To start the server on port `7000` with `10` databases, a `120` second dump
interval and `mydata.json` as dump file, run:

    python redis_clone.py -p 7000 -n 10 -i 120 -f mydata.json

Note:
    *   The server listens on `localhost` and the specified `PORT`.
    *   Data is loaded from and periodically dumped to the file.
    *   The server does not handle concurrent connections.
