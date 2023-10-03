# sql-replay-injector

This project is half toy half serious.
It is mainly for experimentation but I used some variations of it to run some real performance testing against databases at $work.

It contains all the primitives needed to run a file by chunk (fixed memory usage) and run the queries against a MySQL or a PostgreSQL database. And thus fully asynchronously thanks to the tokio runtime.

It also allows to control the response time for the queries and compare the result between 2 databases or against a fixed value. Logging functionality is **very** limited (let's say non-existent) on purpose.

In addition, it supports a limited, yet easily customizable data structure to have an idea about the latency distribution