Log Sorting Challenge
======================

PROBLEM:

You have four servers running.
Each one of them has several lines of log files from an HTTP server, sorted by the request time.
Write a program that separates all data from each user in its own file.
You may put the resulting files in any server(s) you like, but all data from a given user must be in a single file.

PS: your code does not need to be really distributed. You may simulate the servers as threads, processes, etc.

Input Sample:
177.126.180.83 - - [15/Aug/2013:13:54:38 -0300] "GET /meme.jpg HTTP/1.1" 200 2148 "-" "userid=5352b590-05ac-11e3-9923-c3e7d8408f3a"
177.126.180.83 - - [15/Aug/2013:13:54:38 -0300] "GET /lolcats.jpg HTTP/1.1" 200 5143 "-" "userid=f85f124a-05cd-11e3-8a11-a8206608c529"
177.126.180.83 - - [15/Aug/2013:13:57:48 -0300] "GET /lolcats.jpg HTTP/1.1" 200 5143 "-" "userid=5352b590-05ac-11e3-9923-c3e7d8408f3a"

SOLUTION:

The challenge was solved with a map-reduce like algorithm.

There are four server node threads and an additional coordinator thread (that could be running in any of the servers
in a real scenario).
The Coordinator knows about all active server nodes, but the nodes don't know about each other and they don't
communicate directly.

SIMPLIFICATIONS / ASSUMPTIONS:

- Each server node has only one log input file (although I assume it is terabytes sized)

THE ALGORITHM:

1. Coordinator starts a map task in each of the server nodes
	The map task:
	1.a. Reads the input log in chunks of L lines (since the file may be enormous)
	1.b. When a chunk is finished, it appends each line of the chunk in a separate file (a bucket) for each user. This preserves the original order of each user logs in the bucket.
	1.c Node notifies Coordinator that the map is over

2. When all map tasks are over, Coordinator starts reduce task in each server node.
	Reduce task:
	2.a. Tries to consolidate all nodes that it found locally in the previous map task.
		 But before consolidating a user, it asks permission for the Coordinator. 
		 Maybe other node can own a user first, thus the Coordinator must control racing conditions.

	2.b. For each user being consolidated: 
		- The node downloads the user bucket produced by map tasks in all other nodes
		- And merges the sorted buckets line by line in the final output file

3. Finally the Coordinator prints which server node ended up responsible for each consolidated user log.

CODE ORGANIZATION:

There are four modules:

1) log_processor.py: 
	The main module that solves the challenge.
	It starts the simulation threads and contains the Coordinator algorithm.

2) nodes.py:
	Algorithm for the server nodes.

3) log_parser.py:
	Helper module that parses the log entries.

4) util.py
	Helper module that merge files line by line.

Each server node is simulated using a folder. 
Therefore, there are four of them (server_A, server_B, server_C, server_D).
The intermediary buckets produced by map tasks are stored in the "buckets" subfolder.
The output file for each user is stored in the "processed_logs" subfolder.

HOW TO RUN: 

python log_processor.py

The code was tested on Python 2.7 on Mac OS X. 
There are no third-party dependencies.

IMPROVEMENTS:

1) This algorithm assumes similar distribution of log entries between users and nodes.

Since each server node defines which user it is going to consolidate, we may end up with idle server nodes
(that consolidated users with few log entries, for instance).
The Coordinator could be used to leverage idle server nodes starting new reduce tasks with users that were found only on other nodes.

2) Space concerns:

Although it is O(n) space-wise, the algorithm triplicates the total storage taken by logs in the four servers.
It could be improved by deleting buckets after its use. 
In addition to that, in case a server needs a bucket that is in other server, it could process it as a stream
(and not by copying the whole bucket and processing it locally afterwards).
