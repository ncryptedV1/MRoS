# MRoS
MapReduce on Steroids - Project for the Distributed Systems lecture at DHBW Karlsruhe

*Spoiler*: This implementation of MapReduce isn't actually on steroids, as for the following reasons:
1. We don't support the use of drugs of any kind.
2. The goal of this project is to re-implement the fundamental concept of how MapReduce works in an easy to understand manner. Actually going for performance would require very complex algorithmic implementations as well as the use of a distributed file system.
3. The developers of known implementions such as Apache Hadoop are way more knowledgable of the topic and "might" have got a few more resources and time at their disposal.

Still, this project is a good visualization of the fundamental principle of MapReduce. Take a look into the code - I promise it's easy to understand. 😉

## Requirements
- Python (tested on 3.7)

## Usage
1. Start the workers (e.g. two workers - the ports can be chosen at will):
    1. `python worker.py --host localhost --port 8001`
    2. `python worker.py --host localhost --port 8002`
2. Start the master (again, ports can be chosen at will): `python master.py --host localhost --port 8000 --worker-hosts localhost localhost --worker-ports 8001 8002`
3. Send your MapReduce-requests, such as shown in word counter example: `python word_counter.py`
4. Enjoy, take a sip of your favorite coffee and wait for your results (if you actually spent time generating a sufficiently large example that you have to wait for an answer)!
