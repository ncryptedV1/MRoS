# MRoS
MapReduce on Steroids - Project for Distributed Systems lecture at DHBW Karlsruhe \
*Spoiler*: This isn't actually a MapReduce implementation on steroids, which has multiple sensible reason:
1. We don't support the use of drugs of any kind.
2. The developers of known implementions such as Apache Hadoop are way more knowledgable of the topic and might've got a few more resources and time at their disposal.

Still, this project is a good visualization of the general principle of MapReduce. Take a look into the code - I promise it's mostly easy to understand. 😉

## Requirements
- Python (tested on 3.7)

## Usage
1. Start the workers
  1. `python worker.py --host localhost --port 8001`
  2. `python worker.py --host localhost --port 8002`
2. Start the master: `python master.py --host localhost --port 8000 --worker-hosts localhost localhost --worker-ports 8001 8002`
3. Send requests, such as the client.py example: `python client.py`
4. Enjoy, take a sip of your favorite coffee and wait (if you actually spent time generating a sufficiently large example that you have to wait for an answer) for your results!
