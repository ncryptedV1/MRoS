# MRoS
MapReduce on Steroids - Project for the Distributed Systems lecture at DHBW Karlsruhe

*Spoiler*: This implementation of MapReduce isn't actually on steroids, as for the following reasons:
1. We don't support the use of drugs of any kind.
2. The goal of this project is to re-implement the fundamental concept of how MapReduce works in an easy to understand manner. Actually going for performance would require very complex algorithmic implementations as well as the use of a distributed file system.
3. The developers of known implementions such as Apache Hadoop are way more knowledgable of the topic and "might" have got a few more resources and time at their disposal.

Still, this project is a good visualization of the fundamental principle of MapReduce. Take a look into the code - I promise it's easy to understand. 😉

## Requirements
- Python (tested on 3.7 & 3.10)

## Capabilities


## Limitations
Our prototype is still in the early stages and has several limitations, but we tried to stay true to the original concept
where possible and viable given the short time we had. The algorithmic idea of MapReduce is simple, but implementing it
properly takes careful planning and requires optimizations.

Our current prototype is just a rough outline and doesn't have all the features yet. One limitation is that it does not
rely on distributed filesystems, which the original does. Hence, there is no network of intertwined nodes that can
self-orchestrate. Instead, we rely on a setup where a controller starts and manages several worker processes.
The controller is responsible for accepting client requests. Those contain the data along with the map() and reduce()
functions they want to see applied. The controller then splits the data into fair chunks for each worker, sends them over,
gathers the results (in multiple steps) and transmits it back to the requesting client.

In addition, establishing communication between the workers is not a straightforward task. In the original implementation,
the mapper processes would distribute the resulting data to the reducers based on their corresponding keys. However, we
have simplified this process in our prototype. Instead, the controller sends the data and map() function in a first step,
then the workers perform the mapping in parallel. The result is sent back to the controller, which then shuffles the data
and redistributes it to the reducers. Using this approach, no interaction between the workers must be implemented. However,
it also means that multiple transmissions to the controller are needed.

The system currently allows for only one pass of map() and reduce() for each request to the controller. More complex
scenarios can be realized by submitting multiple "independent" requests to the controller, each with its own map() and
reduce() function. An example of which is shown in the `matrix_multiplication.py` script. Furthermore, if there is a
requirement for more complex scenarios that involve multiple map() or reduce() steps, our system accommodates this as well.
In such cases, the client can access the controller multiple times, each time with different map() and reduce() functions
specified. An example of this can be seen in the "matrix_multiplication.py" script.

Despite its limitations, the implementation is capable of effectively processing data sets by leveraging the logical
separation of map() and reduce() functions. To ensure fairness, the data is divided into manageable chunks, ensuring
that each worker receives an equitable workload. The processing follows a sequential pattern of map() operations
followed by reduce() operations. Although the implementation is rudimentary, it allows for basic data processing using
the MapReduce paradigm.

## Usage
1. Start the master (e.g. with five workers - ports are assigned automatically):
   1. `python master.py --host localhost --port 8000 --worker-host localhost --worker-amount 5`

This configuration describes a master (orchestrator) that listens to `localhost:8000`.
It manages 5 workers that are also hosted on `localhost`, their ports are automatically assigned.

2. Describe your MapReduce request, such as shown in the word counter example: `python word_counter.py`
3. Enjoy, take a sip of your favorite coffee and wait for your results (if you actually spent time generating a sufficiently large example that you have to wait for an answer)!

## Examples
We've included two examples on how a MapReduce-request could be sent using our MapReduce prototype:
- [`word_counter.py`](https://github.com/ncryptedV1/MRoS/blob/main/word_counter.py)
- [`matrix_multiplication.py`](https://github.com/ncryptedV1/MRoS/blob/main/matrix_multiplication.py)

The word counter is one of the most trivial examples for show-casing MapReduce.
The matrix multiplication example on the other hand, is a bit more sophisticated as it requires two separate MapReduce-requests - one for retrieving all required cell products and the other for summing those products up for each cell in the final matrix.
