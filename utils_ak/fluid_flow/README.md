# About 

This is a library for modelling fluid flow in a pipeline.

This is useful to model factory processes, like melting and cooling of metals, or packing of chemicals.

# How it works 

The building blocks are the actors.

There are two types of actors:
- Pipes 
- Junctions (other actors). Note, the term junction is not used in the code models. It's just a name for the non-pipe actors.

Junctions are connected by pipes, and together they form a dag, a pipeline. 

Each junction define the pressure that is put on its pipes (order is not important).

THe different betwee input and output pressure of the pipes is the speed of the flow.


Example 1:
Container 1 (max output pressure: 2, initial amount: 5) -> Pipe 1 -> Container 2 (max input pressure: 1)
The speed of the flow is 1. In 5 seconds all the amount of the input container will be transferred to the output container.

To better understand the possibilities of the library, see the tests in the following order:
- Container
- Processor
- Sequence
- Queue
- Hub
