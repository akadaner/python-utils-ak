Here is the corrected version with all mistakes properly fixed while keeping the original meaning intact:

---

# About

This is a library for modeling fluid flow in a pipeline.

This is useful for modeling factory processes, such as melting and cooling metals or packing chemicals.

# How It Works

The building blocks of the system are actors.

There are two types of actors:
- Pipes
- Junctions (other actors). Note that the term "junction" is not used in the code models; it is just a name for non-pipe actors.

Junctions are connected by pipes, and together they form a **DAG** (directed acyclic graph), which represents the pipeline.

Each junction defines the pressure that is applied to its pipes (the order is not important).

The difference between the input and output pressure of the pipes determines the speed of the flow.

### Example 1:
Container 1 (max output pressure: 2, initial amount: 5) → Pipe 1 → Container 2 (max input pressure: 1)

The speed of the flow is 1. In 5 seconds, the entire amount in the input container will be transferred to the output container.

To better understand the capabilities of the library, review the tests in the following order:
- Container
- Processor
- Sequence
- Queue
- Hub
