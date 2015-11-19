Hybrid MPI (HMPI) is a library containing research on optimizing intra-node message passing communication for modern multi-core systems.

To get started with using HMPI, see the guide on the wiki.

A description of the research that has gone into HMPI is available in several publications. Most recently is Andrew Friedley's Ph.D. thesis:

[Shared Memory Optimizations for Distributed Memory Programming Models](https://github.com/jeffhammond/hmpi/blob/master/afriedle-thesis.pdf)

Publications include:

[Hybrid MPI: Efficient Message Passing for Multi-core Systems](http://htor.inf.ethz.ch/publications/index.php?pub=173)

The above paper contains a description of HMPI, particularly of its current process-based design. The below works are based on an earlier thread-based design.

[Ownership Passing: Efficient Distributed Memory Programming on Multi-core Systems](http://htor.inf.ethz.ch/publications/index.php?pub=161)

[Compiling MPI for Many-Core Systems](http://library.llnl.gov/uhtbin/cgisirsi/arirj7ekiQ/MAIN/300540005/9)

And also related work on collectives was done using HMPI:

[NUMA-Aware Shared Memory Collective Communication for MPI](http://htor.inf.ethz.ch/publications/index.php?pub=163)
