from pycallgraph2 import PyCallGraph
from pycallgraph2.output import GraphvizOutput
from assets.example_test.graph_example import main

with PyCallGraph(output=GraphvizOutput()):
    main()
