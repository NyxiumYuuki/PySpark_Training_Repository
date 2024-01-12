import pygraphviz as pgv


my_graph = pgv.AGraph(id='my_graph', name='my_graph')
my_graph.add_node(
    'RAW_dataset_1',
    label='RAW_dataset_1',
    tooltip='tooltip text \r next line',
    URL='https://google.be/',
    target='_blank'
)
my_graph.add_node(
    'node 1'
)
my_graph.layout(prog='dot')
my_graph.draw(path="../graphviz/my_graph.svg", format="svg")
