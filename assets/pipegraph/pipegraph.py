import json
import inspect
import stack_data
import uuid


def get_node_type_from_func(func: object) -> str:
    if func.__name__.startswith('clean_'):
        node_type = 'Cleaning'
    elif func.__name__.startswith('compute_'):
        node_type = 'Computing'
    else:
        node_type = 'Processing'
    return node_type


def get_stack_frame_id(frame_str: str):
    return frame_str.split('id:')[1]


class PipeGraph(object):
    __json = {
        'nodes': [],
        'links': []
    }
    __node_id = 0
    __link_id = 0

    def __init__(self, func):
        self.__func = func
        if func is not None:
            self.__name = func.__name__
        else:
            self.__name = None

    def __call__(self, *args, **kwargs):
        print(self.__name)
        print(self.__func.__doc__)
        print("-----------")
        print(self.add_node(
            doc=self.__func.__doc__,
        ))

        return self.__func(*args, **kwargs)

    @classmethod
    def json(cls):
        print(json.dumps(cls.__json, indent=4))

    def add_node(self, **kwargs):
        node_type = get_node_type_from_func(self.__func)
        current_id = PipeGraph.__node_id
        frame = inspect.currentframe().f_back.f_back
        current_co_name = stack_data.FrameInfo(frame).code.co_name
        node = {'id': current_id, 'uuid': str(uuid.uuid4()), 'name': self.__name, 'type': node_type, 'before': current_co_name}
        for k, v in kwargs.items():
            node[k] = v
        PipeGraph.__json['nodes'].append(node)
        PipeGraph.__node_id += 1
        return current_id

    @classmethod
    def add_link(cls, source_id: str, target_id: str):
        current_id = cls.__link_id
        cls.__json['links'].append({'id': current_id, 'source_id': source_id, 'target_id': target_id})
        cls.__link_id += 1
        return current_id

    @classmethod
    def remove_node(cls, node_id: int):
        cls.__json['nodes'].remove(node_id)

    @classmethod
    def remove_link(cls, link_id: int):
        cls.__json['links'].remove(link_id)

    @classmethod
    def get_node(cls, node_id: int):
        return cls.__json['nodes'][node_id]

    @classmethod
    def get_link(cls, link_id: int):
        return cls.__json['links'][link_id]

    @classmethod
    def get_nodes(cls):
        return cls.__json['nodes']

    @classmethod
    def get_links(cls):
        return cls.__json['links']

    @classmethod
    def get_node_id(cls):
        return cls.__node_id

    @classmethod
    def get_link_id(cls):
        return cls.__link_id

    @classmethod
    def reset(cls):
        cls.__json = {
            'nodes': [],
            'links': []
        }
        cls.__node_id = 0
        cls.__link_id = 0
