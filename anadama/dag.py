import sys
from operator import add, attrgetter, itemgetter
from collections import defaultdict, deque

import networkx

from .util import (
    SerializableMixin, 
    generator_flatten
)
from . import (
    picklerunner
)

TMP_FILE_DIR = "/tmp"

class DagNode(SerializableMixin):
    def __init__(self, name, action_func, targets, deps, **kwargs):
        self.name = name
        self.action_func = action_func
        self.targets = set(targets)
        self.deps = set(deps)

        self._orig_task = None
        if self.action_func is None:
            self.action_func = self.execute

        # set extra fields for others to play with json
        self.extra_fields = kwargs

        self._cmd = ""
         
    @property
    def _command(self):
        if self._orig_task and not self._cmd:
            self._cmd = picklerunner.tmp(
                self._orig_task, 
                dir=TMP_FILE_DIR
            ).path
        return self._cmd

    def execute(self):
        self.action_func()

    def _custom_serialize(self):
        ret = self.extra_fields
        ret.update({
            "id": hash(self),
            "name": self.name,
            "command": self._command,
            "produces": list(self.targets),
            "depends": list(self.deps),
        })
        return ret
        

    @classmethod
    def from_doit_task(cls, task):
        ret = cls(
            name = task.name,
            action_func = task.execute,
            targets = task.targets,
            deps = task.file_dep,
        )
        ret._orig_task = task
        return ret

    def __hash__(self):
        return hash(self.name)
        
    def __str__(self):
        return "DagNode: " +str(self.name)

    __repr__ = __str__



def item_or_list(key):
    def getter(container_dict):
        return container_dict.get(key, [])
        
    return getter


def _search(node, idx, using):
    set_of_hits = map(lambda x: idx.get(x, []), using(node))
    flattened = reduce(add, set_of_hits, [])
    return list(set(flattened)) # deduped


targets = attrgetter("targets")
def _map_targets_to_children(node, idx):
    """searches for targets; all children of the current node shouldn't at
    the same time be children of other nodes.
    """
    return _search(node, idx, using=targets)


deps = attrgetter("deps")
def _map_deps_to_parent(node, idx):
    """non-destructive search; many different nodes can rely on the same
    parent.
    """
    return _search(node, idx, using=deps)


def taskiter(nodes, idx_by_dep, idx_by_tgt, root_node):
    for node in nodes:
        for child in _map_targets_to_children(node, idx_by_dep):
            yield node, child
        parents = _map_deps_to_parent(node, idx_by_tgt)
        if parents:
            for parent in parents:
                yield parent, node
        else:
            yield root_node, node


def indexby(task_list, attr, using=attrgetter):
    key_func = using(attr)
    idx = defaultdict(list)
    for task in task_list:
        for item in key_func(task):
            idx[item].append(task)
            
    return idx


def assemble(tasks, root_attrs=dict()):
    nodes = [ DagNode.from_doit_task(t) for t in tasks ]
    nodes_by_dep = indexby(nodes, attr="deps")
    nodes_by_target = indexby(nodes, attr="targets")

    root_node = DagNode(name="root",
                        action_func=None, 
                        targets=list(), 
                        deps=list(),
                        **root_attrs)

    dag = networkx.DiGraph()
    dag.add_edges_from(
        taskiter(nodes, nodes_by_dep, nodes_by_target, root_node))
    return dag, nodes


def prune(dag, nodes_to_prune):
    """Remove `nodes_to_prune` from `dag`, making sure that children of
    the pruned node are not removed"""

    prune_set = set(nodes_to_prune)
    while True:
        try:
            node = prune_set.pop()
        except KeyError:
            break

        parents = dag.predecessors(node)
        if all( bool(n in prune_set) for n in parents ):
            to_remove = [node] + parents
            map(prune_set.discard, to_remove)
            dag.remove_nodes_from(to_remove)

    return dag


def filter_tree(task_dicts, filters, hash_key="name"):

    class HashDict(dict):
        def __hash__(self):
            return hash(self[hash_key])
        def __eq__(self, other):
            return self[hash_key] == other[hash_key]

    task_dicts = [ HashDict(task_dict) for task_dict in task_dicts ]
    dag = _assemble_task_dicts(task_dicts)
    for task_dict in task_dicts:
        if any( filter_(task_dict) for filter_ in filters ):
            dag.remove_nodes_from([task_dict]+dag.successors(task_dict))

    return dag.nodes()
            

def _assemble_task_dicts(task_dicts):
    by_dep = indexby(task_dicts, attr="file_dep", using=item_or_list)
    dag = networkx.DiGraph()
    targets = itemgetter("targets")
    for task_dict in task_dicts:
        for child in _search(task_dict, by_dep, using=targets):
            dag.add_edge(task_dict, child)
    return dag
