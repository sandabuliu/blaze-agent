#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import datetime
import datashape
from blaze.utils import available_memory
from blaze.dispatch import dispatch
from blaze.expr.collections import Expr
from blaze import resource, discover, convert, DataFrame, odo, chunks


__author__ = 'tong'
__version__ = '1.0.0'


DEFAULT_CHUNKSIZE = 1000


class AGENT(object):
    def __init__(self, uri, **kwargs):
        self.uri = uri
        self.kwargs = kwargs

    @property
    def path(self):
        return self.uri[6:]

    def agent(self, chunkno=0, chunksize=DEFAULT_CHUNKSIZE):
        try:
            from agent import source, Agent
        except ImportError:
            raise Exception('Please install python-agent by '
                            'pip install git+https://github.com/sandabuliu/python-agent.git')

        if chunkno > 0:
            startline = chunksize * chunkno
        elif chunkno < 0:
            raise Exception('Not Implement')
        else:
            startline = 0
        return Agent(source.File(self.uri[6:], startline=startline), rule=self.kwargs.get('rule'))


@convert.register(DataFrame, AGENT)
def convert_agent(agt, **kwargs):
    return DataFrame([i for i in agt.agent()])


@convert.register(chunks(DataFrame), AGENT)
def convert_agent(agt, **kwargs):
    def _(ag):
        chunks = kwargs.get('chunks', None)
        ret = []
        for i, item in enumerate(ag, 1):
            if chunks == 0:
                return
            ret.append(item)
            if i % kwargs.get('chunksize', DEFAULT_CHUNKSIZE) == 0:
                yield DataFrame(ret)
                if chunks:
                    chunks -= 1
                ret = []
        yield DataFrame(ret)
    chunkno = kwargs.get('chunkno', 0)
    chunksize = kwargs.get('chunksize', 0)
    return chunks(DataFrame)(_(agt.agent(chunkno, chunksize)))


@discover.register(AGENT)
def discover_agent(agt, **kwargs):
    type_map = {
        'date': datetime.datetime.now(),
        'number': 0.0,
        'string': 'abc'
    }

    fieldtypes = agt.agent().parser.fieldtypes
    dtype = discover([{n: type_map.get(t) for n, t in fieldtypes.items()}])
    return datashape.var * dtype.measure


@dispatch(Expr, AGENT)
def pre_compute(expr, data, **kwargs):
    comfortable_memory = min(1e9, available_memory()/4)

    if os.path.getsize(data.path) > comfortable_memory:
        kwargs['chunksize'] = kwargs.get('chunksize') or DEFAULT_CHUNKSIZE

    leaf = expr._leaves()[0]
    if kwargs.get('chunksize'):
        return odo(data, chunks(DataFrame), dshape=leaf.dshape, **kwargs)
    else:
        return odo(data, DataFrame, dshape=leaf.dshape, **kwargs)


def register(uri='agent:.+', priority=12):
    from odo.convert import ooc_types

    @resource.register(uri, priority=priority)
    def resource_agent(uri, **kwargs):
        return AGENT(uri, **kwargs)
    ooc_types.add(AGENT)
