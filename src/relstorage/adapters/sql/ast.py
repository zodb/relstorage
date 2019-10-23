# -*- coding: utf-8 -*-
"""
Syntax elements.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ._util import Resolvable


class LiteralNode(Resolvable):

    __slots__ = (
        'raw',
        'name',
    )

    def __init__(self, raw):
        self.raw = raw
        self.name = 'anon_%x' % (id(self),)

    @property
    def alias(self):
        return self.name

    def __compile_visit__(self, compiler):
        compiler.emit(str(self.raw))

    def resolve_against(self, table):
        return self

class NullNode(LiteralNode):
    __slots__ = ()

    def __init__(self):
        super(NullNode, self).__init__(None)

    def __compile_visit__(self, compiler):
        compiler.emit_null()

class BooleanNode(LiteralNode):

    __slots__ = ()

class TextNode(LiteralNode):
    __slots__ = ()

def as_node(c):
    if c is None:
        return NullNode()
    if isinstance(c, bool):
        return BooleanNode(c)
    if isinstance(c, int):
        return LiteralNode(c)
    if isinstance(c, str):
        return TextNode(c)
    return c



def resolved_against(columns, table):
    resolved = [
        as_node(c).resolve_against(table)
        for c
        in columns
    ]
    return resolved
