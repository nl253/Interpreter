import logging
import re
import threading
import typing
from os.path import join, dirname, abspath
from time import sleep, time
from uuid import uuid4
from random import randint
from collections import ChainMap
from concurrent.futures import Future
from dataclasses import dataclass, field
from datetime import datetime
from functools import reduce
from json import dumps
from logging import getLogger
from operator import add, sub, mul, truediv
from pprint import pformat
from queue import Queue
from traceback import format_tb
from threading import local
from typing import Any, Callable, NoReturn, Iterable, Optional


T = typing.TypeVar('T')

LOG_LVL = logging.WARNING

THREAD_LOCALS = local()

logging.basicConfig(
    level=LOG_LVL,
    format='%(levelname)s %(thread)d %(filename)s:%(lineno)d [%(funcName)s] %(message)s')

log = getLogger(__name__)

REGEX = re.compile('|'.join([
    r'(?P<ellipsis>[.]{3})',
    r'(?P<at>[@])',
    r'(?P<rdarrow>[=][>])',
    r'(?P<lshift>[<][<])',
    r'(?P<rshift>[>][>])',
    r'(?P<fn>fn)',
    r'(?P<nil>nil)',
    r'(?P<if>if)',
    r'(?P<match>match)',
    r'(?P<lchev>[<])',
    r'(?P<rchev>[>])',
    r'(?P<neg>[-])',
    r'(?P<eq>[=])',
    r'(?P<amp>[&])',
    r'(?P<excl>[!])',
    r'(?P<comment>#[^\n\r]*)',
    r'(?P<bool>true|false)',
    r'(?P<lbrack>\[)',
    r'(?P<rbrack>\])',
    r'(?P<str>("[^"]*"|\'[^\']*\'))',
    r'(?P<float>[0-9]+\.[0-9]+)',
    r'(?P<int>[0-9]+)',
    r'(?P<lparen>[(])',
    r'(?P<rparen>[)])',
    r'(?P<var>[_a-z]+[*!?]?)',
]))


class Environment(ChainMap):
    def __setitem__(self, key, value):
        for d in self.maps:
            if key in d:
                d[key] = value
                return
        m: dict[str, Any] = self.maps[0]
        m[key] = value


@dataclass(frozen=True, repr=False)
class ConcurrencyContext:
    actors: dict[str, Any] = field(repr=False, default_factory=dict)
    max_actors: threading.Semaphore = field(repr=True, default_factory=lambda: threading.Semaphore(value=500))
    lk_r: threading.RLock = field(repr=False, default_factory=threading.RLock)
    lk_w: threading.RLock = field(repr=False, default_factory=threading.RLock)
    sleep_duration: float = field(repr=True, default=0.01)
    timeout: float = field(repr=True, default=30000)

    def wait(self):
        with self.lk_r:
            with self.lk_w:
                for actor_id, actor in list(self.actors.items()):
                    if actor_id != THREAD_LOCALS.ident:
                        log.info('waiting for completion of %s', repr(actor))
                        actor.result
                        del self.actors[actor_id]

    def get(self, ident: str) -> Any:
        with self.lk_r:
            a = self.actors[ident]
        return a

    def add(self, a: Any) -> None:
        with self.lk_r:
            with self.lk_w:
                self.actors[a.ident] = a

    def remove(self, ident: str) -> None:
        with self.lk_r:
            with self.lk_w:
                del self.actors[ident]


def fmt(x: Any) -> str:
    match x:
        case Future() as y:
            y: Future
            if y.done() and not y.cancelled():
                return f'& {fmt(y.result())}'
            else:
                return '& ?'
        case str(y):
            y: str
            return ("'" + y + "'") if '"' in y else ('"' + y + '"')
        case int(y) | float(y):
            return str(y)
        case bool(y):
            return str(y).lower()
        case list(y):
            return '[{inner}]'.format(inner=' '.join(map(fmt, y)))
        case None:
            return 'nil'
        case default:
            return repr(default)


@dataclass(repr=False, init=False)
class Actor:
    _thread: threading.Thread
    _queue: Queue
    _result: Optional[Any]
    _ident: str
    _ctxt: ConcurrencyContext
    _start: int
    _end: int

    def __init__(self, ctxt: ConcurrencyContext, fn: Callable, args: Optional[list[Any]] = None, kwargs: Optional[dict[str, Any]] = None):
        self._ctxt = ctxt
        self._queue = Queue()
        self._result = None
        self._ident = str(uuid4())
        self._fn = fn
        self._start = 0
        self._end = 0
        self._thread = threading.Thread(target=self._helper,
                                        args=(args or []),
                                        kwargs=(kwargs or dict()))
        assert self._ctxt.max_actors.acquire(timeout=ctxt.timeout), 'locked!'
        self._thread.start()
        ctxt.add(self)
        log.debug('spawned thread: %s', self.ident)

    @property
    def ident(self) -> str:
        return self._ident

    def _helper(self, *args, **kwargs) -> Any:
        THREAD_LOCALS.ident = self.ident

        if LOG_LVL <= logging.DEBUG:
            log.debug('calling: %s with args: %s and kwargs: %s', str(self._fn), str(args), str(kwargs))

        self._start = int(time())

        try:
            self._result = self._fn(*args, **kwargs)

        except BaseException as e:
            self._result = e

        self._end = int(time())
        self._ctxt.max_actors.release()

        if LOG_LVL <= logging.DEBUG:
            log.debug('result set to be: %s', str(self._result))
            log.debug('took: %d', self.duration)

    @property
    def result(self) -> Any:
        self._thread.join(timeout=self._ctxt.timeout)
        match self._result:
            case BaseException(e):
                raise e
            case default:
                return default

    @property
    def duration(self) -> int:
        if self._end is None:
            return int(time()) - self._start
        else:
            return self._end - self._start

    def send(self, msg: Any) -> None:
        self._queue.put(msg)

    def receive(self) -> Any:
        return self._queue.get()

    def __repr__(self):
        if self._thread.is_alive():
            return f'Actor(id={self.ident}, result="?", duration={self.duration})'
        else:
            return f'Actor(id={self.ident}, result={fmt(self._result)}, duration={self.duration})'


@dataclass(frozen=True, repr=False)
class BuiltInFn:
    fn: Callable[[list[Any], Environment, ConcurrencyContext], Any]
    arity: int

    def __repr__(self) -> str:
        return 'fn {args} => ?'.format(args=' '.join(chr(ord('a') + i) for i in range(self.arity)))


@dataclass(frozen=True, repr=False)
class Token:
    name: str
    value: str

    def __repr__(self):
        return f'{self.name}({repr(self.value)})'


@dataclass(frozen=True)
class BaseAstNode:

    def evaluate(self, ctxt: ConcurrencyContext, local_vars: Environment, global_vars: Environment) -> Any:
        match self:
            case NegativeAstNode(val):
                return -val.evaluate(ctxt, local_vars, global_vars)
            case AstNode('int', val):
                return int(val)
            case AstNode('float', val):
                return float(val)
            case AstNode('str', val):
                return val
            case AstNode('nil'):
                return None
            case ReceiveMsgAstNode():
                actor_id = THREAD_LOCALS.ident
                log.debug('receiving msg for actor with id: %s', actor_id)
                while actor_id not in ctxt.actors:
                    log.warning('actor id %s not in ConcurrencyContext!', actor_id)
                    sleep(ctxt.sleep_duration)
                msg = ctxt.get(actor_id).receive()
                if LOG_LVL <= logging.DEBUG:
                    log.debug('received %s message for actor with id: %s', str(msg), actor_id)
                return msg
            case SendMsgAstNode(actor=actor_node, msg=msg_node):
                actor_node: BaseAstNode
                msg_node: BaseAstNode
                msg = msg_node.evaluate(ctxt, local_vars, global_vars)
                actor_id: str = actor_node.evaluate(ctxt, local_vars, global_vars)
                if LOG_LVL <= logging.DEBUG:
                    log.debug('sending %s message for actor with id: %s', str(msg), actor_id)
                while actor_id not in ctxt.actors:
                    log.debug('actor id %s not in ConcurrencyContext!', actor_id)
                    sleep(ctxt.sleep_duration)
                actor: Actor = ctxt.get(actor_id)
                if LOG_LVL <= logging.DEBUG:
                    log.debug('found actor: %s', str(actor))
                actor.send(msg)
                log.debug('msg added to mailbox')
                return actor_id
            case ListAstNode(items=items):
                return [i.evaluate(ctxt, local_vars, global_vars) for i in items]
            case AstNode('bool', val):
                return val == 'true'
            case MatchAstNode(expr, patterns):
                log.debug('match node')
                value = expr.evaluate(ctxt, local_vars, global_vars)
                for pattern, expr in patterns:
                    match pattern_match(value, pattern, expr, ctxt, local_vars, global_vars):
                        case True, result:
                            return result
                raise Exception('neither of patterns matched expression ' + str(value) + ': ' + str(patterns))
            case IfAstNode(condition=condition, if_true=if_true, if_false=if_false):
                log.debug('if node')
                if condition.evaluate(ctxt, local_vars, global_vars):
                    log.debug('condition was true, skipping if_false clause')
                    return if_true.evaluate(ctxt, local_vars, global_vars)
                else:
                    log.debug('condition was false, skipping if_true clause')
                    return if_false.evaluate(ctxt, local_vars, global_vars)
            case FnDefAstNode(fn=fn):
                fn: CustomFn
                fn.scope_vars = global_vars
                return fn
            case AssignAstNode(var_name=key, var_value=val):
                if LOG_LVL <= logging.DEBUG:
                    log.debug('setting %s variable, value = %s', key, str(val))
                assert key not in global_vars, 'variable bound! current value = ' + str(global_vars[key])
                global_vars[key] = val.evaluate(ctxt, local_vars, global_vars)
                return global_vars[key]
            case VarLookupAstNode(var_name) if var_name in local_vars:
                return local_vars[var_name]
            case VarLookupAstNode(var_name) if var_name in global_vars:
                return global_vars[var_name]
            case VarLookupAstNode(var_name) if var_name in BUILT_INS:
                log.debug(var_name)
                return BUILT_INS[var_name]
            case WaitAsyncExprAstNode(expr):
                expr: BaseAstNode
                match expr.evaluate(ctxt, local_vars, global_vars):
                    case str(actor_id):
                        log.debug('waiting for actor with id: %s', actor_id)
                        while actor_id not in ctxt.actors:
                            log.warning('actor id %s not in ConcurrencyContext!', actor_id)
                            sleep(ctxt.sleep_duration)
                        actor = ctxt.get(actor_id)
                        if LOG_LVL <= logging.DEBUG:
                            log.debug('found actor: %s', str(actor))
                        result = actor.result
                        if LOG_LVL <= logging.DEBUG:
                            log.debug('got result: %s', fmt(result))
                        return result
                    case default:
                        raise Exception('expected actor to be awaited on but found: ' + str(default))

            case AsyncExprAstNode(expr):
                return Actor(ctxt=ctxt, fn=expr.evaluate, args=[ctxt, local_vars, global_vars]).ident

            case FnCallAstNode(fn, args):
                fn: BaseAstNode
                evaluated_args = [a.evaluate(ctxt, local_vars, global_vars) for a in args]
                if LOG_LVL <= logging.DEBUG:
                    log.debug('fn call %s: args: %s', str(fn), pformat(evaluated_args))
                match fn.evaluate(ctxt, local_vars, global_vars):
                    case CustomFn() as f:
                        return f.evaluate(ctxt, evaluated_args, global_vars)
                    case BuiltInFn(fn=fn):
                        fn: Callable[[list[Any], Environment, ConcurrencyContext], Any]
                        return fn(evaluated_args, global_vars, local_vars, ctxt)
                    case default:
                        raise Exception('expected function to evaluate but found: {0}'.format(str(default)))

            case default:
                raise Exception('evaluation: no idea what to do with: {0}'.format(str(default)))

    def simplify(self):
        return self
        # match self:
        #
        #     case FnCallAstNode(fn, children, False):
        #         return FnCallAstNode(fn, tuple(c.simplify() for c in children)).simplify()
        #
        #     case FnCallAstNode('add', [AstNode('int', '0'), *rest] | [*rest, AstNode('int', '0')], True):
        #         return rest[0]
        #
        #     case FnCallAstNode('and', [AstNode('bool', 'false'), *rest] | [*rest, AstNode('bool', 'false')], True):
        #         return AstNode('bool', 'false')
        #
        #     case FnCallAstNode('or', [AstNode('bool', 'true'), *rest] | [*rest, AstNode('bool', 'true')], True):
        #         return AstNode('bool', 'true')
        #
        #     case FnCallAstNode('sub', [*rest, AstNode('int', '0')], True):
        #         return rest[0]
        #
        #     case FnCallAstNode('not', [AstNode('bool', 'true')], True):
        #         return AstNode('bool', 'false')
        #
        #     case FnCallAstNode('not', [AstNode('bool', 'false')], True):
        #         return AstNode('bool', 'true')
        #
        #     case FnCallAstNode('mul', [AstNode('int', '0'), *_] | [*_, AstNode('int', '0')], True):
        #         return AstNode('int', '0')
        #
        #     case FnCallAstNode('mul', [AstNode('int', '1'), *rest] | [*rest, AstNode('int', '1')], True):
        #         return rest[0]
        #
        #     case FnCallAstNode('div', [*rest, AstNode('int', '1')], True):
        #         return rest[0]
        #
        #     case FnCallAstNode('div', [AstNode(type1, val1), AstNode(type2, val2)], True) if type1 == type2 and val1 == val2:
        #         return AstNode('int', '1')
        #
        #     case FnCallAstNode('str', [AstNode('int' | 'str' | 'float', val)], True):
        #         return AstNode('str', str(val))
        #
        #     case default:
        #         return default


@dataclass(frozen=True, repr=False)
class AstNode(BaseAstNode):
    type: str
    value: str

    def __repr__(self):
        match self.type:
            case 'int' | 'float':
                return self.value
            case _:
                return repr(self.value)


@dataclass(frozen=True, repr=False)
class NegativeAstNode(BaseAstNode):
    value: BaseAstNode

    def __repr__(self):
        return '-' + repr(self.value)


@dataclass(frozen=True, repr=False)
class PatternPairAstNode(BaseAstNode):
    pattern: BaseAstNode
    expr: BaseAstNode

    def __repr__(self):
        return '@' + repr(self.pattern) + ' ' + repr(self.expr)


@dataclass(frozen=True, repr=False)
class VarLookupAstNode(BaseAstNode):
    var_name: str

    def __repr__(self):
        return self.var_name


@dataclass(frozen=True, repr=False)
class EllipsisAstNode(BaseAstNode):
    var_name: str

    def __repr__(self):
        return '...{var_name}'.format(var_name=repr(self.var_name))


@dataclass(frozen=False, repr=False)
class CustomFn:
    params: tuple[str, ...]
    node: BaseAstNode
    scope_vars: Environment

    def evaluate(self, ctxt: ConcurrencyContext, args: list[Any], global_vars: Environment) -> Any:
        assert len(args) == len(self.params), f'not enough args supplied, expected {len(self.params)} but got {len(args)}'

        additional_maps = list(self.scope_vars.maps)

        for m in global_vars.maps:
            if m not in additional_maps:
                additional_maps.append(m)

        return self.node.evaluate(ctxt,
                                  Environment({p_name: p_val for p_name, p_val in zip(self.params, args)}),
                                  Environment(*additional_maps))

    def __repr__(self) -> str:
        try:
            return 'fn {args} => {expr}'.format(args=' '.join(self.params), expr=repr(self.node))
        except Exception as e:
            return 'fn ? => ?'


@dataclass(frozen=True, repr=False)
class ListAstNode(BaseAstNode):
    items: tuple[BaseAstNode, ...]

    def __repr__(self):
        return '[{inner}]'.format(inner=' '.join(map(repr, self.items)))


@dataclass(frozen=True, repr=False)
class FnArgsAstNode(BaseAstNode):
    items: tuple[BaseAstNode, ...]

    def __repr__(self):
        return '<{inner}>'.format(inner=' '.join(map(repr, self.items)))


@dataclass(frozen=True, repr=False)
class FnCallAstNode(BaseAstNode):
    fn: BaseAstNode
    args: tuple[BaseAstNode, ...]
    simplified: bool = field(default=False, repr=False)

    def __repr__(self):
        return '(f)[args]'.format(f=repr(self.fn), args=' '.join(map(repr, self.args)))


@dataclass(frozen=True, repr=False)
class AsyncExprAstNode(BaseAstNode):
    expr: BaseAstNode

    def __repr__(self) -> str:
        return '& {expr}'.format(expr=repr(self.expr))


@dataclass(frozen=True, repr=False)
class WaitAsyncExprAstNode(BaseAstNode):
    fn: BaseAstNode

    def __repr__(self) -> str:
        return '! {expr}'.format(expr=repr(self.fn))


@dataclass(frozen=True, repr=False)
class FnDefAstNode(BaseAstNode):
    fn: CustomFn

    def __repr__(self):
        return repr(self.fn)


@dataclass(frozen=True, repr=False)
class SendMsgAstNode(BaseAstNode):
    msg: BaseAstNode
    actor: BaseAstNode

    def __repr__(self):
        try:
            return repr(self.msg) + ' >> ' + repr(self.actor)
        except Exception as e:
            return '? >> ?'


@dataclass(frozen=True, repr=False)
class ReceiveMsgAstNode(BaseAstNode):

    def __repr__(self):
        return '<<'


@dataclass(frozen=True, repr=False)
class AssignAstNode(BaseAstNode):
    var_name: str
    var_value: BaseAstNode

    def __repr__(self):
        return '{var_name} = {expr}'.format(var_name=self.var_name, expr=repr(self.var_value))


@dataclass(frozen=True, repr=False)
class IfAstNode(BaseAstNode):
    condition: BaseAstNode
    if_true: BaseAstNode
    if_false: BaseAstNode

    def __repr__(self):
        return 'if {condition} {if_true} {if_false}'.format(condition=repr(self.condition),
                                                            if_true=repr(self.if_true),
                                                            if_false=repr(self.if_false))


def chunks(n: int, xs: tuple[T, ...]) -> Iterable[tuple[T]]:
    """
    >>> tuple(chunks(2, (1, 2, 3, 4, 5)))
    ((1, 2), (3, 4), (5,))
    >>> tuple(chunks(1, (1, 2)))
    ((1,), (2,))
    >>> tuple(chunks(1, tuple()))
    ()
    """
    left = 0
    right = n
    while left < len(xs):
        yield xs[left:right]
        left += n
        right += n


@dataclass(frozen=True, repr=False)
class MatchAstNode(BaseAstNode):
    expr: BaseAstNode
    patterns: tuple[tuple[BaseAstNode, BaseAstNode], ...]

    def __repr__(self):
        patterns: list[str] = [fmt(p) for p, _ in self.patterns]
        max_len: int = reduce(max, map(len, patterns), 0)
        patterns_aligned = [s.ljust(max_len) for s in patterns]
        expressions = [fmt(e) for _, e in self.patterns]
        return '''match {expr} [
    {patterns}
]'''.format(expr=repr(self.expr), patterns='\n'.join(' '.join(('@{e}'.format(e=pair[0]), pair[1])) for pair in zip(patterns_aligned, expressions)))


def pattern_match_check(val: Any, pattern: BaseAstNode, local_vars: Environment, global_vars: Environment) -> tuple[bool, Optional[dict[str, Any]]]:
    match pattern:
        case NegativeAstNode(node):
            node: BaseAstNode
            return pattern_match_check(-val, node, local_vars, global_vars)
        case AstNode(type='int', value=value) if int(value) == val:
            return True, None
        case AstNode(type='float', value=value) if float(value) == val:
            return True, None
        case AstNode(type='str', value=value) if value == val:
            return True, None
        case AstNode(type='bool', value='true') if val is True:
            return True, None
        case AstNode(type='bool', value='false') if val is False:
            return True, None
        case AstNode(type='nil', value=value) if val is None:
            return True, None
        case VarLookupAstNode(var_name) if (var_name in global_vars and global_vars[var_name] == val) or var_name not in global_vars:
            return True, {var_name: val}
        case ListAstNode(list_node_items) if isinstance(val, list) and len(val) >= len(list_node_items) > 0 and isinstance(list_node_items[-1], EllipsisAstNode):
            match list(list_node_items):
                case [*items, EllipsisAstNode(var_name)]:
                    items: tuple[BaseAstNode, ...]
                    val: list[Any]
                    local_vars_ = {var_name: val[len(items):]}
                    for v, p in zip(val[:len(items)], items):
                        match pattern_match_check(v, p, local_vars, global_vars):
                            case False, _:
                                return False, None
                            case True, d if d:
                                local_vars_.update(d)
                    return True, local_vars_

        case ListAstNode(items) if isinstance(val, list) and len(items) == len(val):
            local_vars_ = dict()
            for v, p in zip(val, items):
                match pattern_match_check(v, p, local_vars, global_vars):
                    case False, _:
                        return False, None
                    case True, d if d:
                        local_vars_.update(d)
            return True, local_vars_

        case default:
            return False, None


def pattern_match(val: Any,
                  pattern: BaseAstNode,
                  expr: BaseAstNode,
                  ctxt: ConcurrencyContext,
                  local_vars: Environment,
                  global_vars: Environment) -> tuple[bool, Optional[Any]]:
    match pattern:
        case NegativeAstNode(value):
            value: BaseAstNode
            return pattern_match(-val, value, expr, ctxt, local_vars, global_vars)
        case AstNode(type='int', value=value) if int(value) == val:
            return True, expr.evaluate(ctxt, local_vars, global_vars)
        case AstNode(type='float', value=value) if float(value) == val:
            return True, expr.evaluate(ctxt, local_vars, global_vars)
        case AstNode(type='str', value=value) if value == val:
            return True, expr.evaluate(ctxt, local_vars, global_vars)
        case AstNode(type='bool', value='true') if val is True:
            return True, expr.evaluate(ctxt, local_vars, global_vars)
        case AstNode(type='bool', value='false') if val is False:
            return True, expr.evaluate(ctxt, local_vars, global_vars)
        case AstNode(type='nil') if val is None:
            return True, expr.evaluate(ctxt, local_vars, global_vars)
        case VarLookupAstNode(var_name) if (var_name in global_vars and global_vars[var_name] == val) or var_name not in global_vars:
            return True, expr.evaluate(ctxt, Environment({var_name: val}, *local_vars.maps), global_vars)
        case ListAstNode(list_node_items) if isinstance(val, list) and len(list_node_items) > 0 and isinstance(list_node_items[-1], EllipsisAstNode):
            match list(list_node_items):
                case [*items, EllipsisAstNode(var_name)]:
                    items: tuple[BaseAstNode, ...]
                    val: list[Any]
                    local_vars_ = {var_name: val[len(items):]}
                    for v, p in zip(val[:len(items)], items):
                        match pattern_match_check(v, p, local_vars, global_vars):
                            case False, _:
                                return False, None
                            case True, d if d:
                                local_vars_.update(d)
                    if LOG_LVL <= logging.DEBUG:
                        log.debug('evaluating a pattern with ..., local vars: %s', str(local_vars))
                    return True, expr.evaluate(ctxt, Environment(local_vars_, *local_vars.maps), global_vars)

        case ListAstNode(items) if isinstance(val, list) and len(val) == len(items):
            items: tuple[BaseAstNode, ...]
            val: list[Any]
            local_vars_ = dict()
            for v, p in zip(val, items):
                match pattern_match_check(v, p, local_vars, global_vars):
                    case False, _:
                        return False, None
                    case True, d if d:
                        local_vars_.update(d)
            return True, expr.evaluate(ctxt, Environment(local_vars_, *local_vars.maps), global_vars)

        case default:
            return False, None


@dataclass(frozen=True)
class Result:
    node: BaseAstNode
    rest: list[Token]


def panic(msg: str) -> NoReturn:
    raise Exception(msg)


BUILT_INS: dict[str, BuiltInFn] = {
    # math
    '-': BuiltInFn(lambda args, local_vars, global_vars, ctxt: -args[0], 1),
    'add': BuiltInFn(lambda args, local_vars, global_vars, ctxt: reduce(add, args), 2),
    'sub': BuiltInFn(lambda args, local_vars, global_vars, ctxt: reduce(sub, args), 2),
    'mul': BuiltInFn(lambda args, local_vars, global_vars, ctxt: reduce(mul, args), 2),
    'div': BuiltInFn(lambda args, local_vars, global_vars, ctxt: reduce(truediv, args), 2),

    'rand': BuiltInFn(lambda args, local_vars, global_vars, ctxt: randint(args[0], args[1]), 2),

    # lists
    'filter': BuiltInFn(lambda args, local_vars, global_vars, ctxt: [x for x in args[1] if (args[0].evaluate(ctxt, [x], local_vars, global_vars) if isinstance(args[0], CustomFn) else args[0].fn(ctxt, x, global_vars))], 2),

    # strings
    'cat': BuiltInFn(lambda args, local_vars, global_vars, ctxt: reduce(add, args), 2),

    # date / time
    'now': BuiltInFn(lambda args, local_vars, global_vars, ctxt: datetime.now().strftime('%c'), 0),

    # logical
    'not': BuiltInFn(lambda args, local_vars, global_vars, ctxt: not args[0], 1),
    'and': BuiltInFn(lambda args, local_vars, global_vars, ctxt: reduce(lambda x, y: x and y, args), 2),
    'or': BuiltInFn(lambda args, local_vars, global_vars, ctxt: reduce(lambda x, y: x or y, args), 2),

    # lists
    'nth': BuiltInFn(lambda args, local_vars, global_vars, ctxt: args[1][args[0]], 2),
    'len': BuiltInFn(lambda args, local_vars, global_vars, ctxt: len(args[0]), 1),


    # comparison
    'eq?': BuiltInFn(lambda args, local_vars, global_vars, ctxt: reduce(lambda x, y: x == y, args), 2),
    'gt?': BuiltInFn(lambda args, local_vars, global_vars, ctxt: reduce(lambda x, y: x > y, args), 2),
    'lt?': BuiltInFn(lambda args, local_vars, global_vars, ctxt: reduce(lambda x, y: x < y, args), 2),
    'ge?': BuiltInFn(lambda args, local_vars, global_vars, ctxt: reduce(lambda x, y: x >= y, args), 2),
    'le?': BuiltInFn(lambda args, local_vars, global_vars, ctxt: reduce(lambda x, y: x <= y, args), 2),

    # conversion
    'json': BuiltInFn(lambda args, local_vars, global_vars, ctxt: dumps(args[0]), 1),
    'str': BuiltInFn(lambda args, local_vars, global_vars, ctxt: str(args[0]), 1),

    'eval': BuiltInFn(lambda args, local_vars, global_vars, ctxt: args[0].evaluate(ctxt, [], local_vars, global_vars), 1),

    # concurrency
    'self': BuiltInFn(lambda args, local_vars, global_vars, ctxt: THREAD_LOCALS.ident, 0),
    'sleep': BuiltInFn(lambda args, local_vars, global_vars, ctxt: sleep(args[0]), 1),

    # runtime
    'panic': BuiltInFn(lambda args, local_vars, global_vars, ctxt: panic(args[0]), 1),
}


def parse(tokens: list[Token]) -> Result:
    outcome: Result = None

    match tokens:

        case [Token(name='comment'), *rest]:
            return parse(rest)

        case [Token(name='ellipsis'), *rest]:
            match parse(rest):
                case Result(VarLookupAstNode(var_name), rest_after_var_node):
                    return Result(EllipsisAstNode(var_name), rest_after_var_node)
                case default:
                    raise Exception('expected var name after ... but found: ' + str(default))

        case [Token(name='at'), *rest]:
            pair_pattern = parse(rest)
            match_expr = parse(pair_pattern.rest)
            return Result(PatternPairAstNode(pair_pattern.node, match_expr.node), match_expr.rest)

        case [Token(name=('rparen' | 'rdarrow' | 'rchev') as name, value=value), *rest]:
            return Result(AstNode(name, value), rest)

        case [Token(name=('bool' | 'int' | 'float' | 'nil' | 'eq' | 'rbrack') as name, value = value), *rest]:
            outcome = Result(AstNode(name, value), rest)

        case [Token(name='neg'), *rest]:
            value = parse(rest)
            outcome = Result(NegativeAstNode(value.node), value.rest)

        case [Token(name='excl'), *rest]:
            match parse(rest):
                case Result(n, rest2):
                    outcome = Result(WaitAsyncExprAstNode(n), rest2)
                case default:
                    raise Exception('expected expr call after & but found: {0}'.format(str(default)))

        case [Token(name='lshift'), *rest]:
            outcome = Result(ReceiveMsgAstNode(), rest)

        case [Token(name='amp'), *rest]:
            match parse(rest):
                case Result(n, rest2):
                    outcome = Result(AsyncExprAstNode(n), rest2)
                case default:
                    raise Exception('expected expr call after & but found: {0}'.format(str(default)))

        case [Token(name='lparen'), *rest]:
            result = parse(rest)
            rparen = parse(result.rest)
            match rparen.node:
                case AstNode('rparen', ')'):
                    outcome = Result(result.node, rparen.rest)
                case default:
                    raise Exception('expected rparen ) but found: {0}'.format(str(default)))

        # strings
        case [Token(name='str', value=val), *rest]:
            outcome = Result(AstNode('str', val[1:-1]), rest)

        # list
        case [Token(name='lbrack'), *rest]:
            children: list[BaseAstNode] = []
            next_child = parse(rest)
            current_rest = next_child.rest

            while True:
                match next_child.node:
                    case AstNode(type='rbrack'):
                        outcome = Result(ListAstNode(tuple(children)), current_rest)
                        break
                    case n:
                        children.append(n)
                        next_child = parse(current_rest)
                        current_rest = next_child.rest

        # fn args
        case [Token(name='lchev'), *rest]:
            children: list[BaseAstNode] = []
            next_child = parse(rest)
            current_rest = next_child.rest

            while True:
                match next_child.node:
                    case AstNode(type='rchev'):
                        outcome = Result(FnArgsAstNode(tuple(children)), current_rest)
                        break
                    case n:
                        children.append(n)
                        next_child = parse(current_rest)
                        current_rest = next_child.rest

        # match stmt
        case [Token(name='match'), *rest_after_match]:
            match parse(rest_after_match):
                case Result(match_expr, rest_after_expr):
                    match_expr: BaseAstNode
                    rest_after_expr: list[Token]
                    pairs = []
                    current_rest = rest_after_expr
                    while len(current_rest) > 0 and current_rest[0].name == 'at':
                        if LOG_LVL <= logging.INFO:
                            log.info('looking for next pattern pair, current rest: %s', str(current_rest))
                        match parse(current_rest):
                            case Result(PatternPairAstNode(pair_pattern, pair_expr), rest_after_current):
                                current_rest = rest_after_current
                                pair = (pair_pattern, pair_expr)
                                if LOG_LVL <= logging.DEBUG:
                                    log.debug('collected pattern pair', str(pair))
                                pairs.append(pair)
                        assert len(pairs) > 0, 'no pattern pairs in match stmt'
                        log.debug('collected %d pattern pairs', len(pairs))
                        outcome = Result(MatchAstNode(match_expr, tuple(pairs)), current_rest)

                case default2:
                    raise Exception('expected expr after match, found: {0}'.format(str(default2)))

        # if stmt
        case [Token(name='if'), *rest]:
            match parse(rest):
                case Result(condition, rest2):
                    match parse(rest2):
                        case Result(if_true, rest3):
                            match parse(rest3):
                                case Result(if_false, rest4):
                                    outcome = Result(IfAstNode(condition, if_true, if_false), rest4)

                                case default4:
                                    raise Exception('expected if_false after if_true, found: {0}'.format(str(default4)))

                        case default3:
                            raise Exception('expected if_true after if, found: {0}'.format(str(default3)))

                case default2:
                    raise Exception('expected condition after if, found: {0}'.format(str(default2)))

        # function definition
        case [Token(name='fn'), *rest]:

            params: list[str] = []

            next_child = parse(rest)

            while True:
                match next_child.node:
                    case AstNode(type='rdarrow'):
                        break
                    case VarLookupAstNode(param_name):
                        params.append(param_name)
                        next_child = parse(next_child.rest)
                    case default:
                        raise Exception('expected var node in param list but found: ' + str(default))

            fn_body_result = parse(next_child.rest)

            fn_node = FnDefAstNode(CustomFn(tuple(params),
                                            fn_body_result.node.simplify(),
                                            Environment()))

            outcome = Result(fn_node, fn_body_result.rest)

        # assignment
        case [Token('var', var_name), *rest] if rest and rest[0].name == "eq":

            # skip over =
            eq_result = parse(rest)

            # get value
            value_result = parse(eq_result.rest)

            outcome = Result(AssignAstNode(var_name, value_result.node), value_result.rest)

        case [Token('var', var_name), *rest]:
            outcome = Result(VarLookupAstNode(var_name), rest)

        case default:
            raise Exception('parsing: no idea what to do with: {0}'.format(str(default)))

    if LOG_LVL <= logging.DEBUG:
        log.debug('outcome pre lookahead: %s', pformat(outcome))

    # lookahead
    match outcome:

        # send to actor
        case Result(node=msg_node, rest=[Token(name='rshift'), *rest_after_rshift]):
            rest_after_rshift: list[Token]
            msg_node: BaseAstNode
            match parse(rest_after_rshift):
                case Result(node=actor_node, rest=rest_after_actor):
                    actor_node: BaseAstNode
                    rest_after_actor: list[Token]
                    return Result(node=SendMsgAstNode(msg=msg_node, actor=actor_node), rest=rest_after_actor)
                case _:
                    return outcome

        # function call
        case Result(node=fn_node, rest=[Token(name='lchev'), *rest_after_lchev] as rest_after_fn):
            match parse(rest_after_fn):
                case Result(node=FnArgsAstNode(items=arg_list), rest=rest_after_fn_args):
                    return Result(node=FnCallAstNode(fn_node, arg_list), rest=rest_after_fn_args)
                case _:
                    return outcome

        case _:
            return outcome


def get_tokens(expr: str) -> Iterable[Token]:
    matches = REGEX.finditer(expr)
    for m in matches:
        for k, v in m.groupdict().items():
            if v is not None:
                yield Token(k, v)
                break


def _interpret(ctxt: ConcurrencyContext, expr: str) -> Iterable[Any]:
    result = None
    tokens = None
    simplified = None
    global_vars = Environment()
    local_vars = Environment()
    count = 0

    try:
        tokens = list(get_tokens(expr))
        while len(tokens) > 0:
            match parse(tokens):
                case Result(node, rest):
                    tokens = rest
                    simplified: BaseAstNode = node.simplify()
                    expr = simplified.evaluate(ctxt, local_vars, global_vars)
                    if LOG_LVL <= logging.DEBUG:
                        count += 1
                        log.debug('evaluated %d statements', count)
                        log.debug('%d statement result = %s', count, fmt(expr))

        print('result = {}'.format(fmt(expr)))

        ctxt.wait()

        return expr

    except Exception as e:
        log.error('actors')
        log.error(pformat(ctxt))
        log.error(str(type(e)) + ' ' + str(e))
        log.error('tokens')
        log.error(pformat(tokens))
        log.error('result')
        log.error(pformat(result))
        log.error('simplified')
        log.error(pformat(simplified))
        log.error('expr')
        log.error(expr)
        log.error(''.join(format_tb(e.__traceback__)))
        raise e


def interpret(expr: str) -> Any:
    with open(join(dirname(abspath(__name__)), 'stdlib')) as f:
        expr = '\n\n\n'.join((f.read(), expr))
    ctxt = ConcurrencyContext()
    log.debug('about to spawn actor to interpret expr')
    a = Actor(ctxt, _interpret, args=[ctxt, expr])
    log.debug('main actor: %s', str(a))
    return a.result


if __name__ == '__main__':
    t_0 = time()
    expr = '''
    x = 22
    y = 33

    match [[33 33] 33]
        @[x x] 'x matched'
        @[[y y] y] 'y matched'
        @otherwise 'no!'

    '''

    log.debug('about to interpret expr %s', expr)
    interpret(expr)
    t_e = time()
    log.info('took: %f seconds', t_e - t_0)
