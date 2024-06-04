from __future__ import annotations

import importlib
import inspect
from asyncio import get_event_loop
from typing import List, Optional, Awaitable, Callable, Union
from threading import Thread
from .container_abc import ContainerNodeABC
from ..util import raise_f
__all__ = ['ContainerNode','ContainerNodePassive', 'ContainerNodeThread', 'ContainerNodeAsync', 'set_src_module']

_src_module : Optional[object]= None
def set_src_module(m):
    global _src_module
    _src_module = m


class ContainerNode(ContainerNodeABC):
    src_module = None
    @classmethod
    def emit_params(cls):
        raise NotImplementedError(f'{cls.__name__} method emit_params not implemented')

    @classmethod
    def check_params(cls, params):
        def extract_type(param):
            if param is None:
                return None, None
            elif hasattr(param, '__name__'): # класс
                return param.__name__, param
            elif hasattr(param, '__class__'): # объект
                return param.__class__.__name__, param.__class__
            else:
                raise_f(ValueError('Unknown parameter type'))

    def compare_type(exp, fact):
        def compare_classes(exp_str, fact_class):
            standard_classes = ['str']
            if exp_str in standard_classes:
                if exp_str == 'str':
                    return issubclass(fact_class, str)
            else:
                exp = getattr(_src_module, exp_str)
                if not exp:
                    raise Exception('All expected classes should be available in src module, this one is not: '+ exp_str)
                return issubclass(fact_class, exp)

        return (False, False) if exp is None \
            else (True, False) if exp.startswith('Optional') and fact is None \
            else (True, False) if exp.startswith('Optional') and compare_classes(exp[9:-1], fact) \
            else (True, True) if exp == '*' \
            else (True, False) if compare_classes(exp,fact) \
            else (False, False)

    def get_item_or_none(lst, index):
        return None if index >= len(lst) else lst[index]

    def join_with_none(lst:List[Optional[str]], chr:str):
        return chr.join(['None' if x is None else x for x in lst])

    def compare_types(exp, fact):
        match_all = False
        for i,f in enumerate(fact):
            match_this, match_all_this = compare_type(get_item_or_none(exp, i), f[1])
            if not (match_all or match_this):
                raise ValueError(f'Invalid parameters: {join_with_none([f[0] for f in fact_params],",")}, expected: {join_with_none(expected_params,",")}')
            else:
                match_all = match_all or match_all_this

        fact_params = list(map(lambda x: extract_type(x), params)) #tuple name, class
        expected_params = list(cls.emit_params())
        compare_types(expected_params, fact_params)

    def __init__(self, *args):
        ContainerNodeABC.__init__(self)
        self.check_params(args)
        self._abort_flag = False
        pass

    @property
    def abort_flag(self):
        if not '_abort_flag' in self.__dict__:
            print('No _abort_flag class: ',self.__class__.__name__)
        return self._abort_flag

    def abort(self):
        self._abort_flag = True

    async def launch(self):
        pass

class ContainerNodePassive(ContainerNode):
    def __init__(self, *args):
        ContainerNode.__init__(self, *args)


class ContainerNodeThread(ContainerNode):
    def __init__(self, *args):
        ContainerNode.__init__(self, *args)
        self._thread = Thread(target=self._run)
        self._thread.start()

    def _run(self):
        while not self.abort_flag:
            self.process()
    def process(self):
        ...

class ContainerNodeAsync(ContainerNode):
    def __init__(self, *args):
        ContainerNode.__init__(self, *args)
        self._loop = get_event_loop()
        self.tasks = [self._loop.create_task(self._run())]

    @property
    def loop(self):
        return self._loop

    def start_task(self, routine:Union[Callable, Awaitable]):
        task = self._loop.create_task(routine() if callable(routine) else routine)
        self.tasks.append(task)
        return task

    def abort(self):
        super().abort()
        [i.cancel() for i in self.tasks if not i.done()]

    async def _run(self):
        while not self.abort_flag:
            if not await self.process():
                break

    async def launch(self):
        pass