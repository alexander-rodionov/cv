from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List, Optional, Dict, Type
from .container_abc import ContainerDefinitionABC, ContainerManagerABC, ContainerNodeABC
from . import ContainerManager

__all__ = ['ContainerDefinition']

from ..util import raise_f


@dataclass
class ContainerItem:
value: Any = None
params: Optional[List[Any]] = None

class ContainerDefinition(ContainerDefinitionABC):

    def __init__(self, name):
        self.container_name = name
        self.structure : Dict[Type[ContainerNodeABC], ContainerItem]= {}
        self.instances : Dict[Type[ContainerNodeABC], ContainerNodeABC]= {}

    def __add__(self, other):
        # первым элементом кортежа идет ContainerNode
        assert issubclass(other[0], ContainerNodeABC), f'{other[0]} is not ContainerNodeABC'
        # проверить типы параметров фактические и формальные
        other[0].check_params(other[1:])
        # проверить, что все фактические парметры уже есть в структуре или добавить, если они не требуеют параметров
        for p in other[1:]:
            if p is None:
                continue
            if isinstance(p, type) and issubclass(p, ContainerNodeABC) and p not in self.structure:
                if not p.emit_params():
                    self.structure = {p:ContainerItem(p,[]), **self.structure}
                else:
                    raise_f(ValueError(f'{str(p)} is not in structure and requires parameters'))
        self.structure={**self.structure, **{other[0]:ContainerItem(value=other[0],params=other[1:])}}
        return self

    def instantiate(self, manager: ContainerManager):
        def prepare_params(params):
            return [self.instances[p] if isinstance(p, type) and issubclass(p, ContainerNodeABC) else p for p in params]

    for item in self.structure.values():
        if isinstance(item.value, type):
            if not issubclass(item.value, ContainerNodeABC):
                raise_f(ValueError(f'{str(item.value)} is not a ContainerNodeABC'))
            params = prepare_params(item.params)
            self.instances[item.value] = item.value(*params)

    async def launch(self, manager: ContainerManagerABC):
        print(f'launching container {self.container_name}')
        for i in self.instances.values():
            try:
                await i.launch()
            except Exception as e:
                print(f'Failed to launch {i.__class__.__name__} with exception {e}')

    def abort(self):
        [i.abort() for i in self.instances.values()]
