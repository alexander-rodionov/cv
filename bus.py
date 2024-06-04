from __future__ import annotations
import codecs
import os
import pickle
from asyncio import Event, wait_for, sleep, PriorityQueue, Queue, CancelledError
from collections import defaultdict, namedtuple
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import partial
import aiofiles
from aiohttp.web_exceptions import HTTPException, HTTPNotFound, HTTPInternalServerError, HTTPBadRequest, \
    HTTPRequestTimeout, HTTPUnauthorized
from typing import Any, Dict, List, Optional, AsyncIterator, Tuple, Callable, Type, Union, Set
from aiohttp import web, TCPConnector, ClientSession, ClientTimeout
from aiohttp.web_response import json_response

from .job_manager import JobUsageException, Job, JobManager
from ..util import short_id
from .. import ContainerNodeAsync
from ..abc import BusABC, BusPointABC, SysSettingsABC, SysConfigABC, LoggerABC, BusCommandABC, BusMessageABC, \
    ParametrizedABC
from ..enums import BusMessageType, BusMessageErrorType, BusClientConnectorStatus, BusState
from .mtls import MtlsMixin

__all__ = ['Bus', 'BusPoint', 'BusCommand', 'BusPointABC', 'NoResponseSentinel', 'BusFile','BusHTTPUnauthorized']

class NotDeliveredSentinel:...

class BusHTTPUnauthorized(HTTPUnauthorized):
    """This is required to overcome pickle issue with aiohttp exception handling"""
    def __init__(self, foo):
    HTTPUnauthorized.__init__(self)

@dataclass
class BusSettings:
    """
    Класс для хранения настроек шины
    """
    MTLS_ENABLED = False # Нужно ли использовать MTLS
    SERVER_PORT_SHIFT = 0 # сдвиг порта шины, необходимо для отладки сетевого взаимодействия через эмулятор сбоев сети
    MAX_RETRY_COUNT = 10 # максимальное количество попыток отправки сообщения
    RETRY_DELAY = 1 # sec, время повторной попытки отправки сообщения
    HC_HTTP_TIMEOUT = ClientTimeout(total=5) # время ожидания ответа от хоста при проверке состояния
    CALL_HTTP_TIMEOUT = ClientTimeout(total=30) # время ожидания ответа от хоста при отправке
    SCAN_HTTP_TIMEOUT = ClientTimeout(total=5) # время ожидания ответа от хоста при сканировании
    DOWNLOAD_HTTP_TIMEOUT = ClientTimeout(total=300) # время ожидания ответа от хоста при загрузке файла
    FAST_CYCLE_SCAN_TIMEOUT = 30 # sec, время ожидания ответа от хоста при сканировании в быстром режиме
    SCAN_CYCLE_TIMEOUT = 60 # sec, время ожидания ответа от хоста при сканировании в обычном режиме
    HC_CYCLE_TIMEOUT = 0.5 # sec, время ожидания ответа от хоста при проверке состояния
    DOWNLOAD_CYCLE_TIMEOUT = 60 # sec, время ожидания ответа от хоста при загрузке файла
    RETRY_CYCLE_TIMEOUT = 0.5 # sec, время ожидания при повторной попытке отправки сообщения
    DUPLICATE_CLEARANCE = 1 # sec, время накопления очереди для очистки дубликатов
    TEMPORARY_DIRECTORY = './tmp' # папка для временных файлов
    FILE_CHUNK_SIZE = 2 ** 16 # размер одного файла в байтах
    CALL_URL = '/call' # URL для отправки сообщений
    SCAN_URL = '/scan' # URL для сканирования
    HC_URL = '/hc' # URL для проверки состояния хоста
    DOWNLOAD_URL = '/download' # URL для загрузки файла
    WEB_SHUTDOWN_TIMEOUT = 60.0 # sec, время ожидания ответа от хоста при выключении хоста
    WEB_KEEPALIVE_TIMEOUT = 75.0 # sec, время ожидания ответа от хоста при переподключении
    WEB_BACKLOG = 128 # максимальное количество сообщений в буфере веб-сервера

class DeliveryFailure(Exception):
    """
    Исключение, возникающее при ошибке доставки сообщения
    """
    ...


class NoResponseSentinel:
    """
    Класс заглушка для отсутствия ответа от хоста
    """
    ...


class BusPoint(BusPointABC, ParametrizedABC):
    """
    Класс точки взаимодействия
    """

    @staticmethod
    def AsyncJobUsageExceptionCatcher(f):
        async def wrapper(*args, **kwargs):
            try:
                return await f(*args, **kwargs)
            except Exception as e:
                raise JobUsageException(e)

    @staticmethod
    def JobUsageExceptionCatcher(f):
        def wrapper(*args, **kwargs):
            try:
                return f(*args, **kwargs)
            except Exception as e:
                raise JobUsageException(e)


    @staticmethod
    def command_handler(f) -> Callable:
        """
        Декоратор функции для обработки команды
        """
        f.command_name = f.__name__
        return f

    @classmethod
    def point_name(cls) -> str:
        """
        Возвращает имя точки взаимодействия
        @return: str - имя точки взаимодействия
        """
        return cls.__name__

    def __init__(self, bus: BusABC, settings: SysSettingsABC = None, config: SysConfigABC = None, log: LoggerABC = None,
                            local=False, _id: str = None):
        BusPointABC.__init__(self, bus) # Инициализация базового класса
        ParametrizedABC.__init__(self, settings or bus.settings, config or bus.config,
            log or bus.log) # Инициализация базового класса
        self._local = local # Локальный режим работы
        self._id = _id if _id else self.__class__.__name__ # Идентификатор точки взаимодействия, заданный или созданный
        self._bus.register(self) # Регистрация точки взаимодействия в шине
        self._threads: Dict[str, Event] = {} # перечень тем сообщений, ожидающих ответа
        self._inbox: Dict[str, Optional[BusMessageABC, BusCommandABC]] = {} # очередь ответов
        self._cmd: Dict[str, Callable] = {} # перечень команд, функций, помеченных декориратором команды

        # Ищем декорированные команды в классе наследнике
        for i in dir(self):
            try:
                fun = getattr(self, i)
                if callable(fun) and hasattr(fun, 'command_name'): # ищем команды помеченные декоратом команды
                    self._cmd[i] = fun
            except AttributeError:
                pass

    @property
    def local(self) -> bool:
        return self._local

    @property
    def id(self):
        """
        Идентификатор точки взаимодействия
        @return: str - идентификатор точки взаимодействия
        """
        return self._id

    def __repr__(self) -> str:
        """
        Возвращает строковое представление точки взаимодействия
        @return: str - строковое представление точки взаимодействия
        """
        return f'{self.__class__.__name__}: {"|".join([str(i) for i in self._cmd])}'

    def to_me(self, message: Union[BusMessageABC, BusCommandABC]) -> bool:
        """
        Проверяет, отправлено ли сообщение данной точке взаимодействия
        @param message: BusMessageABC или BusCommandABC - сообщение, полученное точкой взаимодействия
        @return: bool - является ли сообщение отправленным данной точке взаимодействия
        """
        return message.to == self._id

    async def send(self, message) -> None:
        """
        Отправляет сообщение в шину
        @param message: BusMessageABC или BusCommandABC - сообщение для отправки
        """
        await self._bus.submit(message)

    async def send_wait(self, message: Union[BusMessageABC, BusCommandABC], new_from: str = None) -> \
                Optional[Union[BusMessageABC, BusCommandABC]]:
        """Послать сообщение в шину и синхронно ожидать ответа
        @param message: BusMessageABC или BusCommandABC - сообщение для отправки
        @param new_from: str - идентификатор отправителя сообщения, если изменяется
        @return: BusMessageABC или BusCommandABC - полученное сообщение ответ
        """
        assert isinstance(message, BusMessageABC) or isinstance(message, BusCommandABC)
        is_command = isinstance(message, BusCommandABC)
        message.from_ = new_from if new_from else message.from_
        message.point = self
        await self._bus.submit(message)
        ev = Event()
        self._threads[message.thread] = ev
        try:
            await wait_for(ev.wait(), self._bus.settings.BUS_EVENT_TIMEOUT)
        except TimeoutError:
            self._inbox[message.thread] = NotDeliveredSentinel
            return None
        data = self._inbox[message.thread]
        del self._inbox[message.thread]
        del self._threads[message.thread]
        if data is NotDeliveredSentinel:
            return BusCommand.error_from_message(message, BusMessageErrorType.NOT_DELIVERED.name, 'Message not delivered after retries')
        return BusCommand.from_message(data) if is_command else data

    async def send_wait_body(self, to, body):
        message = BusMessage(thread=short_id(), to=to, fr=self.id, body=body)
        return await self.send_wait(message)

    async def bus_wait_command(self, cmd: BusCommandABC):
        return BusCommand.from_message(await self.send_wait(cmd, new_from=self._id))

    async def bus_wait_command_http(self, cmd: BusCommandABC, fatal_exception: Union[Type[Exception],
        Type[HTTPException]] = HTTPInternalServerError,
        request_exception: Union[Type[Exception], Type[HTTPException]] = HTTPBadRequest,
        timeout_exception: Union[Type[Exception], Type[HTTPException]] = HTTPRequestTimeout):

        def generate_exception(ex_type, ex_message):
            return ex_type(text=ex_message) if issubclass(ex_type, HTTPException) else ex_type(ex_message)

        res = await self.bus_wait_command(cmd)
        if not res: # no response
            raise generate_exception(fatal_exception, f'No response from {cmd.to}')
        if res.error:
            if res.error_type == BusMessageErrorType.NOT_DELIVERED.name:
                raise generate_exception(timeout_exception, res.error)
            if res.error_type == BusMessageErrorType.USAGE.name:
                raise generate_exception(request_exception, res.error)
            elif res.error_type == BusMessageErrorType.INTERNAL.name:
                raise generate_exception(fatal_exception, res.error)
            elif res.error_type == BusMessageErrorType.HTTP.name:
                exception = pickle.loads(codecs.decode(res.error.encode(), "base64"))
                raise exception
            else:
                raise Exception('Unknown command error type')
        return res.result

    async def on_message(self, message: BusMessageABC) -> bool:
        if message.thread in self._threads:
            # ожидание ответа на запрос по #thread, поиск и установка события, которое ожидает отправитель
            self._inbox[message.thread] = message
            self._threads[message.thread].set()
            return False
        if message.type == BusMessageType.COMMAND and message.to == self._id:
            # если сообщение это команда мне, то обработаем его как команду
            # если запрос не в треде запросов, и это команда, то пытаемся вызвать обработчик команд
            command = BusCommand.from_message(message)
            try:
                await self.on_command(command)
            except HTTPException as e:
                # Команды могут возвращать HTTP исключения, которые надо транслировать через сообщения вызывающему команду
                await self._bus.submit(BusCommand.error_from_message(command,
                        BusMessageErrorType.HTTP.name,
                        codecs.encode(pickle.dumps(e), "base64").decode()))
            except JobUsageException as e:
                # Или ошибки, вызванные осмысленными причинами (валидация и т.п.)
                await self._bus.submit(BusCommand.error_from_message(command,
                BusMessageErrorType.USAGE.name, str(e)))
            except Exception as e:
            # Либо незапланированые исключения
                await self._bus.submit(
                    BusCommand.error_from_message(command, BusMessageErrorType.INTERNAL.name, str(e)))
            return False
        else:
            # если запрос не в треде и не команда, то требуется переопределить метод унаследованным обработчиком
            # сообщений, а в базовом классе ничего не делать и вернуть признак необходимости обработки
            return True

    async def on_command(self, command):
        if command.command in self._cmd:
            # по коду команды найдем обработчик
            handler = self._cmd[command.command]
            # и вызовем его
            command_result = await handler(**command.params) if command.params else await handler()
            if command_result is not None:
                assert isinstance(command_result, dict) or isinstance(command_result, str) or isinstance(command_result, list) \
                        or isinstance(command_result, NoResponseSentinel) or command_result == NoResponseSentinel
                command_result = None if command_result == NoResponseSentinel else command_result
                # отправим ответ запрашивающему, если команда вернула какой-то результат
                await self._bus.submit(BusCommand.reply_from_message(command, command_result))
        else:
            # нет такой команды среди зарегистрированных
            self.log.err(f'BUS: Command {command.command} not in handlers of {self}')

    def cancel_send_wait(self, message: BusMessageABC):
        if message.thread in self._threads:
            self._inbox[message.thread] = NotDeliveredSentinel
            self._threads[message.thread].set()
class TimeLimitedSet:
    """Класс для хранения временных интервалов, в которых могут содержаться элементы."""
    TimedTuple = namedtuple('Timed', ['timestamp', 'value'])

    def __init__(self, storage_limit_sec: int):
        self.storage_limit_sec = storage_limit_sec
        self.s: Set = set()
        self.timed: List[TimeLimitedSet.TimedTuple] = []

    def clear_up(self):
        """Находит элементы, время которых превышает storage_limit_sec, и удаляет их из списка."""
        split_timestamp = datetime.now()
        self.s = self.s - set(map(lambda y: y.value, filter(lambda x: x.timestamp < split_timestamp, self.timed)))
        self.timed = list(filter(lambda x: x.timestamp >= split_timestamp, self.timed))

    def add(self, value):
        """Добавляет элемент в список временного хранения, очищает истекшие элементы, если они есть."""
        self.clear_up()
        self.s.add(value)
        self.timed.append(TimeLimitedSet.TimedTuple(datetime.now() + timedelta(seconds=self.storage_limit_sec), value))

    def __contains__(self, item):
        """Проверяет наличие элементов в списке"""
        self.clear_up()
        return self.s.__contains__(item)

    def __repr__(self):
        return self.s.__repr__()


class Bus(ContainerNodeAsync, BusABC, Job):
    def __init__(self, jobman: JobManager, config_extension: str):
        """Получает на вход ссылку на объект JobManager, на котором работает этот сервис, и название файла
        конфигурации, содержащей настройки веб серверов различных сервисов приложения (сетевая карта приложения)."""
        ContainerNodeAsync.__init__(self)
        BusABC.__init__(self, jobman.settings, jobman.config, jobman.log)
        self._q: Queue = Queue() # очередь сообщений на обработку
        self.create_workers()
        Job.__init__(self, jobman)
        self._clients: Dict[str, BusPointABC] = {}
        self._state = BusState.BOOTING
        self._dlq: Queue = Queue() # очередь для загрузки файлов
        self._retry_q: PriorityQueue = PriorityQueue() # очередь для переотправки сообщений
        assert config_extension in self.config.ext
        conf = self.config.ext[config_extension]
        self.duplicate_set = TimeLimitedSet(BusSettings.DUPLICATE_CLEARANCE)

        # настройка системы маршрутизации
        self.routes = []
        self.routes_dict = {}
        # Работаем в разделе bus_services, содержащем настройки веб серверов шины данных
        # Если настроена маршрутизация, то есть отправка сообщений адресату через третий сервис передатчик, то загружаем
        # Комбинации [отправитель, посредник, получаетль]
        if 'routes' in conf['bus_services']:
            self.routes_dict = defaultdict(lambda: {})
            self.routes = conf['bus_services']['routes']
            # И преобразуем его в словарь отправитель/получаетель = посдерник, чтобы понимать какому посреднику посылать
            # сообщение, направленное определенному получателю
            for i in self.routes:
                self.routes_dict[i[0]][i[2]] = i[1]
                self.routes_dict = dict(self.routes_dict)

        # Создание сервера
        cert_path = conf['cert_path']
        self.current_service_name = conf['current_bus_service']
        current_service = conf['bus_services'][self.current_service_name]
        self.server_host = current_service['host']
        self.server_port = current_service['port']
        server_cert = cert_path + current_service['cert']
        server_ca_cert = cert_path + current_service['ca_cert']
        server_key = cert_path + current_service['key']

        self.server = BusServerConnector(
            bus=self, host=self.server_host, port=self.server_port, ca_cert=server_ca_cert, cert=server_cert,
            key=server_key)

        # получение списка сервисов обмена системы, для дальнейшей инициализации реестра сервисов
        client_list = {i: conf['bus_services'][i] for i in conf['bus_services'] if
                i != self.current_service_name and i != 'routes'}

        # Создание реестра сервисаов адресатов
        self.gr = BusGlobalRegistry(bus=self, client_list=client_list, cert_path=cert_path)

    @classmethod
    def emit_params(cls):
        return ['JobManager', 'str']

    def register(self, point: BusPointABC) -> None:
    # регистрация клиенских точек в сервисе
        if point.id in self._clients:
            raise Exception(
                f'Double register client in bus {type(point).__name__} point_id {point.id} dict = {self._clients}')
        self._clients[point.id] = point

    @Job.taskmethod
    async def run_server(self):
        # Запуск http сервера шины данного сервиса (BusServerConnector)
        # Выполнен как отдельный асинхронный процесс
        await self.server.start()
        return None

    @Job.taskmethod
    async def retry_queue_processor(self):
        # бесконечный цикл из job manager while not return
        if self._retry_q.empty():
            # если в очереди ничего нет, то повторить после таймаута
            await sleep(BusSettings.RETRY_CYCLE_TIMEOUT)
            return True
        time_to_send, msg = await self._retry_q.get()
        if time_to_send > datetime.now():
            # Если сообщение еще не пора отправить, то кладем его обратно и ждем
            await self._retry_q.put((time_to_send, msg))
            await sleep(BusSettings.RETRY_CYCLE_TIMEOUT)
        else:
            # проверяем валидно ли сообщение (не превышено число повторных попыток)
            if msg.is_valid:
                # помещаем в очередь на отправку
                await self._q.put(msg)
            else:
                if msg.point is not None:
                    msg.point.cancel_send_wait(msg)
                # если по сообщению есть ожидающие ответа, то отправляем ошибку
        return True # продолжаем работу цикла

    @Job.taskmethod
    async def scan_all(self):
        # Асинхронный цикл операции знакомства с соседями. Получает ассортименты обрабатываемых команд другими сервисами, насыщает реестр сервисов
        try:
            # Просканировать все соседние сервисы
            await self.gr.scan_all()
        except CancelledError as e:
            return False
        except Exception as e:
            self.log.err(f'Scan cycle error {str(e)}')
        await sleep(BusSettings.SCAN_CYCLE_TIMEOUT) # разгрузочный таймаут
        return True

    @Job.taskmethod
    async def hc_all(self):
        # Асинхронный цикл операций healthcheck для проверки связанности сервисов
        try:
            # Включается когда сервис уже загружен
            if self.state != BusState.BOOTING:
                # Проверить все связанные сервисы
                await self.gr.healthcheck_all()
        except CancelledError as e:
            return False
        except Exception as e:
            self.log.err(f'Healthckeck cycle error {str(e)}')
        await sleep(BusSettings.HC_CYCLE_TIMEOUT) # разгрузочный таймаут
        return True

    @Job.taskmethod
    async def download_queue(self):
        m: BusMessage
        m = await self._dlq.get() # ожидает поступления заказа на загрузку в очередь загрузки
        await self.download_file(m) # загружкает файл
        m.processed = True # отмечает сообщение как обработанное
        await self._q.put(m) # отправляет помеченное сообщение в очередь отправки для обработки файла в bus point
        await sleep(BusSettings.DOWNLOAD_CYCLE_TIMEOUT) # пауза для разгрузки сервиса
        return True # приводит к работе в цикле job manager

    async def submit(self, message: Union[BusMessageABC, BusCommandABC]) -> None:
    # Проверить, не дубликат ли это уже отправленного недавно сообщения, добавить его в список дубликатов, и положить в очередь отправки
        if message.id not in self.duplicate_set:
            self.duplicate_set.add(message.id)
            await self._q.put(message)

    def change_state(self, current_state_ok: Any) -> None:
        # Если сервис сейчас загружается
        if self._state == BusState.BOOTING:
            # если входное состояние OK (связь есть), то перевести в READY, или оставить в состоянии BOOTING
            self._state = BusState.READY if current_state_ok else BusState.BOOTING
        # Если сервис уже работает
        elif self._state == BusState.READY or self._state == BusState.UNSTABLE:
            # если сломалась связанность, то перевести в UNSTABLE, или оставить как есть
            self._state = BusState.READY if current_state_ok else BusState.UNSTABLE

    def create_workers(self):
        """Создание N workers читающих очередь сообщений и выполняющих обработку"""
        _WORKER_COUNT = 1 # self.settings.BUS_WORKER_COUNT
        self.log.info(f'BUS: Creating {_WORKER_COUNT} workers')

        async def worker_wrapper():
            await self._dispatch(await self._q.get())
            return True

        [self.inject_task_method(worker_wrapper, f'worker_{i}') for i in range(_WORKER_COUNT)]

    async def _dispatch(self, message: BusMessageABC):
        self.log.debug(f'BUS: Dispatch {message}')
        if not message.to: # не задан получаетель, это локальный бродкаст, послать всем клиентам этого сервиса
            [await self._clients[c].on_message(message) for c in self._clients]
        elif message.to in self.clients: # найден локальный клиент
            if message.type == BusMessageType.FILE and not message.processed: # Если требуется загрузка, то сообщение идет в очередь загрузки
                await self._dlq.put(message)
            else: # если это не файл, или он уже скачан, то отправить в локальный bus point на обработку сообщения приемником
                await self._clients[message.to].on_message(message)
        elif message.to in self.gr.clients: # найден глобальный клиент
            try:
                await self._call_remote_client(message)
            except DeliveryFailure:
                # Если вызвать сервис не удалось, то отправить сообщение в очередь повторной отправки
                self.log.warn(f'BUS: GOING FOR RETRY DeliveryFailure {message}')
                await self._retry_q.put(message.retry())
        else: # клиент не найден, либо его не существует в природе, либо сбой связи, поэтому идем в ретраи
            if self.state != BusState.READY: # если внешний сервис отвалился или не успели загрузиться сведения о получателях,то ретрай
                self.log.warn(f'BUS: GOING FOR RETRY DeliveryFailure {message}')
                await self._retry_q.put(
                message.retry()) # если сервис не загружен или не стабилен, то переотправка сообщения до достижения максимального количества ретраев
            else:
                self.log.err(f'BUS: Target BusPoint not found {message} ')

    async def _call_remote_client(self, message):
        # Если отправитель не задат или в числе локальных обработчиков, то отправить от имени текущего сервиса
        # в противном случае найти имя в реестре сервисов (для роутинга)
        if not message.fr or message.fr in self._clients:
            from_service = self.current_service_name
        else:
            from_service = self.gr.clients[message.fr].service_name
        # Если получатель сообщения в списке локальных обработчиков, то получаетем будет текущий сервис
        if message.to in self._clients:
            to_service = self.current_service_name
        # в противном случае найти имя в реестре сервисов
        else:
            to_service = self.gr.clients[message.to].service_name
        try:
            # Если путь к получателю идет через третией сервис и маршрутизацию, то найти сервис маршрутизации
            router = self.routes_dict[from_service][to_service]
        except KeyError:
            # И считать сервисом маршрутизации целевой сервис, если маршрутизации нет
            router = to_service
        if router == self.current_service_name:
            await self.gr.services[to_service].call(message.to_json())
        else:
            await self.gr.services[router].call(message.to_json())

    async def download_file(self, m: BusMessage):
        if m.fr in self._clients: # если отправка самому себе, то ничего не надо делать, файл уже находится на диске
            return
        elif m.fr in self.gr.clients: # если отправка сервису, который есть в реестре
            service_name = self.gr.clients[m.fr].service_name
            # вызвать метод download через клиента этого сервиса и загрузить файл во временную папку
            await self.gr.services[service_name].download(BusSettings.TEMPORARY_DIRECTORY, m.body['file_name'])
        else: # если сервис не найден, то ошибка
            raise Exception(f'Download source not found {m.fr}')

    @property
    def state(self) -> BusState:
        return self._state

    @property
    def clients(self) -> Dict[str, BusPointABC]:
        return self._clients


class BusMessage(BusMessageABC):
    def __init__(self, type_: BusMessageType = BusMessageType.UNKNOWN, thread: Optional[str] = None,
            to: Optional[str] = None, fr: Optional[str] = None,
            body: Optional[Dict[str, Any]] = None, id_: Optional[str] = None,
            point: Optional[BusPointABC] = None
            ):
        self._id = id_ if id_ else short_id()
        self._type = type_
        self._thread = thread
        self._to = to
        self._fr = fr
        self._body = body
        self._retry_count = 0
        self._postpone_till = None
        self._processed = False
        self._point = point

    @property
    def point(self) -> BusPointABC:
        return self._point

    @point.setter
    def point(self, value):
        self._point = value


    @staticmethod
    def from_json(j):
        return BusMessage(type_=BusMessageType[j['type']], thread=j['thread'], to=j['to'], fr=j['fr'],
        body=j['body'], id_=j['id'])

    @property
    def is_valid(self) -> bool:
        return self._retry_count < BusSettings.MAX_RETRY_COUNT

    @property
    def postpone_till(self):
        return self._postpone_till

    def retry(self) -> Tuple[datetime, BusMessageABC]:
        self._retry_count += 1
        self._postpone_till = datetime.now() + timedelta(seconds=BusSettings.RETRY_DELAY)
        return self._postpone_till, self

    def __str__(self) -> str:
        return f'id {self._id} |type {self._type} |thread {self._thread} |to {self._to} |fr {self._fr} | retry {self._retry_count} | valid {self._postpone_till}'

    def to_json(self) -> Dict:
        return dict(id=self._id, type=self._type.name, thread=self._thread, to=self._to, fr=self._fr, body=self._body)

    @property
    def id(self):
        return self._id

    @property
    def thread(self) -> str:
        return self._thread

    @property
    def to(self):
        return self._to

    @to.setter
    def to(self, value):
        self._to = value

    @property
    def fr(self):
        return self._fr

    @fr.setter
    def fr(self, value):
        self._fr = value

    @property
    def body(self):
        return self._body

    @body.setter
    def body(self, value):
        self._body = value

    @property
    def type(self):
        return self._type

    @property
    def processed(self) -> bool:
        return self._processed

    @processed.setter
    def processed(self, value):
        pass


class BusCommand(BusMessage, BusCommandABC):
    def __init__(self, _type: BusMessageType, thread, to, fr, body, point=None):
        BusMessage.__init__(self, _type, thread, to, fr, body, point)
        assert isinstance(body, dict)
        assert 'command' in body and (body['command'] is None or isinstance(body['command'], str))
        assert 'params' in body and (body['params'] is None or isinstance(body['params'], dict))
        assert 'response' in body and (body['response'] is None or isinstance(body['response'], dict))
        assert 'result' in body and (
        body['result'] is None or isinstance(body['result'], dict) or isinstance(body['result'], str)
        or isinstance(body['result'], list)
        )
        assert 'error_type' in body and (body['error_type'] is None or isinstance(body['error_type'], str))
        assert 'error' in body and (body['error'] is None or isinstance(body['error'], str))

    @staticmethod
    def compose_body(command: Optional[str] = None, params: Optional[Dict[str, Any]] = None,
            response: Optional[Dict[str, Any]] = None, result: Optional[Dict[str, Any]] = None,
            error_type: Optional[str] = None, error: Optional[str] = None):
        return dict(command=command, params=params, response=response, result=result, error_type=error_type,
        error=error)

    @staticmethod
    def from_scratch(to: Optional[str] = None, fr: Optional[str] = None, command: Optional[str] = None,
            params: Optional[Dict[str, Any]] = None, response: Optional[Dict[str, Any]] = None,
            point:Optional[BusPointABC]=None):
        return BusCommand(_type=BusMessageType.COMMAND, thread=short_id(), to=to, fr=fr,
            body=BusCommand.compose_body(
            command=command, params=params, response=response), point=point)

    @staticmethod
    def from_message(message: BusMessageABC):
        return BusCommand(_type=BusMessageType.COMMAND, thread=message.thread, to=message.to, fr=message.fr,
            body=message.body, point=message.point)

    @staticmethod
    def reply_from_message(message: BusMessageABC, result=None):
        return BusCommand(_type=BusMessageType.COMMAND, thread=message.thread, to=message.fr, fr=message.to,
    # поменены местами to и fr
                body={**message.body, 'result': result}, point=message.point)

    @classmethod
    def error_from_message(cls, message: BusMessageABC, error_type: str = None, error: str = 'Error'):
        return BusCommand(_type=BusMessageType.COMMAND, thread=message.thread, to=message.fr, fr=message.to,
                body={**message.body, 'result': None, 'error_type': error_type, 'error': error}, point=message.point)

    @property
    def command(self):
        return self._body['command']

    @command.setter
    def command(self, value):
        self._body['command'] = value

    @property
    def params(self):
        return self._body['params']

    @params.setter
    def params(self, value):
        self._body['params'] = value

    @property
    def response(self):
        return self._body['response']

    @response.setter
    def response(self, value):
        self._body['response'] = value

    @property
    def result(self):
        return self._body['result']

    @result.setter
    def result(self, value):
        self._body['result'] = value

    @property
    def error_type(self):
        return self._body['error_type']

    @error_type.setter
    def error_type(self, value):
        self._body['error_type'] = value

    @property
    def error(self):
        return self._body['error']

    @error.setter
    def error(self, value):
        self._body['error'] = value

    def __getitem__(self, key):
    try:
        return self.body['params'][key]
    except KeyError:
        return None


class BusClientConnector(MtlsMixin):
    def __init__(self, settings, config, log,
            global_registry: BusGlobalRegistry, service_name: str, host: str, port: str, ca_cert: str, cert: str,
            key: str):
        MtlsMixin.__init__(self, settings, config, log, enable_mtls=BusSettings.MTLS_ENABLED)
        self.global_registry, self.service_name, self.host, self.port = global_registry, service_name, host, port
        self.url = self.generate_url(host, port)
        self.status: BusClientConnectorStatus = BusClientConnectorStatus.BOOTING
        self.check_timestamp = None
        self._scan_success = False
        self._conn = TCPConnector(ssl=self.generate_ssl_context(ca_cert=ca_cert, cert=cert,
        key=key)) if BusSettings.MTLS_ENABLED else TCPConnector()
        self._session = ClientSession(connector=self._conn, response_class=self.generate_response_class())

    def __repr__(self):
        return f'{self.status.name}'

    def __del__(self):
        self._conn.close()

    def _change_state(self, new_state):
        if self.status == BusClientConnectorStatus.BOOTING and not self._scan_success:
            new_state = BusClientConnectorStatus.BOOTING
            self.status = new_state
            self.check_timestamp = datetime.now()
            self.global_registry.on_state_change()

    @asynccontextmanager
    async def client_session(self) -> AsyncIterator[ClientSession]:
        yield self._session

    async def call(self, payload):
        if self.status != BusClientConnectorStatus.READY:
            raise DeliveryFailure()
        try:
            s: ClientSession
            async with self.client_session() as s:
                async with s.post(self.url + BusSettings.CALL_URL, json=payload, allow_redirects=False,
                        timeout=BusSettings.CALL_HTTP_TIMEOUT) as resp:
                    self.log.debug(f'BUS: CALL {self.url} payload: {payload} result: {resp.status} {await resp.text()}')
                    if resp.status == 200:
                        self._change_state(BusClientConnectorStatus.READY)
                    return await resp.json()
                    else:
                        raise Exception(f'Poor response status {resp.status}')
        except Exception as e:
            self.log.err(f'BUS: CALL failure {e}')
            self._change_state(BusClientConnectorStatus.FAILURE)
            raise DeliveryFailure()

    async def scan(self, on_update_clients: Callable, force: bool) -> Optional[List[str]]:
        if not force and self.status not in (BusClientConnectorStatus.READY, BusClientConnectorStatus.BOOTING):
            self.log.debug(f'BUS: SCAN CALL {self.url} result: CANCELED {self.status}')
            return None
        try:
            s: ClientSession
            async with self.client_session() as s:
                async with s.get(self.url + BusSettings.SCAN_URL, allow_redirects=False,
                            timeout=BusSettings.SCAN_HTTP_TIMEOUT) as resp:
                    self.log.debug(f'BUS: SCAN CALL {self.url} result: {resp.status} {await resp.text()}')
                    if resp.status == 200:
                        self._scan_success = True
                        res = await resp.json()
                        on_update_clients(res)
                        self._change_state(BusClientConnectorStatus.READY)
                        return list()
                    else:
                        raise Exception(f'Poor response status {resp.status}')
        except Exception as e:
            self.log.err(f'BUS: SCAN CALL failure {e}')
            self._change_state(BusClientConnectorStatus.FAILURE)
            return None

    async def healthcheck(self):
        result_success = False
        try:
            s: ClientSession
            async with self.client_session() as s:
                async with s.get(self.url + BusSettings.HC_URL, allow_redirects=False,
                        timeout=BusSettings.HC_HTTP_TIMEOUT) as resp:
                    self.log.debug(f'BUS: HEALTHCHECK {self.url} result: {resp.status} {await resp.text()}')
                    if resp.status == 200:
                        self._change_state(BusClientConnectorStatus.READY)
                        result_success = True
                    else:
                        raise Exception(f'Poor response status {resp.status}')
        except Exception as e:
            self.log.err(f'BUS: HEALTHCHECK failure {e}')
            self._change_state(BusClientConnectorStatus.FAILURE)
        return result_success

    async def download(self, target_dir: str, file_name: str):
        try:
            s: ClientSession
            async with self.client_session() as s:
                async with s.get(self.url + BusSettings.DOWNLOAD_URL, params={'file_name': file_name},
                              allow_redirects=False,
                                timeout=BusSettings.DOWNLOAD_HTTP_TIMEOUT) as resp:
                    self.log.debug(f'BUS: DOWNLOAD {self.url}, status:{resp.status}')
                    if resp.status == 200:
                        self._change_state(BusClientConnectorStatus.READY)
                        f = await aiofiles.open(target_dir + '/' + file_name + '_', mode='wb')
                        async for chunk in resp.content.iter_chunked(BusSettings.FILE_CHUNK_SIZE):
                        await f.write(chunk)
                        await f.write(await resp.read())
                        await f.close()
                    else:
                        raise Exception(f'Poor response status {resp.status}')
        except Exception as e:
            self.log.err(f'BUS: DOWNLOAD failure {e}')
            self._change_state(BusClientConnectorStatus.FAILURE)
            raise DeliveryFailure()


class BusGlobalRegistry(ParametrizedABC):
    def __init__(self, bus: BusABC, client_list, cert_path):
        """
        Инициализация реестра сервисов системы
        @param bus: Шина сообщений данного сервиса
        @param client_list: Список клиентов системы из файла настроек
        @param cert_path: путь к сертификатам https
        """
        ParametrizedABC.__init__(self, bus.settings, bus.config, bus.log)
        self.bus: BusABC = bus
        self.services: Dict[str, BusClientConnector] = {}
        self.clients: Dict[str, BusClientConnector] = {}
        for service_name in client_list:
            self.services[service_name] = BusClientConnector(
                    settings=bus.settings,
                    config=bus.config,
                    log=bus.log,
                    global_registry=self,
                    service_name=service_name,
                    host=client_list[service_name]['host'],
                    port=client_list[service_name]['port'],
                    ca_cert=cert_path + client_list[service_name]['ca_cert'],
                    cert=cert_path + client_list[service_name]['cert'],
                    key=cert_path + client_list[service_name]['key']
                    )

    def __getitem__(self, item):
        return self.services[item]

    def has_all_ready(self):
        return set([self.services[i].status for i in self.services]) == {BusClientConnectorStatus.READY}

    async def scan_all(self):
        def update_clients(service, clients):
        # обновить значения полученные от одного клиента, вынесено в callback из-за конкуренции с обновлением состояний
            self.clients = {**{c: self.clients[c] for c in self.clients if service != self.clients[c]},
            # удалим старых клиентов этого сервиса
            **{r: self.services[service] for r in clients}} # и добавим новых

        async def do_scan(force=False):
            scan_failed = False
            for i in self.services:
                # запуск сканирования соседей по одному, вызывает update_clients и возвращает полученный список сервисов или None
                result: Optional[List[str]] = await self.services[i].scan(partial(update_clients, i),
                            force=force) # ожидается список получателей массивом от сервиса
                scan_failed |= (result is None) # если списка нет, то это сбой сканирования
            return scan_failed

# провести сканирование
        failure_count = 0 # счетчик сбоев для отладки
        await do_scan(force=True) # провести сканирование и записать в свойство класса true/false успешность
        while True:
            if self.has_all_ready():
                break
            await sleep(BusSettings.FAST_CYCLE_SCAN_TIMEOUT) # сделать короткую паузу
            self.log.debug(f'REPEATING SHORT SCAN CYCLE')
            # цикл нужен чтобы при сбоях сканирования проводить сканирование чаще в надежде на восстановление
            await do_scan(force=True) # провести сканирование и записать в свойство класса true/false успешность
            failure_count += 1
            self.bus.log(f'Scan retry failure #{failure_count} ')

    async def healthcheck_all(self, force=False):
        result_success = True
        if not force:
            # выйти, если все клиенты READY, то есть не было зарегистрировано сбоев обмена недавно
            if set([self.services[i].status for i in self.services]) == {BusClientConnectorStatus.READY}:
            return
        for i in self.services:
            if self.services[i].status == BusClientConnectorStatus.FAILURE:
                result_success &= await self.services[i].healthcheck()
        return result_success

    async def call(self, service, payload):
        return await self[service].call(payload)

    def on_state_change(self):
        states = set([self.services[i].status for i in self.services])
        self.bus.change_state(
                not (BusClientConnectorStatus.FAILURE in states)
                and not (BusClientConnectorStatus.BOOTING in states))


class BusServerConnector(ParametrizedABC):
    def __init__(self, bus: BusABC, host: str, port: str, ca_cert: str, cert: str, key: str):
        async def call_handler(request):
        return json_response(await self.on_call(request))

        async def scan_handler(request):
        return json_response(await self.on_scan(request))

        async def hc_handler(_request):
        return json_response(dict(status='healthy'))

        async def download_handler(request):
        return await self.on_download(request)

        ParametrizedABC.__init__(self, bus.settings, bus.config, bus.log)
        self.mtls = MtlsMixin(bus.settings, bus.config, bus.log, enable_mtls=BusSettings.MTLS_ENABLED)
        self.bus: BusABC = bus
        self.host: str = host
        self.port: int = int(port) + BusSettings.SERVER_PORT_SHIFT
        self.ca_cert: str = ca_cert
        self.cert: str = cert
        self.key: str = key

        self.mtls.app.add_routes([
        web.post(BusSettings.CALL_URL, call_handler), # отправка команды
        web.get(BusSettings.SCAN_URL, scan_handler), # cканирование
        web.get(BusSettings.HC_URL, hc_handler), # проверка состояния
        web.get(BusSettings.DOWNLOAD_URL, download_handler) # скачать файл
        ])

    async def on_call(self, request):
        payload = await request.json()
        print('CALL ', payload)
        message = BusMessage.from_json(dict(payload))
        await self.bus.submit(message)

    async def on_scan(self, _request):
        return list([i[0] for i in self.bus.clients.items() if not i[1].local])

    @staticmethod
    async def on_download(request):
        file_name = BusSettings.TEMPORARY_DIRECTORY + '/' + request.rel_url.query['file_name']
        if not os.path.exists(file_name):
            raise HTTPNotFound()
        return web.FileResponse(file_name, chunk_size=BusSettings.FILE_CHUNK_SIZE)

    async def start(self):
        self.bus.log(f'Start bus web server at {self.host}:{self.port}')
        await web._run_app(
            self.mtls.app,
            host=self.host,
            port=self.port,
            shutdown_timeout=BusSettings.WEB_SHUTDOWN_TIMEOUT,
            keepalive_timeout=BusSettings.WEB_KEEPALIVE_TIMEOUT,
            ssl_context=self.mtls.generate_ssl_context(ca_cert=self.ca_cert, cert=self.cert, key=self.key),
            print=self.bus.log.debug,
            backlog=BusSettings.WEB_BACKLOG,
            access_log_format=self.bus.log.web_log_format_template(self.host, self.port),
            access_log=self.bus.log.logger,
            handle_signals=False,
            )


class BusFile(BusMessage):
    @classmethod
    def from_scrath(cls, to, file_name, fr):
        new_command = BusFile(_type=BusMessageType.FILE, thread=short_id(), to=to, fr=fr,
        body={'file_name': file_name})
        return new_command

    def __init__(self, _type: BusMessageType, thread, to, fr, body):
        BusMessage.__init__(self, _type, thread, to, fr, body)