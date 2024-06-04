from asyncio import get_event_loop, sleep
from uuid import uuid4

from .src import ContainerManager, ContainerDefinition, EnvLoader, SysSettings, Secrets, SysConfig, Logger, JobManager, \
        Bus, ContainerNode, Job, BusPoint, BusCommand, HttpServer, UserApi, UserJob, TaskUiApi, TaskJob

class HttpServerUI(HttpServer):...
class HttpServerSDP(HttpServer):...

ContainerManager() \
        .register(ContainerDefinition(name='Container1')
            + (EnvLoader, SysSettings) # Загрузчик переменных среды
            + (Secrets, EnvLoader) # Интеграция с SecMan
            + (SysConfig, None, SysSettings, Secrets, 'connection_points_1') # Загрузчик настроек сервиса
            + (Logger, SysSettings, SysConfig, 'CNT_1') # Подсистема логирования
            + (JobManager, SysSettings, SysConfig, Logger) # Подсистема задач async
            + (HttpServerUI, JobManager, 'http_front','http_front') # Веб сервер фронта
            + (HttpServerSDP, JobManager, 'http_sdp','http_sdp') # Веб сервер фронта
            + (Bus, JobManager, 'connection_points_1') # Подсистема обмена сообщениями
            + (UserJob, Bus, Secrets, '/db')
            + (UserApi, HttpServerUI, Bus)
            + (TaskJob, Bus, '/db')
            + (TaskUiApi, HttpServerUI, Bus)
)
loop = get_event_loop()
loop.create_task(ContainerManager().run())
loop.run_forever()