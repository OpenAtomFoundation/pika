import time

import jury
import redis


class Redis:
    def __init__(self, args=None):
        self.args = args
        if args is None:
            self.args = []

        self.redis = None
        self.host = "127.0.0.1"
        self.port = jury.get_free_port()
        self.dir = f"{jury.get_case_dir()}/redis_{self.port}"
        self.server = jury.Launcher(args=["redis-server", "--port", str(self.port)] + self.args, work_dir=self.dir)
        self.__wait_start()
        jury.log_yellow(f"Redis started(pid={self.server.get_pid()}). redis-cli -p {self.port}")

    def get_address(self):
        return f"127.0.0.1:{self.port}"

    def __wait_start(self, timeout=5):
        timer = jury.Timer()
        while True:
            try:
                self.__create_client()
                self.client.ping()
                break
            except redis.exceptions.ConnectionError:
                time.sleep(0.01)
            except redis.exceptions.ResponseError as e:
                if str(e) == "LOADING Redis is loading the dataset in memory":
                    time.sleep(0.01)
                else:
                    raise e
            if timer.elapsed_time() > timeout:
                raise Exception(f"tair start timeout, {self.dir}")

    def __create_client(self):
        self.client = redis.Redis(port=self.port, single_connection_client=True)
