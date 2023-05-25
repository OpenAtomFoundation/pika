import os

from .redis_ import Redis
import jury


class Cluster:
    def __init__(self, num=3):
        self.nodes = []
        self.num = num
        for i in range(num):
            self.nodes.append(Redis(args=["--cluster-enabled", "yes"]))
        host_port_list = [f"{node.host}:{node.port}" for node in self.nodes]
        jury.log_yellow(f"Redis cluster created, {self.num} nodes. {host_port_list}")
        os.system("redis-cli --cluster-yes --cluster create " + " ".join(host_port_list))

    def push_table(self):
        pass
