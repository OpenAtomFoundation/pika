import os
from pathlib import Path

import jury
import toml

from .constant import PATH_REDIS_SHAKE, PATH_EMPTY_CONFIG_FILE


def get_empty_config():
    with open(PATH_EMPTY_CONFIG_FILE, "r") as f:
        return toml.load(f)


class RedisShake:
    def __init__(self):
        self.server = None
        self.redis = None
        self.dir = f"{jury.get_case_dir()}/redis_shake"
        if not os.path.exists(self.dir):
            Path(self.dir).mkdir(parents=True, exist_ok=True)

    def run(self, toml_config):
        with open(f"{self.dir}/redis-shake.toml", "w") as f:
            toml.dump(toml_config, f)
        self.server = jury.Launcher(args=[PATH_REDIS_SHAKE, "redis-shake.toml"], work_dir=self.dir)
