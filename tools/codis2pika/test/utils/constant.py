from pathlib import Path

BASE_PATH = f"{Path(__file__).parent.parent.parent.absolute()}"  # project path

PATH_REDIS_SHAKE = f"{BASE_PATH}/bin/redis-shake"
PATH_EMPTY_CONFIG_FILE = f"{BASE_PATH}/test/assets/empty.toml"
