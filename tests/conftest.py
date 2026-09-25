"""测试共用：打开 INFO 日志，让位点变化与记录处理流水打印到测试输出。"""

import logging
import sys


def pytest_configure(config):
    logging.basicConfig(
        level=logging.INFO,
        stream=sys.stdout,
        format="%(levelname)s %(name)s: %(message)s",
        force=True,
    )
