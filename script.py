import sys

from loguru import logger

from src.main import main
from src.module.settings import settings

logger.remove()
logger.add(sys.stderr, colorize=True, level=settings.log_level)


if __name__ == '__main__':
    main()
