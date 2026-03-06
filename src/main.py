import pandas as pd
from pathlib import Path
import logging
from logging.handlers import RotatingFileHandler
from dataclasses import dataclass

from weather_etl import WeatherDownloader
from air_pollution_etl import AirPollutionDownloader
from utils import Merger

# Logger setup
LOG_DIR = Path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
log_file = LOG_DIR / "processing.log"

logger = logging.getLogger("weather_downloader")
logger.setLevel(logging.INFO)

# Rotating file handler
fh = RotatingFileHandler(str(log_file), maxBytes=5 * 1024 * 1024, backupCount=3)
fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
fh.setFormatter(fmt)
logger.addHandler(fh)

# Also enable basic console output
ch = logging.StreamHandler()
ch.setFormatter(fmt)
logger.addHandler(ch)








if __name__ == "__main__":
    # AIR POLLUTION TRACK
    air = AirPollutionDownloader()
    air.run()

    weather = WeatherDownloader()
    weather.run()

    merger = Merger()
    merger.join_pollution_weather_data()
    pass
