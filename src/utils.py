import pandas as pd
from pathlib import Path
import logging
from logging.handlers import RotatingFileHandler
from dataclasses import dataclass

from weather_etl import WeatherDownloader


@dataclass
class Merger:
    pollution_file: str = "processed_data/air_pollution.parquet"
    weather_file: str = "processed_data/weather.parquet"

    def __post_init__(self):
        # Logger setup using the same log file as main.py
        self.logger = logging.getLogger("merger")
        self.logger.setLevel(logging.INFO)
        
        # Use the same log file as main.py
        log_dir = Path("logs")
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / "processing.log"
        
        # Rotating file handler
        fh = RotatingFileHandler(str(log_file), maxBytes=5 * 1024 * 1024, backupCount=3)
        fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
        fh.setFormatter(fmt)
        self.logger.addHandler(fh)
        
        # Console handler
        ch = logging.StreamHandler()
        ch.setFormatter(fmt)
        self.logger.addHandler(ch)

    def join_pollution_weather_data(self) -> pd.DataFrame:
        """
        Load air pollution and weather data and join them on timestamps.
        
        The weather data timestamp is created from ROK (year), MC (month), 
        DZ (day), and GG (hour) columns. The pollution data is expected to 
        have a 'timestamp' column already. Weather data is filtered to include
        only specified columns from the JSON configuration file.
        
        Parameters:
        -----------
        pollution_file : str
            Path to the combined air pollution CSV file
        weather_file : str
            Path to the weather data CSV file
        weather_columns_file : str
            Path to JSON file containing list of weather columns to keep
        
        Returns:
        --------
        pd.DataFrame
            Joined dataframe with both pollution and weather data
        
        Raises:
        -------
        FileNotFoundError
            If any file does not exist
        ValueError
            If required columns are missing
        """
        try:
            # Load weather columns configuration
            # logger.info(f"Loading weather columns from {weather_columns_file}")
            # with open(weather_columns_file, 'r') as f:
            #     columns_config = json.load(f)
            # weather_columns_to_keep = columns_config.get('weather_columns', [])
            # logger.info(f"  Will keep {len(weather_columns_to_keep)} weather columns")
            
            # Load air pollution data
            self.logger.info(f"Loading air pollution data from {self.pollution_file}")
            pollution_df = pd.read_parquet(self.pollution_file)
            
            # Convert timestamp to datetime if not already
            if 'timestamp' in pollution_df.columns:
                pollution_df['timestamp'] = pd.to_datetime(pollution_df['timestamp'])
            else:
                raise ValueError("Air pollution data must have a 'timestamp' column")
            
            self.logger.info(f"  Loaded {len(pollution_df)} rows of pollution data")
            
            # Load weather data
            self.logger.info(f"Loading weather data from {self.weather_file}")
            weather_df = pd.read_parquet(self.weather_file)
            
            # Check for required columns to create timestamp
            # required_cols = ['ROK', 'MC', 'DZ', 'GG']
            # if not all(col in weather_df.columns for col in required_cols):
            #     raise ValueError(f"Weather data must have columns: {required_cols}")
            
            # # Create timestamp from ROK, MC, DZ, GG columns
            # logger.info("Creating timestamp column from ROK, MC, DZ, GG")
            # weather_df['timestamp'] = pd.to_datetime(
            #     weather_df[['ROK', 'MC', 'DZ', 'GG']].rename(
            #         columns={'ROK': 'year', 'MC': 'month', 'DZ': 'day', 'GG': 'hour'}
            #     )
            # )
            
            # Filter weather_df to keep only columns that exist and are in the config
            # available_weather_cols = [col for col in weather_columns_to_keep if col in weather_df.columns]
            # cols_to_keep = ['timestamp'] + available_weather_cols
            # weather_df = weather_df[cols_to_keep]
            # logger.info(f"  Weather data filtered to {len(available_weather_cols)} columns")
            self.logger.info(f"  Loaded {len(weather_df)} rows of weather data")
            
            # Join on timestamp
            self.logger.info("Joining data on timestamp")
            joined_df = pd.merge(
                pollution_df, 
                weather_df, 
                on='timestamp', 
                how='inner'
            )
            
            self.logger.info(f"  Joined dataframe has {len(joined_df)} rows and {len(joined_df.columns)} columns")
            self.logger.info(f"  Timestamp range: {joined_df['timestamp'].min()} to {joined_df['timestamp'].max()}")
            if 'Unnamed: 0' in joined_df.columns:
                joined_df = joined_df.drop(columns=['Unnamed: 0'])
            return joined_df
            
        except FileNotFoundError as e:
            self.logger.exception(f"Error: File not found - {e}")
            raise
        except ValueError as e:
            self.logger.exception(f"Error: {e}")
            raise
        except Exception as e:
            self.logger.exception(f"Error joining data: {e}")
            raise