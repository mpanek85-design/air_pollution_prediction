import pandas as pd
import requests
from bs4 import BeautifulSoup
from pathlib import Path
from urllib.parse import urljoin
import os
import logging
from logging.handlers import RotatingFileHandler
import json
from dataclasses import dataclass


@dataclass
class WeatherDownloader():
    start_year: int = 2020
    end_year: int = 2024
    destination: str = "weather_data"
    base_url: str = "https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/dane_meteorologiczne/terminowe/synop"
    header_url: str = "https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/dane_meteorologiczne/terminowe/synop/s_t_nag%c5%82%c3%b3wek.csv"
    station_code: str = "424"
    output_folder: str = "processed_data"
    output_file: str = "weather.parquet"
    weather_columns_file: str = "weather_columns.json"

    def __post_init__(self):
        # Logger setup using the same log file as main.py
        self.logger = logging.getLogger("weather_downloader")
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
    def download_zip_files(self, url: str, destination: str = ".") -> list:
        """
        Download all zip files found in a given web URL.
        
        Parameters:
        -----------
        url : str
            The web URL to search for zip files
        destination : str
            The directory where zip files will be saved (default: current directory)
        
        Returns:
        --------
        list
            List of downloaded file paths
        
        Raises:
        -------
        requests.RequestException
            If the URL cannot be accessed
        """
        downloaded_files = []
        
        try:
            # Create destination directory if it doesn't exist
            Path(destination).mkdir(parents=True, exist_ok=True)
            
            # Fetch the webpage
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            # Parse HTML content
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find all zip file links
            zip_links = []
            for link in soup.find_all('a', href=True):
                href = link['href']
                if href.lower().endswith('.zip') and self.station_code in href:
                    # Convert relative URLs to absolute URLs
                    absolute_url = urljoin(url, href)
                    zip_links.append(absolute_url)
            
            # Download each zip file
            for zip_url in zip_links:
                try:
                    filename = zip_url.split('/')[-1]
                    filepath = os.path.join(destination, filename)
                    
                    self.logger.info(f"Downloading: {filename} from {zip_url}")
                    response = requests.get(zip_url, timeout=30)
                    response.raise_for_status()
                    
                    # Save the file
                    with open(filepath, 'wb') as f:
                        f.write(response.content)
                    
                    downloaded_files.append(filepath)
                    self.logger.info(f"Successfully saved: {filepath}")
                    
                except Exception as e:
                    self.logger.exception(f"Error downloading {zip_url}: {e}")
                    continue
            
            return downloaded_files
            
        except requests.RequestException as e:
            self.logger.exception(f"Error accessing URL {url}: {e}")
            return []

    def download_header_file(self):
        try:
            response = requests.get(self.header_url, timeout=10)
            response.raise_for_status()
            
            filename = self.header_url.split('/')[-1]
            filepath = os.path.join(self.destination, filename)
            
            with open(filepath, 'wb') as f:
                f.write(response.content)

            self.logger.info(f"Successfully saved header file: {filepath}")
            self.header_file = filepath
            return filepath
            
        except Exception as e:
            self.logger.exception(f"Error downloading header file from {self.header_url}: {e}")
            return None

    def load_and_filter_data(self):
        """
        Load and filter air pollution data by scanning a folder (and subfolders)
        for zip files, reading each, filtering by station and combining results.

        Parameters:
        -----------
        header_file : str
            Path to the header CSV used as column names
        source_folder : str
            Directory to search recursively for .zip files containing data

        Returns:
        --------
        pd.DataFrame or None
            Combined and filtered dataframe, or None on error/no data
        """
        try:
            # Load header
            header_df = pd.read_csv(self.header_file)

            # Ensure source folder exists
            source_path = Path(self.destination)
            if not source_path.exists():
                self.logger.warning(f"Source folder does not exist: {self.destination}")
                return None

            # Find all zip files recursively
            zip_paths = sorted([p for p in source_path.rglob('*.zip') if self.station_code in str(p)])
            if not zip_paths:
                self.logger.warning(f"No zip files found under: {self.destination}")
                return None

            data_frames = []
            for zip_path in zip_paths:
                try:
                    self.logger.info(f"Processing zip file: {zip_path}")
                    df = pd.read_csv(str(zip_path), encoding='unicode_escape', compression='zip', header=None, names=header_df.columns)
                    df = df[df.POST.str.replace(' ','')=='WROC£AW-STRACHOWICE']
                    if df is None or df.empty:
                        self.logger.warning(f"Empty dataframe for file: {zip_path}")
                    else:
                        data_frames.append(df)
                        self.logger.info(f"Added data from: {zip_path} rows={len(df)}")
                except Exception as e:
                    self.logger.exception(f"Error processing zip file {zip_path}: {e}")
                    continue

            if not data_frames:
                self.logger.warning(f"No data collected from zip files under: {self.destination}")
                return None

            combined_df = pd.concat(data_frames, ignore_index=True)

            # Filter data based on header columns
            self.logger.info(f"Loading weather columns from {self.weather_columns_file}")
            with open(self.weather_columns_file, 'r') as f:
                columns_config = json.load(f)
            weather_columns_to_keep = columns_config.get('weather_columns', [])
            
            #make timestamp from ROK, MC, DZ, GG columns
            combined_df['timestamp'] = pd.to_datetime(
                combined_df[['ROK', 'MC', 'DZ', 'GG']].rename(
                    columns={'ROK': 'year', 'MC': 'month', 'DZ': 'day', 'GG': 'hour'}
                )            )


            self.logger.info(f"  Will keep {len(weather_columns_to_keep)} weather columns")

            filtered_df = combined_df[['timestamp'] + weather_columns_to_keep]

            filtered_df.to_parquet(Path(self.output_folder) / self.output_file, index=False)

        except Exception as e:
            self.logger.exception(f"Error loading and filtering data from folder {self.destination}: {e}")
            return None

    

    def download(self):
        header = self.download_header_file()
        files = []
        for f in range(self.start_year, self.end_year+1):
            year_url = f"{self.base_url}/{f}/"
            subdestination = Path(self.destination+f'/{f}')
            #subdestination.mkdir(parents=True, exist_ok=True)

            files.extend(self.download_zip_files(year_url, subdestination))
        return header, files

    def run(self):
        self.download()
        self.load_and_filter_data()
