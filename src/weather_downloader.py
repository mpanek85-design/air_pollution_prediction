import pandas as pd
import requests
from bs4 import BeautifulSoup
from pathlib import Path
from urllib.parse import urljoin
import os
import zipfile
import logging
import json
from logging.handlers import RotatingFileHandler
from datetime import datetime
import re

# Logger setup
LOG_DIR = Path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
log_file = LOG_DIR / f"processing.log"

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


def download_zip_files(url: str, destination: str = ".") -> list:
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
            if href.lower().endswith('.zip'):
                # Convert relative URLs to absolute URLs
                absolute_url = urljoin(url, href)
                zip_links.append(absolute_url)
        
        # Download each zip file
        for zip_url in zip_links:
            try:
                filename = zip_url.split('/')[-1]
                filepath = os.path.join(destination, filename)
                
                logger.info(f"Downloading: {filename} from {zip_url}")
                response = requests.get(zip_url, timeout=30)
                response.raise_for_status()
                
                # Save the file
                with open(filepath, 'wb') as f:
                    f.write(response.content)
                
                downloaded_files.append(filepath)
                logger.info(f"Successfully saved: {filepath}")
                
            except Exception as e:
                logger.exception(f"Error downloading {zip_url}: {e}")
                continue
        
        return downloaded_files
        
    except requests.RequestException as e:
        logger.exception(f"Error accessing URL {url}: {e}")
        return []

def download_header_file(url, destination):
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        filename = url.split('/')[-1]
        filepath = os.path.join(destination, filename)
        
        with open(filepath, 'wb') as f:
            f.write(response.content)

        logger.info(f"Successfully saved header file: {filepath}")
        return filepath
        
    except Exception as e:
        logger.exception(f"Error downloading header file from {url}: {e}")
        return None

def load_and_filter_data(header_file, source_folder, station_code):
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
        header_df = pd.read_csv(header_file)

        # Ensure source folder exists
        source_path = Path(source_folder)
        if not source_path.exists():
            logger.warning(f"Source folder does not exist: {source_folder}")
            return None

        # Find all zip files recursively
        zip_paths = sorted([p for p in source_path.rglob('*.zip') if station_code in str(p)])
        if not zip_paths:
            logger.warning(f"No zip files found under: {source_folder}")
            return None

        data_frames = []
        for zip_path in zip_paths:
            try:
                logger.info(f"Processing zip file: {zip_path}")
                df = pd.read_csv(str(zip_path), encoding='unicode_escape', compression='zip', header=None, names=header_df.columns)
                df = df[df.POST.str.replace(' ','')=='WROC£AW-STRACHOWICE']
                if df is None or df.empty:
                    logger.warning(f"Empty dataframe for file: {zip_path}")
                else:
                    data_frames.append(df)
                    logger.info(f"Added data from: {zip_path} rows={len(df)}")
            except Exception as e:
                logger.exception(f"Error processing zip file {zip_path}: {e}")
                continue

        if not data_frames:
            logger.warning(f"No data collected from zip files under: {source_folder}")
            return None

        combined_df = pd.concat(data_frames, ignore_index=True)

        # Filter data based on header columns
        filtered_df = combined_df[header_df.columns]

        return filtered_df

    except Exception as e:
        logger.exception(f"Error loading and filtering data from folder {source_folder}: {e}")
        return None

def download_air_pollution_data(start_year: int, end_year: int, destination: str = "air_pollution_data") -> list:
    """
    Download air pollution data from GIOS archives for a specified year range.
    
    Parameters:
    -----------
    start_year : int
        The starting year for data download (e.g., 2020)
    end_year : int
        The ending year for data download (inclusive, e.g., 2025)
    destination : str
        The directory where data will be saved (default: "air_pollution_data")
    
    Returns:
    --------
    list
        List of downloaded file paths
    
    Raises:
    -------
    ValueError
        If start_year is greater than end_year
    requests.RequestException
        If the URL cannot be accessed
    """
    if start_year > end_year:
        raise ValueError(f"start_year ({start_year}) cannot be greater than end_year ({end_year})")
    
    downloaded_files = []
    base_url = "https://powietrze.gios.gov.pl/pjp/archives"
    
    try:
        # Create destination directory if it doesn't exist
        Path(destination).mkdir(parents=True, exist_ok=True)
        
        # Fetch the main archives page once
        logger.info(f"Fetching archives from {base_url}")
        response = requests.get(base_url, timeout=10)
        response.raise_for_status()
        
        # Parse HTML content
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find all archive file entries with description "Wyniki pomiarów z {year} roku"
        archive_files = soup.find_all('p', class_='archive_file_name')
        
        for file_entry in archive_files:
            try:
                description = file_entry.get_text().strip()
                
                # Extract year from description "Wyniki pomiarów z XXXX roku"
                import re
                year_match = re.search(r'Wyniki pomiarów z (\d{4}) roku', description)
                
                if year_match:
                    year = int(year_match.group(1))
                    
                    # Check if this year is in the requested range
                    if start_year <= year <= end_year:
                        # Find the parent link element
                        parent_link = file_entry.find_parent('a', href=True)
                        
                        if parent_link:
                            href = parent_link['href']
                            absolute_url = urljoin(base_url, href)
                            
                            try:
                                filename = f"air_pollution_{year}.zip"
                                filepath = os.path.join(destination, filename)
                                logger.info(f"Downloading: {description} -> {filename} from {absolute_url}")
                                data_response = requests.get(absolute_url, timeout=30)
                                data_response.raise_for_status()
                                
                                with open(filepath, 'wb') as f:
                                    f.write(data_response.content)
                                
                                downloaded_files.append(filepath)
                                logger.info(f"Successfully saved: {filepath}")
                                
                            except Exception as e:
                                logger.exception(f"Error downloading {absolute_url}: {e}")
                                continue
                            
            except Exception as e:
                print(f"Error processing archive entry: {e}")
                continue
        
        if not downloaded_files:
            logger.warning(f"No data files found for years {start_year}-{end_year}")

        logger.info(f"Download complete. Total files downloaded: {len(downloaded_files)}")
        return downloaded_files
        
    except requests.RequestException as e:
        logger.exception(f"Error accessing URL {base_url}: {e}")
        return []


def unpack_zip_files(source_folder: str, destination_folder: str) -> list:
    """
    Unpack all zip files from a source folder into separate subfolders.
    
    Each zip file will be extracted into its own subfolder named after the zip file
    (without the .zip extension).
    
    Parameters:
    -----------
    source_folder : str
        The folder containing zip files to unpack
    destination_folder : str
        The folder where subfolders and extracted files will be created
    
    Returns:
    --------
    list
        List of successfully extracted file paths (subfolders)
    
    Raises:
    -------
    ValueError
        If source_folder does not exist
    """
    if not os.path.exists(source_folder):
        raise ValueError(f"Source folder does not exist: {source_folder}")
    
    extracted_folders = []
    
    try:
        # Create destination directory if it doesn't exist
        Path(destination_folder).mkdir(parents=True, exist_ok=True)
        
        # Find all zip files in the source folder
        zip_files = [f for f in os.listdir(source_folder) if f.lower().endswith('.zip')]
        
        if not zip_files:
            logger.warning(f"No zip files found in {source_folder}")
            return extracted_folders
        logger.info(f"Found {len(zip_files)} zip file(s) to extract")
        
        # Extract each zip file into its own subfolder
        for zip_filename in zip_files:
            zip_path = os.path.join(source_folder, zip_filename)
            
            try:
                # Create subfolder name based on zip filename (without extension)
                subfolder_name = os.path.splitext(zip_filename)[0]
                extract_path = os.path.join(destination_folder, subfolder_name)
                
                # Create the subfolder
                Path(extract_path).mkdir(parents=True, exist_ok=True)
                
                logger.info(f"Extracting: {zip_filename}")
                
                # Extract the zip file
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(extract_path)
                
                extracted_folders.append(extract_path)
                logger.info(f"Successfully extracted to: {extract_path}")
                
            except zipfile.BadZipFile as e:
                logger.exception(f"Error: {zip_filename} is not a valid zip file: {e}")
                continue
            except Exception as e:
                logger.exception(f"Error extracting {zip_filename}: {e}")
                continue
        logger.info(f"Extraction complete. Total folders extracted: {len(extracted_folders)}")
        return extracted_folders
        
    except Exception as e:
        logger.exception(f"Error during extraction process: {e}")
        return extracted_folders


def process_xlsx_file(file_path: str) -> pd.DataFrame:
    """
    Process a single xlsx file containing air pollution data.
    
    Parameters:
    -----------
    file_path : str
        Path to the xlsx file
    
    Returns:
    --------
    pd.DataFrame
        Processed dataframe with 'timestamp' and pollutant columns
    """
    try:
        # find in which row of the xlsx file there is a column with 'DsWrocWybCon' phrase and skip all rows before it
        df_temp = pd.read_excel(file_path, nrows=10)
        skip_rows = 0
        for i, row in df_temp.iterrows():
            if any('DsWrocWybCon' in str(col) for col in row) and not any('Kod stacji' in str(col) for col in row):
                skip_rows = i+1
                break

        # Read xlsx file, skipping rows before the target column
        df = pd.read_excel(file_path, skiprows=skip_rows)
        
        # Keep only first column and columns containing station indetifier
        columns_to_keep = [col for col in df.columns if 'DsWrocWybCon' in col]
        
        # Get first column name, usually it is 'Kod stanowiska'
        first_col = df.columns[0]

        # if 'Kod stanowiska' not in df.columns:
        #     logger.warning(f"'Kod stanowiska' not found in {file_path}")
        #     return None
        
        # Select relevant columns
        df = df[[first_col] + columns_to_keep]
        
        # Rename first column to 'timestamp'
        df = df.rename(columns={first_col: 'timestamp'})
        
        # Rename DsWrocWybCon columns by extracting pollutant name

        for col in columns_to_keep:
            match = re.search(r'DsWrocWybCon-([^-\s]+)', col)
            if match:
                pollutant_name = match.group(1)
                df = df.rename(columns={col: pollutant_name})
        
        logger.info(f"Processed xlsx file: {file_path} rows={len(df)}")
        return df
        
    except Exception as e:
        logger.exception(f"Error processing xlsx file {file_path}: {e}")
        return None


def process_subfolder(subfolder_path: str) -> pd.DataFrame:
    """
    Process all xlsx files containing '1g' in a single subfolder and merge them.
    
    Parameters:
    -----------
    subfolder_path : str
        Path to the subfolder containing xlsx files
    
    Returns:
    --------
    pd.DataFrame
        Merged dataframe from all files in the subfolder
    """
    try:
        # Find all xlsx files containing '1g' in the subfolder
        xlsx_files = [f for f in os.listdir(subfolder_path) 
                      if f.lower().endswith('.xlsx') and '1g' in f.lower()]
        
        if not xlsx_files:
            logger.warning(f"No xlsx files with '1g' found in {subfolder_path}")
            return None

        logger.info(f"Processing {len(xlsx_files)} file(s) in {subfolder_path}")
        
        dataframes = []
        
        # Process each xlsx file
        for xlsx_file in xlsx_files:
            file_path = os.path.join(subfolder_path, xlsx_file)

            df = process_xlsx_file(file_path)
            
            if df is not None:
                dataframes.append(df)
                logger.info(f"Processed: {xlsx_file} in {subfolder_path}")
            else:
                logger.warning(f"No data extracted from: {xlsx_file} in {subfolder_path}")
        
        if not dataframes:
            logger.warning(f"No valid dataframes extracted from {subfolder_path}")
            return None
        
        # Merge all dataframes from this subfolder on 'timestamp'
        if len(dataframes) > 1:
            merged_df = dataframes[0]
            for df in dataframes[1:]:
                merged_df = merged_df.merge(df, on='timestamp', how='outer')
            logger.info(f"Merged {len(dataframes)} file(s) in {subfolder_path}")
            return merged_df
        else:
            return dataframes[0]
        
    except Exception as e:
        print(f"Error processing subfolder {subfolder_path}: {e}")
        return None


def load_air_pollution_data(extracted_data_folder: str, output_folder: str, output_file: str) -> None:
    """
    Load and combine air pollution data from all subfolders.
    
    Reads xlsx files containing '1g' in their name from each subfolder,
    processes them, merges them within each subfolder, and concatenates
    results from all subfolders.
    
    Parameters:
    -----------
    extracted_data_folder : str
        Path to the folder containing extracted air pollution subfolders
    
    Returns:
    --------
    pd.DataFrame
        Combined dataframe from all subfolders
    """
    if not os.path.exists(extracted_data_folder):
        raise ValueError(f"Extracted data folder does not exist: {extracted_data_folder}")
    
    # Get all subfolders
    subfolders = [f for f in os.listdir(extracted_data_folder)
                  if os.path.isdir(os.path.join(extracted_data_folder, f))]
    
    if not subfolders:
        logger.warning(f"No subfolders found in {extracted_data_folder}")
        return None

    logger.info(f"Found {len(subfolders)} subfolder(s) to process")
    
    combined_dataframes = []
    
    # Process each subfolder
    for subfolder in subfolders:
        subfolder_path = os.path.join(extracted_data_folder, subfolder)
        logger.info(f"Processing subfolder: {subfolder}")

        df = process_subfolder(subfolder_path)

        if df is not None:
            combined_dataframes.append(df)
            logger.info(f"Successfully processed subfolder: {subfolder}")
        else:
            logger.warning(f"No valid data extracted from subfolder: {subfolder}")
        
    if not combined_dataframes:
        logger.warning("No valid dataframes found in any subfolder")
        return None

    # Concatenate all dataframes from different subfolders
    final_df = pd.concat(combined_dataframes, ignore_index=True)

    logger.info(f"Final combined dataframe shape: {final_df.shape}")
    # check if output folder exists, if not create it
    output_path = Path(output_folder)
    output_path.mkdir(parents=True, exist_ok=True)
    final_df.to_parquet(output_path / output_file, index=False)
    return 


def download_weather_data(url:str, destination:str, start_year:int, end_year:int):
    header = download_header_file(url='https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/dane_meteorologiczne/terminowe/synop/s_t_nag%c5%82%c3%b3wek.csv', destination=destination)
    files = []
    for f in range(start_year, end_year+1):
        year_url = f"{url}/{f}/"
        subdestination = Path(destination+f'/{f}')
        #subdestination.mkdir(parents=True, exist_ok=True)

        files.extend(download_zip_files(year_url, subdestination))
    return header, files


def join_pollution_weather_data(pollution_file: str, weather_file: str, weather_columns_file: str = "weather_columns.json") -> pd.DataFrame:
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
        logger.info(f"Loading weather columns from {weather_columns_file}")
        with open(weather_columns_file, 'r') as f:
            columns_config = json.load(f)
        weather_columns_to_keep = columns_config.get('weather_columns', [])
        logger.info(f"  Will keep {len(weather_columns_to_keep)} weather columns")
        
        # Load air pollution data
        logger.info(f"Loading air pollution data from {pollution_file}")
        pollution_df = pd.read_csv(pollution_file)
        
        # Convert timestamp to datetime if not already
        if 'timestamp' in pollution_df.columns:
            pollution_df['timestamp'] = pd.to_datetime(pollution_df['timestamp'])
        else:
            raise ValueError("Air pollution data must have a 'timestamp' column")
        
        logger.info(f"  Loaded {len(pollution_df)} rows of pollution data")
        
        # Load weather data
        logger.info(f"Loading weather data from {weather_file}")
        weather_df = pd.read_csv(weather_file)
        
        # Check for required columns to create timestamp
        required_cols = ['ROK', 'MC', 'DZ', 'GG']
        if not all(col in weather_df.columns for col in required_cols):
            raise ValueError(f"Weather data must have columns: {required_cols}")
        
        # Create timestamp from ROK, MC, DZ, GG columns
        logger.info("Creating timestamp column from ROK, MC, DZ, GG")
        weather_df['timestamp'] = pd.to_datetime(
            weather_df[['ROK', 'MC', 'DZ', 'GG']].rename(
                columns={'ROK': 'year', 'MC': 'month', 'DZ': 'day', 'GG': 'hour'}
            )
        )
        
        # Filter weather_df to keep only columns that exist and are in the config
        available_weather_cols = [col for col in weather_columns_to_keep if col in weather_df.columns]
        cols_to_keep = ['timestamp'] + available_weather_cols
        weather_df = weather_df[cols_to_keep]
        logger.info(f"  Weather data filtered to {len(available_weather_cols)} columns")
        logger.info(f"  Loaded {len(weather_df)} rows of weather data")
        
        # Join on timestamp
        logger.info("Joining data on timestamp")
        joined_df = pd.merge(
            pollution_df, 
            weather_df, 
            on='timestamp', 
            how='inner'
        )
        
        logger.info(f"  Joined dataframe has {len(joined_df)} rows and {len(joined_df.columns)} columns")
        logger.info(f"  Timestamp range: {joined_df['timestamp'].min()} to {joined_df['timestamp'].max()}")
        if 'Unnamed: 0' in joined_df.columns:
            joined_df = joined_df.drop(columns=['Unnamed: 0'])
        return joined_df
        
    except FileNotFoundError as e:
        logger.exception(f"Error: File not found - {e}")
        raise
    except ValueError as e:
        logger.exception(f"Error: {e}")
        raise
    except Exception as e:
        logger.exception(f"Error joining data: {e}")
        raise

if __name__ == "__main__":
    url="https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/dane_meteorologiczne/terminowe/synop"
    destination = "weather_data"

    # download_air_pollution_data(start_year=2020, end_year=2025, destination="air_pollution_data")
    # unpack_zip_files(source_folder="air_pollution_data", destination_folder="extracted_air_pollution_data")
    df = load_air_pollution_data("extracted_air_pollution_data")
    # df = pd.read_csv('combined_pollution.csv')
    #header = download_header_file(url='https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/dane_meteorologiczne/terminowe/synop/s_t_nag%c5%82%c3%b3wek.csv', destination=destination)
    #files = download_zip_files(url, destination)

    # header, files = download_weather_data(url=url, destination=destination, start_year=2020, end_year=2024)
    # header = 'weather_data/s_t_nag%c5%82%c3%b3wek.csv'
    # df = load_and_filter_data(header, destination, station_code='424')

    joined_data = join_pollution_weather_data('combined_pollution2.csv', 'weather_data_combined.csv')
    pass
