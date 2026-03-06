import pandas as pd
import requests
from bs4 import BeautifulSoup
from pathlib import Path
from urllib.parse import urljoin
import os
import zipfile
import logging
from logging.handlers import RotatingFileHandler

import re
from dataclasses import dataclass


@dataclass
class AirPollutionDownloader():
    start_year: int = 2020
    end_year: int = 2024
    destination_raw: str = "air_pollution_data"
    destination_unpack: str = "extracted_air_pollution_data"
    output_folder: str = "processed_data"
    output_file: str = "air_pollution.parquet"
    base_url: str = "https://powietrze.gios.gov.pl/pjp/archives"

    def __post_init__(self):
        # Logger setup using the same log file as main.py
        self.logger = logging.getLogger("air_pollution_downloader")
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

    def run(self):
        #self.download()
        self.unpack_zip_files()
        self.merge_and_store_air_pollution_data()


    def download(self) -> list:
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
        if self.start_year > self.end_year:
            raise ValueError(f"start_year ({self.start_year}) cannot be greater than end_year ({self.end_year})")
        
        downloaded_files = []
        
        try:
            # Create destination directory if it doesn't exist
            Path(self.destination_raw).mkdir(parents=True, exist_ok=True)
            
            # Fetch the main archives page once
            self.logger.info(f"Fetching archives from {self.base_url}")
            response = requests.get(self.base_url, timeout=10)
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
                        if self.start_year <= year <= self.end_year:
                            # Find the parent link element
                            parent_link = file_entry.find_parent('a', href=True)
                            
                            if parent_link:
                                href = parent_link['href']
                                absolute_url = urljoin(self.base_url, href)
                                
                                try:
                                    filename = f"air_pollution_{year}.zip"
                                    filepath = os.path.join(self.destination_raw, filename)
                                    self.logger.info(f"Downloading: {description} -> {filename} from {absolute_url}")
                                    data_response = requests.get(absolute_url, timeout=30)
                                    data_response.raise_for_status()
                                    
                                    with open(filepath, 'wb') as f:
                                        f.write(data_response.content)
                                    
                                    downloaded_files.append(filepath)
                                    self.logger.info(f"Successfully saved: {filepath}")
                                    
                                except Exception as e:
                                    self.logger.exception(f"Error downloading {absolute_url}: {e}")
                                    continue
                                
                except Exception as e:
                    print(f"Error processing archive entry: {e}")
                    continue
            
            if not downloaded_files:
                self.logger.warning(f"No data files found for years {self.start_year}-{self.end_year}")

            self.logger.info(f"Download complete. Total files downloaded: {len(downloaded_files)}")
            return
            
        except requests.RequestException as e:
            self.logger.exception(f"Error accessing URL {self.base_url}: {e}")
            return



    def unpack_zip_files(self) -> list:
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
        if not os.path.exists(self.destination_raw):
            raise ValueError(f"Source folder does not exist: {self.destination_raw}")
        
        extracted_folders = []
        
        try:
            # Create destination directory if it doesn't exist
            Path(self.destination_unpack).mkdir(parents=True, exist_ok=True)
            
            # Find all zip files in the source folder
            zip_files = [f for f in os.listdir(self.destination_raw) if f.lower().endswith('.zip')]
            
            if not zip_files:
                self.logger.warning(f"No zip files found in {self.destination_raw}")
                return extracted_folders
            self.logger.info(f"Found {len(zip_files)} zip file(s) to extract")
            
            # Extract each zip file into its own subfolder
            for zip_filename in zip_files:
                zip_path = os.path.join(self.destination_raw, zip_filename)
                
                try:
                    # Create subfolder name based on zip filename (without extension)
                    subfolder_name = os.path.splitext(zip_filename)[0]
                    extract_path = os.path.join(self.destination_unpack, subfolder_name)
                    
                    # Create the subfolder
                    Path(extract_path).mkdir(parents=True, exist_ok=True)
                    
                    self.logger.info(f"Extracting: {zip_filename}")
                    
                    # Extract the zip file
                    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                        zip_ref.extractall(extract_path)
                    
                    extracted_folders.append(extract_path)
                    self.logger.info(f"Successfully extracted to: {extract_path}")
                    
                except zipfile.BadZipFile as e:
                    self.logger.exception(f"Error: {zip_filename} is not a valid zip file: {e}")
                    continue
                except Exception as e:
                    self.logger.exception(f"Error extracting {zip_filename}: {e}")
                    continue
            self.logger.info(f"Extraction complete. Total folders extracted: {len(extracted_folders)}")
            return extracted_folders
            
        except Exception as e:
            self.logger.exception(f"Error during extraction process: {e}")
            return extracted_folders

    def merge_and_store_air_pollution_data(self) -> None:
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
        if not os.path.exists(self.destination_unpack):
            raise ValueError(f"Extracted data folder does not exist: {self.destination_unpack}")
        
        # Get all subfolders
        subfolders = [f for f in os.listdir(self.destination_unpack)
                    if os.path.isdir(os.path.join(self.destination_unpack, f))]
        
        if not subfolders:
            self.logger.warning(f"No subfolders found in {self.destination_unpack}")
            return None

        self.logger.info(f"Found {len(subfolders)} subfolder(s) to process")
        
        combined_dataframes = []
        
        # Process each subfolder
        for subfolder in subfolders:
            subfolder_path = os.path.join(self.destination_unpack, subfolder)
            self.logger.info(f"Processing subfolder: {subfolder}")

            df = self.process_subfolder(subfolder_path)

            if df is not None:
                combined_dataframes.append(df)
                self.logger.info(f"Successfully processed subfolder: {subfolder}")
            else:
                self.logger.warning(f"No valid data extracted from subfolder: {subfolder}")
            
        if not combined_dataframes:
            self.logger.warning("No valid dataframes found in any subfolder")
            return None

        # Concatenate all dataframes from different subfolders
        final_df = pd.concat(combined_dataframes, ignore_index=True)

        self.logger.info(f"Final combined dataframe shape: {final_df.shape}")
        # check if output folder exists, if not create it
        output_path = Path(self.output_folder)
        output_path.mkdir(parents=True, exist_ok=True)
        final_df.to_parquet(output_path / self.output_file, index=False)
        return 

    def process_subfolder(self, subfolder_path: str) -> pd.DataFrame:
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
                self.logger.warning(f"No xlsx files with '1g' found in {subfolder_path}")
                return None

            self.logger.info(f"Processing {len(xlsx_files)} file(s) in {subfolder_path}")
            
            dataframes = []
            
            # Process each xlsx file
            for xlsx_file in xlsx_files:
                file_path = os.path.join(subfolder_path, xlsx_file)

                df = self.process_xlsx_file(file_path)
                
                if df is not None:
                    dataframes.append(df)
                    self.logger.info(f"Processed: {xlsx_file} in {subfolder_path}")
                else:
                    self.logger.warning(f"No data extracted from: {xlsx_file} in {subfolder_path}")
            
            if not dataframes:
                self.logger.warning(f"No valid dataframes extracted from {subfolder_path}")
                return None
            
            # Merge all dataframes from this subfolder on 'timestamp'
            if len(dataframes) > 1:
                merged_df = dataframes[0]
                for df in dataframes[1:]:
                    merged_df = merged_df.merge(df, on='timestamp', how='outer')
                self.logger.info(f"Merged {len(dataframes)} file(s) in {subfolder_path}")
                return merged_df
            else:
                return dataframes[0]
            
        except Exception as e:
            print(f"Error processing subfolder {subfolder_path}: {e}")
            return None

    
    def process_xlsx_file(self, file_path: str) -> pd.DataFrame:
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
