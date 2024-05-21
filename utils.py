import argparse as args
import json
import logging
import sys
import csv

import pandas as pd

from collections import Counter


class MovieDataSetMetaData:

    def __init__(self, data_set_location: str, save_to: str) -> None:
        """
        Class to read and store data from provided movies data set.
        Data set downloaded from :
        https://www.kaggle.com/rounakbanik/the-movies-dataset

        Required data is in file : movies_metadata.csv

        Methods :
            1. self.load_data : Loads data from CSV file, and we clean bad entries
            2. self.display_requested_data : Gathers and prints displayed data.
        :param data_set_location: Location of the data set.
        :param save_to: Where to write the JSON output file.
        """
        self.data_set_location = data_set_location
        self.data_set_save_to_json = save_to
        self.data_set = None

    @staticmethod
    def _check_if_int(value: str) -> bool:
        """
        Check if a value can be converted to an int.
        :param value: String to be tested
        :return: bool value if it can be converted to an int
        """
        try:
            int(value)
            return True
        except ValueError:
            return False
        except TypeError:
            return False

    @staticmethod
    def _check_if_float(value: str) -> bool:
        """
        Check if a value can be converted to a float.
        :param value: String to be tested
        :return: bool if it can be converted to a float
        """
        try:
            float(value)
            return True
        except ValueError:
            return False
        except TypeError:
            return False

    def _clean_csv_data_from_bad_entries(
        self, int_columns: list, float_columns: list
    ) -> list[str]:
        """
        Provided data set has some broken entries, this function will remove them.
        :param int_columns: List of columns to check if data can be translated to int
        :param float_columns: List of columns to check if data can be translated to float
        :return: List of rows with cleaned data from bad entries
        """
        validated_data = []
        with open(self.data_set_location, "r") as f:
            reader = csv.DictReader(f)
            print(int_columns)
            print(float_columns)
            for row in reader:
                valid_row = all(self._check_if_int(row[col]) for col in int_columns)
                valid_row = valid_row and all(
                    self._check_if_float(row[col]) for col in float_columns
                )
                if valid_row:
                    validated_data.append(row)
                else:
                    continue
        logging.info(f"After cleaning we have {len(validated_data)} entries")
        return validated_data

    def _load_data(self) -> pd.DataFrame:
        """
        Method for loading data from csv file.
        Steps taken:
            1. Load data from CSV file and clean bad entries.
            2. Return cleaned data set.
            3. Load data to a pd.DataFrame.
        :return: data_set (pd.DataFrame): Data read from the csv file
        """
        columns_to_read = [
            "id",
            "original_title",
            "release_date",
            "vote_average",
            "genres",
        ]

        cleand_list_of_rows = self._clean_csv_data_from_bad_entries(
            int_columns=[columns_to_read[0]],
            float_columns=[
                columns_to_read[3],
            ],
        )
        try:
            data_set = pd.DataFrame(
                cleand_list_of_rows,
                columns=columns_to_read,
            )

        except FileNotFoundError:
            logging.error(f"Data set file {self.data_set_location} not found.")

        except pd.errors.EmptyDataError:
            logging.error(f"Data set file {self.data_set_location} empty.")

        except pd.errors.ParserError:
            logging.error(f"Data set file {self.data_set_location} parser error.")

        self.data_set = data_set

    def _set_data_types_data_set(self):

        # Set int values.
        self.data_set["vote_average"] = self.data_set["vote_average"].astype(float)
        self.data_set["genres"] = self.data_set["genres"].str.replace("'", '"')
        self.data_set["genres"] = self.data_set["genres"].apply(json.loads)

    def load_data(self) -> None:
        """
        Load data, and provide samples.
        :return: None
        """
        logging.info(f"Loading data from {self.data_set_location}")
        self._load_data()
        logging.info(f"Finished loading data ")
        logging.info("Setting data types for data set :")
        self._set_data_types_data_set()
        logging.info(f"Data types : \n {self.data_set.dtypes}")
        logging.info(f"Example entry \n {self.data_set.iloc[:6, :6]}")

    def _get_movies_count_released_by_year(self) -> pd.Series:
        """
        Get the number of released movies by year.
        :return: NUmber of movies released each year
        """
        self.data_set["years"] = self.data_set["release_date"].str[:4]

        return self.data_set["years"].value_counts().sort_index()

    def _get_movies_per_genres(self) -> pd.DataFrame:
        """
        Count number of movies per genres.
        :return: DataFrame with all the genres and how many occurrences are there in the Data Set
        """
        genre_counter = Counter()
        # Convert to from str JSON object all genres :
        for genres in self.data_set["genres"]:
            genre_name = [genre["name"] for genre in genres]
            genre_counter.update(genre_name)

        genre_count = pd.DataFrame.from_dict(
            genre_counter, orient="index", columns=["count"]
        ).sort_index()

        return genre_count

    def display_requested_information(self):
        """
        Collect and display final information :
            1. Number of movies in data set
            2. Average vote
            3. Top 5 movies
            4. Movies released each year
            5. Movies in each genre
        :return:
        """

        # Set pandas to print all rows :

        pd.set_option("display.max_rows", None)
        # Get data :
        movies_released_each_year = self._get_movies_count_released_by_year()
        movies_by_genre = self._get_movies_per_genres()

        # Print data :
        logging.info(
            f"Number of unique movies: {self.data_set['original_title'].nunique()} "
        )
        logging.info(
            f"The average vote for a movie is : {self.data_set['vote_average'].mean()}"
        )
        top_five = self.data_set.nlargest(5, "vote_average")
        logging.info(f"Top five movies are : \n {top_five}")
        logging.info(f"Movies released by year : \n {movies_released_each_year}")
        logging.info(f"Movies count by genre : \n {movies_by_genre}")

    def save_data_to_json(self):
        logging.info(f"Saving data to {self.data_set_save_to_json}")
        try:
            self.data_set.to_json(
                self.data_set_save_to_json, orient="records", lines=True
            )
        except FileNotFoundError:
            logging.error(f"Data set file {self.data_set_save_to_json} not found.")

        except Exception as e:
            logging.error(e)


def create_argparser() -> args.ArgumentParser:
    """
    Create argument parser for script
    :return: args.Parser object with parameters
    """
    parser = args.ArgumentParser()
    parser.add_argument(
        "--data_set_location",
        default="./data-sets/movies_metadata.csv",
        help="Please provide location of the movie data set",
    )
    parser.add_argument(
        "--save_to",
        default="data_set_json.json",
        help="Please provide location to save the data set in JSON format",
    )
    parser.add_argument(
        "--logging_location",
        default="logs.log",
        help="Please provide location of the logging file",
    )
    parser.add_argument(
        "--logging_level", default="INFO", help="Please provide logging level"
    )

    return parser


def create_logger(logging_location: str, logging_level: str) -> None:
    """
    Create a logging object.
    :param logging_location:(str) Location of the logging file
    :param logging_level:(str) Logging level
    """
    logging.root.handlers = []

    logging.basicConfig(
        level=logging_level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(logging_location),
            logging.StreamHandler(sys.stdout),
        ],
    )

    logging.info(
        f"Started logging with file {logging_location} and default level {logging_level}"
    )
