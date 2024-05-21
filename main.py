from utils import create_argparser
from utils import MovieDataSetMetaData
from utils import create_logger


def main(data_set_location, save_to, logging_location, logging_level):
    """
    Main program function.
    Purpose : Read CSV file data with movie metadata and perform :
              1. Load data in a DataFrame in object MovieDataSetMetaData.
              2. Print to stdout number of unique movies.
              3. Print to stdout average rating of all movies.
              4. Print to stdout top 5 Highest rated movies.
              5. Print to stdout n of movies released each year.
              6. Print to stdout movies in each genre.
              7. Save to JSON file the data set
    :param data_set_location:
    :param save_to: Save data set location in JSON format
    :param logging_location:
    :param logging_level:
    :return: None
    """
    create_logger(logging_location, logging_level)
    data_frame = MovieDataSetMetaData(data_set_location, save_to)
    data_frame.load_data()
    data_frame.display_requested_information()
    data_frame.save_data_to_json()

if __name__ == "__main__":
    args = create_argparser().parse_args()
    main(
        args.data_set_location, args.save_to, args.logging_location, args.logging_level
    )
