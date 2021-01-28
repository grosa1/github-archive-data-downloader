"""
Retrieve github archieve data from: https://www.githubarchive.org/
"""
import os
import datetime
from concurrent.futures import ThreadPoolExecutor
import requests
import pandas as pd

DEFAULT_WORKERS = 10
DEFAULT_OUTPUT_DIRECTORY = os.path.expanduser('~/github-archive-data')


def get_github_hourly_data(args):
    """
    Obtain the github archieve json.gz for the given datetime (hour)

    > If output_root_directory not given, 'github-archive-data' directory will be created under '~'.
    :param args: (tuple) (desired_hour_datetime, output_root_directory)
    :returns: (tuple) GITHUB_ARCHIVE_URL, LOCAL_FILEPATH
    """
    desired_hour_datetime, output_root_directory = args

    if not output_root_directory:
        output_root_directory = DEFAULT_OUTPUT_DIRECTORY
        if not os.path.exists(output_root_directory):
            os.mkdir(output_root_directory)
            print(f'> Created: {output_root_directory}')

    # create date directory if it does not exist
    # date_directory = desired_hour_datetime.strftime('%Y%m%d')
    # date_directory_fullpath = os.path.join(output_root_directory, date_directory)
    date_directory_fullpath = os.path.join(output_root_directory)
    if not os.path.exists(date_directory_fullpath):
        os.mkdir(date_directory_fullpath)
        print(f'> Created: {date_directory_fullpath}')

    # Uses Github archive file format: 2015-01-{01..30}-{0..23}.json.gz
    # http://data.githubarchive.org/2015-01-{01..30}-{0..23}.json.gz
    filename = '{year}-{month:02}-{day:02}-{hour}.json.gz'.format(year=desired_hour_datetime.year,
                                                                  month=desired_hour_datetime.month,
                                                                  day=desired_hour_datetime.day,
                                                                  hour=desired_hour_datetime.hour)
    gh_data_url = f'http://data.githubarchive.org/{filename}'
    response = requests.get(gh_data_url, stream=True)

    output_filepath = os.path.join(date_directory_fullpath, filename)
    if not os.path.exists(output_filepath):
        with open(output_filepath, 'wb') as out_json_gz:
            for chunk in response.iter_content():
                out_json_gz.write(chunk)
    else:
        print(f'FILE EXISTS, SKIPPING: {output_filepath}')

    return gh_data_url, output_filepath


def get_day_datetimes(start_datetime, end_datetime):
    """
    Datetime hour generator.
    Will generate the hourly datetime objects starting with the given 'start_datetime', up and until the number of days given.
    (only full days supported)

    :param start_datetime: (datetime) datetime (hour will be ignored)
    :param end_datetime: (datetime) datetime (hour will be ignored) Number of days to generate
    """
    return pd.date_range(start=start_datetime, end=end_datetime).to_pydatetime()

def collect_github_archive(initial_datetime, end_datetime, output_directory=DEFAULT_OUTPUT_DIRECTORY, workers=DEFAULT_WORKERS):
    """
    Download github archive data in parallel with a thread pool.
    :param initial_datetime: (datetime) initial datetime
    :param end_datetime: (datetime) end datetime
    :return:
    """
    with ThreadPoolExecutor(max_workers=workers) as executor:
        datetime_hours = get_day_datetimes(initial_datetime, end_datetime)
        args = [(d, output_directory) for d in datetime_hours]
        results = executor.map(get_github_hourly_data, args)
        return results


def date_string(s):
    """
    Date string parser that parses
    :param s: (str) Expect a date string in the format 'YYYY-MM-DD'
    :return: datetime.datetime(YYYY, MM, DD, 0, 0, 0)
    """
    try:
        return datetime.datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        msg = f"Not a valid date (Expected format, 'YYYY-MM-DD'): '{s}'."
        raise argparse.ArgumentTypeError(msg)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--start-date', '-s',
                        dest='start_datetime',
                        required=True,
                        type=date_string,
                        help='Date to start data collection in the format, "YYYY-MM-DD".')
    parser.add_argument('--end-date', '-e',
                        dest='end_datetime',
                        required=True,
                        type=date_string,
                        help='Date to end data collection in the format, "YYYY-MM-DD".')
    parser.add_argument('--output-directory', '-o',
                        dest='output_directory',
                        default=DEFAULT_OUTPUT_DIRECTORY,
                        help='The directory to save downloaded data (date directories, in the format YYYYMMDD, will be created).')
    parser.add_argument('--workers', '-w',
                        type=int,
                        default=DEFAULT_WORKERS,
                        help='Number of workers to use to download in parallel.')
    args = parser.parse_args()

    print(f'START-DATE: {args.start_datetime}')
    print(f'END-DATE: {args.end_datetime}')
    print(f'OUTPUT-DIRECTORY: {args.output_directory}')
    print(f'WORKERS: {args.workers}')
    results = collect_github_archive(args.start_datetime, args.end_datetime, args.output_directory, args.workers)
    for url, local_filepath in results:
        print(url, local_filepath)


