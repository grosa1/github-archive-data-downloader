# github-archive-data-downloader README

This is a script to download hourly data from http://www.githubarchive.org for the specified number of days.
 

## Usage

```
usage: download_github_archive.py [-h] --start-date START_DATETIME --end-date END_DATETIME
                                  [--output-directory OUTPUT_DIRECTORY]
                                  [--workers WORKERS]

Retrieve github archieve data from: https://www.githubarchive.org/

optional arguments:
  -h, --help            show this help message and exit
  --start-date START_DATETIME, -s START_DATETIME
                        Date to start data collection in the format, "YYYY-MM-DD"
  --end-date END_DATETIME, -e END_DATETIME
                        Date to end data collection in the format, "YYYY-MM-DD"
  --output-directory OUTPUT_DIRECTORY, -o OUTPUT_DIRECTORY
                        The directory to save downloaded data
  ```
<!--               
  --workers WORKERS, -w WORKERS
                        Number of workers to use to download in parallel.
-->

Example:
```
python download_github_archive.py --start-date 2020-4-1 --end-date 2020-5-31
```
