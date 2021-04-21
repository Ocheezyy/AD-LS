# AD-LS

## Note

This project is form a production environment and all sensitive data has been redacted. If you would like to utilize this, search for "#" and replace it with whatever variables are needed.

## Summary

This utility watches over a directory waiting on new spreadsheets to be created or moved to it. It will then process the spreadsheet comparing the data towards what was in our database. It then generates a spreadsheet with the results and sends an email with the output.

## Setup

- Install [SQL Server ODBC Driver 13](https://www.microsoft.com/en-us/download/details.aspx?id=50420)
- Install [Python 3.8+](https://www.python.org/downloads/)
- Create venv with requirements.txt
- Start application

## Packages

The packages can also be found in _requirements.txt_

- [pandas~=1.1.0](https://pypi.org/project/pandas/)
- [openpyxl~=3.0.4](https://pypi.org/project/openpyxl/)
- [numpy~=1.19.1](https://pypi.org/project/numpy/)
- [cryptography~=3.0](https://pypi.org/project/cryptography/)
- [pyodbc~=4.0.30](https://pypi.org/project/pyodbc/)

## Files and Directories

- main.py
  - Where the main process/loop is located
- Re-Process.py
  - This is used to re-process files upon any errors
- logger.py
  - Where the logging class is located
- install-packages.bat
  - This is a batch file that will install all packages in requirements
- data(Directory)
  - Where output spreadsheets are stored, and local db keeping track of files processed
- logs(Directory)
  - Holds all log files
- conn(Directory)
  - Contains encrypted connections strings
