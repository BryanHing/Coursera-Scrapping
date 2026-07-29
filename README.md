# Coursera-Scrapping

This Python project discovers Coursera course links and extracts course details
into Google Sheets.

## Setup

Install Python, Google Chrome, and the project dependencies:

```powershell
python -m pip install -r requirements.txt
```

Modern Selenium automatically manages the compatible ChromeDriver.

## Usage

First, discover course links:

```powershell
python extract_course_link.py
```

The links are written to `xpaths.txt`. Then extract the course details:

```powershell
python extract_to_sheet.py
```

Place the Google service-account file beside the scripts using the filename
configured in `extract_to_sheet.py`. Share the destination spreadsheet with
the service account's email address.

The credential file is excluded from Git and must never be committed.
