# Real-Time Cart-Abandonment Fraud Detector

## Student
Johnry Christian Paduhilao
#101002576

## NOTE FOR SOME FILES:
- **SCREENSHOT.png** - screenshot of  alert output firing in the Spark console 
- **EXPLAINATION.md** - why I chose this window type, and where my pipeline requires state

## Prerequisite

- Java 17 or newer (PySpark 4.x requires it; Java 11 will not work).
- Python 3.8 or newer.

## Setup

Create a virtual environment and install the dependencies:

    # mac / linux
    python -m venv .venv
    source .venv/bin/activate
    pip install -r requirements.txt

    # windows powershell
    python -m venv .venv
    .venv\Scripts\Activate.ps1
    pip install -r requirements.txt

    # windows cmd
    python -m venv .venv
    .venv\Scripts\activate.bat
    pip install -r requirements.txt

### Windows additional requirements — Hadoop winutils

Spark on Windows needs two native binaries to read the local filesystem.
Without them I got an `UnsatisfiedLinkError` on `NativeIO$Windows.access0`.

1. Download `winutils.exe` and `hadoop.dll` from
   https://github.com/cdarlint/winutils/tree/master/hadoop-3.3.6/bin

2. Create the folder `C:\hadoop\bin` and place both files there.

3. Set the environment variable permanently (run once in PowerShell):

    Ex. [System.Environment]::SetEnvironmentVariable("HADOOP_HOME", "C:\hadoop", "User")

4. Add `C:\hadoop\bin` to your user PATH, then restart the code terminal to make sure the changes takes effect.

## The data (CHOICE: DATASET C - eCommerce Behavior Data)

I used the public **"eCommerce behavior data from multi category store"** dataset
from Kaggle. Downloaded one of the monthly files and then created the sample.csv file from it. sample.csv is 50000 records from 2019-Nov.csv data

## How to run

1. Lay out the data so the program can read it like a stream:

       python simulate_stream.py

2. Start the detector:

       python fraud_detection.py

## Files

- `fraud_detection.py` - the streaming job.
- `simulate_stream.py` - transforms the sample into the watched folder the job reads.
- `requirements.txt` - Python dependencies (`pyspark`); install via the venv setup above.
