# VWAP-NASDAQ
Calculating running Volume-Weighted Average Price(VWAP) Nasdaq every hour

Steps to run the project:

    1. Open terminal and do inside project directory
    2. Run "pip3 install -r requirements.txt" in the terminal
    3. Run "python3 main.py"
    4. It will ask for the path of the data file. Provide the path including the file name. Eg. "/Users/suryashukla/Developer/data/01302019.NASDAQ_ITCH50.gz" (without quotes)
    5. Wait till the project is running. You can see the progress in the terminal itself. The result would be stored in the out/ directory in the project directory. 
    6. Data of each hour is displayed in text file with its hour number in the out/ directory.