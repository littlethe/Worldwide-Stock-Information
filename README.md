# Worldwide-Stock-Information
Getting information of worldwide stocks and ETFs by using yfinance.

There are about 200000 stocks and ETFs in the world.
Those codes are designed to generate a long list of worldwide stocks and ETFs by using the package yfinance.
The filename of the long list is TickerInfo_Output.csv.
Originally, the size of the long list was 87MB, therefore it can not be uploaded to Github. (The limit of one file on GitHub is 25MB)
Therefore, to reduce the list size, I only kept a few stocks and removed the most stocks from the list.

The data of yfinance is from Yahoo Finance.
The long list contains information about each stock's name, price, EPS, currency, country...
Moreover, the list can record the situation of getting data, such as succeeded or failed, status, issue...
Then, we can find which stock is unavailable or available on Yahoo Finance.

The information of a stock from yfinance contains its category(industry and sector), but there is no category for ETFs.
Thus, I also wrote a function to categorize ETFs. (kind, target area, target field, leverage, the duration of bonds)

You only need to execute TickerGetting.py to generate TickerInfo_Output.csv.

