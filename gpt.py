from mysql.connector import Error
from mysql.connector import pooling
import pandas as pd
import matplotlib.pyplot as plt

# Create a connection pool
db_pool = pooling.MySQLConnectionPool(
    pool_name="nse_pool",
    pool_size=5,
    host='localhost',
    user='root',
    password='Aiman@786',
    database='nse',
    autocommit=True,
    connect_timeout=120
)


def fetch_table_data(table_name, specific_date):
    try:
        # Get a connection from the pool
        connection = db_pool.get_connection()

        if connection.is_connected():
            cursor = connection.cursor(dictionary=True)
            query = f"SELECT * FROM {table_name} WHERE DATE(Timestamp) = '{specific_date}'"
            cursor.execute(query)
            rows = cursor.fetchall()
            if rows:
                # Convert fetched data into a DataFrame
                df = pd.DataFrame(rows)

                df.rename(
                    columns={'Timestamp': 'timestamp', 'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close',
                             'Volume': 'volume'}, inplace=True)

                # Convert 'timestamp' column to datetime
                df['timestamp'] = pd.to_datetime(df['timestamp'])

                # Resample data to 5-minute intervals and aggregate OHLCV data
                df.set_index('timestamp', inplace=True)
                df = df.resample('5min').agg({
                    'open': 'first',
                    'high': 'max',
                    'low': 'min',
                    'close': 'last',
                    'volume': 'sum'
                }).dropna()

                return df

            else:
                print("No data found in the specified date range.")
    except Error as e:
        print("Error while connecting to MySQL", e)


def classify_candle(open_price, close_price):
    if close_price > open_price:
        return 1  # Green candle
    elif close_price < open_price:
        return 0  # Red candle
    else:
        return 0.5  # Neutral candle (closing price equals opening price)


def sma_calculate(close_prices, window=50):
    sma = close_prices['close'].rolling(window=window, min_periods=1).mean()
    return sma


def ma_categories(close_prices, ma, open_prices):
    categories = []
    for close_price, ma_value, open_price in zip(close_prices, ma, open_prices):
        if ma_value < close_price:
            categories.append(1)  # Close price is above MA
        elif ma_value > close_price:
            categories.append(0)  # MA is above close price
        else:
            categories.append(0.5)  # Close price is equal to MA
    # Format categories to display 0.5 with decimal points and others as whole numbers
    categories = [f'{value:.1f}' if value == 0.5 else int(value) for value in categories]
    return categories


key = 'NSE_EQ|INE009A01021'
table_name = key.lower().replace('|', '_')
specific_date = '2024-03-28'

# Fetch data for the specific date and table
data_df = fetch_table_data(table_name, specific_date)

# Classify candles and print 0 or 1
for index, row in data_df.iterrows():
    print(classify_candle(row['open'], row['close']))

close_prices = fetch_table_data(table_name, specific_date)
sma = sma_calculate(close_prices)

# Calculate MA categories
ma_categories = ma_categories(close_prices['close'], sma, close_prices['open'])
ma_data = pd.DataFrame({'Close': close_prices['close'], 'MA': sma, 'Category': ma_categories})
# to print without timestamp
ma_data.reset_index(drop=True, inplace=True)
print(ma_data['Category'].to_string(index=False))

plt.figure(figsize=(10, 6))
plt.plot(close_prices.index, close_prices['close'], label='Close Price')
plt.plot(sma.index, sma, label='SMA (20 days)', color='red')

plt.legend()
plt.grid(True)
plt.show()
