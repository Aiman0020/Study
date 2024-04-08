from datetime import datetime, timedelta
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


def fetch_table_data(table_name, start_date, end_date):
    try:
        # Get a connection from the pool
        connection = db_pool.get_connection()

        if connection.is_connected():
            cursor = connection.cursor(dictionary=True)
            query = f"SELECT * FROM {table_name} WHERE DATE(Timestamp) BETWEEN '{start_date}' AND '{end_date}'"
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
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()


def get_data_for_specific_date_and_previous_day(table_name, specific_date):
    specific_date_data = fetch_table_data(table_name, specific_date, specific_date)
    previous_date = (datetime.strptime(specific_date, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
    previous_date_data = fetch_table_data(table_name, previous_date, previous_date)
    return specific_date_data, previous_date_data


def print_combined_data(specific_date_data, previous_date_data):
    combined_data = pd.concat([previous_date_data, specific_date_data])
    return combined_data


def sma(data, period=30, column='close'):
    return data[column].rolling(window=period).mean()


def classify_candle(open_price, close_price):
    if close_price > open_price:
        return 2  # Green candle
    elif close_price < open_price:
        return 1  # Red candle
    else:
        return 1.5  # Neutral candle (closing price equals opening price)

def calculate_result():
    pass

key = 'NSE_EQ|INE009A01021'
table_name = key.lower().replace('|', '_')
specific_date = '2024-03-28'

specific_date_data, previous_date_data = get_data_for_specific_date_and_previous_day(table_name, specific_date)

# for index, row in specific_date_data.iterrows():
#     print(classify_candle(row['open'], row['close']))

df = print_combined_data(specific_date_data, previous_date_data)
df['sma20'] = sma(df, 20)
df['sma50'] = sma(df, 50)



final = df[df.index.date == pd.Timestamp(f'{specific_date}').date()]
# print(final)


# sma_50 = df['sma50']
# sma_50 = sma_50[sma_50.index.date == pd.Timestamp(f"{specific_date}").date()]
# sma_20 = df['sma20']
# sma_20 = sma_20[sma_20.index.date == pd.Timestamp(f"{specific_date}").date()]

# plt.figure(figsize=(10, 6))
# plt.plot(specific_date_data.index, specific_date_data['close'], label='Close Price')
# plt.plot(sma_50.index, sma_50, label='SMA (50 days)', color='green')
# plt.plot(sma_20.index, sma_20, label='SMA (20 days)', color='red')
# plt.legend()
# plt.grid(True)
# plt.show()
