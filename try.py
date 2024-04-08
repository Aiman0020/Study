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


def calculate_result(row):
    # Based on the candle classification and SMA conditions, assign scenario values
    if row['candle_classification'] == 2:  # Green candle
        if row['sma20'] < row['close'] and row['sma50'] < row['close']:
            return 2  # Scenario 4: G + A + D
        elif row['sma20'] < row['close'] and row['sma50'] > row['close']:
            return 3  # Scenario 3: G + A + C
        elif row['sma20'] > row['close'] and row['sma50'] < row['close']:
            return 5  # Scenario 2: G + B + D
        elif row['sma20'] > row['close'] and row['sma50'] > row['close']:
            return 4  # Scenario 1: G + B + C
        elif row['sma20'] < row['close'] and row['sma50'] > row['high']:
            return 6  # Scenario 5: G + B + E
        elif row['sma20'] > row['close'] and row['sma50'] < row['low']:
            return 7  # Scenario 6: G + C + F
        elif row['sma20'] > row['close'] and row['sma50'] > row['low'] and row['sma50'] < row['high']:
            return 8  # Scenario 7: G + A + F
        elif row['sma20'] < row['close'] and row['sma50'] < row['high'] and row['sma50'] > row['low']:
            return 9  # Scenario 8: G + D + E
        elif row['sma20'] < row['close'] and row['sma50'] > row['low'] and row['sma50'] < row['high']:
            return 10  # Scenario 9: G + E + F
        elif row['sma20'] > row['close'] and row['sma50'] < row['high'] and row['sma50'] > row['low']:
            return 11  # Scenario 10: G + E + C
        elif row['sma20'] < row['close'] and row['sma50'] < row['low'] and row['sma50'] > row['high']:
            return 12  # Scenario 11: G + F + D
        elif row['sma20'] > row['close'] and row['sma50'] > row['low'] and row['sma50'] < row['high']:
            return 13  # Scenario 12: G + F + A
    elif row['candle_classification'] == 1:  # Red candle
        if row['sma20'] < row['close'] and row['sma50'] < row['close']:
            return -2  # Scenario 1: Red Candle with both MAs below Close
        elif row['sma20'] < row['close'] and row['sma50'] > row['close']:
            return -3  # Scenario 2: Red Candle with 20 MA below Close and 50 MA above Close
        elif row['sma20'] > row['close'] and row['sma50'] < row['close']:
            return -4  # Scenario 3: Red Candle with 20 MA above Close and 50 MA below Close
        elif row['sma20'] > row['close'] and row['sma50'] > row['close']:
            return -5  # Scenario 4: Red Candle with both MAs above Close
        elif row['sma20'] < row['close'] and row['sma50'] > row['high']:
            return -6  # Scenario 5: Red Candle with 20 MA below Close, 50 MA between High and Low
        elif row['sma20'] > row['close'] and row['sma50'] < row['low']:
            return -7  # Scenario 6: Red Candle with 50 MA below Close, 20 MA between High and Low
        elif row['sma20'] > row['close'] and row['sma50'] > row['low'] and row['sma50'] < row['high']:
            return -8  # Scenario 7: Red Candle with 20 MA above Close, 50 MA between High and Low
        elif row['sma20'] < row['close'] and row['sma50'] < row['low'] and row['sma50'] > row['high']:
            return -9  # Scenario 8: Red Candle with 50 MA above Close, 20 MA between High and Low
        elif row['sma20'] < row['close'] and row['sma50'] > row['low'] and row['sma50'] < row['high']:
            return -10  # Scenario 9: Red Candle with both MAs between High and Low
        elif row['sma20'] > row['close'] and row['sma50'] < row['high'] and row['sma50'] > row['low']:
            return -11  # Scenario 10: Red Candle with 20 MA between High and Low, 50 MA below Close
        elif row['sma20'] < row['close'] and row['sma50'] < row['high'] and row['sma50'] > row['low']:
            return -12  # Scenario 11: Red Candle with 50 MA between High and Low, 20 MA above Close
        elif row['sma20'] > row['close'] and row['sma50'] > row['high'] and row['sma50'] < row['low']:
            return -13  # Scenario 12: Red Candle with 20 MA between High and Low, 50 MA above Close

key = 'NSE_EQ|INE009A01021'
table_name = key.lower().replace('|', '_')
specific_date = '2024-03-28'

specific_date_data, previous_date_data = get_data_for_specific_date_and_previous_day(table_name, specific_date)

# Add a column for candle classification
specific_date_data['candle_classification'] = specific_date_data.apply(lambda x: classify_candle(x['open'], x['close']), axis=1)

# Calculate SMA values for 20 and 50 days
specific_date_data['sma20'] = sma(specific_date_data, 20)
specific_date_data['sma50'] = sma(specific_date_data, 50)

# Calculate the scenario result for each candle
specific_date_data['scenario_result'] = specific_date_data.apply(calculate_result, axis=1)

# Print or store the results
print(specific_date_data['scenario_result'])

# Plotting with scenario values or results
plt.figure(figsize=(10, 6))
plt.plot(specific_date_data.index, specific_date_data['close'], label='Close Price')
# Plot SMA (50 days) and SMA (20 days)
plt.plot(specific_date_data.index, specific_date_data['sma50'], label='SMA (50 days)', color='green')
plt.plot(specific_date_data.index, specific_date_data['sma20'], label='SMA (20 days)', color='red')
# Annotate the scenario results on the plot
for index, row in specific_date_data.iterrows():
    plt.annotate(str(row['scenario_result']), xy=(index, row['close']), xytext=(index, row['close'] + 10),
                 textcoords='offset points', ha='center', va='bottom', fontsize=8, color='blue')
plt.legend()
plt.grid(True)
plt.show()
