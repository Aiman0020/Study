import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import LabelEncoder
from sklearn.svm import SVC
import warnings

# Load the dataset
df = pd.read_csv('moving_avg.csv')

# Initialize LabelEncoder
en = LabelEncoder()

# Encode the 'Trend' column
df['Trend_n'] = en.fit_transform(df['Trend'])

# Define features (X) and target variable (y)
X = df.drop(['Trend', 'Trend_n'], axis=1)
y = df['Trend_n']

value = [
1,	0,	0,	1,	1,	0,	0,	0,	1,	0,	1,	0,	0,	1,	0,	0,	0,	1,	0,	0,	0,	0,	0,	1,	1,	1,	1,	0,	0,	0,	0,	1,	0,	1,	0,	0,	1,	1,	0,	0,	0,	1,	1,	0,	0,	0,	0,	0,	0,	0,	1,	1,	1,	0,	0,	1,	1,	0,	0,	0,	1,	1,	1,	1,	0,	0,	1,	1,	1,	0,	1,	1,	1,	1,	1,	0.5,	0,	0,	1,	1,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	1,	0,	0,	1,	1,	1,	1,	1,	1,	1,	1,	1,

]

# Reshape value to a 2D array
value = [value]

# Initialize and fit the Support Vector Classifier model
sv = SVC(max_iter=10000)
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    sv.fit(X, y)

# Initialize and fit the Logistic Regression model
lon = LogisticRegression(max_iter=10000)
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    lon.fit(X, y)

# Predict the trend for the provided values using Logistic Regression
predicted_trend = lon.predict(value)

# Decode the predicted trend
predicted_trend_label = en.inverse_transform(predicted_trend)

# Output the predicted trend
print(f"Predicted Trend: {predicted_trend_label}")
