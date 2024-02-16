import pandas as pd
import re

# Sample DataFrame
data = {'Product_names': ['Product 3(5)', 'Product 4(1),Product 5', 'Product 3(1)', 'Product 1,Product 2(10)', 'Product 6']}
df = pd.DataFrame(data)

# Function to remove counts in parentheses
def remove_counts(text):
    return re.sub(r'\(\d+\)', '', text)

# Apply the function to the 'Product_names' column
df['Product_names'] = df['Product_names'].apply(remove_counts)

# Display the modified DataFrame
print(df)