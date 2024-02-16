import pandas as pd
import random
from datetime import datetime, timedelta

# Generate random data
def generate_random_data():
    titles = ['Title A', 'Title B', 'Title C', 'Title D', 'Title E']
    names = ['John Doe', 'Jane Smith', 'Alice Johnson', 'Bob Brown', 'Emma Davis']
    filetypes = ['pdf', 'doc', 'xlsx', 'txt']
    modified_dates = [datetime(2023, 1, 1) + timedelta(days=random.randint(1, 365)) for _ in range(5)]
    years = [random.randint(2010, 2023) for _ in range(5)]
    countries = ['USA', 'UK', 'Canada', 'Australia']
    classifications = ['Class A', 'Class B', 'Class C']
    categories = ['Category A', 'Category B', 'Category C']
    application_types = ['Type 1', 'Type 2', 'Type 3']
    paths = ['/path/to/file1', '/path/to/file2', '/path/to/file3', '/path/to/file4', '/path/to/file5']
    file_sizes = [random.randint(100, 1000) for _ in range(5)]
    product_names = ['Product 1,Product 2', 'Product 3', 'Product 4,Product 5', 'Product 6', 'Product 7']
    diseases = ['Disease 1', 'Disease 2,Disease 3', 'Disease 4', 'Disease 5', 'Disease 6']
    pests = ['Pest 1', 'Pest 2', 'Pest 3,Pest 4', 'Pest 5,Pest 6', 'Pest 7']
    crops = ['Crop 1', 'Crop 2', 'Crop 3,Crop 4', 'Crop 5', 'Crop 6']
    efficacies = ['High', 'Medium', 'Low']
    selectivities = ['Yes', 'No']
    trials = ['Field', 'Greenhouse']
    languages = ['English', 'French', 'Spanish']

    data = {
        'Title': random.choices(titles, k=5),
        'Name': random.choices(names, k=5),
        'Filetype': random.choices(filetypes, k=5),
        'Modified': random.choices(modified_dates, k=5),
        'Year': random.choices(years, k=5),
        'Country': random.choices(countries, k=5),
        'Classification': random.choices(classifications, k=5),
        'Category_for_one_page': random.choices(categories, k=5),
        'Application_type': random.choices(application_types, k=5),
        'path': random.choices(paths, k=5),
        'filesize': random.choices(file_sizes, k=5),
        'Product_names': random.choices(product_names, k=5),
        'Diseases': random.choices(diseases, k=5),
        'pest': random.choices(pests, k=5),
        'crop': random.choices(crops, k=5),
        'efficacy': random.choices(efficacies, k=5),
        'selectivity': random.choices(selectivities, k=5),
        'Field_or_greenhouse_trial': random.choices(trials, k=5),
        'Trial_report': [True, False, True, False, True],
        'summary': ['Summary A', 'Summary B', 'Summary C', 'Summary D', 'Summary E'],
        'language': random.choices(languages, k=5)
    }
    return data

# Create DataFrame
data = generate_random_data()
df = pd.DataFrame(data)

# Save to Excel
df.to_excel("random_data.xlsx", index=False)
