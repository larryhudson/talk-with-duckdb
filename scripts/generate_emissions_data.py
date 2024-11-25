import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# Set random seed for reproducibility
np.random.seed(42)

# Constants
N_RECORDS = 10000
START_DATE = datetime(2022, 1, 1)
END_DATE = datetime(2023, 12, 31)

# Sample data
FACILITIES = [
    ('HQ', 'New York', 'USA'),
    ('Manufacturing Plant 1', 'Detroit', 'USA'),
    ('Distribution Center', 'Chicago', 'USA'),
    ('R&D Center', 'Boston', 'USA'),
    ('European Office', 'London', 'UK'),
    ('Asian Factory', 'Shanghai', 'China')
]

DEPARTMENTS = [
    'Manufacturing',
    'Logistics',
    'Office Operations',
    'Research',
    'Sales',
    'IT'
]

EMISSION_SOURCES = {
    'Electricity': ('Scope 2', 'kWh'),
    'Natural Gas': ('Scope 1', 'therms'),
    'Vehicle Fleet': ('Scope 1', 'gallons'),
    'Employee Commuting': ('Scope 3', 'km'),
    'Waste Management': ('Scope 3', 'tons'),
    'Business Travel': ('Scope 3', 'km')
}

def generate_emissions_data():
    data = []

    # Generate random dates
    dates = [START_DATE + timedelta(days=x) for x in
            np.random.randint(0, (END_DATE - START_DATE).days, N_RECORDS)]

    for _ in range(N_RECORDS):
        facility, city, country = random.choice(FACILITIES)
        source = random.choice(list(EMISSION_SOURCES.keys()))
        scope, unit = EMISSION_SOURCES[source]

        # Generate realistic consumption values based on source
        if source == 'Electricity':
            consumption = np.random.normal(50000, 10000)
        elif source == 'Natural Gas':
            consumption = np.random.normal(2000, 500)
        elif source == 'Vehicle Fleet':
            consumption = np.random.normal(500, 100)
        elif source == 'Employee Commuting':
            consumption = np.random.normal(1000, 200)
        elif source == 'Waste Management':
            consumption = np.random.normal(50, 10)
        else:  # Business Travel
            consumption = np.random.normal(2000, 500)

        # Calculate emissions (simplified conversion factors)
        if source == 'Electricity':
            emissions = consumption * 0.0004  # kWh to metric tons CO2
        elif source == 'Natural Gas':
            emissions = consumption * 0.005  # therms to metric tons CO2
        elif source == 'Vehicle Fleet':
            emissions = consumption * 0.008  # gallons to metric tons CO2
        elif source in ['Employee Commuting', 'Business Travel']:
            emissions = consumption * 0.0002  # km to metric tons CO2
        else:  # Waste Management
            emissions = consumption * 0.1  # tons to metric tons CO2

        data.append({
            'date': dates[_],
            'facility': facility,
            'city': city,
            'country': country,
            'department': random.choice(DEPARTMENTS),
            'emission_source': source,
            'scope': scope,
            'consumption_value': round(consumption, 2),
            'consumption_unit': unit,
            'emissions_mt_co2e': round(emissions, 3)
        })

    df = pd.DataFrame(data)
    return df

if __name__ == "__main__":
    df = generate_emissions_data()
    output_file = "carbon_emissions_data.csv"
    df.to_csv(output_file, index=False)
    print(f"Generated {len(df)} records of carbon emissions data in {output_file}")
    print("\nSample of the data:")
    print(df.head())
    print("\nSummary statistics:")
    print(df.describe())

