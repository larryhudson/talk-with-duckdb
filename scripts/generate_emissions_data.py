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

# Emission factors with their CO2 impact in kg
EMISSION_FACTORS = {
    'electricity_kwh': {'scope': 'Scope 2', 'unit': 'kWh', 'kg_co2e': 0.4},  # Grid electricity
    'natural_gas_therm': {'scope': 'Scope 1', 'unit': 'therms', 'kg_co2e': 5.0},  # Natural gas burning
    'gasoline_liter': {'scope': 'Scope 1', 'unit': 'liters', 'kg_co2e': 2.3},  # Vehicle fuel
    'diesel_liter': {'scope': 'Scope 1', 'unit': 'liters', 'kg_co2e': 2.7},  # Heavy vehicle fuel
    'flight_short_haul': {'scope': 'Scope 3', 'unit': 'km', 'kg_co2e': 0.15},  # Short flights (<1500km)
    'flight_long_haul': {'scope': 'Scope 3', 'unit': 'km', 'kg_co2e': 0.11},  # Long flights (>1500km)
    'rail_travel': {'scope': 'Scope 3', 'unit': 'km', 'kg_co2e': 0.04},  # Train travel
    'waste_landfill': {'scope': 'Scope 3', 'unit': 'kg', 'kg_co2e': 0.5},  # Landfill waste
    'waste_recycled': {'scope': 'Scope 3', 'unit': 'kg', 'kg_co2e': 0.1},  # Recycled waste
    'water_supply': {'scope': 'Scope 3', 'unit': 'cubic_meters', 'kg_co2e': 0.344},  # Water usage
}

# Activities that can generate emissions
ACTIVITIES = {
    'office_power': {'factor': 'electricity_kwh', 'base_amount': 1000, 'variance': 200},
    'heating': {'factor': 'natural_gas_therm', 'base_amount': 100, 'variance': 30},
    'company_cars': {'factor': 'gasoline_liter', 'base_amount': 150, 'variance': 50},
    'delivery_trucks': {'factor': 'diesel_liter', 'base_amount': 400, 'variance': 100},
    'business_flights_short': {'factor': 'flight_short_haul', 'base_amount': 800, 'variance': 200},
    'business_flights_long': {'factor': 'flight_long_haul', 'base_amount': 2000, 'variance': 500},
    'train_travel': {'factor': 'rail_travel', 'base_amount': 500, 'variance': 100},
    'general_waste': {'factor': 'waste_landfill', 'base_amount': 1000, 'variance': 200},
    'recycling': {'factor': 'waste_recycled', 'base_amount': 800, 'variance': 150},
    'water_consumption': {'factor': 'water_supply', 'base_amount': 100, 'variance': 20},
}

# Facilities
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

def generate_emissions_data():
    data = []

    # Generate random dates
    dates = [START_DATE + timedelta(days=int(x)) for x in
            np.random.randint(0, (END_DATE - START_DATE).days, N_RECORDS)]

    for _ in range(N_RECORDS):
        facility, city, country = random.choice(FACILITIES)
        activity_name = random.choice(list(ACTIVITIES.keys()))
        activity = ACTIVITIES[activity_name]
        
        # Get corresponding emission factor
        emission_factor = EMISSION_FACTORS[activity['factor']]
        
        # Generate consumption based on activity's base amount and variance
        consumption = np.random.normal(activity['base_amount'], activity['variance'])
        consumption = max(0, consumption)  # Ensure no negative values
        
        # Calculate emissions in metric tons CO2e
        emissions = (consumption * emission_factor['kg_co2e']) / 1000  # Convert kg to metric tons

        data.append({
            'date': dates[_],
            'facility': facility,
            'city': city,
            'country': country,
            'department': random.choice(DEPARTMENTS),
            'activity': activity_name,
            'emission_source': activity['factor'],
            'scope': emission_factor['scope'],
            'consumption_value': round(consumption, 2),
            'consumption_unit': emission_factor['unit'],
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

