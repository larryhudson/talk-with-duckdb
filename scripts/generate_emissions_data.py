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
    'EF001': {'name': 'electricity_kwh', 'scope': 'Scope 2', 'unit': 'kWh', 'kg_co2e': 0.4, 'description': 'Grid electricity'},
    'EF002': {'name': 'natural_gas_therm', 'scope': 'Scope 1', 'unit': 'therms', 'kg_co2e': 5.0, 'description': 'Natural gas burning'},
    'EF003': {'name': 'gasoline_liter', 'scope': 'Scope 1', 'unit': 'liters', 'kg_co2e': 2.3, 'description': 'Vehicle fuel'},
    'EF004': {'name': 'diesel_liter', 'scope': 'Scope 1', 'unit': 'liters', 'kg_co2e': 2.7, 'description': 'Heavy vehicle fuel'},
    'EF005': {'name': 'flight_short_haul', 'scope': 'Scope 3', 'unit': 'km', 'kg_co2e': 0.15, 'description': 'Short flights (<1500km)'},
    'EF006': {'name': 'flight_long_haul', 'scope': 'Scope 3', 'unit': 'km', 'kg_co2e': 0.11, 'description': 'Long flights (>1500km)'},
    'EF007': {'name': 'rail_travel', 'scope': 'Scope 3', 'unit': 'km', 'kg_co2e': 0.04, 'description': 'Train travel'},
    'EF008': {'name': 'waste_landfill', 'scope': 'Scope 3', 'unit': 'kg', 'kg_co2e': 0.5, 'description': 'Landfill waste'},
    'EF009': {'name': 'waste_recycled', 'scope': 'Scope 3', 'unit': 'kg', 'kg_co2e': 0.1, 'description': 'Recycled waste'},
    'EF010': {'name': 'water_supply', 'scope': 'Scope 3', 'unit': 'cubic_meters', 'kg_co2e': 0.344, 'description': 'Water usage'}
}

# Activities that can generate emissions
ACTIVITIES = {
    'ACT001': {'name': 'office_power', 'emission_factor_id': 'EF001', 'base_amount': 1000, 'variance': 200},
    'ACT002': {'name': 'heating', 'emission_factor_id': 'EF002', 'base_amount': 100, 'variance': 30},
    'ACT003': {'name': 'company_cars', 'emission_factor_id': 'EF003', 'base_amount': 150, 'variance': 50},
    'ACT004': {'name': 'delivery_trucks', 'emission_factor_id': 'EF004', 'base_amount': 400, 'variance': 100},
    'ACT005': {'name': 'business_flights_short', 'emission_factor_id': 'EF005', 'base_amount': 800, 'variance': 200},
    'ACT006': {'name': 'business_flights_long', 'emission_factor_id': 'EF006', 'base_amount': 2000, 'variance': 500},
    'ACT007': {'name': 'train_travel', 'emission_factor_id': 'EF007', 'base_amount': 500, 'variance': 100},
    'ACT008': {'name': 'general_waste', 'emission_factor_id': 'EF008', 'base_amount': 1000, 'variance': 200},
    'ACT009': {'name': 'recycling', 'emission_factor_id': 'EF009', 'base_amount': 800, 'variance': 150},
    'ACT010': {'name': 'water_consumption', 'emission_factor_id': 'EF010', 'base_amount': 100, 'variance': 20}
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

def export_reference_data():
    # Export emission factors
    ef_data = []
    for ef_id, factor in EMISSION_FACTORS.items():
        ef_data.append({
            'emission_factor_id': ef_id,
            'name': factor['name'],
            'scope': factor['scope'],
            'unit': factor['unit'],
            'kg_co2e': factor['kg_co2e'],
            'description': factor['description']
        })
    ef_df = pd.DataFrame(ef_data)
    ef_df.to_csv('emission_factors.csv', index=False)

    # Export activities
    act_data = []
    for act_id, activity in ACTIVITIES.items():
        act_data.append({
            'activity_id': act_id,
            'name': activity['name'],
            'emission_factor_id': activity['emission_factor_id'],
            'base_amount': activity['base_amount'],
            'variance': activity['variance']
        })
    act_df = pd.DataFrame(act_data)
    act_df.to_csv('activities.csv', index=False)

def generate_emissions_data():
    data = []

    # Generate random dates
    dates = [START_DATE + timedelta(days=int(x)) for x in
            np.random.randint(0, (END_DATE - START_DATE).days, N_RECORDS)]

    for _ in range(N_RECORDS):
        facility, city, country = random.choice(FACILITIES)
        activity_id = random.choice(list(ACTIVITIES.keys()))
        activity = ACTIVITIES[activity_id]
        
        # Get corresponding emission factor
        emission_factor = EMISSION_FACTORS[activity['emission_factor_id']]
        
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
            'activity_id': activity_id,
            'activity_name': activity['name'],
            'emission_factor_id': activity['emission_factor_id'],
            'scope': emission_factor['scope'],
            'consumption_value': round(consumption, 2),
            'consumption_unit': emission_factor['unit'],
            'emissions_mt_co2e': round(emissions, 3)
        })

    df = pd.DataFrame(data)
    return df

if __name__ == "__main__":
    # Export reference data first
    export_reference_data()
    print("Exported reference data to emission_factors.csv and activities.csv")
    
    # Generate emissions data
    df = generate_emissions_data()
    output_file = "carbon_emissions_data.csv"
    df.to_csv(output_file, index=False)
    print(f"\nGenerated {len(df)} records of carbon emissions data in {output_file}")
    print("\nSample of the data:")
    print(df.head())
    print("\nSummary statistics:")
    print(df.describe())

