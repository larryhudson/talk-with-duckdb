import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# Set random seed for reproducibility
np.random.seed(42)

# Constants
N_RECORDS = 50000  # Increased to cover 10 years of data
START_DATE = datetime(2014, 1, 1)
END_DATE = datetime(2023, 12, 31)

# Growth parameters
ANNUAL_GROWTH_RATE = 0.15  # 15% annual growth
RANDOM_GROWTH_VARIANCE = 0.05  # 5% random variance in growth

# Emission factors with their CO2 impact in kg
EMISSION_FACTORS = {
    # Electricity by region (Scope 2)
    'EF001': {'name': 'electricity_usa', 'scope': 'Scope 2', 'unit': 'kWh', 'kg_co2e': 0.42, 'description': 'US Grid electricity', 'region': 'USA'},
    'EF002': {'name': 'electricity_eu', 'scope': 'Scope 2', 'unit': 'kWh', 'kg_co2e': 0.23, 'description': 'EU Grid electricity', 'region': 'EU'},
    'EF003': {'name': 'electricity_china', 'scope': 'Scope 2', 'unit': 'kWh', 'kg_co2e': 0.61, 'description': 'China Grid electricity', 'region': 'China'},
    'EF004': {'name': 'electricity_india', 'scope': 'Scope 2', 'unit': 'kWh', 'kg_co2e': 0.82, 'description': 'India Grid electricity', 'region': 'India'},
    
    # Stationary combustion (Scope 1)
    'EF010': {'name': 'natural_gas_therm', 'scope': 'Scope 1', 'unit': 'therms', 'kg_co2e': 5.3, 'description': 'Natural gas burning'},
    'EF011': {'name': 'diesel_generator', 'scope': 'Scope 1', 'unit': 'liters', 'kg_co2e': 2.68, 'description': 'Diesel generator'},
    'EF012': {'name': 'lpg_stationary', 'scope': 'Scope 1', 'unit': 'kg', 'kg_co2e': 2.98, 'description': 'LPG stationary combustion'},
    
    # Vehicle fleet (Scope 1)
    'EF020': {'name': 'gasoline_car', 'scope': 'Scope 1', 'unit': 'liters', 'kg_co2e': 2.31, 'description': 'Passenger car - gasoline'},
    'EF021': {'name': 'diesel_car', 'scope': 'Scope 1', 'unit': 'liters', 'kg_co2e': 2.68, 'description': 'Passenger car - diesel'},
    'EF022': {'name': 'electric_car', 'scope': 'Scope 1', 'unit': 'kWh', 'kg_co2e': 0.0, 'description': 'Electric vehicle charging'},
    'EF023': {'name': 'hybrid_car', 'scope': 'Scope 1', 'unit': 'liters', 'kg_co2e': 2.15, 'description': 'Hybrid vehicle'},
    'EF024': {'name': 'heavy_truck_diesel', 'scope': 'Scope 1', 'unit': 'liters', 'kg_co2e': 2.72, 'description': 'Heavy goods vehicle'},
    
    # Business travel (Scope 3)
    'EF030': {'name': 'flight_domestic', 'scope': 'Scope 3', 'unit': 'km', 'kg_co2e': 0.18, 'description': 'Domestic flights (<500km)'},
    'EF031': {'name': 'flight_short_haul', 'scope': 'Scope 3', 'unit': 'km', 'kg_co2e': 0.15, 'description': 'Short-haul flights (500-1500km)'},
    'EF032': {'name': 'flight_long_haul_economy', 'scope': 'Scope 3', 'unit': 'km', 'kg_co2e': 0.11, 'description': 'Long-haul flights economy'},
    'EF033': {'name': 'flight_long_haul_business', 'scope': 'Scope 3', 'unit': 'km', 'kg_co2e': 0.32, 'description': 'Long-haul flights business'},
    'EF034': {'name': 'rail_travel_electric', 'scope': 'Scope 3', 'unit': 'km', 'kg_co2e': 0.03, 'description': 'Electric train travel'},
    'EF035': {'name': 'rail_travel_diesel', 'scope': 'Scope 3', 'unit': 'km', 'kg_co2e': 0.07, 'description': 'Diesel train travel'},
    
    # Waste management (Scope 3)
    'EF040': {'name': 'waste_landfill_usa', 'scope': 'Scope 3', 'unit': 'kg', 'kg_co2e': 0.58, 'description': 'Landfill waste USA', 'region': 'USA'},
    'EF041': {'name': 'waste_landfill_eu', 'scope': 'Scope 3', 'unit': 'kg', 'kg_co2e': 0.48, 'description': 'Landfill waste EU', 'region': 'EU'},
    'EF042': {'name': 'waste_recycled_paper', 'scope': 'Scope 3', 'unit': 'kg', 'kg_co2e': 0.08, 'description': 'Recycled paper waste'},
    'EF043': {'name': 'waste_recycled_plastic', 'scope': 'Scope 3', 'unit': 'kg', 'kg_co2e': 0.12, 'description': 'Recycled plastic waste'},
    'EF044': {'name': 'waste_incinerated', 'scope': 'Scope 3', 'unit': 'kg', 'kg_co2e': 0.58, 'description': 'Incinerated waste'},
    
    # Water and refrigerants (Scope 3)
    'EF050': {'name': 'water_supply', 'scope': 'Scope 3', 'unit': 'cubic_meters', 'kg_co2e': 0.344, 'description': 'Water supply'},
    'EF051': {'name': 'water_treatment', 'scope': 'Scope 3', 'unit': 'cubic_meters', 'kg_co2e': 0.708, 'description': 'Water treatment'},
    'EF052': {'name': 'refrigerant_r410a', 'scope': 'Scope 1', 'unit': 'kg', 'kg_co2e': 2088.0, 'description': 'R410A refrigerant leakage'},
    'EF053': {'name': 'refrigerant_r134a', 'scope': 'Scope 1', 'unit': 'kg', 'kg_co2e': 1430.0, 'description': 'R134A refrigerant leakage'},
    
    # Manufacturing processes (Scope 1)
    'EF060': {'name': 'steel_production', 'scope': 'Scope 1', 'unit': 'tonnes', 'kg_co2e': 1800.0, 'description': 'Steel manufacturing'},
    'EF061': {'name': 'cement_production', 'scope': 'Scope 1', 'unit': 'tonnes', 'kg_co2e': 900.0, 'description': 'Cement production'},
    'EF062': {'name': 'aluminum_production', 'scope': 'Scope 1', 'unit': 'tonnes', 'kg_co2e': 1600.0, 'description': 'Aluminum production'}
}

# Activities that can generate emissions
ACTIVITIES = {
    'ACT001': {'name': 'office_power_usa', 'emission_factor_id': 'EF001', 'base_amount': 1000, 'variance': 200},
    'ACT002': {'name': 'office_power_eu', 'emission_factor_id': 'EF002', 'base_amount': 1000, 'variance': 200},
    'ACT003': {'name': 'natural_gas_usage', 'emission_factor_id': 'EF010', 'base_amount': 150, 'variance': 50},
    'ACT004': {'name': 'company_vehicles', 'emission_factor_id': 'EF020', 'base_amount': 400, 'variance': 100},
    'ACT005': {'name': 'delivery_fleet', 'emission_factor_id': 'EF024', 'base_amount': 800, 'variance': 200},
    'ACT006': {'name': 'business_flights_domestic', 'emission_factor_id': 'EF030', 'base_amount': 2000, 'variance': 500},
    'ACT007': {'name': 'business_flights_international', 'emission_factor_id': 'EF032', 'base_amount': 5000, 'variance': 1000},
    'ACT008': {'name': 'waste_management_usa', 'emission_factor_id': 'EF040', 'base_amount': 1000, 'variance': 200},
    'ACT009': {'name': 'paper_recycling', 'emission_factor_id': 'EF042', 'base_amount': 800, 'variance': 150},
    'ACT010': {'name': 'water_usage', 'emission_factor_id': 'EF050', 'base_amount': 100, 'variance': 20}
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

def calculate_growth_factor(date):
    """Calculate growth factor based on time progression"""
    years_from_start = (date - START_DATE).days / 365.25
    # Base growth from compound annual growth
    base_growth = (1 + ANNUAL_GROWTH_RATE) ** years_from_start
    # Add random variance
    random_factor = 1 + np.random.uniform(-RANDOM_GROWTH_VARIANCE, RANDOM_GROWTH_VARIANCE)
    return base_growth * random_factor

def generate_emissions_data():
    data = []

    # Generate evenly distributed dates
    date_range = pd.date_range(start=START_DATE, end=END_DATE, periods=N_RECORDS)
    
    for date in date_range:
        # Calculate growth factor for this date
        growth_factor = calculate_growth_factor(date)
        
        facility, city, country = random.choice(FACILITIES)
        activity_id = random.choice(list(ACTIVITIES.keys()))
        activity = ACTIVITIES[activity_id]
        
        # Get corresponding emission factor
        emission_factor = EMISSION_FACTORS[activity['emission_factor_id']]
        
        # Generate consumption based on activity's base amount and variance
        # Apply growth factor to base amount
        adjusted_base = activity['base_amount'] * growth_factor
        adjusted_variance = activity['variance'] * growth_factor
        consumption = np.random.normal(adjusted_base, adjusted_variance)
        consumption = max(0, consumption)  # Ensure no negative values
        
        # Calculate emissions in metric tons CO2e
        emissions = (consumption * emission_factor['kg_co2e']) / 1000  # Convert kg to metric tons

        data.append({
            'date': date,
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

