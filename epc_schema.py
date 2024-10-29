import polars as pl

schema_cols = {
    'LMK_KEY': 'VARCHAR',
    'ADDRESS1': 'VARCHAR',
    'ADDRESS2': 'VARCHAR',
    'ADDRESS3': 'VARCHAR',
    'POSTCODE': 'VARCHAR',
    'BUILDING_REFERENCE_NUMBER': 'VARCHAR',
    'CURRENT_ENERGY_RATING': 'VARCHAR',
    'POTENTIAL_ENERGY_RATING': 'VARCHAR',
    'CURRENT_ENERGY_EFFICIENCY': 'INTEGER',
    'POTENTIAL_ENERGY_EFFICIENCY': 'INTEGER',
    'PROPERTY_TYPE': 'VARCHAR',
    'BUILT_FORM': 'VARCHAR',
    'INSPECTION_DATE': 'DATE',
    'LOCAL_AUTHORITY': 'VARCHAR',
    'CONSTITUENCY': 'VARCHAR',
    'COUNTY': 'VARCHAR',
    'LODGEMENT_DATE': 'DATE',
    'TRANSACTION_TYPE': 'VARCHAR',
    'ENVIRONMENT_IMPACT_CURRENT': 'INTEGER',
    'ENVIRONMENT_IMPACT_POTENTIAL': 'INTEGER',
    'ENERGY_CONSUMPTION_CURRENT': 'DOUBLE',
    'ENERGY_CONSUMPTION_POTENTIAL': 'DOUBLE',
    'CO2_EMISSIONS_CURRENT': 'DECIMAL(10,2)',
    'CO2_EMISS_CURR_PER_FLOOR_AREA': 'DECIMAL(10,2)',
    'CO2_EMISSIONS_POTENTIAL': 'DECIMAL(10,2)',
    'LIGHTING_COST_CURRENT': 'DOUBLE',
    'LIGHTING_COST_POTENTIAL': 'DOUBLE',
    'HEATING_COST_CURRENT': 'DOUBLE',
    'HEATING_COST_POTENTIAL': 'DOUBLE',
    'HOT_WATER_COST_CURRENT': 'DOUBLE',
    'HOT_WATER_COST_POTENTIAL': 'DOUBLE',
    'TOTAL_FLOOR_AREA': 'DECIMAL(10,2)',
    'ENERGY_TARIFF': 'VARCHAR',
    'MAINS_GAS_FLAG': 'VARCHAR',
    'FLOOR_LEVEL': 'VARCHAR',
    'FLAT_TOP_STOREY': 'VARCHAR',
    'FLAT_STOREY_COUNT': 'DECIMAL(10,2)',
    'MAIN_HEATING_CONTROLS': 'VARCHAR',
    'MULTI_GLAZE_PROPORTION': 'DOUBLE',
    'GLAZED_TYPE': 'VARCHAR',
    'GLAZED_AREA': 'VARCHAR',
    'EXTENSION_COUNT': 'INTEGER',
    'NUMBER_HABITABLE_ROOMS': 'DOUBLE',
    'NUMBER_HEATED_ROOMS': 'DOUBLE',
    'LOW_ENERGY_LIGHTING': 'INTEGER',
    'NUMBER_OPEN_FIREPLACES': 'INTEGER',
    'HOTWATER_DESCRIPTION': 'VARCHAR',
    'HOT_WATER_ENERGY_EFF': 'VARCHAR',
    'HOT_WATER_ENV_EFF': 'VARCHAR',
    'FLOOR_DESCRIPTION': 'VARCHAR',
    'FLOOR_ENERGY_EFF': 'VARCHAR',
    'FLOOR_ENV_EFF': 'VARCHAR',
    'WINDOWS_DESCRIPTION': 'VARCHAR',
    'WINDOWS_ENERGY_EFF': 'VARCHAR',
    'WINDOWS_ENV_EFF': 'VARCHAR',
    'WALLS_DESCRIPTION': 'VARCHAR',
    'WALLS_ENERGY_EFF': 'VARCHAR',
    'WALLS_ENV_EFF': 'VARCHAR',
    'SECONDHEAT_DESCRIPTION': 'VARCHAR',
    'SHEATING_ENERGY_EFF': 'VARCHAR',
    'SHEATING_ENV_EFF': 'VARCHAR',
    'ROOF_DESCRIPTION': 'VARCHAR',
    'ROOF_ENERGY_EFF': 'VARCHAR',
    'ROOF_ENV_EFF': 'VARCHAR',
    'MAINHEAT_DESCRIPTION': 'VARCHAR',
    'MAINHEAT_ENERGY_EFF': 'VARCHAR',
    'MAINHEAT_ENV_EFF': 'VARCHAR',
    'MAINHEATCONT_DESCRIPTION': 'VARCHAR',
    'MAINHEATC_ENERGY_EFF': 'VARCHAR',
    'MAINHEATC_ENV_EFF': 'VARCHAR',
    'LIGHTING_DESCRIPTION': 'VARCHAR',
    'LIGHTING_ENERGY_EFF': 'VARCHAR',
    'LIGHTING_ENV_EFF': 'VARCHAR',
    'MAIN_FUEL': 'VARCHAR',
    'WIND_TURBINE_COUNT': 'DOUBLE',
    'HEAT_LOSS_CORRIDOR': 'VARCHAR',
    'UNHEATED_CORRIDOR_LENGTH': 'DECIMAL(10,2)',
    'FLOOR_HEIGHT': 'DECIMAL(10,2)',
    'PHOTO_SUPPLY': 'DOUBLE',
    'SOLAR_WATER_HEATING_FLAG': 'VARCHAR',
    'MECHANICAL_VENTILATION': 'VARCHAR',
    'PROPERTY_ADDRESS': 'VARCHAR',
    'LOCAL_AUTHORITY_LABEL': 'VARCHAR',
    'CONSTITUENCY_LABEL': 'VARCHAR',
    'POSTTOWN': 'VARCHAR',
    'CONSTRUCTION_AGE_BAND': 'VARCHAR',
    'LODGEMENT_DATETIME': 'TIMESTAMP',
    'TENURE': 'VARCHAR',
    'FIXED_LIGHTING_OUTLETS_COUNT': 'DOUBLE',
    'LOW_ENERGY_FIXED_LIGHT_COUNT': 'DOUBLE',
    'UPRN': 'VARCHAR',
    'UPRN_SOURCE': 'VARCHAR',
    'filename': 'VARCHAR'
}

cols_schema_adjusted_polars = {
 'lmk-key': pl.Utf8,
 'postcode': pl.Utf8,
 'local-authority': pl.Utf8,
 'property-type': pl.Utf8,
 'lodgement-datetime': pl.Utf8,
 'transaction-type': pl.Utf8,
 'tenure': pl.Utf8,
 'mains-gas-flag': pl.Utf8,
 'hot-water-energy-eff': pl.Utf8,
 'windows-description': pl.Utf8,
 'windows-energy-eff': pl.Utf8,
 'walls-description': pl.Utf8,
 'walls-energy-eff': pl.Utf8,
 'roof-description': pl.Utf8,
 'roof-energy-eff': pl.Utf8,
 'mainheat-description': pl.Utf8,
 'mainheat-energy-eff': pl.Utf8,
 'mainheat-env-eff': pl.Utf8,
 'main-heating-controls': pl.Utf8,
 'mainheatcont-description': pl.Utf8,
 'mainheatc-energy-eff': pl.Utf8,
 'main-fuel': pl.Utf8,
 'solar-water-heating-flag': pl.Utf8,
 'construction-age-band': pl.Utf8,
 'current-energy-rating': pl.Utf8,
 'potential-energy-rating': pl.Utf8,
 'current-energy-efficiency': pl.Utf8,
 'potential-energy-efficiency': pl.Utf8,
 'built-form': pl.Utf8,
 'constituency': pl.Utf8,
 'floor-description': pl.Utf8,
 'environment-impact-current': pl.Int64,
 'environment-impact-potential': pl.Int64,
 'energy-consumption-current': pl.Int64,
 'energy-consumption-potential': pl.Int64,
 'co2-emiss-curr-per-floor-area': pl.Int64,
 'co2-emissions-current': pl.Float64,
 'co2-emissions-potential': pl.Float64,
 'lighting-cost-current': pl.Int64,
 'lighting-cost-potential': pl.Int64,
 'heating-cost-current': pl.Int64,
 'heating-cost-potential': pl.Int64,
 'hot-water-cost-current': pl.Int64,
 'hot-water-cost-potential': pl.Int64,
 'total-floor-area': pl.Float64,
 'number-habitable-rooms': pl.Int64,
 'number-heated-rooms': pl.Int64,
 'photo-supply': pl.Float64,
 'uprn': pl.Int64,
 'building-reference-number': pl.Int64}

cols_schema_nondom_polars = {
    'LMK_KEY': pl.Utf8,
    'POSTCODE': pl.Utf8,
    'BUILDING_REFERENCE_NUMBER': pl.Int64,
    'ASSET_RATING': pl.Int64,
    'ASSET_RATING_BAND': pl.Utf8,
    'PROPERTY_TYPE': pl.Utf8,
    'LOCAL_AUTHORITY': pl.Utf8,
    'CONSTITUENCY': pl.Utf8,
    'TRANSACTION_TYPE': pl.Utf8,
    'STANDARD_EMISSIONS': pl.Float64,
    'TYPICAL_EMISSIONS': pl.Float64,
    'TARGET_EMISSIONS': pl.Float64,
    'BUILDING_EMISSIONS': pl.Float64,
    'BUILDING_LEVEL': pl.Int64,
    'RENEWABLE_SOURCES': pl.Utf8,
    'LODGEMENT_DATETIME': pl.Utf8,
    'UPRN': pl.Utf8
    }

all_cols_polars = {
    'lmk-key': pl.Utf8,
    'address1': pl.Utf8,
    'address2': pl.Utf8,
    'address3': pl.Utf8,
    'postcode': pl.Utf8,
    'building-reference-number': pl.Utf8,
    'current-energy-rating': pl.Utf8,
    'potential-energy-rating': pl.Utf8,
    'current-energy-efficiency': pl.Int32,
    'potential-energy-efficiency': pl.Int32,
    'property-type': pl.Utf8,
    'built-form': pl.Utf8,
    'inspection-date': pl.Date,
    'local-authority': pl.Utf8,
    'constituency': pl.Utf8,
    'county': pl.Utf8,
    'lodgement-date': pl.Date,
    'transaction-type': pl.Utf8,
    'environment-impact-current': pl.Int32,
    'environment-impact-potential': pl.Int32,
    'energy-consumption-current': pl.Float64,
    'energy-consumption-potential': pl.Float64,
    'co2-emissions-current': pl.Float64,
    'co2-emiss-curr-per-floor-area': pl.Float64,
    'co2-emissions-potential': pl.Float64,
    'lighting-cost-current': pl.Float64,
    'lighting-cost-potential': pl.Float64,
    'heating-cost-current': pl.Float64,
    'heating-cost-potential': pl.Float64,
    'hot-water-cost-current': pl.Float64,
    'hot-water-cost-potential': pl.Float64,
    'total-floor-area': pl.Float64,
    'energy-tariff': pl.Utf8,
    'mains-gas-flag': pl.Utf8,
    'floor-level': pl.Utf8,
    'flat-top-storey': pl.Utf8,
    'flat-storey-count': pl.Float64,
    'main-heating-controls': pl.Utf8,
    'multi-glaze-proportion': pl.Float64,
    'glazed-type': pl.Utf8,
    'glazed-area': pl.Utf8,
    'extension-count': pl.Int32,
    'number-habitable-rooms': pl.Float64,
    'number-heated-rooms': pl.Float64,
    'low-energy-lighting': pl.Int32,
    'number-open-fireplaces': pl.Int32,
    'hotwater-description': pl.Utf8,
    'hot-water-energy-eff': pl.Utf8,
    'hot-water-env-eff': pl.Utf8,
    'floor-description': pl.Utf8,
    'floor-energy-eff': pl.Utf8,
    'floor-env-eff': pl.Utf8,
    'windows-description': pl.Utf8,
    'windows-energy-eff': pl.Utf8,
    'windows-env-eff': pl.Utf8,
    'walls-description': pl.Utf8,
    'walls-energy-eff': pl.Utf8,
    'walls-env-eff': pl.Utf8,
    'secondheat-description': pl.Utf8,
    'sheating-energy-eff': pl.Utf8,
    'sheating-env-eff': pl.Utf8,
    'roof-description': pl.Utf8,
    'roof-energy-eff': pl.Utf8,
    'roof-env-eff': pl.Utf8,
    'mainheat-description': pl.Utf8,
    'mainheat-energy-eff': pl.Utf8,
    'mainheat-env-eff': pl.Utf8,
    'mainheatcont-description': pl.Utf8,
    'mainheatc-energy-eff': pl.Utf8,
    'mainheatc-env-eff': pl.Utf8,
    'lighting-description': pl.Utf8,
    'lighting-energy-eff': pl.Utf8,
    'lighting-env-eff': pl.Utf8,
    'main-fuel': pl.Utf8,
    'wind-turbine-count': pl.Float64,
    'heat-loss-corridor': pl.Utf8,
    'unheated-corridor-length': pl.Float64,
    'floor-height': pl.Float64,
    'photo-supply': pl.Float64,
    'solar-water-heating-flag': pl.Utf8,
    'mechanical-ventilation': pl.Utf8,
    'property-address': pl.Utf8,
    'local-authority-label': pl.Utf8,
    'constituency-label': pl.Utf8,
    'posttown': pl.Utf8,
    'construction-age-band': pl.Utf8,
    'lodgement-datetime': pl.Datetime,
    'tenure': pl.Utf8,
    'fixed-lighting-outlets-count': pl.Float64,
    'low-energy-fixed-light-count': pl.Float64,
    'uprn': pl.Utf8,
    'uprn-source': pl.Utf8
}