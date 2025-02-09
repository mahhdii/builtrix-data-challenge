

DROP TABLE IF EXISTS Power_Table;
DROP TABLE IF EXISTS CFP_Table;
DROP TABLE IF EXISTS Temperature_Table;


CREATE TABLE Power_Table (
    timestamp TIMESTAMP PRIMARY KEY,
    HVAC_Power_kW FLOAT,
    time_diff FLOAT
);


CREATE TABLE CFP_Table (
    timestamp TIMESTAMP PRIMARY KEY,
    renewable_biomass FLOAT,
    renewable_hydro FLOAT,
    renewable_solar FLOAT,
    renewable_wind FLOAT,
    renewable_geothermal FLOAT,
    renewable_otherrenewable FLOAT,
    renewable FLOAT,
    nonrenewable_coal FLOAT,
    nonrenewable_gas FLOAT,
    nonrenewable_nuclear FLOAT,
    nonrenewable_oil FLOAT,
    nonrenewable FLOAT,
    hydropumpedstorage FLOAT,
    unknown FLOAT,
    region_id FLOAT,
    country_id FLOAT,
    month FLOAT,  -- Change to FLOAT
    day FLOAT,    -- Change to FLOAT
    time_diff FLOAT
);



CREATE TABLE Temperature_Table (
    timestamp TIMESTAMP PRIMARY KEY,
    time_diff FLOAT,
    AHU_01_FreshAir_Temp_C FLOAT,
    AHU_01_SupplyAir_Temp_C FLOAT,
    AHU_01_Exhaust_Temp_C FLOAT,
    AHU_02_FreshAir_Temp_C FLOAT,
    AHU_02_SupplyAir_Temp_C FLOAT,
    AHU_02_Exhaust_Temp_C FLOAT,

    AHU_N01_FreshAir_Temp_C FLOAT,
    AHU_N01_SupplyAir_Temp_C FLOAT,
    AHU_N02_FreshAir_Temp_C FLOAT,
    AHU_N02_SupplyAir_Temp_C FLOAT,
    AHU_N03_FreshAir_Temp_C FLOAT,
    AHU_N03_SupplyAir_Temp_C FLOAT
);
