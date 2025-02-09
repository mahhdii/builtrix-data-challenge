
DROP TABLE IF EXISTS Energy_Breakdown;
CREATE TABLE Energy_Consumption AS
SELECT
    DATE_TRUNC('hour', timestamp) AS hour,
    AVG(HVAC_Power_kW) AS avg_power_kWh
FROM power_table
GROUP BY hour;




DROP TABLE IF EXISTS Energy_Breakdown;

CREATE TABLE Energy_Breakdown AS
SELECT
    ec.hour,
    ec.avg_power_kWh,
    cfp.renewable,
    cfp.nonrenewable,
    (ec.avg_power_kWh * cfp.renewable) AS renewable_energy,
    (ec.avg_power_kWh * cfp.nonrenewable) AS nonrenewable_energy
FROM Energy_Consumption ec
JOIN cfp_table cfp
ON ec.hour = cfp.timestamp;

