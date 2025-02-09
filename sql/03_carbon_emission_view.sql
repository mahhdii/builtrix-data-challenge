
DROP VIEW IF EXISTS Monthly_Carbon_Emission;


CREATE VIEW Monthly_Carbon_Emission AS
SELECT
    DATE_TRUNC('month', hour) AS month,
    SUM(renewable_energy * 25) AS renewable_emissions,
    SUM(nonrenewable_energy * 600) AS nonrenewable_emissions,
    SUM(renewable_energy * 25 + nonrenewable_energy * 600) AS total_emissions
FROM Energy_Breakdown
GROUP BY month;
