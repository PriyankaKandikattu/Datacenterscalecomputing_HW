---1
SELECT da.animal_type, COUNT(DISTINCT da.animal_key) AS animal_count
FROM dim_animal da
INNER JOIN fact_table_outcomes fo ON da.animal_key = fo.animal_key
GROUP BY da.animal_type;

---2
SELECT COUNT(animal_key) AS animals_with_multiple_outcomes
FROM (
    SELECT animal_key
    FROM fact_table_outcomes
    GROUP BY animal_key
    HAVING COUNT(*) > 1
) subquery;

--3
SELECT month, COUNT(*) AS outcome_count
FROM dim_datetime
INNER JOIN fact_table_outcomes ON dim_datetime.date_id = fact_table_outcomes.date_id
GROUP BY month
ORDER BY outcome_count DESC
LIMIT 5;

--4-1
SELECT
    CASE
        WHEN CAST(age_years AS INTEGER) < 1 THEN 'Kitten'
        WHEN CAST(age_years AS INTEGER) >= 1 AND CAST(age_years AS INTEGER) <= 10 THEN 'Adult'
        WHEN CAST(age_years AS INTEGER) > 10 THEN 'Senior'
        else 'Other'
    END AS age_group,
    COUNT(*) AS count
FROM dim_animal da
WHERE da.animal_type = 'Cat'
GROUP BY age_group;

--4-2
SELECT
    CASE
        WHEN CAST(da.age_years AS INTEGER) < 1 THEN 'Kitten'
        WHEN CAST(da.age_years AS INTEGER) >= 1 AND CAST(da.age_years AS INTEGER) <= 10 THEN 'Adult'
        WHEN CAST(da.age_years AS INTEGER) > 10 THEN 'Senior'
    END AS age_group,
    COUNT(*) AS count
FROM dim_animal da
JOIN fact_table_outcomes fo ON da.animal_key = fo.animal_key
JOIN dim_outcome_type ot ON fo.outcome_id = ot.outcome_id
WHERE da.animal_type = 'Cat' and 
ot.outcome_type = 'Adopted'
GROUP BY age_group;



---5
SELECT 
    dd.date_,
    COUNT(fo.outcome_key) AS outcomes,
    SUM(COUNT(fo.outcome_key)) OVER (ORDER BY dd.date_) AS cumulative_outcomes
FROM dim_datetime dd
LEFT JOIN fact_table_outcomes fo ON dd.date_id = fo.date_id
GROUP BY dd.date_
ORDER BY dd.date_;


