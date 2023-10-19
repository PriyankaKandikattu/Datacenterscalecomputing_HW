DROP TABLE IF exists dim_animal;

CREATE TABLE dim_animal (
    animal_key INT PRIMARY KEY,
    animal_id VARCHAR,
    animal_name VARCHAR,
    date_of_birth DATE,
    animal_type VARCHAR,
    neuter_status VARCHAR,
    sex VARCHAR,
    age_years VARCHAR,
    breed VARCHAR
);

DROP TABLE IF EXISTS dim_datetime;
CREATE TABLE dim_datetime (
    date_id INT PRIMARY KEY,
    date_ DATE,
    day_of_week VARCHAR,
    month VARCHAR,
    quarter VARCHAR,
    year VARCHAR
);

DROP TABLE IF EXISTS dim_outcome_type;
CREATE TABLE dim_outcome_type (
    outcome_id INT PRIMARY KEY,
    outcome_type VARCHAR
);



DROP TABLE IF EXISTS fact_table_outcomes;
CREATE TABLE fact_table_outcomes (
    outcome_key SERIAL PRIMARY KEY,
    date_id INT REFERENCES dim_datetime(date_id),
    animal_key INT REFERENCES dim_animal(animal_key),
    outcome_id INT REFERENCES dim_outcome_type(outcome_id)
);
