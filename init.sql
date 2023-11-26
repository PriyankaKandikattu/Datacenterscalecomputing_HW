
DROP TABLE IF EXISTS fact_outcomes;
DROP TABLE IF EXISTS dim_animals;
DROP TABLE IF EXISTS dim_dates;
DROP TABLE IF EXISTS dim_outcome_types;





CREATE TABLE if not exists dim_animals (
    animal_key INT PRIMARY KEY,
    animal_id VARCHAR,
    animal_name VARCHAR,
    dob VARCHAR,
    animal_type VARCHAR,
    sterilization_status VARCHAR,
    gender VARCHAR,
    age_years VARCHAR,
    breed VARCHAR,
    color varchar
);


CREATE TABLE if not exists dim_dates (
    date_key INT PRIMARY KEY,
    date_recorded VARCHAR,
    day_of_week VARCHAR,
    month_recorded VARCHAR,
    quarter_recorded VARCHAR,
    year_recorded VARCHAR
);


CREATE TABLE if not exists dim_outcome_types (
    outcome_type_key INT PRIMARY KEY,
    outcome_type VARCHAR
);



CREATE TABLE if not exists fact_outcomes (
    date_key INT REFERENCES dim_dates(date_key),
    animal_key INT REFERENCES dim_animals(animal_key),
    outcome_type_key INT REFERENCES dim_outcome_types(outcome_type_key)
);
