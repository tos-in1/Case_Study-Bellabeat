# merging of march_april & april_may dataset for easier and faster cleaning
# since they both have the same data structure

CREATE TABLE account-creation-trial.bellabeat_fitbit.merged AS
SELECT *
FROM
  account-creation-trial.bellabeat_fitbit.march_april
UNION ALL
SELECT *
FROM
  account-creation-trial.bellabeat_fitbit.april_may;

# Checking out the whole dataset and total number of rows

SELECT*
FROM
  account-creation-trial.bellabeat_fitbit.merged;

# Removing of irrelevant columns from the table

CREATE OR REPLACE TABLE account-creation-trial.bellabeat_fitbit.merged AS
SELECT *
EXCEPT(TrackerDistance,LoggedActivitiesDistance)
FROM
  account-creation-trial.bellabeat_fitbit.merged;

# Starting to clean dataset proper
# making sure of the right formating, renaming and rounding floats to 1 decimal place

CREATE OR REPLACE TABLE account-creation-trial.bellabeat_fitbit.merged AS 
SELECT
  TRIM(CAST(Id AS STRING)) AS UserId,
  FORMAT_DATE('%Y-%m-%d', CAST(ActivityDate AS DATE)) AS ActivityDate,
  CAST(TotalSteps AS INT64) AS TotalSteps,
  ROUND(CAST(TotalDistance AS FLOAT64), 1) AS TotalDis,
  ROUND(CAST(VeryActiveDistance AS FLOAT64), 1) AS VeryActiveDis,
  ROUND(CAST(ModeratelyActiveDistance AS FLOAT64), 1) AS ModActiveDis,
  ROUND(CAST(LightActiveDistance AS FLOAT64), 1) AS LgtActiveDis,
  ROUND(CAST(SedentaryActiveDistance AS FLOAT64), 1) AS SedentaryActiveDis,
  CAST(VeryActiveMinutes AS INT64) AS VeryActMin,
  CAST(FairlyActiveMinutes AS INT64) AS FairlyActMin,
  CAST(LightlyActiveMinutes AS INT64) AS LightlyActMin,
  CAST(SedentaryMinutes AS INT64) AS SedentaryMin,
  CAST(Calories AS INT64) AS Calories
FROM
  account-creation-trial.bellabeat_fitbit.merged;

# checking of duplicate by qualifying and assigning row number 
# partitioned by activitydate and order based on the total steps taken

SELECT*
FROM(
    SELECT *,
      ROW_NUMBER() OVER(
      PARTITION BY UserId, ActivityDate 
      ORDER BY TotalSteps DESC) AS Row_n
      FROM account-creation-trial.bellabeat_fitbit.merged)
WHERE row_n > 1;

# I have been able to identify duplicates, now the next step is to update 
# the table and remove such duplicates 

CREATE OR REPLACE TABLE account-creation-trial.bellabeat_fitbit.merged AS
SELECT*
FROM(
SELECT *,
  ROW_NUMBER() OVER(
    PARTITION BY UserId, ActivityDate 
    ORDER BY TotalSteps DESC) AS Row_n
FROM account-creation-trial.bellabeat_fitbit.merged)
WHERE row_n = 1;

# It is unrealistic to have 0 step taken in a day and 0 calories recorded.
# Next step is to remove rows of 0 steps and calories

CREATE OR REPLACE TABLE account-creation-trial.bellabeat_fitbit.merged AS
SELECT*
FROM account-creation-trial.bellabeat_fitbit.merged
WHERE TotalSteps > 0 AND Calories > 0;

# checking for null in either the userid, total steps, activityDate or calories
SELECT *
FROM
  account-creation-trial.bellabeat_fitbit.merged
WHERE UserId IS NULL
  OR ActivityDate IS NULL
  OR TotalSteps IS NULL
  OR Calories IS NULL;

# Since there are no null data, then next is to check if the active minutes add up.
# Addition of very, faily, lightly and sedentary min should add up to 24hrs or
# 1440min as it is in this case.
SELECT*
  FROM(
    SELECT *,
      (VeryActMin + FairlyActMin + LightlyActMin + SedentaryMin) AS TotalMin
    FROM
      account-creation-trial.bellabeat_fitbit.merged);

# Deleting row from the table where sedentary minutes is exactly 24hrs with 0 activity
# recorded 

CREATE OR REPLACE TABLE account-creation-trial.bellabeat_fitbit.merged AS
SELECT*
FROM 
  account-creation-trial.bellabeat_fitbit.merged
WHERE
  SedentaryMin < 1440;

# Dropping of row_n column

CREATE OR REPLACE TABLE account-creation-trial.bellabeat_fitbit.merged AS
SELECT *
EXCEPT(Row_n)
FROM
  account-creation-trial.bellabeat_fitbit.merged;

# Adding Day of the week and rearranging column for analysis

CREATE OR REPLACE TABLE account-creation-trial.bellabeat_fitbit.merged AS
SELECT
  UserId,
  ActivityDate,
  FORMAT_DATE('%A', DATE(ActivityDate))AS Day_of_week,
  TotalSteps,
  TotalDis,
  VeryActiveDis,
  ModActiveDis,
  LgtActiveDis,
  SedentaryActiveDis,
  VeryActMin,
  FairlyActMin,
  LightlyActMin,
  SedentaryMin,
  Calories
FROM
  account-creation-trial.bellabeat_fitbit.merged;
