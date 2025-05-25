# BELLABEAT_FITBIT ANALYSIS

# QUESTIONS FOR DATA EXPLORATION

# 1. What is the percentage of activity distribution per day?

# 2. Whats the daily average steps, calories burnt and active minutes?

# 3. What is the correlation between calories, total steps and active minutes?

# 4. What days of the week are users most and least active?

# 5. What is the average step, activemin and burned calories by weektype

# EXPLORATION
SELECT*
FROM
  account-creation-trial.bellabeat_fitbit.merged;

# THE BIG PICTURE
# 1. What is the percentage of activity distribution per day?

SELECT
  ROUND(((ActiveMin/1440) * 100), 1) AS ActiveMin,
  ROUND(((SedentaryMin/1440) * 100), 1) AS SedentaryMin,
  ROUND(100 - ((ActiveMin + SedentaryMin) / 1440) * 100, 1) AS UnaccountedMin
FROM(
    SELECT
      AVG(FairlyActMin + LightlyActMin + VeryActMin) AS ActiveMin,
      AVG(SedentaryMin) AS SedentaryMin
    FROM
      account-creation-trial.bellabeat_fitbit.merged);

# 2. What is the average number of calories burned, steps and activemin per day

SELECT
  CAST(AVG(Calories) AS INT64) AS AvgCalories,
  CAST(AVG(TotalSteps) AS INT64) AS AvgSteps,
  CAST(AVG((FairlyActMin + LightlyActMin + VeryActMin)) AS INT64) AS AvgActiceMin
FROM
  account-creation-trial.bellabeat_fitbit.merged;


# What is the average,minimum and maximum number of steps users take per day?

SELECT 
  CAST(AVG(TotalSteps) AS INT64) AS AvgSteps,
  MIN(TotalSteps) AS MinSteps,
  MAX(TotalSteps) AS MaxSteps
FROM
  account-creation-trial.bellabeat_fitbit.merged;


# 3. What is the correlation between calories, total steps and active minutes?
# Take to tableau for Vizz

SELECT
  Calories,
  TotalSteps,
  (FairlyActMin + LightlyActMin + VeryActMin) AS ActiveMin
FROM
  account-creation-trial.bellabeat_fitbit.merged;

# 4. What days of the week are users most and least active?

SELECT
  Day_of_week,
  CAST(AVG(Calories) AS INT64) AS AvgCalories,
  CAST(AVG(TotalSteps) AS INT64) AS AvgSteps,
  CAST(AVG((FairlyActMin + LightlyActMin + VeryActMin)) AS INT64) AS AvgActiceMin
FROM
  account-creation-trial.bellabeat_fitbit.merged
GROUP BY 
  Day_of_week
ORDER BY
   CASE 
    WHEN Day_of_week = 'Monday' THEN 1
    WHEN Day_of_week = 'Tuesday' THEN 2
    WHEN Day_of_week = 'Wednesday' THEN 3
    WHEN Day_of_week = 'Thursday' THEN 4
    WHEN Day_of_week = 'Friday' THEN 5
    WHEN Day_of_week = 'Saturday' THEN 6
    WHEN Day_of_week = 'Sunday' THEN 7
  END;

# 5. When in the week are users most and least active

SELECT
  CASE
    WHEN Day_of_week IN ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday') THEN 'Weekday'
    WHEN Day_of_week IN ('Saturday', 'Sunday') THEN 'Weekend'
  END AS Daytype,
  CAST(AVG(Calories) AS INT64) AS AvgCalories,
  CAST(AVG(TotalSteps) AS INT64) AS AvgSteps,
  CAST(AVG((FairlyActMin + LightlyActMin + VeryActMin)) AS INT64) AS AvgActiveMin
FROM
  account-creation-trial.bellabeat_fitbit.merged
GROUP BY
  Daytype;
