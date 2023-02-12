    
SELECT DISTINCT athlete.id AS athlete_id,
    --date/time
    gmt_local_time_difference.local_date,
    extract(dow from gmt_local_time_difference.local_date) AS local_date_dow,
    extract(epoch from gmt_local_time_difference.gmt_local_difference)/3600 AS gmt_offset,
    --nullfill((array_agg(timezones.timezone ORDER BY start_date_local DESC))[1]) OVER (ORDER BY gmt_local_time_difference.local_date ASC)  AS timezone, --timezone of the day's last activity
    --gc_wellness stress
    round(aggr_gc_original_wellness_stress_tracking.gc_well_avg_daytime_stress,1) AS gc_well_avg_daytime_stress,
    ROUND(ema(coalesce(gc_well_avg_daytime_stress::numeric, 0), 0.3333) over (order by gmt_local_time_difference.local_date asc),2) as gc_well_avg_daytime_stress_3d_ema,
    round(aggr_gc_original_wellness_stress_tracking.gc_well_morning_stress,1) AS gc_well_morning_stress,
    round(aggr_gc_original_wellness_stress_tracking.gc_well_midday_stress,1) AS gc_well_midday_stress,
    round(aggr_gc_original_wellness_stress_tracking.gc_well_evn_stress,1) AS gc_well_evn_stress,
    round(aggr_gc_original_wellness_stress_tracking.gc_well_10pm_stress,1) AS gc_well_10pm_stress,
    ROUND(ema(coalesce(gc_well_evn_stress::numeric, 0), 0.3333) over (order by gmt_local_time_difference.local_date asc),2) as gc_well_evn_stress_3d_ema,
    --gc_wellness hr
    round(aggr_gc_original_wellness_hr_tracking.gc_well_avg_daytime_hr,0) AS gc_well_avg_daytime_hr,
    ROUND(ema(coalesce(gc_well_avg_daytime_hr::numeric, 0), 0.3333) over (order by gmt_local_time_difference.local_date asc),2) as gc_well_avg_daytime_hr_3d_ema,
    round(aggr_gc_original_wellness_hr_tracking.gc_well_morning_hr,0) AS gc_well_morning_hr,
    round(aggr_gc_original_wellness_hr_tracking.gc_well_midday_hr,0) AS gc_well_midday_hr,
    round(aggr_gc_original_wellness_hr_tracking.gc_well_evn_hr,0) AS gc_well_evn_hr,
    ROUND(ema(coalesce(gc_well_evn_hr::numeric, 0), 0.3333) over (order by gmt_local_time_difference.local_date asc),2) as gc_well_evn_hr_3d_ema,
    aggr_gc_original_wellness_hr_tracking.gc_well_min_daytime_hr,
    round(aggr_gc_original_wellness_hr_tracking.gc_well_sleep_hr,1) AS gc_well_sleep_hr,
    --gc_wellness daily summary
    aggr_garmin_connect_wellness.wellness_resting_heart_rate,
    aggr_garmin_connect_wellness.wellness_total_steps,
    aggr_garmin_connect_wellness.wellness_moderate_intensity_minutes,
    aggr_garmin_connect_wellness.wellness_vigorous_intensity_minutes,
    --activity
    COALESCE(aggr_strava_activity_summary.act_daily_activities,0) AS act_daily_activities,
    
    nullfill(aggr_strava_activity_summary.act_last_alt) OVER (ORDER BY gmt_local_time_difference.local_date ASC) AS act_last_alt,--if null use previous day value
    ROUND(nullfill(aggr_strava_activity_summary.act_last_alt_ema) OVER (ORDER BY gmt_local_time_difference.local_date ASC),2) AS act_last_alt_ema,--if null use previous day value
    
    nullfill(aggr_strava_activity_summary.act_last_long) OVER (ORDER BY gmt_local_time_difference.local_date ASC) AS act_last_long,--if null use previous day value
    nullfill(aggr_strava_activity_summary.act_last_lat) OVER (ORDER BY gmt_local_time_difference.local_date ASC) AS act_last_lat,--if null use previous day value
    COALESCE(aggr_strava_activity_summary.act_daily_suffer_score,0) AS act_daily_suffer_score,
    COALESCE(aggr_strava_activity_summary.act_evn_suffer_score,0) AS act_evn_suffer_score,
    COALESCE(aggr_strava_activity_summary.act_daily_elapsed_time,0) AS act_daily_elapsed_time,
    COALESCE(aggr_strava_activity_summary.act_evn_elapsed_time,0) AS act_evn_elapsed_time,
    COALESCE(aggr_strava_activity_summary.act_type_ride,'0') AS act_type_ride,
    COALESCE(aggr_strava_activity_summary.act_type_run,'0') AS act_type_run,
    COALESCE(aggr_strava_activity_summary.act_type_swim,'0') AS act_type_swim,
    COALESCE(aggr_strava_activity_summary.act_type_ski,'0') AS act_type_ski,
    ROUND(ema(coalesce(act_daily_suffer_score::numeric, 0), 0.1428) over (order by gmt_local_time_difference.local_date asc),2) as act_pmc_atl,
    ROUND(ema(coalesce(act_daily_suffer_score::numeric, 0), 0.0238) over (order by gmt_local_time_difference.local_date asc),2) as act_pmc_ctl,
    ROUND(ema(coalesce(act_daily_suffer_score::numeric, 0), 0.3333) over (order by gmt_local_time_difference.local_date asc),2) as act_pmc_3d_ema,
    ROUND(ema(coalesce(act_daily_suffer_score::numeric, 0), 0.0238) over (order by gmt_local_time_difference.local_date asc),2) 
    - ROUND(ema(coalesce(act_daily_suffer_score::numeric, 0), 0.1428) over (order by gmt_local_time_difference.local_date asc),2) as act_pmc_tsb,
    --nutrition
    --COALESCE(aggr_mfp_nutrition.nutr_evn_food_items,'None') AS nutr_evn_food_items,
    aggr_mfp_nutrition.nutr_daily_calories,
    aggr_mfp_nutrition.nutr_daily_carbohydrates,
    aggr_mfp_nutrition.nutr_daily_protein,
    aggr_mfp_nutrition.nutr_daily_fat,
    aggr_mfp_nutrition.nutr_daily_fiber,
    aggr_mfp_nutrition.nutr_daily_sugar,
    ROUND(ema(coalesce(nutr_daily_fiber::numeric, 0), 0.3333) over (order by gmt_local_time_difference.local_date asc),2) as nutr_daily_fiber_3d_ema,
    ROUND(ema(coalesce(nutr_daily_caffeine::numeric, 0), 0.3333) over (order by gmt_local_time_difference.local_date asc),2) as nutr_daily_caffeine_3d_ema,
    ROUND(ema(coalesce(nutr_daily_alcohol::numeric, 0), 0.3333) over (order by gmt_local_time_difference.local_date asc),2) as nutr_daily_alcohol_3d_ema,
    ROUND(ema(coalesce(nutr_daily_protein::numeric, 0), 0.3333) over (order by gmt_local_time_difference.local_date asc),2) as nutr_daily_pretein_3d_ema,
    COALESCE(aggr_mfp_nutrition.nutr_morning_calories,0) AS nutr_morning_calories,
    COALESCE(aggr_mfp_nutrition.nutr_midday_calories,0) AS nutr_midday_calories,
    COALESCE(aggr_mfp_nutrition.nutr_evn_calories,0) AS nutr_evn_calories,
    COALESCE(aggr_mfp_nutrition.nutr_morning_carbohydrates,0) AS nutr_morning_carbohydrates,
    COALESCE(aggr_mfp_nutrition.nutr_midday_carbohydrates,0) AS nutr_midday_carbohydrates,
    COALESCE(aggr_mfp_nutrition.nutr_evn_carbohydrates,0) AS nutr_evn_carbohydrates,
    COALESCE(aggr_mfp_nutrition.nutr_morning_protein,0) AS nutr_morning_protein,
    COALESCE(aggr_mfp_nutrition.nutr_midday_protein,0) AS nutr_midday_protein,
    COALESCE(aggr_mfp_nutrition.nutr_evn_protein,0) AS nutr_evn_protein,
    COALESCE(aggr_mfp_nutrition.nutr_morning_fat,0) AS nutr_morning_fat,
    COALESCE(aggr_mfp_nutrition.nutr_midday_fat,0) AS nutr_midday_fat,
    COALESCE(aggr_mfp_nutrition.nutr_evn_fat,0) AS nutr_evn_fat,
    COALESCE(aggr_mfp_nutrition.nutr_morning_fiber,0) AS nutr_morning_fiber,
    COALESCE(aggr_mfp_nutrition.nutr_midday_fiber,0) AS nutr_midday_fiber,
    COALESCE(aggr_mfp_nutrition.nutr_evn_fiber,0) AS nutr_evn_fiber,
    COALESCE(aggr_mfp_nutrition.nutr_morning_sugar,0) AS nutr_morning_sugar,
    COALESCE(aggr_mfp_nutrition.nutr_midday_sugar,0) AS nutr_midday_sugar,
    COALESCE(aggr_mfp_nutrition.nutr_evn_sugar,0) AS nutr_evn_sugar,
    COALESCE(aggr_mfp_nutrition.nutr_daily_caffeine,0) AS nutr_daily_caffeine,
    COALESCE(aggr_mfp_nutrition.nutr_evn_caffeine,0) AS nutr_evn_caffeine,
    COALESCE(aggr_mfp_nutrition.nutr_daily_alcohol,0) AS nutr_daily_alcohol,
    COALESCE(aggr_mfp_nutrition.nutr_evn_alcohol,0) AS nutr_evn_alcohol,
    --weather
    nullfill(aggr_weather.weather_min_temperature) OVER (ORDER BY gmt_local_time_difference.local_date ASC) AS weather_min_temperature,--if null use previous day value
    nullfill(aggr_weather.weather_max_temperature) OVER (ORDER BY gmt_local_time_difference.local_date ASC) AS weather_max_temperature,--if null use previous day value
    nullfill(aggr_weather.weather_avg_temperature) OVER (ORDER BY gmt_local_time_difference.local_date ASC) AS weather_avg_temperature,--if null use previous day value
    nullfill(aggr_weather.weather_avg_dew_point) OVER (ORDER BY gmt_local_time_difference.local_date ASC) AS weather_avg_dew_point,--if null use previous day value
    nullfill(aggr_weather.weather_avg_humidity) OVER (ORDER BY gmt_local_time_difference.local_date ASC) AS weather_avg_humidity,--if null use previous day value
    nullfill(aggr_weather.weather_avg_precipitation) OVER (ORDER BY gmt_local_time_difference.local_date ASC) AS weather_avg_precipitation,--if null use previous day value
    nullfill(aggr_weather.weather_avg_wind_direction) OVER (ORDER BY gmt_local_time_difference.local_date ASC) AS weather_avg_wind_direction,--if null use previous day value
    nullfill(aggr_weather.weather_avg_wind_speed) OVER (ORDER BY gmt_local_time_difference.local_date ASC) AS weather_avg_wind_speed,--if null use previous day value
    nullfill(aggr_weather.weather_avg_air_pressure) OVER (ORDER BY gmt_local_time_difference.local_date ASC) AS weather_avg_air_pressure,--if null use previous day value
    nullfill(aggr_weather.weather_stddev_air_pressure) OVER (ORDER BY gmt_local_time_difference.local_date ASC) AS weather_stddev_air_pressure,--if null use previous day value
    --body composition
    nullfill(aggr_garmin_connect_body_composition.body_water) OVER (ORDER BY gmt_local_time_difference.local_date ASC) AS body_water,
    nullfill(aggr_garmin_connect_body_composition.weight_gm) OVER (ORDER BY gmt_local_time_difference.local_date ASC) AS weight_gm,
    nullfill(aggr_garmin_connect_body_composition.body_mass_index) OVER (ORDER BY gmt_local_time_difference.local_date ASC) AS body_mass_index,
    --oura_sleep
    lead(coalesce(aggr_oura_sleep_daily_summary.oura_sleep_rmssd,1)) over (order by local_date desc) AS oura_sleep_rmssd_yest,
    ROUND(ema(coalesce(aggr_oura_sleep_daily_summary.oura_sleep_rmssd::numeric, 0), 0.3333) over (order by gmt_local_time_difference.local_date asc),2) as oura_sleep_rmssd_3d_ema,
    ROUND(ema(coalesce(aggr_oura_sleep_daily_summary.oura_sleep_rmssd::numeric, 0), 0.1428) over (order by gmt_local_time_difference.local_date asc),2) as oura_sleep_rmssd_7d_ema,
    ROUND(ema(coalesce(aggr_oura_sleep_daily_summary.oura_sleep_rmssd::numeric, 0), 0.0238) over (order by gmt_local_time_difference.local_date asc),2) as oura_sleep_rmssd_42d_ema,
    lead(coalesce(aggr_oura_sleep_daily_summary.oura_sleep_duration,1)) over (order by local_date desc) AS oura_sleep_duration_yest,
    lead(coalesce(aggr_oura_sleep_daily_summary.oura_sleep_awake,1)) over (order by local_date desc) AS oura_sleep_awake_yest,
    lead(coalesce(aggr_oura_sleep_daily_summary.oura_sleep_light,1)) over (order by local_date desc) AS oura_sleep_light_yest,
    lead(coalesce(aggr_oura_sleep_daily_summary.oura_sleep_rem,1)) over (order by local_date desc) AS oura_sleep_rem_yest,
    lead(coalesce(aggr_oura_sleep_daily_summary.oura_sleep_deep,1)) over (order by local_date desc) AS oura_sleep_deep_yest,
    lead(coalesce(aggr_oura_sleep_daily_summary.oura_sleep_breath_average,1)) over (order by local_date desc) AS oura_sleep_breath_average_yest,
    lead(coalesce(aggr_oura_sleep_daily_summary.oura_sleep_temperature_deviation,1)) over (order by local_date desc) AS oura_sleep_temperature_deviation_yest,
    aggr_oura_sleep_daily_summary.oura_sleep_rmssd AS oura_sleep_rmssd,
    round(aggr_oura_sleep_detail.oura_sleep_morning_rmssd,0) AS oura_sleep_morning_rmssd,
    round(aggr_oura_sleep_daily_summary.oura_sleep_rmssd,-1)::text AS oura_sleep_rmssd_class,
    aggr_oura_sleep_daily_summary.oura_sleep_rmssd_baseline,
    aggr_oura_sleep_daily_summary.oura_sleep_rmssd_dev,
    sign(aggr_oura_sleep_daily_summary.oura_sleep_rmssd_dev) AS oura_sleep_dev_sign
FROM athlete
LEFT JOIN gmt_local_time_difference ON gmt_local_time_difference.athlete_id = athlete.id
LEFT JOIN timezones ON timezones.timestamp_local::date = gmt_local_time_difference.local_date
LEFT JOIN (SELECT 
           garmin_connect_wellness.calendar_date::date AS calendar_date_local,
           MAX(garmin_connect_wellness.wellness_resting_heart_rate) AS wellness_resting_heart_rate,
           MAX(garmin_connect_wellness.wellness_total_steps) AS wellness_total_steps,
           MAX(garmin_connect_wellness.wellness_moderate_intensity_minutes) AS wellness_moderate_intensity_minutes,
           MAX(garmin_connect_wellness.wellness_vigorous_intensity_minutes) AS wellness_vigorous_intensity_minutes
           FROM garmin_connect_wellness
           GROUP BY (garmin_connect_wellness.calendar_date::date)) aggr_garmin_connect_wellness ON aggr_garmin_connect_wellness.calendar_date_local = gmt_local_time_difference.local_date
LEFT JOIN (SELECT DISTINCT 
           (gc_original_wellness_stress_tracking.stress_level_time::timestamp+gmt_local_time_difference.gmt_local_difference)::date AS stress_level_local_date,
           AVG(gc_original_wellness_stress_tracking.stress_level_value) 
               FILTER (WHERE gc_original_wellness_stress_tracking.stress_level_time::time+gmt_local_time_difference.gmt_local_difference  > '07:00:00'::time
                             AND gc_original_wellness_stress_tracking.stress_level_time::time+gmt_local_time_difference.gmt_local_difference  < '22:00:00'::time 
                             AND gc_original_wellness_stress_tracking.stress_level_value > 0
                      ) AS gc_well_avg_daytime_stress,
           AVG(gc_original_wellness_stress_tracking.stress_level_value) 
               FILTER (WHERE gc_original_wellness_stress_tracking.stress_level_time::time+gmt_local_time_difference.gmt_local_difference  > '07:00:00'::time
                             AND gc_original_wellness_stress_tracking.stress_level_time::time+gmt_local_time_difference.gmt_local_difference  < '12:00:00'::time 
                             AND gc_original_wellness_stress_tracking.stress_level_value > 0
                      ) AS gc_well_morning_stress,
           AVG(gc_original_wellness_stress_tracking.stress_level_value) 
               FILTER (WHERE gc_original_wellness_stress_tracking.stress_level_time::time+gmt_local_time_difference.gmt_local_difference  > '12:00:00'::time
                             AND gc_original_wellness_stress_tracking.stress_level_time::time+gmt_local_time_difference.gmt_local_difference  < '17:00:00'::time 
                             AND gc_original_wellness_stress_tracking.stress_level_value > 0
                      ) AS gc_well_midday_stress,
           AVG(gc_original_wellness_stress_tracking.stress_level_value) 
               FILTER (WHERE gc_original_wellness_stress_tracking.stress_level_time::time+gmt_local_time_difference.gmt_local_difference  > '17:00:00'::time
                             AND gc_original_wellness_stress_tracking.stress_level_time::time+gmt_local_time_difference.gmt_local_difference  < '22:00:00'::time 
                             AND gc_original_wellness_stress_tracking.stress_level_value > 0
                      ) AS gc_well_evn_stress,
           AVG(gc_original_wellness_stress_tracking.stress_level_value) 
               FILTER (WHERE gc_original_wellness_stress_tracking.stress_level_time::time+gmt_local_time_difference.gmt_local_difference  > '21:30:00'::time
                             AND gc_original_wellness_stress_tracking.stress_level_time::time+gmt_local_time_difference.gmt_local_difference  < '22:00:00'::time 
                             AND gc_original_wellness_stress_tracking.stress_level_value > 0
                      ) AS gc_well_10pm_stress
           FROM athlete
           LEFT JOIN gmt_local_time_difference ON gmt_local_time_difference.athlete_id = athlete.id
           LEFT JOIN gc_original_wellness_stress_tracking ON gc_original_wellness_stress_tracking.stress_level_time::date = gmt_local_time_difference.local_date
           GROUP BY 
               (gc_original_wellness_stress_tracking.stress_level_time::timestamp+gmt_local_time_difference.gmt_local_difference)::date 
           ) aggr_gc_original_wellness_stress_tracking ON aggr_gc_original_wellness_stress_tracking.stress_level_local_date = gmt_local_time_difference.local_date
LEFT JOIN (SELECT DISTINCT 
           (gc_original_wellness_hr_tracking.timestamp::timestamp+gmt_local_time_difference.gmt_local_difference)::date AS hr_timestamp_local_date,
           AVG(gc_original_wellness_hr_tracking.heart_rate) 
               FILTER (WHERE gc_original_wellness_hr_tracking.timestamp::time+gmt_local_time_difference.gmt_local_difference > '07:00:00'::time
                             AND gc_original_wellness_hr_tracking.timestamp::time+gmt_local_time_difference.gmt_local_difference < '22:00:00'::time 
                             AND gc_original_wellness_hr_tracking.heart_rate > 0
                       ) AS gc_well_avg_daytime_hr,
           AVG(gc_original_wellness_hr_tracking.heart_rate) 
               FILTER (WHERE gc_original_wellness_hr_tracking.timestamp::time+gmt_local_time_difference.gmt_local_difference > '07:00:00'::time
                             AND gc_original_wellness_hr_tracking.timestamp::time+gmt_local_time_difference.gmt_local_difference < '12:00:00'::time 
                             AND gc_original_wellness_hr_tracking.heart_rate > 0
                      ) AS gc_well_morning_hr,
           AVG(gc_original_wellness_hr_tracking.heart_rate) 
               FILTER (WHERE gc_original_wellness_hr_tracking.timestamp::time+gmt_local_time_difference.gmt_local_difference > '12:00:00'::time
                             AND gc_original_wellness_hr_tracking.timestamp::time+gmt_local_time_difference.gmt_local_difference < '17:00:00'::time 
                             AND gc_original_wellness_hr_tracking.heart_rate > 0
                      ) AS gc_well_midday_hr,
           AVG(gc_original_wellness_hr_tracking.heart_rate) 
               FILTER (WHERE gc_original_wellness_hr_tracking.timestamp::time+gmt_local_time_difference.gmt_local_difference > '17:00:00'::time
                             AND gc_original_wellness_hr_tracking.timestamp::time+gmt_local_time_difference.gmt_local_difference  < '22:00:00'::time 
                             AND gc_original_wellness_hr_tracking.heart_rate > 0
                      ) AS gc_well_evn_hr,
           MIN(gc_original_wellness_hr_tracking.heart_rate) 
               FILTER (WHERE gc_original_wellness_hr_tracking.timestamp::time+gmt_local_time_difference.gmt_local_difference > '07:00:00'::time
                             AND gc_original_wellness_hr_tracking.timestamp::time+gmt_local_time_difference.gmt_local_difference < '22:00:00'::time 
                             AND gc_original_wellness_hr_tracking.heart_rate > 0
                      ) AS gc_well_min_daytime_hr,
           AVG(gc_original_wellness_hr_tracking.heart_rate) 
               FILTER (WHERE gc_original_wellness_hr_tracking.timestamp::time+gmt_local_time_difference.gmt_local_difference > '00:00:00'::time
                             AND gc_original_wellness_hr_tracking.timestamp::time+gmt_local_time_difference.gmt_local_difference  < '06:00:00'::time 
                             AND gc_original_wellness_hr_tracking.heart_rate > 0
                      ) AS gc_well_sleep_hr
           FROM athlete
           LEFT JOIN gmt_local_time_difference ON gmt_local_time_difference.athlete_id = athlete.id
           LEFT JOIN gc_original_wellness_hr_tracking ON gc_original_wellness_hr_tracking.timestamp::date = gmt_local_time_difference.local_date
           GROUP BY 
               (gc_original_wellness_hr_tracking.timestamp::timestamp+gmt_local_time_difference.gmt_local_difference)::date
          ) aggr_gc_original_wellness_hr_tracking ON aggr_gc_original_wellness_hr_tracking.hr_timestamp_local_date = gmt_local_time_difference.local_date
LEFT JOIN (SELECT 
           strava_activity_summary.start_date_local::date AS start_date_local,
           count(strava_activity_summary.start_date_local::date) AS act_daily_activities,
           
           --Enter COALESCE substitute value for home trainer/virtual ride altitude (in my case its 33).
           COALESCE((array_agg(elev_high ORDER BY start_date_local DESC) FILTER (WHERE strava_activity_summary.type != 'VirtualRide'))[1],33) AS act_last_alt, --altitude of the last activity
           ema(COALESCE((array_agg(elev_high ORDER BY start_date_local DESC) FILTER (WHERE strava_activity_summary.type != 'VirtualRide'))[1]::numeric,33), 0.1428) over (order by start_date_local::date asc) as act_last_alt_ema,
            
           (array_agg(start_longitude ORDER BY start_date_local DESC) FILTER (WHERE strava_activity_summary.type != 'VirtualRide'))[1] AS act_last_long, --longitude of the last activity
           (array_agg(start_latitude ORDER BY start_date_local DESC) FILTER (WHERE strava_activity_summary.type != 'VirtualRide'))[1] AS act_last_lat, --latitude of the last activity
           sum(strava_activity_summary.suffer_score) AS act_daily_suffer_score,
           SUM(strava_activity_summary.suffer_score) FILTER (WHERE strava_activity_summary.start_date_local::time > '17:00:00'::time) AS act_evn_suffer_score,
           sum(strava_activity_summary.elapsed_time) AS act_daily_elapsed_time,
           SUM(strava_activity_summary.elapsed_time) FILTER (WHERE strava_activity_summary.start_date_local::time > '17:00:00'::time) AS act_evn_elapsed_time,
           STRING_AGG(strava_activity_summary.type::text, ';'::text) AS act_type,
           MAX(strava_activity_summary.type) FILTER (WHERE strava_activity_summary.type LIKE '%Ride%') AS act_type_ride,
           MAX(strava_activity_summary.type) FILTER (WHERE strava_activity_summary.type LIKE '%Run%') AS act_type_run,
           MAX(strava_activity_summary.type) FILTER (WHERE strava_activity_summary.type LIKE '%Swim%') AS act_type_swim,
           MAX(strava_activity_summary.type) FILTER (WHERE strava_activity_summary.type LIKE '%Ski%') AS act_type_ski,
           STRING_AGG(strava_activity_summary.type::text, ';'::text) FILTER (WHERE strava_activity_summary.start_date_local::time > '17:00:00'::time) AS act_evn_type
           FROM strava_activity_summary
           GROUP BY (strava_activity_summary.start_date_local::date)) aggr_strava_activity_summary ON aggr_strava_activity_summary.start_date_local = gmt_local_time_difference.local_date
LEFT JOIN (SELECT 
           mfp_nutrition.date,
           STRING_AGG(mfp_nutrition.food_item::text, ';'::text) FILTER (WHERE  mfp_nutrition.meal = '9pm-12am' OR mfp_nutrition.meal = '6pm-9pm') AS nutr_evn_food_items,
           sum(mfp_nutrition.calories) AS nutr_daily_calories,
           SUM(mfp_nutrition.calories) FILTER (WHERE  mfp_nutrition.meal = '9pm-12am' OR mfp_nutrition.meal = '6pm-9pm') AS nutr_evn_calories,
           SUM(mfp_nutrition.calories) FILTER (WHERE  mfp_nutrition.meal = '3pm-6pm' OR mfp_nutrition.meal = '12pm-3pm') AS nutr_midday_calories,
           SUM(mfp_nutrition.calories) FILTER (WHERE  mfp_nutrition.meal = '9am-12pm' OR mfp_nutrition.meal = '6am-9am') AS nutr_morning_calories,
           sum(mfp_nutrition.carbohydrates) AS nutr_daily_carbohydrates,
           SUM(mfp_nutrition.carbohydrates) FILTER (WHERE  mfp_nutrition.meal = '9pm-12am' OR mfp_nutrition.meal = '6pm-9pm') AS nutr_evn_carbohydrates,
           SUM(mfp_nutrition.carbohydrates) FILTER (WHERE  mfp_nutrition.meal = '3pm-6pm' OR mfp_nutrition.meal = '12pm-3pm') AS nutr_midday_carbohydrates,
           SUM(mfp_nutrition.carbohydrates) FILTER (WHERE  mfp_nutrition.meal = '9am-12pm' OR mfp_nutrition.meal = '6am-9am') AS nutr_morning_carbohydrates,
           sum(mfp_nutrition.protein) AS nutr_daily_protein,
           SUM(mfp_nutrition.protein) FILTER (WHERE  mfp_nutrition.meal = '9pm-12am' OR mfp_nutrition.meal = '6pm-9pm') AS nutr_evn_protein,
           SUM(mfp_nutrition.protein) FILTER (WHERE  mfp_nutrition.meal = '3pm-6pm' OR mfp_nutrition.meal = '12pm-3pm') AS nutr_midday_protein,
           SUM(mfp_nutrition.protein) FILTER (WHERE  mfp_nutrition.meal = '9am-12pm' OR mfp_nutrition.meal = '6am-9am') AS nutr_morning_protein,
           sum(mfp_nutrition.fat) AS nutr_daily_fat,
           SUM(mfp_nutrition.fat) FILTER (WHERE  mfp_nutrition.meal = '9pm-12am' OR mfp_nutrition.meal = '6pm-9pm') AS nutr_evn_fat,
           SUM(mfp_nutrition.fat) FILTER (WHERE  mfp_nutrition.meal = '3pm-6pm' OR mfp_nutrition.meal = '12pm-3pm') AS nutr_midday_fat,
           SUM(mfp_nutrition.fat) FILTER (WHERE  mfp_nutrition.meal = '9am-12pm' OR mfp_nutrition.meal = '6am-9am') AS nutr_morning_fat,
           sum(mfp_nutrition.fiber) AS nutr_daily_fiber,
           SUM(mfp_nutrition.fiber) FILTER (WHERE  mfp_nutrition.meal = '9pm-12am' OR mfp_nutrition.meal = '6pm-9pm') AS nutr_evn_fiber,
           SUM(mfp_nutrition.fiber) FILTER (WHERE  mfp_nutrition.meal = '3pm-6pm' OR mfp_nutrition.meal = '12pm-3pm') AS nutr_midday_fiber,
           SUM(mfp_nutrition.fiber) FILTER (WHERE  mfp_nutrition.meal = '9am-12pm' OR mfp_nutrition.meal = '6am-9am') AS nutr_morning_fiber,
           sum(mfp_nutrition.sugar) AS nutr_daily_sugar,
           SUM(mfp_nutrition.sugar) FILTER (WHERE  mfp_nutrition.meal = '9pm-12am' OR mfp_nutrition.meal = '6pm-9pm') AS nutr_evn_sugar,
           SUM(mfp_nutrition.sugar) FILTER (WHERE  mfp_nutrition.meal = '3pm-6pm' OR mfp_nutrition.meal = '12pm-3pm') AS nutr_midday_sugar,
           SUM(mfp_nutrition.sugar) FILTER (WHERE  mfp_nutrition.meal = '9am-12pm' OR mfp_nutrition.meal = '6am-9am') AS nutr_morning_sugar,
           count(mfp_nutrition.food_item) FILTER (WHERE LOWER(mfp_nutrition.food_item) ~ 'coffee' OR LOWER(mfp_nutrition.food_item) ~ 'coke' OR LOWER(mfp_nutrition.food_item) ~ 'pepsi' OR LOWER(mfp_nutrition.food_item) ~ 'espresso') AS nutr_daily_caffeine,
           count(mfp_nutrition.food_item) FILTER (WHERE (mfp_nutrition.meal = '9pm-12am' OR mfp_nutrition.meal = '6pm-9pm' OR mfp_nutrition.meal = '3pm-6pm') AND (LOWER(mfp_nutrition.food_item) ~ 'coffee' OR LOWER(mfp_nutrition.food_item) ~ 'coke' OR LOWER(mfp_nutrition.food_item) ~ 'pepsi') OR LOWER(mfp_nutrition.food_item) ~ 'espresso') AS nutr_evn_caffeine,
           count(mfp_nutrition.food_item) FILTER (WHERE (mfp_nutrition.meal = '9pm-12am' OR mfp_nutrition.meal = '6pm-9pm') AND (LOWER(mfp_nutrition.food_item) ~ 'beer' OR LOWER(mfp_nutrition.food_item) ~ 'wine' OR LOWER(mfp_nutrition.food_item) ~ 'ale' OR LOWER(mfp_nutrition.food_item) ~ 'vodka')) AS nutr_evn_alcohol,
           count(mfp_nutrition.food_item) FILTER (WHERE LOWER(mfp_nutrition.food_item) ~ 'beer' OR LOWER(mfp_nutrition.food_item) ~ 'wine' OR LOWER(mfp_nutrition.food_item) ~ 'ale' OR LOWER(mfp_nutrition.food_item) ~ 'vodka') AS nutr_daily_alcohol
           FROM mfp_nutrition
           GROUP BY mfp_nutrition.date
          ) aggr_mfp_nutrition ON aggr_mfp_nutrition.date = gmt_local_time_difference.local_date        
LEFT JOIN ( SELECT DISTINCT
                (weather.timestamp_gmt::timestamp+gmt_local_time_difference.gmt_local_difference)::date AS weather_timestamp_local_date,
                min(weather.temperature) AS weather_min_temperature,
                max(weather.temperature) AS weather_max_temperature,
                round(avg(weather.temperature),1) AS weather_avg_temperature,
                round(avg(weather.dew_point),1) AS weather_avg_dew_point,
                round(avg(weather.relative_humidity),1) AS weather_avg_humidity,
                round(avg(weather.precipitation),3) AS weather_avg_precipitation,
                round(avg(weather.wind_direction),1) AS weather_avg_wind_direction,
                round(avg(weather.wind_speed),1) AS weather_avg_wind_speed,
                round(avg(weather.sea_air_pressure),1) AS weather_avg_air_pressure,
                round(stddev(weather.sea_air_pressure),2) AS weather_stddev_air_pressure
            FROM athlete
                LEFT JOIN gmt_local_time_difference ON gmt_local_time_difference.athlete_id = athlete.id
                LEFT JOIN weather ON weather.timestamp_gmt::date = gmt_local_time_difference.local_date
            GROUP BY (weather.timestamp_gmt::timestamp+gmt_local_time_difference.gmt_local_difference)::date
          ) aggr_weather ON aggr_weather.weather_timestamp_local_date = gmt_local_time_difference.local_date
LEFT JOIN ( SELECT 
            garmin_connect_body_composition.timestamp::date AS timestamp,
            max(garmin_connect_body_composition.body_water) AS body_water,
            max(garmin_connect_body_composition.weight_gm) AS weight_gm,
            max(garmin_connect_body_composition.bmi) AS body_mass_index
            FROM garmin_connect_body_composition
            GROUP BY (garmin_connect_body_composition.timestamp::date)) aggr_garmin_connect_body_composition ON aggr_garmin_connect_body_composition.timestamp = gmt_local_time_difference.local_date
LEFT JOIN ( WITH lin_regr AS 
              (SELECT 
                 regr_slope(rmssd,date_part('epoch', summary_date::date)) rmmsd_slope,
                 regr_intercept(rmssd,date_part('epoch', summary_date::date)) rmssd_intercept 
               FROM oura_sleep_daily_summary)                                  
            SELECT 
            oura_sleep_daily_summary.summary_date::date AS summary_date,
            max(oura_sleep_daily_summary.rmssd) AS oura_sleep_rmssd,
            max(oura_sleep_daily_summary.duration) AS oura_sleep_duration,
            max(oura_sleep_daily_summary.awake) AS oura_sleep_awake,
            max(oura_sleep_daily_summary.light) AS oura_sleep_light,
            max(oura_sleep_daily_summary.rem) AS oura_sleep_rem,
            max(oura_sleep_daily_summary.deep) AS oura_sleep_deep,
            max(oura_sleep_daily_summary.breath_average) AS oura_sleep_breath_average,
            max(oura_sleep_daily_summary.temperature_deviation) AS oura_sleep_temperature_deviation,
            -- RMSSD baseline
            max(lin_regr.rmmsd_slope * date_part('epoch', summary_date::date) + lin_regr.rmssd_intercept) as oura_sleep_rmssd_baseline,
            --Deviations from RMSSD baseline in %
            max((((rmssd) - 
               (lin_regr.rmmsd_slope * date_part('epoch', summary_date::date) + lin_regr.rmssd_intercept)) /
               (lin_regr.rmmsd_slope * date_part('epoch', summary_date::date) + lin_regr.rmssd_intercept)) * 100)
            as oura_sleep_rmssd_dev
            FROM oura_sleep_daily_summary,lin_regr
            GROUP BY (oura_sleep_daily_summary.summary_date::date)) aggr_oura_sleep_daily_summary ON aggr_oura_sleep_daily_summary.summary_date = gmt_local_time_difference.local_date
LEFT JOIN (SELECT DISTINCT 
           oura_sleep_detail.timestamp_gmt::date AS oura_sleep_detail_date,
           AVG(oura_sleep_detail.rmssd_5min) AS oura_sleep_rmssd,--avg rmssd from midnight local time
           AVG(oura_sleep_detail.rmssd_5min)--avg morning rmssd (4-14am local time) 
               FILTER (WHERE oura_sleep_detail.timestamp_gmt::time+gmt_local_time_difference.gmt_local_difference > '00:04:00'::time
                             AND oura_sleep_detail.timestamp_gmt::time+gmt_local_time_difference.gmt_local_difference < '12:00:00'::time 
                             AND oura_sleep_detail.rmssd_5min > 0
                       ) AS oura_sleep_morning_rmssd
           FROM athlete
           LEFT JOIN gmt_local_time_difference ON gmt_local_time_difference.athlete_id = athlete.id
           LEFT JOIN oura_sleep_detail ON oura_sleep_detail.timestamp_gmt::date = gmt_local_time_difference.local_date
           GROUP BY 
               oura_sleep_detail.timestamp_gmt::date
          ) aggr_oura_sleep_detail ON aggr_oura_sleep_detail.oura_sleep_detail_date = gmt_local_time_difference.local_date
--WHERE aggr_oura_sleep_daily_summary.oura_sleep_rmssd IS NOT null
GROUP BY athlete.id, 
         gmt_local_time_difference.local_date, 
         gmt_local_time_difference.gmt_local_difference,
         aggr_gc_original_wellness_stress_tracking.gc_well_avg_daytime_stress,
         aggr_gc_original_wellness_stress_tracking.gc_well_morning_stress,
         aggr_gc_original_wellness_stress_tracking.gc_well_midday_stress,
         aggr_gc_original_wellness_stress_tracking.gc_well_evn_stress,
         aggr_gc_original_wellness_stress_tracking.gc_well_10pm_stress,
         aggr_gc_original_wellness_hr_tracking.gc_well_avg_daytime_hr,
         aggr_gc_original_wellness_hr_tracking.gc_well_morning_hr,
         aggr_gc_original_wellness_hr_tracking.gc_well_midday_hr,
         aggr_gc_original_wellness_hr_tracking.gc_well_evn_hr,
         aggr_gc_original_wellness_hr_tracking.gc_well_min_daytime_hr, --from 7am till 23pm only
         aggr_gc_original_wellness_hr_tracking.gc_well_sleep_hr, --from midnight till 6am only
         aggr_garmin_connect_wellness.wellness_resting_heart_rate, --the lowest 30 minute average in a 24 hour period 
         aggr_garmin_connect_wellness.wellness_total_steps,
         aggr_garmin_connect_wellness.wellness_moderate_intensity_minutes,
         aggr_garmin_connect_wellness.wellness_vigorous_intensity_minutes,
         aggr_strava_activity_summary.act_daily_activities,
         aggr_strava_activity_summary.act_last_alt,
         aggr_strava_activity_summary.act_last_alt_ema,
         aggr_strava_activity_summary.act_last_long,
         aggr_strava_activity_summary.act_last_lat,
         aggr_strava_activity_summary.act_daily_suffer_score,
         aggr_strava_activity_summary.act_evn_suffer_score,
         aggr_strava_activity_summary.act_daily_elapsed_time,
         aggr_strava_activity_summary.act_evn_elapsed_time,
         aggr_strava_activity_summary.act_type_ride,
         aggr_strava_activity_summary.act_type_run,
         aggr_strava_activity_summary.act_type_swim,
         aggr_strava_activity_summary.act_type_ski,
         aggr_strava_activity_summary.act_evn_type,
         aggr_mfp_nutrition.nutr_evn_food_items,
         aggr_mfp_nutrition.nutr_daily_calories,
         aggr_mfp_nutrition.nutr_morning_calories,
         aggr_mfp_nutrition.nutr_midday_calories,
         aggr_mfp_nutrition.nutr_evn_calories,
         aggr_mfp_nutrition.nutr_daily_carbohydrates,
         aggr_mfp_nutrition.nutr_morning_carbohydrates,
         aggr_mfp_nutrition.nutr_midday_carbohydrates,
         aggr_mfp_nutrition.nutr_evn_carbohydrates,
         aggr_mfp_nutrition.nutr_daily_protein,
         aggr_mfp_nutrition.nutr_morning_protein,
         aggr_mfp_nutrition.nutr_midday_protein,
         aggr_mfp_nutrition.nutr_evn_protein,
         aggr_mfp_nutrition.nutr_daily_fat,
         aggr_mfp_nutrition.nutr_morning_fat,
         aggr_mfp_nutrition.nutr_midday_fat,
         aggr_mfp_nutrition.nutr_evn_fat,
         aggr_mfp_nutrition.nutr_daily_fiber,
         aggr_mfp_nutrition.nutr_morning_fiber,
         aggr_mfp_nutrition.nutr_midday_fiber,
         aggr_mfp_nutrition.nutr_evn_fiber,
         aggr_mfp_nutrition.nutr_daily_sugar,
         aggr_mfp_nutrition.nutr_morning_sugar,
         aggr_mfp_nutrition.nutr_midday_sugar,
         aggr_mfp_nutrition.nutr_evn_sugar,
         aggr_mfp_nutrition.nutr_daily_caffeine,
         aggr_mfp_nutrition.nutr_evn_caffeine,
         aggr_mfp_nutrition.nutr_daily_alcohol,
         aggr_mfp_nutrition.nutr_evn_alcohol,
         aggr_weather.weather_min_temperature,
         aggr_weather.weather_max_temperature,
         aggr_weather.weather_avg_temperature,
         aggr_weather.weather_avg_dew_point,
         aggr_weather.weather_avg_humidity,
         aggr_weather.weather_avg_precipitation,
         aggr_weather.weather_avg_wind_direction,
         aggr_weather.weather_avg_wind_speed,
         aggr_weather.weather_avg_air_pressure,
         aggr_weather.weather_stddev_air_pressure,
         aggr_garmin_connect_body_composition.body_water,
         aggr_garmin_connect_body_composition.weight_gm,
         aggr_garmin_connect_body_composition.body_mass_index,
         aggr_oura_sleep_daily_summary.oura_sleep_duration,
         aggr_oura_sleep_daily_summary.oura_sleep_awake,
         aggr_oura_sleep_daily_summary.oura_sleep_light,
         aggr_oura_sleep_daily_summary.oura_sleep_rem,
         aggr_oura_sleep_daily_summary.oura_sleep_deep,
         aggr_oura_sleep_daily_summary.oura_sleep_breath_average,
         aggr_oura_sleep_daily_summary.oura_sleep_temperature_deviation,
         aggr_oura_sleep_daily_summary.oura_sleep_rmssd,
         aggr_oura_sleep_detail.oura_sleep_morning_rmssd,
         aggr_oura_sleep_daily_summary.oura_sleep_rmssd_baseline,
         aggr_oura_sleep_daily_summary.oura_sleep_rmssd_dev
         
ORDER BY gmt_local_time_difference.local_date DESC; 
