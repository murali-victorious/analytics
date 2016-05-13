
--Stream load - Deleting last 7 days data
DELETE FROM public.stream_load
WHERE local_datetime::date>='2016-04-16 00:00:00'

;
--Inserting data from last 7 days from Vic Tables
Insert into public.stream_load
(
SELECT app, os, local_datetime, kingdom, phylum, float1
FROM public.vic_event
INNER JOIN vicky.apps
ON vic_event.app=apps.redshift_app_name
WHERE kingdom='app_performance'
AND phylum='stream_load'
AND local_datetime::date>='2016-04-16 00:00:00'
AND local_datetime::date<getdate()::date
AND float1>0
)