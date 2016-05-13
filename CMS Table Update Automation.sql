--Table feed the total active users number displayed in the CMS
--delete data from 4/30
DELETE FROM public.CMS_vic_total_AU
WHERE date>='2016-04-30 00:00:00'
;

--Adding data through 4/30 to tables
--CMS Vic Total Active Users

INSERT INTO public.CMS_vic_total_AU
(
SELECT distinct app, event_datetime as date, user_map_tag as user_id
FROM   
        (
        SELECT ROW_NUMBER() over(PARTITION BY app, os, user_map_tag, event_datetime::date ORDER BY event_datetime) AS row_num, *
        FROM   public.vic_raw_event
        WHERE kingdom='session'
        AND phylum='session_start'
        AND user_map_tag NOT LIKE 'dt%'
        AND event_datetime>='2016-04-30 00:00:00'
        AND event_datetime<getdate()::date
        ) 
WHERE  row_num = 1
)
