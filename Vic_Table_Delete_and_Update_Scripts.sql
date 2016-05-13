-- Deleting Data


delete from public.vic_comp_install
where date>'2016-04-15';

delete from public.vic_comp_registration
where date>'2016-04-15';

delete from public.vic_comp_dau
where date>'2016-04-15';

delete from public.vic_comp_engagement
where date>'2016-04-15';

delete from public.vic_comp_creation
where date>'2016-04-15';

delete from public.vic_comp_tileview
where date>'2016-04-15';

delete from public.vic_comp_view
where date>'2016-04-15';

delete from public.vic_comp_repost
where date>'2016-04-15';

delete from public.vic_comp_retention
where first_date>'2016-04-15';


delete from public.vic_comp_timeinapp
where date>'2016-04-15';


-- Vic Install

insert into public.vic_comp_install
(
SELECT 
A.app
, apps.name as SQL_app_name 
, cast(TO_CHAR(local_datetime::DATE, 'YYYY-MM-DD') as date) as date
, A.os
, datediff(day,L.launch_date,cast(TO_CHAR(local_datetime::DATE, 'YYYY-MM-DD') as date)) as days_since_launch
,COUNT(DISTINCT user_map_tag) as installs
FROM 
(SELECT ROW_NUMBER() OVER(PARTITION BY app, os, user_map_tag ORDER BY event_datetime) as row_num
, *
FROM public.vic_install
) A
join public.launch_date L
on A.app = L.app
and A.os=L.os
and  cast(TO_CHAR(local_datetime::DATE, 'YYYY-MM-DD') as date)>=L.launch_date
INNER JOIN vicky.apps
on A.app=apps.redshift_app_name
WHERE row_num = 1
AND local_datetime::date>'2016-04-15'
AND local_datetime::date<getdate()::date
GROUP BY 1,2,3,4,5
ORDER BY 1,2,3,4,5
);

-- Vic Registration

insert into public.vic_comp_registration
(
SELECT 
app,
cast(TO_CHAR(local_datetime::DATE, 'YYYY-MM-DD') as date) as date,
os,
COUNT(*) AS registrations,
COUNT(DISTINCT event.user_map_tag) AS unique_registered_users 
FROM public.vic_event as event
WHERE event."kingdom" = 'registration'
AND local_datetime::date>'2016-04-15'
AND local_datetime::date<getdate()::date
GROUP BY 1,2,3
);

-- Vic DAU

insert into public.vic_comp_dau
(
SELECT 
dau.app 
, l.SQL_app_name
, cast(TO_CHAR(dau.local_datetime::DATE, 'YYYY-MM-DD') as date) as date
, dau.os
, datediff(day,l.launch_date, CAST(TO_CHAR(dau.local_datetime::date, 'YYYY-MM-DD') as date)) as days_since_launch
, COUNT(DISTINCT dau.user_map_tag) as dau 
FROM 
(SELECT ROW_NUMBER() OVER(PARTITION BY app, os, user_map_tag, local_datetime::DATE ORDER BY event_datetime) as row_num
, *
FROM public.vic_dau
) dau
LEFT JOIN public.vic_install install
on dau.app = install.app
and dau.user_map_tag = install.user_map_tag
and dau.event_datetime>=install.event_datetime
JOIN public.launch_date l 
on dau.app=l.app
AND dau.os=l.os
WHERE row_num = 1
AND l.app_status IN ('live','inactive')
AND dau.local_datetime::date>'2016-04-15'
AND dau.local_datetime::date<getdate()::date
GROUP BY 1,2,3,4,5
);


-- Vic Engagement

insert into public.vic_comp_engagement
(
SELECT app, cast(TO_CHAR(local_datetime::DATE, 'YYYY-MM-DD') as date) as date
,event."os",COUNT(*) AS engagements,COUNT(DISTINCT event.user_map_tag) AS engagers 

FROM public.vic_event event 

WHERE event."kingdom" = 'engagement'
AND local_datetime::date>'2016-04-15'
AND local_datetime::date<getdate()::date
GROUP BY 1,2,3
);


-- Vic View and Video View

insert into public.vic_comp_view
(
SELECT app
, cast(TO_CHAR(local_datetime::DATE, 'YYYY-MM-DD') as date) as date
, event."os"
, COUNT(*) AS views,COUNT(DISTINCT event.user_map_tag) AS viewers
, sum(case when event."class"='video' then 1 else 0 end) as video_views
, count(distinct case when event."class"='video' then user_map_tag else null end) as video_viewers
FROM public.vic_event event  

WHERE event."kingdom" = 'view'
and event."phylum" = 'start'
AND local_datetime::date>'2016-04-15'
AND local_datetime::date<getdate()::date
GROUP BY 1,2,3
);


-- Vic Tile View

insert into public.vic_comp_tileview
(
SELECT app, cast(TO_CHAR(local_datetime::DATE, 'YYYY-MM-DD') as date) as date
,event."os",COUNT(*) AS tile_views,COUNT(DISTINCT event.user_map_tag) AS tile_viewers 

FROM public.vic_event event  

WHERE event."kingdom" = 'tile_view'
and event."phylum" = 'start'
AND local_datetime::date>'2016-04-15'
AND local_datetime::date<getdate()::date
GROUP BY 1,2,3
);


-- Vic Creation

insert into public.vic_comp_creation
(
SELECT 
app,
cast(TO_CHAR(local_datetime::DATE, 'YYYY-MM-DD') as date) as date,
event."os",
COUNT(*) AS creations,
COUNT(DISTINCT event.user_map_tag) AS creators 

FROM public.vic_event event  

WHERE event."kingdom" LIKE '%creation%'
AND local_datetime::date>'2016-04-15'
AND local_datetime::date<getdate()::date
GROUP BY 1,2,3
);


-- Vic Time in App

insert into public.vic_comp_timeinapp
( 
SELECT 
app,
cast(TO_CHAR(local_datetime::DATE, 'YYYY-MM-DD') as date) as date,
os
,COUNT(*) AS sessions
,COUNT(DISTINCT event.user_map_tag) AS session_starters,
SUM(float1) AS time_in_msec 
FROM public.vic_event event
WHERE event."kingdom" = 'session' 
AND event."phylum" = 'session_end'
AND event.float1>=0
AND event.float1<=86400000
AND local_datetime::date>'2016-04-15'
AND local_datetime::date<getdate()::date
GROUP BY 1,2,3
);


-- Vic Repost

insert into public.vic_comp_repost
(
SELECT 
app, 
cast(TO_CHAR(local_datetime::DATE, 'YYYY-MM-DD') as date) as date
,event."os",COUNT(*) AS reposts,COUNT(DISTINCT event.user_map_tag) AS reposters 

FROM public.vic_event event

WHERE event."kingdom" = 'engagement'
AND event."phylum" = 'repost'
AND local_datetime::date>'2016-04-15'
AND local_datetime::date<getdate()::date
GROUP BY 1,2,3
);

-- Vic Retention

insert into public.vic_comp_retention
(
SELECT
app,
apps.name as SQL_app_name,
os,
cast(first_date as date) as first_date,
MAX(case when daysAfterInstall = 0 then start_count end) as start_count,
MAX(case when daysAfterInstall = 1 then return_count end) as D1_Retention,
MAX(case when daysAfterInstall = 2 then return_count end) as D2_Retention,
MAX(case when daysAfterInstall = 3 then return_count end) as D3_Retention,
MAX(case when daysAfterInstall = 4 then return_count end) as D4_Retention,
MAX(case when daysAfterInstall = 5 then return_count end) as D5_Retention,
MAX(case when daysAfterInstall = 6 then return_count end) as D6_Retention,
MAX(case when daysAfterInstall = 7 then return_count end) as D7_Retention,
MAX(case when daysAfterInstall = 8 then return_count end) as D8_Retention,
MAX(case when daysAfterInstall = 9 then return_count end) as D9_Retention,
MAX(case when daysAfterInstall = 10 then return_count end) as D10_Retention,
MAX(case when daysAfterInstall = 11 then return_count end) as D11_Retention,
MAX(case when daysAfterInstall = 12 then return_count end) as D12_Retention,
MAX(case when daysAfterInstall = 13 then return_count end) as D13_Retention,
MAX(case when daysAfterInstall = 14 then return_count end) as D14_Retention,
MAX(case when daysAfterInstall = 30 then return_count end) as D30_Retention,
MAX(case when daysAfterInstall = 60 then return_count end) as D60_Retention,
MAX(case when daysAfterInstall = 90 then return_count end) as D90_Retention

FROM

(

SELECT 
start_count.app,
start_count.os,
To_char(start_count.first_date, 'YYYY-MM-DD') AS first_date, 
return_count.nday                             AS daysAfterInstall, 
start_count.start_count, 
return_count.return_count
        
FROM   

(
SELECT app, os, first_date, Count(device_tag) AS start_count 
FROM
        (
        SELECT app, os, start_count.device_tag, convert_timezone('UTC','US/Pacific',event_datetime) :: DATE AS first_date 
        FROM   
                (
                SELECT * 
                FROM   
                        (
                        SELECT Row_number() over(PARTITION BY app,os,device_tag ORDER BY event_datetime) AS row_num, 
                               * 
                        FROM   public.vic_raw_event 
                        WHERE event_datetime>='2016-04-16 07:00:00'
                        AND convert_timezone('UTC','US/Pacific',event_datetime)::date<getdate()::date
                        AND kingdom='install'
                        ) installs_with_row_num 
                WHERE  row_num = 1 
               ) start_count 
        WHERE  1 = 1
        ) a 
GROUP  BY 1,2,3
) start_count  
left join 
(
SELECT   r.app,
         r.os,
         r.first_date, 
         r.nday, 
         Count(r.device_tag) AS return_count 
         FROM   
                (
                SELECT starters.app,
                starters.os,
                starters.device_tag, 
                starters.first_date, 
                returners.return_date, 
                returners.return_date - starters.first_date AS nday 
                  FROM   
                        (
                        SELECT app,os,start_count.device_tag, convert_timezone('UTC','US/Pacific',event_datetime) :: DATE AS first_date 
                        FROM   
                                (
                                SELECT * 
                                FROM   
                                        (
                                        SELECT Row_number() over(PARTITION BY app,os,device_tag ORDER BY event_datetime) AS row_num, 
                                        * 
                                        FROM   public.vic_raw_event 
                                        WHERE event_datetime>='2016-04-16 07:00:00'
                                        AND convert_timezone('UTC','US/Pacific',event_datetime)::date<getdate()::date
                                        AND kingdom='install'
                                        ) installs_with_row_num 
                                WHERE  row_num = 1 
                                ) start_count 
                        WHERE  1 = 1
                        ) starters 
                        inner join 
                        (
                        SELECT DISTINCT app,os,device_tag, convert_timezone('UTC','US/Pacific',event_datetime) :: DATE AS return_date 
                        FROM   public.vic_raw_event 
                        WHERE event_datetime>='2016-04-16 07:00:00'
                        AND convert_timezone('UTC','US/Pacific',event_datetime)::date<getdate()::date
                        AND kingdom='session'
                        AND phylum='session_start'
                        )returners 
                        ON starters.device_tag = returners.device_tag
                        and starters.app = returners.app
                        and starters.os=returners.os 
                        AND returners.return_date >= starters.first_date
                ) r 
GROUP  BY 1, 2,3,4
) return_count
                            
ON start_count.app = return_count.app
AND start_count.os = return_count.os
AND start_count.first_date = return_count.first_date

 
ORDER  BY 1,2,3,4
)
INNER JOIN vicky.apps
ON app=apps.redshift_app_name
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4
)