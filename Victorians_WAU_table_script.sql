--Victorians WAU - Deleting data from cutoff point
DELETE FROM public.victorians_wau
WHERE week_ending_in>='2016-04-21 00:00:00'
;

--Updated Data for Victorians WAU added to table
INSERT INTO public.victorians_wau
(
SELECT d.app, 
d.os,
d.week_ending_in, 
d.wau, 
weekly_sessions,
weekly_time_msec
FROM 
	(
	SELECT app
	, os
	, date_trunc('week', dateadd('day',4,local_datetime))+2 as week_ending_in
	, COUNT(distinct user_map_tag) as WAU
    FROM public.vic_dau
    WHERE app='victorians'
	AND local_datetime>='2016-04-21 00:00:00'
    GROUP BY 1,2,3
    ORDER BY 1,2,3
    ) d
LEFT JOIN 
	(
	SELECT app,
	os,
	date_trunc('week', dateadd('day',4,date))+2 as week_ending_in,
	SUM(sessions) as weekly_sessions, 
	SUM(time_in_msec) as weekly_time_msec
	FROM public.vic_comp_timeinapp 
	WHERE app='victorians'
	AND date>='2016-04-21 00:00:00'
	GROUP BY 1,2,3
	) as H
ON d.app=H.app
AND d.os=H.os 
AND d.week_ending_in=H.week_ending_in
AND d.week_ending_in>='2016-04-21 00:00:00'
)
