
--Delete last 7 days of data from Creator Sessions
DELETE FROM public.creator_sessions
WHERE date>='2016-04-16 00:00:00'

;
--Insert updated date from last 7 days of data from Creator Sessions
INSERT INTO public.creator_sessions
(
select
distinct
d.app,
d.date,
A.sessions as sessions,
A.session_starters as session_starters,
A.time_in_msec as time_in_msec,
B.native_posts as native_posts
from
        (
        SELECT distinct app, date
        FROM public.vic_comp_dau
        WHERE date>='2016-04-16 00:00:00'
        AND date<getdate()::date
        GROUP BY 1,2
        ) as d
LEFT JOIN 
        (
        SELECT 
        app
        ,cast(TO_CHAR(local_datetime::DATE, 'YYYY-MM-DD') as date) as date
        ,COUNT(*) AS sessions
        ,COUNT(DISTINCT user_map_tag) AS session_starters,
        SUM(float1) AS time_in_msec 
        FROM public.vic_event 
        WHERE "kingdom" LIKE 'session' 
        AND "phylum" LIKE 'session_end'
        and user_map_tag in
                (
                select ('ut'||id) as user_map_tag from vicky.users
                where access_level like '%api_owner%'
                )
        AND local_Datetime>='2016-04-16 00:00:00'
        AND local_datetime<getdate()::date
        GROUP BY 1,2
        ORDER BY 1,2,3
        ) as A
ON d.date=a.date
AND d.app=a.app
LEFT JOIN 
        (
        select
        redshift_app_name,
        date,
        count(distinct post_id) as native_posts
        from
                (
                select
                distinct
                a.redshift_app_name,
                s.id as post_id,
                s.name as post_name,
                date_trunc('day',convert_timezone('UTC','US/Pacific',s.created_at)) as date
                from vicky.sequences s
                join vicky.nodes n
                on s.id = n.sequence_id
                join vicky.contentitems ci
                on n.id = ci.node_id
                join vicky.apps a
                on s.app_id = a.id
                join vicky.users u
                on s.created_by = u.id
                where ci.remote_source = 'app'
                and s.category not like '%repost%'
                and u.access_level like '%api_owner%'
                AND s.created_at>='2016-04-16 07:00:00'
                AND convert_timezone('UTC','US/Pacific',s.created_at)::date<getdate()::date
                )
        group by 1,2
        order by 1,2
        ) as B
ON D.app=B.redshift_app_name
and D.date = B.date
LEFT join public.launch_date ld
on D.app = ld.app
and D.date>=ld.launch_date
WHERE (D.date<ld.deactivation_date OR ld.deactivation_date is null)
AND ld.app_status IN ('live','inactive')
AND (A.sessions>0 OR B.native_posts>0)
order by 1,2,3
)