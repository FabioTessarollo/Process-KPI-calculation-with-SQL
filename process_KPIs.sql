--PostgreSQL
--for each day of the calendar, one or multiple ranges that are to consider in the KPIs calculation
CREATE TABLE schema.process_ranges_to_conser (
	day_id date NOT NULL,
	valid_range_start_id int4 NOT NULL,
	valid_range_end int4 NOT NULL
)
DISTRIBUTED BY (day_id, valid_range_start_id);

--source table of processes
CREATE TABLE schema.process_tracking (
	process_id varchar(50) NULL,
	event_datetime timestamp NULL, --timestamp of the event
	"content" varchar(50) NULL,  --event 
	process_status varchar(50) NULL,  --new status of the process
	update_date timestamp NULL,
	insert_date timestamp NULL
)
DISTRIBUTED BY (process_id);


--target table
CREATE TABLE schema.ft_process_kpis (
	process_id varchar(10) NOT NULL,
	t0 timestamp NULL,
	t1 timestamp NULL,
	t2 timestamp NULL,
	t3 timestamp NULL,
	t4 timestamp NULL,
	t5 timestamp NULL,
	KPI_T1_T0 int4 NULL,
	KPI_T2_T0 int4 NULL,
	KPI_T3_T0 int4 NULL,
	KPI_T5_T4 int4 NULL,
	insert_date date NULL
)
DISTRIBUTED by(process_id);


insert into ret_mkt_dm.ft_time_to_yes  (
	 process_id
	,t0
	,t1
	,t2
	,t3
	,t4
	,t5
	,KPI_T1_T0
	,KPI_T2_T0
	,KPI_T3_T0
	,KPI_T5_T4
	,insert_date
)
with 
--getting the relevant timestamps for process KPIs calculation
tracking_timestamps_query as (   
		select process_id ,                                                                                                          
		,min(case when "content" = 'ACTION_2' and process_status in ('STATUS_1') then event_datetime end)  as T1                                                             
		,min(case when "content" = 'ACTION_2' and process_status in ('STATUS_2') then event_datetime end)  as T1
		,min(case when "content" = 'ACTION_2' and process_status in ('STATUS_3') then event_datetime end)  as T2
		,min(case when "content" = 'ACTION_2' and process_status in ('STATUS_4') then event_datetime end)  as T3
		,min(case when "content" = 'ACTION_3' and process_status in ('STATUS_5') then event_datetime end)  as T4                    
		,min(case when "content" = 'ACTION_4' and process_status in ('STATUS_6') then event_datetime end)  as T5
		from schema.process_tracking
		group by process_id 
) 
--getting the ranges not to consider for process KPIs calculation
, process_ranges_not_to_consider as (
		select start_blocking.process_id, start_blocking, end_blocking , start_blocking::date as day_id, end_blocking - start_blocking as blocking_range
		from (
			select process_id , rank() over(partition by process_id order by start_blocking) as blocking_range_number,
			start_blocking
			from (
				select process_id,
				case when process_status = 'BLOCK' and lag(process_status) over(partition by process_id order by event_datetime) <> 'BLOCK' then event_datetime end as start_blocking
		        from schema.process_tracking 
				) start_blocking
			) start_blocking
		left join (
			select process_id , rank() over(partition by process_id order by end_blocking) as blocking_range_number,
			end_blocking
			from (
				select process_id,
				case when process_status = 'BLOCK' and lead(process_status) over(partition by process_id order by event_datetime) <> 'BLOCK' then event_datetime end as end_blocking
		        from schema.process_tracking 
				) end_blocking
			) end_blocking
		on start_blocking.process_id = end_blocking.process_id
		and start_blocking.blocking_range_number = end_blocking.blocking_range_number
		where end_blocking - start_blocking > '0 minutes'::interval
) 
--hour --> int
, tracking_timestamps as (
		select process_id , 
		 T0
		,T1
		,T2
		,T3
		,T4
		,T5
		,to_char(T0, 'HH24')::int as T0_int
		,to_char(T1, 'HH24')::int as T1_int
		,to_char(T2, 'HH24')::int as T2_int
        ,to_char(T3, 'HH24')::int as T3_int
		,to_char(T4, 'HH24')::int as T4_int
		,to_char(T5, 'HH24')::int as T5_int
		from tracking_timestamps_query
)
--process ranges to consider
, process_ranges_to_conser_1 as (
		select 
		day_id,
		valid_range_start_id as valid_range_start_id_int,
		valid_range_end as valid_range_end_int,
		to_timestamp(to_char(day_id, 'YYYYMMDD') || lpad(valid_range_start_id::varchar(2), 2, '0'), 'YYYYMMDDHH24') as valid_range_start_id
		to_timestamp(to_char(day_id, 'YYYYMMDD') || lpad(valid_range_end::varchar(2), 2, '0'), 'YYYYMMDDHH24') as valid_range_end,
		from schema.process_ranges_to_conser
) 
, process_ranges_to_conser as (
		select 
		day_id,
		valid_range_end - valid_range_start_id as validing_hours,
		valid_range_start_id_int,
		valid_range_end_int,
		valid_range_start_id,
		valid_range_end
		from process_ranges_to_conser_1
)  
-- calculation of intervals (--) for the ranges (__) containing T0 and T1:  |_____T0----| |________| |________| |----T1_____|
-- and subtraction of ranges not to conider
, in_day_ranges as (
select 
    tracking_timestamps.*,
    --T1T0
		(case when T0_day.valid_range_end > tracking_timestamps.T0 then T0_day.valid_range_end - tracking_timestamps.T0 else '0 Seconds'::interval end)
	  + (case when tracking_timestamps.T1 > T1_day.valid_range_start_id then tracking_timestamps.T1 - T1_day.valid_range_start_id else '0 Seconds'::interval end) 
	  - (case when stop_first_t0.start_blocking < tracking_timestamps.T0 then stop_first_t0.end_blocking - tracking_timestamps.T0 else '0 Seconds'::interval end)
	  - (case when stop_first_t0.end_blocking > T0_day.valid_range_end then T0_day.valid_range_end - tracking_timestamps.T0 else '0 Seconds'::interval end)
	  - (case when stop_first_t0.end_blocking < T0_day.valid_range_end and stop_first_t0.start_blocking > T0_day.valid_range_start_id
	  	then stop_first_t0.end_blocking - stop_first_t0.start_blocking else '0 Seconds'::interval end)
	  - (case when stop_first_t1.start_blocking < T1_day.valid_range_start_id then stop_first_t1.end_blocking - T1_day.valid_range_start_id else '0 Seconds'::interval end)
	  - (case when stop_first_t1.end_blocking > tracking_timestamps.T1 then T1_day.valid_range_end - tracking_timestamps.T1 else '0 Seconds'::interval end)
	  - (case when stop_first_t1.start_blocking > T1_day.valid_range_start_id and stop_first_t1.end_blocking < tracking_timestamps.T1
	  	then stop_first_t1.end_blocking - stop_first_t1.start_blocking else '0 Seconds'::interval end)
  	as KPI_T1_T0,
  	--T2T0
	    (case when T0_day.valid_range_end > tracking_timestamps.T0 then T0_day.valid_range_end - tracking_timestamps.T0 else '0 Seconds'::interval end)
	  + (case when tracking_timestamps.T2 > T2_day.valid_range_start_id then tracking_timestamps.T2 - T2_day.valid_range_start_id else '0 Seconds'::interval end) 
	  - (case when stop_first_t0.start_blocking < tracking_timestamps.T0 then stop_first_t0.end_blocking - tracking_timestamps.T0 else '0 Seconds'::interval end)
	  - (case when stop_first_t0.end_blocking > T0_day.valid_range_end then T0_day.valid_range_end - tracking_timestamps.T0 else '0 Seconds'::interval end)
	  - (case when stop_first_t0.end_blocking < T0_day.valid_range_end and stop_first_t0.start_blocking > T0_day.valid_range_start_id
	  	then stop_first_t0.end_blocking - stop_first_t0.start_blocking else '0 Seconds'::interval end)
	  - (case when stop_first_t2.start_blocking < T2_day.valid_range_start_id then stop_first_t2.end_blocking - T2_day.valid_range_start_id else '0 Seconds'::interval end)
	  - (case when stop_first_t2.end_blocking > tracking_timestamps.T2 then T2_day.valid_range_end - tracking_timestamps.T2 else '0 Seconds'::interval end)
	  - (case when stop_first_t2.start_blocking > T2_day.valid_range_start_id and stop_first_t2.end_blocking < tracking_timestamps.T2
	  	then stop_first_t2.end_blocking - stop_first_t2.start_blocking else '0 Seconds'::interval end)
    as KPI_T2_T0,
   --T3T0
    (case when T0_day.valid_range_end > tracking_timestamps.T0 then T0_day.valid_range_end - tracking_timestamps.T0 else '0 Seconds'::interval end)
  + (case when tracking_timestamps.T3 > T3_day.valid_range_start_id then tracking_timestamps.T3 - T3_day.valid_range_start_id else '0 Seconds'::interval end) 
	  - (case when stop_first_t0.start_blocking < tracking_timestamps.T0 then stop_first_t0.end_blocking - tracking_timestamps.T0 else '0 Seconds'::interval end)
	  - (case when stop_first_t0.end_blocking > T0_day.valid_range_end then T0_day.valid_range_end - tracking_timestamps.T0 else '0 Seconds'::interval end)
	  - (case when stop_first_t0.end_blocking < T0_day.valid_range_end and stop_first_t0.start_blocking > T0_day.valid_range_start_id
	  	then stop_first_t0.end_blocking - stop_first_t0.start_blocking else '0 Seconds'::interval end)
	  - (case when stop_first_t3.start_blocking < T3_day.valid_range_start_id then stop_first_t3.end_blocking - T3_day.valid_range_start_id else '0 Seconds'::interval end)
	  - (case when stop_first_t3.end_blocking > tracking_timestamps.T3 then T3_day.valid_range_end - tracking_timestamps.T3 else '0 Seconds'::interval end)
	  - (case when stop_first_t3.start_blocking > T3_day.valid_range_start_id and stop_first_t3.end_blocking < tracking_timestamps.T3
	  	then stop_first_t3.end_blocking - stop_first_t3.start_blocking else '0 Seconds'::interval end)
    as KPI_T3_T0,
    
    (case when T4_day.valid_range_end > tracking_timestamps.T4 then T4_day.valid_range_end - tracking_timestamps.T4 else '0 Seconds'::interval end)
  + (case when tracking_timestamps.T5 > T5_day.valid_range_start_id then tracking_timestamps.T5 - T5_day.valid_range_start_id else '0 Seconds'::interval end) as KPI_T5_T4,
  T0_day.valid_range_start_id as T0_range_id,
  T1_day.valid_range_start_id as T1_range_id,
  T2_day.valid_range_start_id as T2_range_id,
  T3_day.valid_range_start_id as T3_range_id,
  T4_day.valid_range_start_id as T4_range_id,
  T5_day.valid_range_start_id as T5_range_id
from tracking_timestamps
left join process_ranges_to_conser_1 T0_day
on T0_day.day_id = tracking_timestamps.T0::date
and T0_day.valid_range_start_id_int <= tracking_timestamps.T0_int
and T0_day.valid_range_end_int > tracking_timestamps.T0_int
left join process_ranges_to_conser_1 T1_day
on T1_day.day_id = tracking_timestamps.T1::date
and T1_day.valid_range_start_id_int <= tracking_timestamps.T1_int
and T1_day.valid_range_end_int > tracking_timestamps.T1_int
left join process_ranges_to_conser_1 T2_day
on T2_day.day_id = tracking_timestamps.T2::date
and t2_day.valid_range_start_id_int <= tracking_timestamps.t2_int
and t2_day.valid_range_end_int > tracking_timestamps.t2_int
left join process_ranges_to_conser_1 T3_day
on T3_day.day_id = tracking_timestamps.T3::date
and t3_day.valid_range_start_id_int <= tracking_timestamps.t3_int
and t3_day.valid_range_end_int > tracking_timestamps.t3_int
left join process_ranges_to_conser_1 T4_day
on T4_day.day_id = tracking_timestamps.T4::date
and t4_day.valid_range_start_id_int <= tracking_timestamps.t4_int
and t4_day.valid_range_end_int > tracking_timestamps.t4_int
left join process_ranges_to_conser_1 T5_day
on T5_day.day_id = tracking_timestamps.T5::date
and t5_day.valid_range_start_id_int <= tracking_timestamps.t5_int
and t5_day.valid_range_end_int > tracking_timestamps.t5_int
left join process_ranges_not_to_consider stop_first_t0
on stop_first_t0.day_id = T0_day.day_id
and stop_first_t0.end_blocking > tracking_timestamps.T0
and stop_first_t0.start_blocking <= T0_day.valid_range_end
and stop_first_t0.process_id = tracking_timestamps.process_id
left join process_ranges_not_to_consider stop_first_t1
on stop_first_t1.day_id = T1_day.day_id
and stop_first_t1.end_blocking > T1_day.valid_range_start_id
and stop_first_t1.start_blocking <= tracking_timestamps.t1
and stop_first_t1.process_id = tracking_timestamps.process_id
left join process_ranges_not_to_consider stop_first_t2
on stop_first_t2.day_id = T2_day.day_id
and stop_first_t2.end_blocking > T2_day.valid_range_start_id
and stop_first_t2.start_blocking <= tracking_timestamps.t2
and stop_first_t2.process_id = tracking_timestamps.process_id
left join process_ranges_not_to_consider stop_first_t3
on stop_first_t3.day_id = T3_day.day_id
and stop_first_t3.end_blocking > T3_day.valid_range_start_id
and stop_first_t3.start_blocking <= tracking_timestamps.t3
and stop_first_t3.process_id = tracking_timestamps.process_id
left join process_ranges_not_to_consider stop_first_t4
on stop_first_t4.day_id = T4_day.day_id
and stop_first_t4.end_blocking > tracking_timestamps.T4
and stop_first_t4.start_blocking <= T4_day.valid_range_end
and stop_first_t4.process_id = tracking_timestamps.process_id
left join process_ranges_not_to_consider stop_first_t5
on stop_first_t5.day_id = T5_day.day_id
and stop_first_t5.end_blocking > T5_day.valid_range_start_id
and stop_first_t5.start_blocking <= tracking_timestamps.t5
and stop_first_t5.process_id = tracking_timestamps.process_id
)
-- sum of the intervals of ranges between T0 and T1   |_____T0_____| |--------| |--------| |_____T1_____|
-- for each process KPI
, T1_T0 as (
		select tracking_timestamps.process_id, 
			  sum(coalesce(T1_T0.validing_hours, '0 seconds'::interval))		
			- sum(coalesce(T1_T0.valid_range_end - partial_blok_start.start_blocking, '0 seconds'::interval)) 
			- sum(coalesce(partial_blok_end.end_blocking - T1_T0.valid_range_start_id, '0 seconds'::interval))	
		as KPI_T1_T0
		from tracking_timestamps
		left join process_ranges_to_conser T1_T0
		on T1_T0.valid_range_start_id > tracking_timestamps.T0
		and T1_T0.valid_range_end < tracking_timestamps.T1
		left join process_ranges_not_to_consider
		on  T1_T0.valid_range_start_id > process_ranges_not_to_consider.start_blocking
		and process_ranges_not_to_consider.end_blocking > T1_T0.valid_range_end
		and process_ranges_not_to_consider.process_id = tracking_timestamps.process_id
		left join process_ranges_not_to_consider partial_blok_start
		on  T1_T0.valid_range_start_id::Date = partial_blok_start.start_blocking::DATE
		and T1_T0.valid_range_start_id < partial_blok_start.start_blocking
		and T1_T0.valid_range_end > partial_blok_start.start_blocking
		and partial_blok_start.process_id = tracking_timestamps.process_id
		left join process_ranges_not_to_consider partial_blok_end
		on  T1_T0.valid_range_end::Date = partial_blok_end.end_blocking::DATE
		and T1_T0.valid_range_start_id < partial_blok_end.end_blocking
		and T1_T0.valid_range_end > partial_blok_end.end_blocking
		and partial_blok_end.process_id = tracking_timestamps.process_id
		where process_ranges_not_to_consider.process_id is null
		group by tracking_timestamps.process_id
)
, T2_T0 as (
		select tracking_timestamps.process_id, 
			  sum(coalesce(T2_T0.validing_hours, '0 seconds'::interval))		
			- sum(coalesce(T2_T0.valid_range_end - partial_blok_start.start_blocking, '0 seconds'::interval)) 
			- sum(coalesce(partial_blok_end.end_blocking - T2_T0.valid_range_start_id, '0 seconds'::interval))	
		as KPI_T2_T0
		from tracking_timestamps
		left join process_ranges_to_conser T2_T0
		on T2_T0.valid_range_start_id > tracking_timestamps.T0
		and T2_T0.valid_range_end < tracking_timestamps.T2
		left join process_ranges_not_to_consider
		on  T2_T0.valid_range_start_id > process_ranges_not_to_consider.start_blocking
		and process_ranges_not_to_consider.end_blocking > T2_T0.valid_range_end
		and process_ranges_not_to_consider.process_id = tracking_timestamps.process_id
		left join process_ranges_not_to_consider partial_blok_start
		on  T2_T0.valid_range_start_id::Date = partial_blok_start.start_blocking::DATE
		and T2_T0.valid_range_start_id < partial_blok_start.start_blocking
		and T2_T0.valid_range_end > partial_blok_start.start_blocking
		and partial_blok_start.process_id = tracking_timestamps.process_id
		left join process_ranges_not_to_consider partial_blok_end
		on  T2_T0.valid_range_end::Date = partial_blok_end.end_blocking::DATE
		and T2_T0.valid_range_start_id < partial_blok_end.end_blocking
		and T2_T0.valid_range_end > partial_blok_end.end_blocking
		and partial_blok_end.process_id = tracking_timestamps.process_id
		where process_ranges_not_to_consider.process_id is null
		group by tracking_timestamps.process_id
)
, T3_T0 as (
		select tracking_timestamps.process_id, 
			  sum(coalesce(T3_T0.validing_hours, '0 seconds'::interval))		
			- sum(coalesce(T3_T0.valid_range_end - partial_blok_start.start_blocking, '0 seconds'::interval)) 
			- sum(coalesce(partial_blok_end.end_blocking - T3_T0.valid_range_start_id, '0 seconds'::interval))	
		as KPI_T3_T0
		from tracking_timestamps
		left join process_ranges_to_conser T3_T0
		on T3_T0.valid_range_start_id > tracking_timestamps.T0
		and T3_T0.valid_range_end < tracking_timestamps.T3
		left join process_ranges_not_to_consider
		on  T3_T0.valid_range_start_id > process_ranges_not_to_consider.start_blocking
		and process_ranges_not_to_consider.end_blocking > T3_T0.valid_range_end
		and process_ranges_not_to_consider.process_id = tracking_timestamps.process_id
		left join process_ranges_not_to_consider partial_blok_start
		on  T3_T0.valid_range_start_id::Date = partial_blok_start.start_blocking::DATE
		and T3_T0.valid_range_start_id < partial_blok_start.start_blocking
		and T3_T0.valid_range_end > partial_blok_start.start_blocking
		and partial_blok_start.process_id = tracking_timestamps.process_id
		left join process_ranges_not_to_consider partial_blok_end
		on  T3_T0.valid_range_end::Date = partial_blok_end.end_blocking::DATE
		and T3_T0.valid_range_start_id < partial_blok_end.end_blocking
		and T3_T0.valid_range_end > partial_blok_end.end_blocking
		and partial_blok_end.process_id = tracking_timestamps.process_id
		where process_ranges_not_to_consider.process_id is null
		group by tracking_timestamps.process_id
)
, T5_T4 as (
		select tracking_timestamps.process_id, 
			  sum(coalesce(T5_T4.validing_hours, '0 seconds'::interval))		
			- sum(coalesce(T5_T4.valid_range_end - partial_blok_start.start_blocking, '0 seconds'::interval)) 
			- sum(coalesce(partial_blok_end.end_blocking - T5_T4.valid_range_start_id, '0 seconds'::interval))	
		as KPI_T5_T4
		from tracking_timestamps
		left join process_ranges_to_conser T5_T4
		on T5_T4.valid_range_start_id > tracking_timestamps.T4
		and T5_T4.valid_range_end < tracking_timestamps.T5
		left join process_ranges_not_to_consider
		on  T5_T4.valid_range_start_id > process_ranges_not_to_consider.start_blocking
		and process_ranges_not_to_consider.end_blocking > T5_T4.valid_range_end
		and process_ranges_not_to_consider.process_id = tracking_timestamps.process_id
		left join process_ranges_not_to_consider partial_blok_start
		on  T5_T4.valid_range_start_id::Date = partial_blok_start.start_blocking::DATE
		and T5_T4.valid_range_start_id < partial_blok_start.start_blocking
		and T5_T4.valid_range_end > partial_blok_start.start_blocking
		and partial_blok_start.process_id = tracking_timestamps.process_id
		left join process_ranges_not_to_consider partial_blok_end
		on  T5_T4.valid_range_end::Date = partial_blok_end.end_blocking::DATE
		and T5_T4.valid_range_start_id < partial_blok_end.end_blocking
		and T5_T4.valid_range_end > partial_blok_end.end_blocking
		and partial_blok_end.process_id = tracking_timestamps.process_id
		where process_ranges_not_to_consider.process_id is null
		group by tracking_timestamps.process_id
)
--final select
	select 
	distinct 
	in_day_ranges.process_id,
	t0,
	t1,
	t2,
	t3,
	t4,
	t5,
	case
		when t0 is null then null
		when t0::date = t1::date and T0_range_id = T1_range_id --t1 and t0 are in the same range
			then (extract (epoch  from t1 - t0 )/60)::int
		    -coalesce((case when stop_01.start_blocking > t0 and stop_01.end_blocking < t1 then 
		    	(extract (epoch  from stop_01.end_blocking - stop_01.start_blocking )/60)::int end), 0)
		    -coalesce((case when stop_01.start_blocking < t0 then 
		    	(extract (epoch  from stop_01.end_blocking - t0 )/60)::int end), 0) 
		    -coalesce((case when stop_01.end_blocking > t1 then 
		    	(extract (epoch  from t1 - stop_01.start_blocking )/60)::int end), 0)
		else (extract (epoch  from coalesce(T1_T0.KPI_T1_T0, '0 Minutes'::interval) + coalesce(in_day_ranges.KPI_T1_T0, '0 Minutes'::interval) )/60)::int end 
	as KPI_T1_T0,
		
	case
		when t2 is null then null
		when t0::date = t2::date and T0_range_id = T2_range_id
			then (extract (epoch  from t2 - t0 )/60)::int 
		    -coalesce((case when stop_02.start_blocking > t0 and stop_02.end_blocking < t2 then 
		    	(extract (epoch  from stop_02.end_blocking - stop_02.start_blocking )/60)::int end), 0) 
		    -coalesce((case when stop_02.start_blocking < t0 then 
		    	(extract (epoch  from stop_02.end_blocking - t0 )/60)::int end), 0)
		    -coalesce((case when stop_02.end_blocking > t2 then 
		    	(extract (epoch  from t2 - stop_02.start_blocking )/60)::int end), 0)
		else (extract (epoch  from coalesce(T2_T0.KPI_T2_T0, '0 Minutes'::interval) + coalesce(in_day_ranges.KPI_T2_T0, '0 Minutes'::interval) )/60)::int end 
	as KPI_T2_T0,
	
	case
		when t3 is null then null
	    when t0::date = t3::date and T0_range_id = T3_range_id
			then (extract (epoch  from t3 - t0 )/60)::int 
		    -coalesce((case when stop_03.start_blocking > t0 and stop_03.end_blocking < t3 then 
		    	(extract (epoch  from stop_03.end_blocking - stop_03.start_blocking )/60)::int end), 0)
		    -coalesce((case when stop_03.start_blocking < t0 then 
		    	(extract (epoch  from stop_03.end_blocking - t0 )/60)::int end), 0)
		    -coalesce((case when stop_03.end_blocking > t3 then 
		    	(extract (epoch  from t3 - stop_03.start_blocking )/60)::int end), 0)
		else (extract (epoch  from coalesce(T3_T0.KPI_T3_T0, '0 Minutes'::interval) + coalesce(in_day_ranges.KPI_T3_T0, '0 Minutes'::interval) )/60)::int end 
	as KPI_T3_T0,
	
	case
	    when t5 is null then null
	    when t4::date = t5::date and T4_range_id = T5_range_id
			then (extract (epoch  from t5 - t4 )/60)::int 
		    -coalesce((case when stop_45.start_blocking > t4 and stop_45.end_blocking < t5 then 
		    	(extract (epoch  from stop_45.end_blocking - stop_45.start_blocking )/60)::int end), 0)
		    -coalesce((case when stop_45.start_blocking < t4 then 
		    	(extract (epoch  from stop_45.end_blocking - t4 )/60)::int end), 0)
		    -coalesce((case when stop_45.end_blocking > t5 then 
		    	(extract (epoch  from t5 - stop_45.start_blocking )/60)::int end), 0)
		else (extract (epoch  from coalesce(T5_T4.KPI_T5_T4, '0 Minutes'::interval) + coalesce(in_day_ranges.KPI_T5_T4, '0 Minutes'::interval) )/60)::int end 
	as KPI_T5_T4,
	
	current_date
	
	from 
	in_day_ranges
	left join T1_T0
	on in_day_ranges.process_id = T1_T0.process_id
	left join T2_T0
	on in_day_ranges.process_id = T2_T0.process_id
	left join T3_T0
	on in_day_ranges.process_id = T3_T0.process_id
	left join T5_T4
	on in_day_ranges.process_id = T5_T4.process_id
	left join process_ranges_not_to_consider stop_01
    on stop_01.process_id = in_day_ranges.process_id
    and stop_01.end_blocking > in_day_ranges.t0
    and stop_01.start_blocking < in_day_ranges.t1
	left join process_ranges_not_to_consider stop_02
    on stop_02.process_id = in_day_ranges.process_id
    and stop_02.end_blocking > in_day_ranges.t0
    and stop_02.start_blocking < in_day_ranges.t2
	left join process_ranges_not_to_consider stop_03
    on stop_03.process_id = in_day_ranges.process_id
    and stop_03.end_blocking > in_day_ranges.t0
    and stop_03.start_blocking < in_day_ranges.t3
	left join process_ranges_not_to_consider stop_45
    on stop_45.process_id = in_day_ranges.process_id
    and stop_45.end_blocking > in_day_ranges.t4
    and stop_45.start_blocking < in_day_ranges.t5
