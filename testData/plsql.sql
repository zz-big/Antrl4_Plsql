select
t3.name as project_name,
t3.description project_description,
t2.modify_by modify_user,
TIMESTAMPDIFF(SECOND,t1.start_time,t1.end_time) execute_time,
t2.name  process_name,
t2.description process_description,
CASE
WHEN t1.state = 0 THEN
        'commit succeeded'
WHEN t1.state = 1 THEN
        'running'
WHEN t1.state = 2 THEN
        'commit succeeded'
WHEN t1.state = 3 THEN
        'pause'
WHEN t1.state = 4 THEN
        'prepare to stop'
WHEN t1.state = 5 THEN
        'stop'
WHEN t1.state = 6 THEN
        'fail'
WHEN t1.state = 7 THEN
        'succeed'
WHEN t1.state = 8 THEN
        'need fault tolerance'
WHEN t1.state = 9 THEN
        'kill'
WHEN t1.state = 10 THEN
        'wait for thread'
WHEN t1.state = 11 THEN
        'wait for dependency to complete'
ELSE '状态为空'
END as task_state,
t1.name ,
t1.task_type,
t1.process_definition_id,
t1.process_instance_id,
t1.task_json ,
t1.submit_time ,
t1.start_time ,
t1.end_time ,
t1.host   ,
t1.app_link ,
t1.executor_id
from  (select * from dolphinscheduler.t_ds_task_instance   where DATE_FORMAT(submit_time,'%Y-%m-%d') = date_sub(curdate(),interval 1 day) ) t1
left join dolphinscheduler.t_ds_process_definition t2
left  join  dolphinscheduler.t_ds_project t3
on t2.project_id = t3.id
on  t1.process_definition_id = t2.id
order by t3.name desc