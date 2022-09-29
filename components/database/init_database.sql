-----------------------------------------------------------------------------
-- CoSIMS PostgreSQL configuration
-- This file contains some SQL specific to PostgreSQL.
-----------------------------------------------------------------------------

create schema cosims;

-----------------------------------------------------------------------------

create extension if not exists "uuid-ossp";

-----------------------------------------------------------------------------

/*
Log level status, expressed as an integer with the same values as in logging.py,
so we can request only the messages with e.g. status > WARNING
*/
create table cosims.log_levels (

  id smallint not null unique,
  name text not null unique
);
insert into cosims.log_levels values
  (50, 'CRITICAL'),
  (40, 'ERROR'),
  (30, 'WARNING'),
  (20, 'INFO'),
  (10, 'DEBUG');

/*
Job status.
*/
create table cosims.job_status (

  id smallint not null unique,
  name text not null unique
);
insert into cosims.job_status values -- CAUTION: values must be the same as in the job_status.py enum module.
  (1, 'initialized'),
  (2, 'configured'),
  (3, 'ready'),
  (4, 'queued'),
  (5, 'started'),
  (6, 'pre_processing'),
  (7, 'processing'),
  (8, 'post_processing'),
  (9, 'processed'),
  (10, 'start_publication'),
  (11, 'published'),
  (12, 'done'),
  (13, 'internal_error'),
  (14, 'external_error'),
  (15, 'error_checked'),
  (16, 'cancelled');

/*
Parent table for the CoSIMS jobs. Holds the attributes that are shared by all the job types.
Implemented by the Job Python class.
*/
create table cosims.parent_jobs (

  id bigserial primary key,
  unique_id uuid default uuid_generate_v4(),
  name text,
  priority text check (
    priority in (
      'nrt',
      'delayed',
      'reprocessing'
    )
  ),
  nomad_id text,
  tile_id text not null,
  next_log_level smallint references cosims.log_levels(id) not null,
  next_log_file_path text,
  print_to_orch boolean,
  last_status_id smallint,
  last_status_change_id bigint,
  last_status_change_date timestamp,
  last_status_error_subtype text,
  error_raised boolean default false,
  si_processing_image text
);

/*
Table for FSC RLIE.
Implemented by the FscRlieJob Python class.
*/
create table cosims.fsc_rlie_jobs (

  id bigserial primary key,
  fk_parent_job_id bigint references cosims.parent_jobs(id) on delete cascade not null unique,
  nrt boolean,
  l1c_id text not null,
  l1c_id_list text,
  l1c_reference_job boolean default true,
  l1c_cloud_cover decimal not null,
  l1c_snow_cover decimal not null,
  l1c_path text not null,
  l1c_path_list text,
  l2a_path_in text,
  l2a_path_out text,
  save_full_l2a boolean default false,
  job_id_for_last_valid_l2a decimal,
  l2a_status text check (
    l2a_status in (
      'pending',
      'generated',
      'generation_aborted',
      'deleted'
    )
  ),
  n_jobs_run_since_last_init decimal,
  n_l2a_produced_since_last_init decimal,
  dtm_path text,
  fsc_infos json,
  rlie_infos json,
  fsc_path text,
  rlie_path text,
  measurement_date timestamp,
  l1c_sensing_time timestamp,
  l1c_esa_creation_date timestamp,
  l1c_esa_publication_date timestamp,
  l1c_dias_publication_date timestamp,
  fsc_completion_date timestamp,
  rlie_completion_date timestamp,
  fsc_json_publication_date timestamp,
  rlie_json_publication_date timestamp,
  maja_mode text check (
    maja_mode in (
      'nominal',
      'backward',
      'init'
    )
  ),
  maja_threads smallint default 1,
  maja_other_params jsonb,
  maja_return_code smallint,
  backward_reprocessing_run boolean,
  reprocessing_context text default 'None',
  -- Ensure only 1 job will be created for a given L1C in NRT context, and allow to
  -- store several reprocessing jobs for a given L1C with different reprocessing contexts
  unique (l1c_id, nrt, reprocessing_context)
);

/*
Table for PSA ARLIE.
Implemented by the PsaArlieJob Python class.
*/
create table cosims.psa_arlie_jobs (

  id serial primary key,
  reprocessing boolean,
  fk_parent_job_id int references cosims.parent_jobs(id) on delete cascade not null unique,
  request_id int,
  product_type text check(
    product_type in (
      'PSA-WGS84',
      'PSA-LAEA',
      'ARLIE'
    )
  ),
  hydro_year timestamp,
  month timestamp,
  first_product_measurement_date timestamp,
  last_product_measurement_date timestamp,
  input_paths text,
  result_infos json,
  result_path text,
  result_completion_date timestamp,
  result_json_publication_date timestamp
);

/*
Table for WDS and SWS jobs.
Implemented by the WdsSwsJob Python class.
*/
create table cosims.sws_wds_jobs (

  id bigserial primary key,
  fk_parent_job_id bigint references cosims.parent_jobs(id) on delete cascade not null unique,
  nrt boolean,
  wds_infos json,
  wds_path text,
  wds_completion_date timestamp,
  wds_json_publication_date timestamp,
  sws_infos json,
  sws_path text,
  sws_completion_date timestamp,
  sws_json_publication_date timestamp,
  measurement_date timestamp,
  s1_id_list text,
  s1_path_list text,
  s1_esa_publication_latest_date timestamp,
  s1_dias_publication_latest_date timestamp,
  assembly_id text,
  assembly_master_job_id bigint, --self.id if master or other_job.id
  assembly_status text check (
    assembly_status in (
      'pending',
      'generated',
      'empty',
      'generation_aborted',
      'deleted'
    )
  ),
  assembly_params json,
  assembly_path text,
  assembly_return_code int,
  assembly_reference_job boolean,
  fsc_id_list text,
  fsc_path_list text,
  fsc_creation_latest_date timestamp,
  fsc_publication_latest_date timestamp,
  reprocessing_context text default 'None',
  -- Ensure only 1 job will be created for a given s1_id_list in NRT context, and allow to
  -- store several reprocessing jobs with different reprocessing contexts
  unique (s1_id_list, fsc_id_list, nrt, reprocessing_context)
);

/*
Table for GFSC jobs.
Implemented by the GfscJob Python class.
*/
create table cosims.gfsc_jobs (

  id bigserial primary key,
  fk_parent_job_id bigint references cosims.parent_jobs(id) on delete cascade not null unique,
  nrt boolean,
  missions text,
  triggering_product_id text,
  triggering_product_publication_date timestamp,
  fsc_id_list text,
  fsc_publication_date_list text,
  fsc_measurement_date_list text,
  wds_id_list text,
  wds_publication_date_list text,
  wds_measurement_date_list text,
  sws_id_list text,
  sws_publication_date_list text,
  sws_measurement_date_list text,
  gfsc_id_list text,
  obsolete_product_id_list text,
  gfsc_infos json,
  gfsc_id text,
  gfsc_path text,
  sensing_start_date timestamp,
  sensing_end_date timestamp,
  completion_date timestamp,
  product_date timestamp,
  curation_timestamp timestamp,
  aggregation_timespan smallint,
  gfsc_json_publication_date timestamp,
  overriding_job_id text,
  reprocessing_context text default 'None',
  -- Ensure only 1 job will be created for a given gfsc in NRT context, and allow to
  -- store several reprocessing jobs with different reprocessing contexts
  unique (gfsc_id, nrt, reprocessing_context)
);

/*
Table for RLIE S1 jobs.
Implemented by the RlieS1Job Python class.
*/
create table cosims.rlies1_jobs (

    id bigserial primary key,
    fk_parent_job_id bigint references cosims.parent_jobs(id) on delete cascade not null unique,
    s1grd_id text,
    product_path text,
    measurement_date_start timestamp,
    measurement_date_end timestamp,
    s1grd_dias_publication_date timestamp,
    s1grd_esa_publication_date timestamp,
    s2tile_ids_json text,
    rlies1_products_completion_date timestamp,
    rlies1_product_paths_json text,
    rlies1_product_json_submitted_json text,
    rlies1_products_publication_date timestamp,
    reprocessing_context text,
  -- Ensure only 1 job will be created for a given s1grd in NRT context, and allow to
  -- store several reprocessing jobs with different reprocessing contexts
  unique (s1grd_id, measurement_date_start, s1grd_dias_publication_date, reprocessing_context)
);

/*
Table for RLIE S1S2 jobs.
Implemented by the RlieS1S2Job Python class.
*/
create table cosims.rlies1s2_jobs (

    id bigserial primary key,
    fk_parent_job_id bigint references cosims.parent_jobs(id) on delete cascade not null unique,
    process_date timestamp,
    tile_id_dup text,
    rlies1_product_paths_json text,
    rlies1_publication_latest_date timestamp,
    rlies2_product_paths_json text,
    rlies2_publication_latest_date timestamp,
    measurement_date_rlies1s2 timestamp,
    rlies1s2_completion_date timestamp,
    rlies1s2_path text,
    rlies1s2_json_submitted_json text,
    publication_date_rlies1s2 timestamp,
    reprocessing_context text,
  -- Ensure only 1 job will be created for a given rlies1 and rlies2 context, and allow to
  -- store several reprocessing jobs with different reprocessing contexts
  unique (process_date, tile_id_dup)
);

/*
Table for test jobs, jobs for integration validation purpose.
Implemented by the TestJob Python class.
*/
create table cosims.test_jobs (

  id serial primary key,
  fk_parent_job_id int references cosims.parent_jobs(id) on delete cascade not null unique,
  measurement_date timestamp,
  completion_date timestamp
);

/*
Any simple job other than FSC/RLIE, PSA or ARLIE, e.g. the job creation,
configuration and execution processes.
Used only to attach log messages to these processes.
Implemented by the OtherJob Python class.
*/
create table cosims.other_jobs (

  id bigserial primary key,
  fk_parent_job_id bigint references cosims.parent_jobs(id) on delete cascade not null unique
);

/*
Information about a job execution.
1 to many relationship between 1 job and many executions:
each job can be executed several times.
Implemented by the ExecutionInfo Python class.
*/
create table cosims.execution_info (

  id bigserial primary key,
  fk_parent_job_id bigint references cosims.parent_jobs(id) on delete cascade not null,
  min_log_level smallint references cosims.log_levels(id) not null,
  log_file_path text
);

/*
Message attached to a job execution.
Many to many relationship between many job executions and messages:
Each job can have many messages, and each unique message can be attached
to many executions (identical messages are not duplicated).
Implemented by the ExecutionMessage Python class.
*/
create table cosims.execution_messages (

  id bigserial primary key,
  body text not null
);

/*
Implemented by the ExecutionInfoToMessage Python class.
Note: fk_parent_job_id == execution_info.fk_parent_job_id. It is duplicated here to be used by PostgREST.
*/
create table cosims.execution_info_to_messages (

  id bigserial primary key,
  fk_execution_info_id bigint references cosims.execution_info(id) on delete cascade not null,
  fk_execution_message_id bigint references cosims.execution_messages(id) on delete cascade not null,
  fk_parent_job_id bigint references cosims.parent_jobs(id) on delete cascade not null,
  time timestamp not null,
  log_level smallint references cosims.log_levels(id) not null
);

/*
One entry (=status change) in the job status history, with the status change date.
*/
create table cosims.job_status_changes (

  id bigserial primary key,
  fk_parent_job_id bigint references cosims.parent_jobs(id) on delete cascade not null,
  time timestamp default current_timestamp,
  error_subtype text,
  error_message text,
  job_status smallint references cosims.job_status(id) not null
);

/*
Table for PSA ARLIE job creation request.
Implemented by the JobCreationRequest Python class.
*/
create table cosims.job_creation_request (

  id serial primary key,
  request_status text check(
    request_status in (
      'open',
      'closed',
      'error'
    )
  ),
  create_job text check(
    create_job in (
      'PSA-WGS84',
      'PSA-LAEA',
      'ARLIE'
    )
  ) not null,
  hydro_year timestamp,
  month timestamp
);

/*
Table for external parameters.
Implemented by the ExternalParameters Python class.

Modification in this table MUST be reported in the table cosims.system_parameters_history
and trigger cosims.system_parameters_history_trigger
*/
create table cosims.system_parameters (

  id bigserial primary key,
  max_number_of_worker_instances smallint default 210,
  max_number_of_vcpus smallint default 920,
  max_time_for_worker_without_nomad_allocation smallint default 15,
  max_ratio_of_vcpus_to_be_used float default 0.95,
  max_number_of_extra_large_worker_instances smallint default 2,
  job_types_list text default '["fsc_rlie_job", "rlies1_job", "rlies1s2_job", "sws_wds_job", "gfsc_job"]',
  s1_search_default_duration_in_days smallint default 7,
  s1_processing_start_date timestamp default '2021-06-24',
  s2_search_default_duration_in_days smallint default 7,
  s2_processing_start_date timestamp default '2020-05-01',
  gapfilling_search_default_duration_in_days smallint default 7,
  maja_consecutive_jobs_threshold_value smallint default 60,
  maja_backward_required_job_number smallint default 8,
  activate_backward_reprocessing boolean default true,
  rabbitmq_communication_endpoint text default 'hidden_value',
  docker_image_for_si_processing text default 'hidden_value',
  docker_image_for_test_job_processing text default 'hidden_value',
  docker_image_for_rliepart2_processing text default 'hidden_value',
  docker_image_for_gfsc_processing text default 'hidden_value',
  docker_image_for_sws_wds_processing text default 'hidden_value',
  worker_init_package_tag text default 'hidden_value',
  ssp_aux_version text default 'V20211119',
  job_creation_loop_sleep smallint default 5,
  job_configuration_loop_sleep smallint default 5,
  job_execution_loop_sleep smallint default 5,
  job_publication_loop_sleep smallint default 5,
  rlies1s2_min_delay_from_end_of_day_hours smallint default 8,
  rlies1s2_max_delay_from_end_of_day_hours_wait_for_rlie_products smallint default 24,
  rlies1s2_min_search_window_days smallint default 7,
  rlies1s2_max_search_window_days smallint default 31,
  rlies1s2_max_search_window_days_absolute smallint,
  rlies1s2_earliest_date timestamp default '2021-05-01',
  rlies1s2_sleep_seconds_between_loop bigint,
  gfsc_daily_jobs_creation_start_date timestamp,
  constraint onerow_uni check (id = 1) -- ensure only one row will be created
);

/*
History table of cosims.system_parameters parameters.
On update on cosims.system_parameters, a row with the new values is inserted here by a trigger
*/
create table cosims.system_parameters_history (
  history_id bigserial primary key,
  time timestamp not null default current_timestamp, -- history timestamps
  -- Every fields bellow are the fields from cosims.system_parameters
  id bigint,
  max_number_of_worker_instances smallint,
  max_number_of_vcpus smallint,
  max_time_for_worker_without_nomad_allocation smallint,
  max_ratio_of_vcpus_to_be_used float,
  max_number_of_extra_large_worker_instances smallint,
  job_types_list text,
  s1_search_default_duration_in_days smallint,
  s1_processing_start_date timestamp,
  s2_search_default_duration_in_days smallint,
  s2_processing_start_date timestamp,
  gapfilling_search_default_duration_in_days smallint,
  maja_consecutive_jobs_threshold_value smallint,
  maja_backward_required_job_number smallint,
  activate_backward_reprocessing boolean,
  rabbitmq_communication_endpoint text,
  docker_image_for_si_processing text,
  docker_image_for_test_job_processing text,
  docker_image_for_rliepart2_processing text,
  docker_image_for_gfsc_processing text,
  docker_image_for_sws_wds_processing text,
  worker_init_package_tag text,
  ssp_aux_version text,
  job_creation_loop_sleep smallint,
  job_configuration_loop_sleep smallint,
  job_execution_loop_sleep smallint,
  job_publication_loop_sleep smallint,
  rlies1s2_min_delay_from_end_of_day_hours smallint,
  rlies1s2_max_delay_from_end_of_day_hours_wait_for_rlie_products smallint,
  rlies1s2_min_search_window_days smallint,
  rlies1s2_max_search_window_days smallint,
  rlies1s2_max_search_window_days_absolute smallint,
  rlies1s2_earliest_date timestamp,
  rlies1s2_sleep_seconds_between_loop bigint,
  gfsc_daily_jobs_creation_start_date timestamp
);

/* insertion trigger into cosims.system_parameters_history */
create function cosims.system_parameters_history_trigger() returns trigger as $system_parameters_history_trigger$
begin
  insert into cosims.system_parameters_history (
    id,
    max_number_of_worker_instances,
    max_number_of_vcpus,
    max_time_for_worker_without_nomad_allocation,
    max_ratio_of_vcpus_to_be_used,
    max_number_of_extra_large_worker_instances,
    job_types_list,
    s1_search_default_duration_in_days,
    s1_processing_start_date,
    s2_search_default_duration_in_days,
    s2_processing_start_date,
    gapfilling_search_default_duration_in_days,
    maja_consecutive_jobs_threshold_value,
    maja_backward_required_job_number,
    activate_backward_reprocessing,
    rabbitmq_communication_endpoint,
    docker_image_for_si_processing,
    docker_image_for_test_job_processing,
    docker_image_for_rliepart2_processing,
    docker_image_for_gfsc_processing,
    docker_image_for_sws_wds_processing,
    worker_init_package_tag,
    ssp_aux_version,
    job_creation_loop_sleep,
    job_configuration_loop_sleep,
    job_execution_loop_sleep,
    job_publication_loop_sleep,
    rlies1s2_min_delay_from_end_of_day_hours,
    rlies1s2_max_delay_from_end_of_day_hours_wait_for_rlie_products,
    rlies1s2_min_search_window_days,
    rlies1s2_max_search_window_days,
    rlies1s2_max_search_window_days_absolute,
    rlies1s2_earliest_date,
    rlies1s2_sleep_seconds_between_loop,
    gfsc_daily_jobs_creation_start_date
  ) values (
    new.id,
    new.max_number_of_worker_instances,
    new.max_number_of_vcpus,
    new.max_time_for_worker_without_nomad_allocation,
    new.max_ratio_of_vcpus_to_be_used,
    new.max_number_of_extra_large_worker_instances,
    new.job_types_list,
    new.s1_search_default_duration_in_days,
    new.s1_processing_start_date,
    new.s2_search_default_duration_in_days,
    new.s2_processing_start_date,
    new.gapfilling_search_default_duration_in_days,
    new.maja_consecutive_jobs_threshold_value,
    new.maja_backward_required_job_number,
    new.activate_backward_reprocessing,
    new.rabbitmq_communication_endpoint,
    new.docker_image_for_si_processing,
    new.docker_image_for_test_job_processing,
    new.docker_image_for_rliepart2_processing,
    new.docker_image_for_gfsc_processing,
    new.docker_image_for_sws_wds_processing,
    new.worker_init_package_tag,
    new.ssp_aux_version,
    new.job_creation_loop_sleep,
    new.job_configuration_loop_sleep,
    new.job_execution_loop_sleep,
    new.job_publication_loop_sleep,
    new.rlies1s2_min_delay_from_end_of_day_hours,
    new.rlies1s2_max_delay_from_end_of_day_hours_wait_for_rlie_products,
    new.rlies1s2_min_search_window_days,
    new.rlies1s2_max_search_window_days,
    new.rlies1s2_max_search_window_days_absolute,
    new.rlies1s2_earliest_date,
    new.rlies1s2_sleep_seconds_between_loop,
    new.gfsc_daily_jobs_creation_start_date
  );
  return new;
end
  $system_parameters_history_trigger$ language plpgsql;

/* system_parameters history trigger declaration */
create trigger system_parameters_history_trigger before insert or update on cosims.system_parameters
for each row execute procedure cosims.system_parameters_history_trigger();

-- Initialize system_parameters table with id = 1
-- this will also create the first history entry
insert into cosims.system_parameters values (1);

/*
Table for dashboard health check.
*/
create table cosims.health ();


/* Index on foreign keys */
-- parent_jobs table
CREATE INDEX parent_jobs_next_log_level ON cosims.parent_jobs USING btree (next_log_level);
CREATE INDEX parent_jobs_last_status_id ON cosims.parent_jobs USING btree (last_status_id);
CREATE INDEX parent_jobs_last_status_change_id ON cosims.parent_jobs USING btree (last_status_change_id);
-- execution_info table
CREATE INDEX execution_info_fk_parent_job_id ON cosims.execution_info USING btree (fk_parent_job_id);
-- execution_info table
CREATE INDEX execution_info_min_log_level ON cosims.execution_info USING btree (min_log_level);
-- execution_info_to_messages table
CREATE INDEX execution_info_to_messages_fk_execution_info_id ON cosims.execution_info_to_messages USING btree (fk_execution_info_id);
CREATE INDEX execution_info_to_messages_fk_execution_message_id ON cosims.execution_info_to_messages USING btree (fk_execution_message_id);
CREATE INDEX execution_info_to_messages_fk_parent_job_id ON cosims.execution_info_to_messages USING btree (fk_parent_job_id);
CREATE INDEX execution_info_to_messages_log_level ON cosims.execution_info_to_messages USING btree (log_level);
-- job_status_changes table
CREATE INDEX job_status_changes_fk_parent_job_id ON cosims.job_status_changes USING btree (fk_parent_job_id);
CREATE INDEX job_status_changes_job_status ON cosims.job_status_changes USING btree (job_status);

/* Index for sorting the job list on the dashboard */
CREATE INDEX parent_jobs_tile_id ON cosims.parent_jobs USING btree (tile_id);
CREATE INDEX parent_jobs_last_status_change_date ON cosims.parent_jobs USING btree (last_status_change_date);
CREATE INDEX fsc_rlie_jobs_l1c_esa_publication_date ON cosims.fsc_rlie_jobs USING btree (l1c_esa_publication_date);
CREATE INDEX fsc_rlie_jobs_l1c_dias_publication_date ON cosims.fsc_rlie_jobs USING btree (l1c_dias_publication_date);
CREATE INDEX fsc_rlie_jobs_maja_mode ON cosims.fsc_rlie_jobs USING btree (maja_mode);

-----------------------------------------------------------------------------
-- SQL views

-- Regroup FSC/RLIE job, parent_job, and job_status info on the same view
create view cosims.fsc_rlie_jobs_view as
  select frj.*,  -- select all info relative to FSC/RLIE job
    pj.name,  -- select parent_job name
    pj.priority,  -- select parent_job priority
    pj.nomad_id,  -- select parent_job nomad_id
    pj.tile_id,  -- select parent_job tile_id
    pj.next_log_level,  -- select parent_job next_log_level
    pj.next_log_file_path,  -- select parent_job next_log_file_path
    pj.print_to_orch,  -- select parent_job print_to_orch
    pj.last_status_id as status_id, -- select parent_job last status id
    pj.last_status_change_id, -- select parent_job last status change id
    pj.last_status_change_date as status_change_time, -- select parent_job last status change date
    pj.error_raised as error_occurred, -- select parent_job error raised (if an error occurred during job lifecycle)
    pj.last_status_error_subtype as status_error_subtype, -- select job last status change error subtype
    js.name as status_name  -- select job last status name

  from cosims.fsc_rlie_jobs frj -- any information from fsc_rlie_jobs table
  join cosims.parent_jobs pj on (frj.fk_parent_job_id=pj.id) -- join info from adequat parent job from parent job table
  join cosims.job_status as js on (js.id=pj.last_status_id); -- retreive last status name from its id

-- Regroup Test job, parent_job, and job_status info on the same view
create view cosims.test_jobs_view as
  select tj.*,  -- select all info relative to Test job
    pj.name,  -- select parent_job name
    pj.priority,  -- select parent_job priority
    pj.nomad_id,  -- select parent_job nomad_id
    pj.next_log_level,  -- select parent_job next_log_level
    pj.next_log_file_path,  -- select parent_job next_log_file_path
    pj.last_status_id as status_id, -- select parent_job last status id
    pj.last_status_change_id, -- select parent_job last status change id
    pj.last_status_change_date as status_change_time, -- select parent_job last status change date
    pj.error_raised as error_occurred, -- select parent_job error raised (if an error occurred during job lifecycle)
    pj.last_status_error_subtype as status_error_subtype, -- select job last status change error subtype
    js.name as status_name  -- select job last status name

  from cosims.test_jobs tj -- any information from fsc_rlie_jobs table
  join cosims.parent_jobs pj on (tj.fk_parent_job_id=pj.id) -- join info from adequat parent job from parent job table
  join cosims.job_status as js on (js.id=pj.last_status_id); -- retreive last status name from its id

-- Regroup SWS_WDS job, parent_job, and job_status info on the same view
create view cosims.sws_wds_jobs_view as
  select swj.*,  -- select all info relative to XXXXXX job
    pj.name,  -- select parent_job name
    pj.priority,  -- select parent_job priority
    pj.nomad_id,  -- select parent_job nomad_id
    pj.tile_id,  -- select parent_job tile_id
    pj.next_log_level,  -- select parent_job next_log_level
    pj.next_log_file_path,  -- select parent_job next_log_file_path
    pj.print_to_orch,  -- select parent_job print_to_orch
    pj.last_status_id as status_id, -- select parent_job last status id
    pj.last_status_change_id, -- select parent_job last status change id
    pj.last_status_change_date as status_change_time, -- select parent_job last status change date
    pj.error_raised as error_occurred, -- select parent_job error raised (if an error occurred during job lifecycle)
    pj.last_status_error_subtype as status_error_subtype, -- select job last status change error subtype
    js.name as status_name  -- select job last status name

  from cosims.sws_wds_jobs swj -- any information from sws_wds_jobs table
  join cosims.parent_jobs pj on (swj.fk_parent_job_id=pj.id) -- join info from adequat parent job from parent job table
  join cosims.job_status as js on (js.id=pj.last_status_id); -- retreive last status name from its id

-- Regroup GFSC job, parent_job, and job_status info on the same view
create view cosims.gfsc_jobs_view as
  select gj.*,  -- select all info relative to XXXXXX job
    pj.name,  -- select parent_job name
    pj.priority,  -- select parent_job priority
    pj.nomad_id,  -- select parent_job nomad_id
    pj.tile_id,  -- select parent_job tile_id
    pj.next_log_level,  -- select parent_job next_log_level
    pj.next_log_file_path,  -- select parent_job next_log_file_path
    pj.print_to_orch,  -- select parent_job print_to_orch
    pj.last_status_id as status_id, -- select parent_job last status id
    pj.last_status_change_id, -- select parent_job last status change id
    pj.last_status_change_date as status_change_time, -- select parent_job last status change date
    pj.error_raised as error_occurred, -- select parent_job error raised (if an error occurred during job lifecycle)
    pj.last_status_error_subtype as status_error_subtype, -- select job last status change error subtype
    js.name as status_name  -- select job last status name

  from cosims.gfsc_jobs gj -- any information from gfsc_jobs table
  join cosims.parent_jobs pj on (gj.fk_parent_job_id=pj.id) -- join info from adequat parent job from parent job table
  join cosims.job_status as js on (js.id=pj.last_status_id); -- retreive last status name from its id

-- Regroup RLIE S1 job, parent_job, and job_status info on the same view
create view cosims.rlies1_jobs_view as
  select r1j.*,  -- select all info relative to XXXXXX job
    pj.name,  -- select parent_job name
    pj.priority,  -- select parent_job priority
    pj.nomad_id,  -- select parent_job nomad_id
    pj.tile_id,  -- select parent_job tile_id
    pj.next_log_level,  -- select parent_job next_log_level
    pj.next_log_file_path,  -- select parent_job next_log_file_path
    pj.print_to_orch,  -- select parent_job print_to_orch
    pj.last_status_id as status_id, -- select parent_job last status id
    pj.last_status_change_id, -- select parent_job last status change id
    pj.last_status_change_date as status_change_time, -- select parent_job last status change date
    pj.error_raised as error_occurred, -- select parent_job error raised (if an error occurred during job lifecycle)
    pj.last_status_error_subtype as status_error_subtype, -- select job last status change error subtype
    js.name as status_name  -- select job last status name

  from cosims.rlies1_jobs r1j -- any information from rlies1_jobs table
  join cosims.parent_jobs pj on (r1j.fk_parent_job_id=pj.id) -- join info from adequat parent job from parent job table
  join cosims.job_status as js on (js.id=pj.last_status_id); -- retreive last status name from its id

-- Regroup RLIE S1-S2 job, parent_job, and job_status info on the same view
create view cosims.rlies1s2_jobs_view as
  select r12j.*,  -- select all info relative to XXXXXX job
    pj.name,  -- select parent_job name
    pj.priority,  -- select parent_job priority
    pj.nomad_id,  -- select parent_job nomad_id
    pj.tile_id,  -- select parent_job tile_id
    pj.next_log_level,  -- select parent_job next_log_level
    pj.next_log_file_path,  -- select parent_job next_log_file_path
    pj.print_to_orch,  -- select parent_job print_to_orch
    pj.last_status_id as status_id, -- select parent_job last status id
    pj.last_status_change_id, -- select parent_job last status change id
    pj.last_status_change_date as status_change_time, -- select parent_job last status change date
    pj.error_raised as error_occurred, -- select parent_job error raised (if an error occurred during job lifecycle)
    pj.last_status_error_subtype as status_error_subtype, -- select job last status change error subtype
    js.name as status_name  -- select job last status name

  from cosims.rlies1s2_jobs r12j -- any information from rlies1s2_jobs table
  join cosims.parent_jobs pj on (r12j.fk_parent_job_id=pj.id) -- join info from adequat parent job from parent job table
  join cosims.job_status as js on (js.id=pj.last_status_id); -- retreive last status name from its id

-----------------------------------------------------------------------------
-- PostgREST needs some init, configuration...
-- This is mainly taken from the PostgREST documentation.
create role web_anonymous nologin;

grant usage on schema cosims to web_anonymous;
create role authenticator noinherit login password 'mysecretpassword';
grant web_anonymous to authenticator;

grant all on cosims.parent_jobs to web_anonymous;
grant select on cosims.parent_jobs to web_anonymous;
grant usage, select on sequence cosims.parent_jobs_id_seq to web_anonymous;

grant select on cosims.log_levels to web_anonymous;

grant select on cosims.job_status to web_anonymous;

grant all on cosims.fsc_rlie_jobs to web_anonymous;
grant select on cosims.fsc_rlie_jobs to web_anonymous;
grant usage, select on sequence cosims.fsc_rlie_jobs_id_seq to web_anonymous;

grant all on cosims.rlies1_jobs to web_anonymous;
grant select on cosims.rlies1_jobs to web_anonymous;
grant usage, select on sequence cosims.rlies1_jobs_id_seq to web_anonymous;

grant all on cosims.rlies1s2_jobs to web_anonymous;
grant select on cosims.rlies1s2_jobs to web_anonymous;
grant usage, select on sequence cosims.rlies1s2_jobs_id_seq to web_anonymous;

grant all on cosims.psa_arlie_jobs to web_anonymous;
grant select on cosims.psa_arlie_jobs to web_anonymous;
grant usage, select on sequence cosims.psa_arlie_jobs_id_seq to web_anonymous;

grant all on cosims.sws_wds_jobs to web_anonymous;
grant select on cosims.sws_wds_jobs to web_anonymous;
grant usage, select on sequence cosims.sws_wds_jobs_id_seq to web_anonymous;

grant all on cosims.gfsc_jobs to web_anonymous;
grant select on cosims.gfsc_jobs to web_anonymous;
grant usage, select on sequence cosims.gfsc_jobs_id_seq to web_anonymous;

grant all on cosims.test_jobs to web_anonymous;
grant select on cosims.test_jobs to web_anonymous;
grant usage, select on sequence cosims.test_jobs_id_seq to web_anonymous;

grant all on cosims.job_creation_request to web_anonymous;
grant select on cosims.job_creation_request to web_anonymous;
grant usage, select on sequence cosims.job_creation_request_id_seq to web_anonymous;

grant all on cosims.system_parameters_history to web_anonymous;
grant select on cosims.system_parameters_history to web_anonymous;
grant usage, select on sequence cosims.system_parameters_history_history_id_seq to web_anonymous;

grant all on cosims.system_parameters to web_anonymous;
grant select on cosims.system_parameters to web_anonymous;
grant usage, select on sequence cosims.system_parameters_id_seq to web_anonymous;

grant all on cosims.health to web_anonymous;
grant select on cosims.health to web_anonymous;

grant all on cosims.other_jobs to web_anonymous;
grant select on cosims.other_jobs to web_anonymous;
grant usage, select on sequence cosims.other_jobs_id_seq to web_anonymous;

grant all on cosims.execution_info to web_anonymous;
grant select on cosims.execution_info to web_anonymous;
grant usage, select on sequence cosims.execution_info_id_seq to web_anonymous;

grant all on cosims.execution_messages to web_anonymous;
grant select on cosims.execution_messages to web_anonymous;
grant usage, select on sequence cosims.execution_messages_id_seq to web_anonymous;

grant all on cosims.execution_info_to_messages to web_anonymous;
grant select on cosims.execution_info_to_messages to web_anonymous;
grant usage, select on sequence cosims.execution_info_to_messages_id_seq to web_anonymous;

grant all on cosims.job_status_changes to web_anonymous;
grant select on cosims.job_status_changes to web_anonymous;
grant usage, select on sequence cosims.job_status_changes_id_seq to web_anonymous;

grant all on cosims.fsc_rlie_jobs_view to web_anonymous;
grant select on cosims.fsc_rlie_jobs_view to web_anonymous;

grant all on cosims.test_jobs_view to web_anonymous;
grant select on cosims.test_jobs_view to web_anonymous;

grant all on cosims.sws_wds_jobs_view to web_anonymous;
grant select on cosims.sws_wds_jobs_view to web_anonymous;

grant all on cosims.gfsc_jobs_view to web_anonymous;
grant select on cosims.gfsc_jobs_view to web_anonymous;

grant all on cosims.rlies1_jobs_view to web_anonymous;
grant select on cosims.rlies1_jobs_view to web_anonymous;

grant all on cosims.rlies1s2_jobs_view to web_anonymous;
grant select on cosims.rlies1s2_jobs_view to web_anonymous;

-----------------------------------------------------------------------------
-- SQL functions

--
-- Get entries from a table and ID list

create function cosims.parent_jobs_with_ids (parent_job_ids bigint[])
  returns setof cosims.parent_jobs as $$
begin return query select * from cosims.parent_jobs pj where pj.id = any(parent_job_ids);
end $$ language plpgsql stable;

--
-- Get IDs

-- Return job creation parent job IDs (should be only one)
create function cosims.job_creation_ids()
  returns table (id bigint) as $$
begin
  return query select pj.id from cosims.parent_jobs pj where pj.name='job-creation';
end
  $$ language plpgsql immutable;

-- Return job configuration parent job IDs (should be only one)
create function cosims.job_configuration_ids()
  returns table (id bigint) as $$
begin
  return query select pj.id from cosims.parent_jobs pj where pj.name='job-configuration';
end
  $$ language plpgsql immutable;

-- Return job execution parent job IDs (should be only one)
create function cosims.job_execution_ids()
  returns table (id bigint) as $$
begin
  return query select pj.id from cosims.parent_jobs pj where pj.name='job-execution';
end
  $$ language plpgsql immutable;

--
-- Get messages

-- Get existing messages with the given body
create function cosims.messages_with_body (body_arg text)
  returns table (id bigint, body text) as $$
begin
  return query
  select em.id, em.body
  from execution_messages em
  where em.body = body_arg;
end
  $$ language plpgsql immutable;

--
-- Get messages

-- Get messages associated with parent job IDs
create function cosims.job_messages (parent_job_ids bigint[])
  returns table (parent_job_id bigint, job_name text, log_level text, "time" timestamp, body text) as $$
begin
  return query
  select pj.id, pj.name, ll.name, eim.time, em.body
  from cosims.parent_jobs pj
  join cosims.execution_info as ei on (ei.fk_parent_job_id=pj.id)
  join cosims.execution_info_to_messages as eim on (eim.fk_execution_info_id=ei.id)
  join cosims.execution_messages as em on (eim.fk_execution_message_id=em.id)
  join cosims.log_levels as ll on (ll.id=eim.log_level)
  where pj.id = any(parent_job_ids)
  order by eim.time asc;
end
  $$ language plpgsql immutable;


-- Return job creation messages
create function cosims.job_creation_messages()
  returns table (log_level text, "time" timestamp, body text) as $$
begin
  return query select * from cosims.job_messages (array (select * from cosims.job_creation_ids()));
end
  $$ language plpgsql immutable;

-- Return job configuration messages
create function cosims.job_configuration_messages()
  returns table (log_level text, "time" timestamp, body text) as $$
begin
  return query select * from cosims.job_messages (array (select * from cosims.job_configuration_ids()));
end
  $$ language plpgsql immutable;

-- Return job execution messages
create function cosims.job_execution_messages()
  returns table (log_level text, "time" timestamp, body text) as $$
begin
  return query select * from cosims.job_messages (array (select * from cosims.job_execution_ids()));
end
  $$ language plpgsql immutable;

--
-- Get statusNoSQL

-- Get all status changes associated with parent job IDs.
-- If job IDs are not defined: return results for all parent jobs.
create function cosims.job_status_history (
  parent_job_ids bigint[] default null
)
  returns table (parent_job_id bigint, job_name text, status smallint, status_name text, change_id bigint, "time" timestamp) as $$
  select pj.id, pj.name, js.id, js.name, jsc.id, jsc.time
  from cosims.job_status as js
  join cosims.job_status_changes as jsc on (js.id=jsc.job_status)
  join cosims.parent_jobs as pj on (jsc.fk_parent_job_id=pj.id)
  where (parent_job_ids is null) or (pj.id = any(parent_job_ids)) -- check the job ids if defined, else return all
  order by pj.id asc, jsc.id;
$$ language SQL stable;


-- Get last status changes associated with parent job IDs.
-- If job IDs are not defined: return results for all parent jobs.
-- If last status list is defined : only keep results with this these last status.
create function cosims.last_job_status_id (
  parent_job_ids bigint[] default null,
  last_status smallint[] default null
)
  returns table (parent_job_id bigint, status smallint, change_id bigint, "time" timestamp) as $$
  SELECT A.fk_parent_job_id, A.job_status, A.id, A.time FROM cosims.job_status_changes A join (
    SELECT fk_parent_job_id, max(id) as id FROM cosims.job_status_changes -- max(id) = last job status change
    WHERE parent_job_ids is null or fk_parent_job_id = ANY(parent_job_ids)
    GROUP BY fk_parent_job_id -- we cannot filter by status here because of the 'max' aggregation
  ) B on A.id = B.id -- join on job_status_changes id to allow filtering by status, excluding non last job
  WHERE last_status is null or A.job_status = ANY(last_status) -- filter by status
  ;
$$ language SQL stable;

-- Add status name and parent job name to cosims.last_job_status
create function cosims.last_job_status (
  parent_job_ids bigint[] default null,
  last_status smallint[] default null
)
  returns table (parent_job_id bigint, job_name text, status smallint, status_name text, change_id bigint, "time" timestamp) as $$
  select ljs.parent_job_id, pj.name, ljs.status, js.name, ljs.change_id, ljs.time
  from cosims.job_status as js
  join cosims.last_job_status_id(parent_job_ids, last_status) ljs on ljs.status = js.id
  join cosims.parent_jobs pj on pj.id = ljs.parent_job_id
  ;
$$ language SQL stable;

-- Get FSC/RLIE jobs with the given last status (can be a list)
create function cosims.fsc_rlie_jobs_with_last_status (
  last_status smallint[]
)
  returns setof cosims.fsc_rlie_jobs as $$
  select frj.*
  from cosims.fsc_rlie_jobs frj
  join cosims.parent_jobs pj on pj.id = frj.fk_parent_job_id
  WHERE pj.last_status_id = ANY(last_status)
$$ language SQL stable;

-- Get RLIES1 jobs with the given last status (can be a list)
create function cosims.rlies1_jobs_with_last_status (
  last_status smallint[]
)
  returns setof cosims.rlies1_jobs as $$
  select frj.*
  from cosims.rlies1_jobs frj
  join cosims.parent_jobs pj on pj.id = frj.fk_parent_job_id
  WHERE pj.last_status_id = ANY(last_status)
$$ language SQL stable;

-- Get RLIES1S2 jobs with the given last status (can be a list)
create function cosims.rlies1s2_jobs_with_last_status (
  last_status smallint[]
)
  returns setof cosims.rlies1s2_jobs as $$
  select frj.*
  from cosims.rlies1s2_jobs frj
  join cosims.parent_jobs pj on pj.id = frj.fk_parent_job_id
  WHERE pj.last_status_id = ANY(last_status)
$$ language SQL stable;

-- Get GFSC jobs with the given last status (can be a list)
create function cosims.gfsc_jobs_with_last_status (
  last_status smallint[]
)
  returns setof cosims.gfsc_jobs as $$
  select frj.*
  from cosims.gfsc_jobs frj
  join cosims.parent_jobs pj on pj.id = frj.fk_parent_job_id
  WHERE pj.last_status_id = ANY(last_status)
$$ language SQL stable;

-- Get SWS_WDS jobs with the given last status (can be a list)
create function cosims.sws_wds_jobs_with_last_status (
  last_status smallint[]
)
  returns setof cosims.sws_wds_jobs as $$
  select swj.* 
  from cosims.sws_wds_jobs swj
  join cosims.parent_jobs pj on pj.id = swj.fk_parent_job_id
  WHERE pj.last_status_id = ANY(last_status)
$$ language SQL stable;


-- Get FSC/RLIE jobs with the given last status (can be a list), tile_id, and within measurement_date range
create function cosims.fsc_rlie_jobs_with_status_tile_date (
  last_status smallint[],
  tile_id_ref text,
  low_time_bound timestamp,
  high_time_bound timestamp
)
  returns setof cosims.fsc_rlie_jobs as $$
  select frj.*
  from cosims.fsc_rlie_jobs_with_last_status(last_status) frj
  join cosims.parent_jobs pj on frj.fk_parent_job_id=pj.id -- from the parent_job table:
  where pj.tile_id=tile_id_ref -- select only jobs with the specified tile_id
  and frj.measurement_date>=low_time_bound
  and frj.measurement_date<high_time_bound
$$ language SQL stable;



--  get_jobs_within_measurement_date : select jobs between dates

-- needed for RLIE S1 jobs
create function cosims.select_date_rlies1_s1grd_dias_publication_date (
  start_date timestamp,
  end_date timestamp
)
  returns setof cosims.rlies1_jobs as $$
  select frj.*
  from cosims.rlies1_jobs frj
  where frj.s1grd_dias_publication_date>=start_date
  and frj.s1grd_dias_publication_date<end_date
$$ language SQL stable;

-- needed for RLIE S1+S2 jobs
create function cosims.select_date_rlies1_rlies1_products_publication_date (
  start_date timestamp,
  end_date timestamp
)
  returns setof cosims.rlies1_jobs as $$
  select frj.*
  from cosims.rlies1_jobs frj
  where frj.rlies1_products_publication_date>=start_date
  and frj.rlies1_products_publication_date<end_date
$$ language SQL stable;

create function cosims.select_date_rlies1_measurement_date_start (
  start_date timestamp,
  end_date timestamp
)
  returns setof cosims.rlies1_jobs as $$
  select frj.*
  from cosims.rlies1_jobs frj
  where frj.measurement_date_start>=start_date
  and frj.measurement_date_start<end_date
$$ language SQL stable;

-- RLIE S1+S2
create function cosims.select_date_rlies1s2_process_date (
  start_date timestamp,
  end_date timestamp
)
  returns setof cosims.rlies1s2_jobs as $$
  select frj.*
  from cosims.rlies1s2_jobs frj
  where frj.process_date>=start_date
  and frj.process_date<end_date
$$ language SQL stable;

-- FSC+RLIE
create function cosims.select_date_fsc_rlie_rlie_json_publication_date (
  start_date timestamp,
  end_date timestamp
)
  returns setof cosims.fsc_rlie_jobs as $$
  select frj.*
  from cosims.fsc_rlie_jobs frj
  where frj.rlie_json_publication_date>=start_date
  and frj.rlie_json_publication_date<end_date
$$ language SQL stable;

create function cosims.select_date_fsc_rlie_measurement_date (
  start_date timestamp,
  end_date timestamp
)
  returns setof cosims.fsc_rlie_jobs as $$
  select frj.*
  from cosims.fsc_rlie_jobs frj
  where frj.measurement_date>=start_date
  and frj.measurement_date<end_date
$$ language SQL stable;

create function cosims.select_date_sws_wds_measurement_date (
  start_date timestamp,
  end_date timestamp
)
  returns setof cosims.sws_wds_jobs as $$
  select swj.*
  from cosims.sws_wds_jobs swj
  where swj.measurement_date>=start_date
  and swj.measurement_date<=end_date
$$ language SQL stable;


-- Get FSC/RLIE jobs with the given last status (can be a list), tile_id, and within measurement_date range
-- TODO check if possible to optimize the code
create function cosims.fsc_rlie_jobs_with_tile_date (
  tile_id_ref text,
  measurement_time timestamp
)
  returns setof cosims.fsc_rlie_jobs as $$
begin
  return query
  select frj.*
  from cosims.fsc_rlie_jobs frj
  join (
    select id from cosims.parent_jobs pj -- from the parent_job table:
    where (pj.tile_id=tile_id_ref) -- select only jobs with the specified tile_id
  ) pjt
  on (frj.fk_parent_job_id=pjt.id) -- join the jobs status result with tile_id selection

  where (frj.measurement_date=measurement_time); -- select only jobs with suitable measurement date

end
  $$ language plpgsql immutable;


-- Get FSC/RLIE job with most recent measurement time, inferior to the high time bound,
--  and with the specified tile id, only select jobs which didn't fail to produce a L2A yet
create function cosims.get_last_job_with_usable_l2a (
  tile_id_ref text,
  high_measurement_time_bound timestamp,
  high_esa_time_bound timestamp,
  l1c_id_ref text,
  allow_codated_jobs boolean,
  backward_triggered_job boolean
)
  returns setof cosims.fsc_rlie_jobs as $$
  select frj.*
  from cosims.fsc_rlie_jobs frj
  join cosims.parent_jobs pjt -- from the parent_job table:
  on frj.fk_parent_job_id=pjt.id -- join the FSC/RLIE table with tile_id selection
  where (
    pjt.tile_id=tile_id_ref -- select only jobs with the specified tile_id
    and frj.measurement_date<=high_measurement_time_bound -- select only jobs with suitable measurement date
    and frj.l1c_esa_creation_date<high_esa_time_bound -- and with suitable esa creation date
    and frj.l1c_id!=l1c_id_ref -- and with a different l1c ID
    and (
      frj.l2a_status='generated' -- and which produced a L2A
      or (frj.l2a_status='pending' and pjt.last_status_id not in (13,14,16)) -- or will produce a L2A
      or (backward_triggered_job is true and frj.l2a_status='pending' and pjt.last_status_id!=16) -- or we allow jobs in error for backward triggered jobs not to break temporal series, as a backward triggered job should always generate a product
    )
    and (frj.l1c_reference_job is true or allow_codated_jobs is true) -- and allow or not (depending on 'allow_codated_jobs') the job returned to be an old codated version of the current one
    )
 order by frj.measurement_date desc, frj.l1c_esa_creation_date desc, pjt.last_status_change_id desc -- order selected jobs with descending parent job status change id, then esa creation date and then measurement date
  limit 1; -- keep only the first result in the list (with highest measurement date)
$$ language SQL stable;


-- Get FSC/RLIE closest jobs, with measurement date more recent than the
-- low time bound, and focusing on the specified tile. Return at most the
-- specified number of results (results_limit)
create function cosims.fsc_rlie_jobs_following_measurement_with_tile_id (
  tile_id_ref text,
  low_measurement_time_bound timestamp,
  low_l1c_esa_creation_time_bound timestamp,
  results_limit smallint
)
  returns setof cosims.fsc_rlie_jobs as $$
  select frj.*
  from cosims.fsc_rlie_jobs frj
  join cosims.parent_jobs pjt -- from the parent_job table:
  on frj.fk_parent_job_id=pjt.id -- join the FSC/RLIE table with tile_id selection
  where (
    frj.measurement_date>=low_measurement_time_bound -- select only jobs with suitable measurement date
    and frj.l1c_esa_creation_date>low_l1c_esa_creation_time_bound -- ensure it's different job than the 'init' one
    and pjt.tile_id=tile_id_ref -- select only jobs with the specified tile_id
    and frj.l1c_reference_job is true -- and which are the reference for a given L1C
    )
  order by frj.measurement_date asc -- order selected jobs with ascending measurement date
  limit results_limit; -- keep only the specified number of results in the list (with lowest measurement dates)
$$ language SQL stable;


-- Get last FSC/RLIE job processed with MAJA "init" mode on the specified tile
create function cosims.fsc_rlie_job_last_init_with_tile_id_no_backward (
  tile_id_ref text,
  high_time_bound timestamp
)
  returns setof cosims.fsc_rlie_jobs as $$
  select frj.*
  from cosims.fsc_rlie_jobs frj
  join cosims.parent_jobs pjt -- from the parent_job table
  on frj.fk_parent_job_id = pjt.id -- join the FSC/RLIE table with tile_id selection
  where (
    pjt.tile_id = tile_id_ref -- select only jobs with the specified tile_id
    and frj.measurement_date < high_time_bound -- select only jobs with suitable measurement date
    and frj.maja_mode = 'init' -- and processed with MAJA 'init' mode
    and not frj.backward_reprocessing_run -- and backward has not already been run
    and frj.l2a_status = 'generated' -- prevent selecting codated init job which didn't generate products
    )
  order by frj.measurement_date desc -- order selected jobs with descending measurement date
  limit 1; -- keep only the first result in the list (with highest measurement date)
$$ language SQL stable;



-- Get PSA/ARLIE jobs with the given last status (can be a list)
create function cosims.psa_arlie_jobs_with_last_status (
  last_status smallint[]
)
  returns setof cosims.psa_arlie_jobs as $$
  select frj.*
  from cosims.psa_arlie_jobs frj
  join cosims.parent_jobs pj on pj.id = frj.fk_parent_job_id
  WHERE pj.last_status_id = ANY(last_status)
$$ language SQL stable;


-- Get test jobs with the given last status (can be a list)
create function cosims.test_jobs_with_last_status (
  last_status smallint[]
)
  returns setof cosims.test_jobs as $$
  select frj.*
  from cosims.test_jobs frj
  join cosims.parent_jobs pj on pj.id = frj.fk_parent_job_id
  WHERE pj.last_status_id = ANY(last_status)
$$ language SQL stable;


-- Get GFSC jobs inserted in database with a status, product_date and on the specified tile
create function cosims.gfsc_jobs_with_status_product_date_tile (
  last_status smallint[],
  product_date_ref timestamp,
  tile_id_ref text
)
  returns setof cosims.gfsc_jobs as $$
  select gj.*
  from cosims.gfsc_jobs gj
  join cosims.parent_jobs pj -- from the parent_job table
  on gj.fk_parent_job_id = pj.id -- join the GFSC table with tile_id selection
  where (
    pj.tile_id = tile_id_ref -- select only jobs with the specified tile_id
    and pj.last_status_id = ANY(last_status) -- and with the given status
    and gj.product_date = product_date_ref -- and with provided product date
    )
  ;
$$ language SQL stable;

-- Get SWS/WDS job with most recent fsc_publication_latest_date
create function cosims.last_job_with_fsc_publication_latest_date (
)
  returns setof cosims.sws_wds_jobs as $$
  select swj.*
  from cosims.sws_wds_jobs swj
  order by case when swj.fsc_publication_latest_date is null then 1 else 0 end, swj.fsc_publication_latest_date desc
  limit 1; -- keep only the first result in the list (with highest measurement date)
$$ language SQL stable;


-- Empty all DataBase tables
create function cosims.empty_database ()
  returns void as $$
begin
  delete from cosims.parent_jobs;
  delete from cosims.fsc_rlie_jobs;
  delete from cosims.psa_arlie_jobs;
  delete from cosims.test_jobs;
  delete from cosims.rlies1_jobs;
  delete from cosims.rlies1s2_jobs;
  delete from cosims.gfsc_jobs;
  delete from cosims.sws_wds_jobs;
  delete from cosims.other_jobs;
  delete from cosims.execution_info;
  delete from cosims.execution_messages;
  delete from cosims.execution_info_to_messages;
  delete from cosims.job_status_changes;
  delete from cosims.job_creation_request;
end
  $$ language plpgsql volatile;


-- Empty one DataBase table and delete the associated parent jobs
create function cosims.empty_table ()
  returns void as $$
begin
  -- delete parent jobs linked to the jobs of the selected table
  -- which delete all the jobs from the table by cascade
  delete from cosims.parent_jobs pj
  using cosims.sws_wds_jobs swj
  where pj.id = swj.fk_parent_job_id;
end
  $$ language plpgsql volatile;


-- Return all parent_jobs whose local status is unsync with last status from job_status_changes
create or replace function cosims.jobs_satus_unsync_jobs()
  returns setof cosims.parent_jobs as $$
  SELECT pj.* FROM cosims.parent_jobs pj -- select all jobs
  join cosims.last_job_status_id() ljs -- select last status based on job_status_changes
  on pj.id = ljs.parent_job_id -- match jobs and last status
  WHERE ljs.status != pj.last_status_id; -- filter jobs whose status is unsync
$$ language SQL stable;

-- sync all parent_jobs whose local status is unsync with last status from job_status_changes
create or replace function cosims.jobs_status_manual_sync()
    returns void as $$
begin
  if exists (SELECT id from cosims.jobs_satus_unsync_jobs()) then
    UPDATE cosims.parent_jobs SET error_raised = 'true'
    FROM (
      SELECT fk_parent_job_id, max(job_status) FROM cosims.job_status_changes
      WHERE job_status >= 13 and job_status <= 15
      GROUP BY fk_parent_job_id
    ) jsh
    WHERE jsh.fk_parent_job_id = cosims.parent_jobs.id
    AND cosims.parent_jobs.id = ANY(SELECT id from cosims.jobs_satus_unsync_jobs());

    UPDATE cosims.parent_jobs SET
      last_status_id = ljs.status,
      last_status_change_id = ljs.change_id,
      last_status_change_date = ljs.time,
      last_status_error_subtype = lsc.error_subtype
    FROM cosims.last_job_status() as ljs
    join cosims.job_status_changes as lsc on ljs.change_id = lsc.id
    WHERE ljs.parent_job_id = cosims.parent_jobs.id
    AND cosims.parent_jobs.id = ANY(SELECT id from cosims.jobs_satus_unsync_jobs());
  end if;
end
  $$ language plpgsql volatile;

-- Status transition restrictions, called by the trigger connected to the
--  database job_status_changes table. It raises an exception if the transition is not allowed.
create function cosims.check_status_change() returns trigger as $check_status_change$
declare
  old_status smallint;
begin
  select status from cosims.last_job_status_id() where parent_job_id = new.fk_parent_job_id
  into old_status;

  if old_status is null then -- if no status set for this job yet
  -- ensure the first status to be set can only be 'initialized', 'started' (for services jobs),
  -- 'internal_error', or 'cancelled' (for empty codated jobs)
    if new.job_status != all ('{1,5,13,16}'::int[]) then
      raise exception 'Error : invalid job status creation %!', new.job_status;
    end if;
  end if;

  if old_status = 1 then -- if current status is 'initialized'
  -- ensure future status can only be 'configured', 'internal_error', 'external_error', or 'cancelled'
    if new.job_status != all ('{2,13,14,16}'::int[]) then
      raise exception 'Error : invalid job status transition % to %!', old_status, new.job_status;
    end if;
  end if;

  if old_status = 2 then -- if current status is 'configured'
  -- ensure future status can only be 'ready', 'internal_error', or 'cancelled'
    if new.job_status != all ('{3,13,16}'::int[]) then
      raise exception 'Error : invalid job status transition % to %!', old_status, new.job_status;
    end if;
  end if;

  if old_status = 3 then -- if current status is 'ready'
  -- ensure future status can only be 'queued', 'internal_error', 'external_error', or 'cancelled'
    if new.job_status != all ('{4,13,14,16}'::int[]) then
      raise exception 'Error : invalid job status transition % to %!', old_status, new.job_status;
    end if;
  end if;

  if old_status = 4 then -- if current status is 'queued'
  -- ensure future status can only be 'started', or 'internal_error'
    if new.job_status != all ('{5,13}'::int[]) then
      raise exception 'Error : invalid job status transition % to %!', old_status, new.job_status;
    end if;
  end if;

  if old_status = 5 then -- if current status is 'started'
  -- ensure future status can only be 'pre_processing', 'internal_error', 'external_error' or 'cancelled'
    if new.job_status != all ('{6,13,14,16}'::int[]) then
      raise exception 'Error : invalid job status transition % to %!', old_status, new.job_status;
    end if;
  end if;

  if old_status = 6 then -- if current status is 'pre_processing'
  -- ensure future status can only be 'processing', 'internal_error', 'external_error' or 'cancelled'
    if new.job_status != all ('{7,13,14,16}'::int[]) then
      raise exception 'Error : invalid job status transition % to %!', old_status, new.job_status;
    end if;
  end if;

  if old_status = 7 then -- if current status is 'processing'
  -- ensure future status can only be 'post_processing', 'internal_error', 'external_error' or 'cancelled'
    if new.job_status != all ('{8,13,14,16}'::int[]) then
      raise exception 'Error : invalid job status transition % to %!', old_status, new.job_status;
    end if;
  end if;

  if old_status = 8 then -- if current status is 'post_processing'
  -- ensure future status can only be 'processed', 'internal_error', 'external_error' or 'cancelled'
    if new.job_status != all ('{9,13,14,16}'::int[]) then
      raise exception 'Error : invalid job status transition % to %!', old_status, new.job_status;
    end if;
  end if;

  if old_status = 9 then -- if current status is 'processed'
  -- ensure future status can only be 'start_publication', 'done', 'internal_error', 'external_error' or 'cancelled'
    if new.job_status != all ('{10,12,13,14,16}'::int[]) then
      raise exception 'Error : invalid job status transition % to %!', old_status, new.job_status;
    end if;
  end if;

  if old_status = 10 then -- if current status is 'start_publication'
  -- ensure future status can only be 'published', 'internal_error', 'external_error' or 'cancelled'
    if new.job_status != all ('{11,13,14,16}'::int[]) then
      raise exception 'Error : invalid job status transition % to %!', old_status, new.job_status;
    end if;
  end if;

  if old_status = 11 then -- if current status is 'published'
  -- ensure future status can only be 'done'
    if new.job_status != 12 then
      raise exception 'Error : invalid job status transition % to %!', old_status, new.job_status;
    end if;
  end if;

  if old_status = 12 then -- if current status is 'done'
  -- ensure future status can only be 'internal_error' or 'external_error'
    if new.job_status != all ('{13,14}'::int[]) then
      raise exception 'Error : invalid job status transition % to %!', old_status, new.job_status;
    end if;
  end if;

  if old_status = 13 then -- if current status is 'internal_error' -> status shouldn't change
  -- ensure future status can only be 'error_checked', 'internal_error' or 'external_error'
    if new.job_status != all ('{13,14,15}'::int[]) then
      raise exception 'Error : invalid job status transition % to %!', old_status, new.job_status;
    end if;
  end if;

  if old_status = 14 then -- if current status is 'external_error'
  -- ensure future status can only be 'started', 'error_checked', 'internal_error' or 'external_error'
    if new.job_status != all ('{5,13,14,15}'::int[]) then
      raise exception 'Error : invalid job status transition % to %!', old_status, new.job_status;
    end if;
  end if;

  -- if current status is 'error_checked' (old_status = 15)
  -- future status can be any

  if old_status = 16 then -- if current status is 'cancelled'
  -- ensure future status can only be 'internal_error'
    if new.job_status != 13 then
      raise exception 'Error : invalid job status transition % to %!', old_status, new.job_status;
    end if;
  end if;

  -- Update status related columns in Parent Job's table.
  if new.job_status in (13, 14, 15) then
    update cosims.parent_jobs set
      last_status_id = new.job_status,
      last_status_change_id = new.id,
      last_status_change_date = new.time,
      last_status_error_subtype = new.error_subtype,
      error_raised = 'true'
    where id = new.fk_parent_job_id;
    if not found then
      raise exception 'Fail to update parent_jobs status with id % to status %!', new.fk_parent_job_id, new.job_status;
    end if;
  else
    update cosims.parent_jobs set
      last_status_id = new.job_status,
      last_status_change_id = new.id,
      last_status_change_date = new.time,
      last_status_error_subtype = new.error_subtype
    where id = new.fk_parent_job_id;
    if not found then
      raise exception 'Fail to update parent_jobs status with id % to status %!', new.fk_parent_job_id, new.job_status;
    end if;
  end if;

  if not exists (
    select * from cosims.parent_jobs where id = new.fk_parent_job_id and last_status_id = new.job_status
  ) then
    select last_status_id from cosims.parent_jobs where id = new.fk_parent_job_id into old_status;
    raise exception 'Status for job % in table parent_jobs is not last_status % but %', new.fk_parent_job_id, new.job_status, old_status;
  end if;

  return new;
end
  $check_status_change$ language plpgsql;


-- Job name restrictions, called by the trigger connected to the
--  database parent_jobs table. It raises an exception if the name contains a space
--  as it's not correctly parsed by PostgRest V6.0.1 while preparing a request.
create function cosims.check_parent_job_name() returns trigger as $check_parent_job_name$
declare
  old_status smallint;
  old_fsc_rlie_status smallint;
  old_psa_arlie_status smallint;
begin
  if new.name ~ '[ \t\v\b\r\n\u00a0]' then
    raise exception 'Error : invalid job name : % !', new.name;
  end if;

  return new;
end
  $check_parent_job_name$ language plpgsql;

-----------------------------------------------------------------------------
-- Triggers

-- Trigger function, called whenever a change in the job_status_changes table is requested
--  it calls the 'check_status_change' function ensuring the job status transition is valid.
create trigger check_status_change before insert on cosims.job_status_changes
for each row execute procedure cosims.check_status_change();


-- Trigger function, called whenever a change in the parent_jobs table is requested
--  it calls the 'check_parent_job_name' function ensuring the job name is valid.
create trigger check_parent_job_name before insert or update of name on cosims.parent_jobs
for each row execute procedure cosims.check_parent_job_name();
