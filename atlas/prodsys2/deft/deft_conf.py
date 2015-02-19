# config parameters for DEFT handling subroutines
# Mar 2014. A.Klimentov
# Feb 17, 2015. INTR -> ADCR
#
# Last Edit : Feb 17, 2015
#
daemon = {
    'deftDB_host'    : 'ATLAS_DEFT',
    'jediDB_host'    : 'ATLAS_PANDA',
    'deftDB_INTR'    : 'INTR',
    'deftDB_ADCR'    : 'ADCR',
    'jediDB_ADCR'    : 'ADCR',
    'deftDB_reader'  : 'atlas_deft_r',
    'deftDB_writer'  : 'atlas_deft_w',
    'pandamon_reader': 'atlas_pandamon_reader',

# tables
    # DEFT
    't_prodmanager_request'        : 't_prodmanager_request',
    't_prodmanager_request_status' : 't_prodmanager_request_status',
    't_production_step'            : 't_production_step',
    't_production_task'            : 't_production_task',
    't_production_task_p'          : 't_production_task_listpart',
    't_production_dataset'         : 't_production_dataset',
    't_production_container'       : 't_production_container',
    't_projects'                   : 't_projects',
    't_prodsys2_reports'           : 't_prodsys2_reports',
    # DEFT-JEDI
    't_task'               : 't_task',
    't_input_dataset'      : 't_input_dataset',
    't_jedi_tasks'         : 'jedi_tasks',
    't_jedi_datasets'      : 'jedi_datasets',
# defaults
    'user_task_step_id'    : 201, # default step for users tasks
    'user_task_request_id' : 300, # default request for users tasks
}
