"""
deft_conf ... DB config

"""

daemon = {
    'deftDB_host'   : 'ATLAS_DEFT',
    'deftDB'        : 'INTR',
    'deftDB_reader' : 'atlas_deft_r',
    'deftDB_writer' : 'atlas_deft_w',
# tables
    # DEFT
    't_prodmanager_request': 't_prodmanager_request',
    't_production_step'    : 't_production_step',
    't_production_task'    : 't_production_task',
    't_production_dataset' : 't_production_dataset',
    # DEFT-JEDI
    't_task'               : 't_task',
    't_input_dataset'      : 't_input_dataset'
}


