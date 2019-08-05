# Collection of functions to process and laod tables for visualisation
# for a set of schools whose data has been updated through syncthing_data

# Function

from math import ceil
import scripts.clix_platform_data_processing.clix_utils
import scripts.clix_platform_data_processing.get_tools_data
import scripts.clix_platform_data_processing.get_modules_data
import config.clix_config

def get_attendance(schools, tools_data, modules_data):

    return None

def get_modulevisits(schools, modules_data):
    pass

def get_timespent(schools, tools_data):
    pass

def process_school_tables(state, school_prop, chunk):
    '''
    Function to process tables for a set of schools whose
    data has been updated through syncthing
    '''
    list_of_schools = context['ti'].xcom_pull(task_ids='sync_state_data' + state)['school_update_list']
    #list_of_schools = ['2031011-mz11']
    state_data_src = clix_config.local_dst + state

    num_schools = ceil(len(list_of_schools)*school_prop/100)
    schools_to_process = list_of_schools[:num_schools]
    if schools_to_process:
        tools_data = get_tools_data(schools_to_process)
        modules_data = get_modules_data(schools_to_process)
        # To get school attendance data. Time variation of number of unique logins
        # from modules and tools data
        attendance_table = get_attendance_schools(schools_to_process, tools_data, modules_data)
        # To get number of modules visited broken down by subject/domain over time.
        module_visits_table = get_modulevisits_schools(schools_to_process, modules_data)
        # To get time spent on different tools in school over time.
        timespent_tools_table = get_timespent_schools(schools_to_process, tools_data)

    return None
