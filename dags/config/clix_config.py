#Source and destination folders for rsync of syncthing data from prop_schools
remote_src = 'clix_backup/sync-clix.tiss.edu/data/'
local_dst = '/usr/local/airflow/school_syncthing_data_live/'
remote_user = 'parthae'
remote_ip = '103.36.84.176'
remote_passwd = 'uvceece2015'
states = ['mz']
