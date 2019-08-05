# Collection of functions to sync 'syncthing_data' from field with
# local repository of the same.
# This is done at state level.

#import config.clix_config as clix_config
import pexpect
import pandas
import re
import config.clix_config as clix_config

def schools_updated(rsync_log):
    log_text = rsync_log.decode('utf-8')
    pattern = re.compile(r"(\d+-\D{2}\d+)\/(gstudio)")
    schools = set([each[0] for each in pattern.findall(log_text) if each[1] == 'gstudio'])
    return list(schools)

def rsync_data_ssh(state, src, dst, **context):
    '''
    Function to sync state data through ssh.
    '''

    user = clix_config.remote_user
    ip = clix_config.remote_ip
    passwd = clix_config.remote_passwd

    def ssh_sync(src, dst):

        cmd = "rsync -avzhe ssh {0}@{1}:{2} {3}".format(user, ip, src, dst)
        rsync = pexpect.spawn(cmd, timeout=3600)
        import pdb
        pdb.set_trace()
        try:
            i = rsync.expect(['Password:', 'continue connecting (yes/no)?'])
            if i == 0 :
                rsync.sendline(passwd)
            elif i == 1:
                rsync.sendline('yes')
                rsync.expect('Password: ')
                rsync.sendline(passwd)
        except pexpect.EOF:
            print("EOF Exception for Syncing")
            raise Exception('Rysnc didnt work!')

        except pexpect.TIMEOUT:
            print("TIMEOUT Exception Syncing")
            raise Exception('Not enough time given to Rsync!')

        else:
          rsync_log = rsync.read()
          list_of_schools_updated = schools_updated(rsync_log)
          context['ti'].xcom_push('school_update_list', list_of_schools_updated)
        rsync.close()

    return ssh_sync(src, dst)
