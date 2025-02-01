from account_creation import create_creds, reset_password, aws_login
from s3 import sel_bucket, mk_bucket, del_bucket_ap, updt_bucket_ap, ls_bucket, bucketSettings
from s3_bucket import upload_object
import logging

class terminal:
    """terminal class handles storage and execution of terminal commands especially as it relates
    to terminal hiearchy"""
    # __init__ command values,  set each to none until a value is specified
    def __init__(self,
                 terminal_type,
                 cmds_help_dict = {'-res': 'Used as the reset command',
                               '-srv': 'Used as the access service command',
                               '-sel': 'Used as the select command',
                               '-mk': 'Used as the make command',
                               '-del': 'Used as the delete command',
                               '-updt': 'Used as the update command',
                               '-ls': 'Used as the list command',
                               'bucket_settings': 'Provides a list of settings that are set on an s3 bucket',
                               '-upld': 'Used as the upload command',
                               'password': 'Used in comibnation with a command to make changes to a password',
                               's3': 'A simple storage service using s3 buckets and bucket objects',
                               'aws': 'The very first keyword used to access the application in combination with other commands',
                               'login': 'Used to log into the AWS application',
                               'user': 'Used in combination with other commands to control users',
                               'create': 'Used as the create command',
                               'bucket': 'Used in combination with otehr commands to control s3 buckets, typically you would perceed this keyword with the name of an existing bucket'},
                 cmd_prop_order = ['first_cmd', 'second_cmd', 'third_cmd', 'fourth_cmd'], 
                 first_cmd=None,
                 second_cmd=None,
                 third_cmd=None,
                 fourth_cmd=None):
        self.cmds_help_dict = cmds_help_dict
        self.terminal_type = terminal_type
        self.cmd_prop_order = cmd_prop_order
        self.first_cmd = first_cmd
        self.second_cmd = second_cmd
        self.third_cmd = third_cmd
        self.fourth_cmd = fourth_cmd
    
    def set_commands(self, attr, value): 
        """sets command values for terminal object"""
        if attr == 'first_cmd':
            self.first_cmd = value
        elif attr == 'second_cmd':
            self.second_cmd = value
        elif attr == 'third_cmd':
            self.third_cmd = value
        elif attr == 'fourth_cmd':
            self.fourth_cmd = value

    def get_commands(self, attr): 
        """returns stored command values for terminal instances"""
        if attr == 'first_cmd':
            return self.first_cmd
        elif attr == 'second_cmd':
            return self.second_cmd
        elif attr == 'third_cmd':
            return self.third_cmd
        elif attr == 'fourth_cmd':
            return self.fourth_cmd

    def question_command(self, _cmds):
        list(map(lambda x: print(x), [key for key in _cmds]))

    def help_command(self, raw_cmds):
        """Baked in terminal command which provides what a given set of commands does"""
        cpo = self.cmd_prop_order
        if '-help' in raw_cmds:
            cmd_lineage = []
            for key_num in range(raw_cmds.index('-help')):
                cmd_lineage.append(f'{self.get_commands(cpo[key_num])}: {self.cmds_help_dict[self.get_commands(cpo[key_num])]}')
            joined_explanation = " --> ".join(cmd_lineage)
            print(f'{joined_explanation}')
            return True
        else:
            return False

    def run_cmds(self, cmds, param_list:list=[]): 
        """executes each command in order until it reaches None or a callable function"""
        cpo = self.cmd_prop_order
        needs_help = self.help_command([self.get_commands(x) for x in self.cmd_prop_order])
        for cmd_num in range(len(cpo)):
            if needs_help:
                pass
            elif self.get_commands(cpo[cmd_num]) == '?':
                self.question_command(cmds)
            elif self.get_commands(cpo[cmd_num]) is not None:
                cmds = cmds[self.get_commands(cpo[cmd_num])]
                if callable(cmds):
                    cmds(*param_list) #If cmd returns a callable function, call the function

    def term_spec_cmds(self, param_list:list=[]): 
        """command heirchey specified by terminal"""
        if self.terminal_type == 'ap':
            self.run_cmds({'aws': {'login': aws_login, 
                                   'create': {'user': create_creds}}}, param_list)
        elif self.terminal_type == 'acct':
            self.run_cmds({'-res': {'password': reset_password}, 
                           '-srv': {'s3': s3_def_terminal}}, param_list)
        elif self.terminal_type == 's3':
            self.run_cmds({'-sel': {'bucket': sel_bucket},
                           '-mk': {'bucket': mk_bucket},
                           '-del': {'bucket': del_bucket_ap},
                           '-updt': {'bucket': updt_bucket_ap},
                           '-ls': {'bucket': ls_bucket},
                           'bucket_settings': bucketSettings}, param_list)
        elif self.terminal_type == 's3_bucket':
            self.run_cmds({'-upld': upload_object}, param_list)

def dynamic_term_vals(terminal_type, _term_cmd_obj, _term_spec_parm): 
    """function that sets dynamic cmd line arguments into the terminal instance (ex: name of a bucket or bucket object)"""
    if terminal_type == 's3':
        _term_spec_parm[1] = _term_cmd_obj.third_cmd
    elif terminal_type == 's3_bucket':
        _term_spec_parm[2] = _term_cmd_obj.second_cmd
    return _term_spec_parm

def terminal_entry(_terminal_type, cmd_str, term_spec_parm:list=[]): 
    """entry point to instantiate a terminal instance, parameters include terminal type, cmd propmt string, and terminal specific function arguments"""
    term_cmd_obj = terminal(_terminal_type)
    cmd_list = cmd_str.split(' ')
    for i in range(len(cmd_list)):
        if i <= len(term_cmd_obj.cmd_prop_order) - 1:
            term_cmd_obj.set_commands(term_cmd_obj.cmd_prop_order[i], cmd_list[i])
    try:
        term_cmd_obj.term_spec_cmds(dynamic_term_vals(_terminal_type, term_cmd_obj, term_spec_parm))
    except:
        pass

def ap_level_main():
    """allows access into the application access point command line"""
    prompt_command = input(f'AWS> ')
    while prompt_command != 'quit':
        terminal_entry('ap', prompt_command, [acct_level_main])
        prompt_command = input(f'AWS> ')
    quit()

def acct_level_main(_username):
    """allows access into the  account level access point command line"""
    prompt_command = input(f'AWS\\uers\\{_username}> ')
    while prompt_command != 'logout':
        terminal_entry('acct', prompt_command, [_username])
        prompt_command = input(f'AWS\\uers\\{_username}> ')
    ap_level_main()

def s3_def_terminal(_username):
    """allows access into the s3 command line for a logged in user"""
    prompt_command = input(f'AWS\\uers\\{_username}\\s3> ')
    while prompt_command != 'exit':
        terminal_entry('s3', prompt_command, [_username, None, s3_bucket_terminal])
        prompt_command = input(f'AWS\\uers\\{_username}\\s3> ')
    acct_level_main(_username)

def s3_bucket_terminal(_username, bucket):
    """allows access into the s3 bucket command line for a logged in user and specified existing user bucket"""
    prompt_command = input(f'AWS\\users\\{_username}\\s3\\{bucket.name}> ')
    while prompt_command != 'exit':
        terminal_entry('s3_bucket', prompt_command, [_username, bucket, None])
        prompt_command = input(f'AWS\\users\\{_username}\\s3\\{bucket.name}> ')
    s3_def_terminal(_username)

if __name__ == '__main__':
    yes = ' --> '.join(['yes', 'no', 'maybe so'])
    print(f'{yes}')