from account_creation import create_creds, reset_password, neuronXY_login, delete_user
from cortex import sel_node, mk_node, del_node_ap, updt_node_ap, ls_node, nodeSettings, lifecycle_rules
from cortex_node import upload_file, delete_file, update_file
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
                               'node_settings': 'Provides a list of settings that are set on an cortex node',
                               '-upld': 'Used as the upload command',
                               'password': 'Used in comibnation with a command to make changes to a password',
                               'cortex': 'A simple storage service using cortex nodes and node files',
                               'neuronXY': 'The very first keyword used to access the application in combination with other commands',
                               'login': 'Used to log into the NeuronXY application',
                               'user': 'Used in combination with other commands to control users',
                               'create': 'Used as the create command',
                               'node': 'Used in combination with other commands to control cortex nodes, typically you would perceed this keyword with the name of an existing node',
                               'lifecycle_rule': 'Used in combination with the -add and -updt commands to add and update lifecycle rules for nodes',
                               '--perm': 'Permenanet tag, used to override any backup processes (not generally recommended)',
                               '-add': 'Used to add properties to an file that by default does not get created with said properties'},
                 cmds=None):
        self.cmds_help_dict = cmds_help_dict
        self.terminal_type = terminal_type
        if cmds == None:
            cmds = []
        self.cmds = cmds

    def set_commands(self, value): 
        """sets command values for terminal object"""
        self.cmds.append(value)

    def get_commands(self, attr_num): 
        """returns stored command values for terminal instances"""
        try:
            return self.cmds[attr_num]
        except:
            return None

    def question_command(self, _cmds, raw_cmds):
        if ('?' in [self.get_commands(x) for x in range(len(self.cmds))]):
            unaltered_cmds = _cmds.copy()
            if 'cmd_tags' in _cmds:
                del _cmds['cmd_tags']
            for cmd in raw_cmds[0:raw_cmds.index('?')]:
                _cmds = _cmds[cmd]
            if isinstance(_cmds, dict):
                list(map(lambda x: print(x), [key for key in _cmds]))
            try:
                list(map(lambda x: print(x), [key for key in unaltered_cmds['cmd_tags'][raw_cmds[raw_cmds.index('?') - 1]]]))
            except:
                pass
            return True

    def help_command(self, raw_cmds):
        """Baked in terminal command which provides what a given set of commands does"""
        if '-help' in raw_cmds:
            cmd_lineage = []
            for key_num in range(raw_cmds.index('-help')):
                cmd_lineage.append(f'{self.get_commands(key_num)}: {self.cmds_help_dict[self.get_commands(key_num)]}')
            joined_explanation = " --> ".join(cmd_lineage)
            print(f'{joined_explanation}')
            return True

    def run_cmds(self, cmds, param_list:list=[]): 
        """executes each command in order until it reaches None or a callable function"""
        needs_help = self.help_command([self.get_commands(x) for x in range(len(self.cmds))])
        question_cmd = self.question_command(cmds, [self.get_commands(x) for x in range(len(self.cmds))])
        for cmd_num in range(len(self.cmds)):
            if needs_help:
                pass
            elif question_cmd:
                pass
            elif self.get_commands(cmd_num) is not None:
                cmds = cmds[self.get_commands(cmd_num)]
                if callable(cmds):
                    cmds(*param_list) #If cmd returns a callable function, call the function

    def term_spec_cmds(self, param_list:list=[]): 
        """command heirchey specified by terminal"""
        if self.terminal_type == 'ap':
            self.run_cmds({'neuronXY': {'login': neuronXY_login, 
                                   'create': {'user': create_creds},
                                   '-del': {'user': delete_user}}}, param_list)
        elif self.terminal_type == 'acct':
            self.run_cmds({'-res': {'password': reset_password}, 
                           '-srv': {'cortex': cortex_def_terminal}}, param_list)
        elif self.terminal_type == 'cortex':
            self.run_cmds({'-sel': {'node': sel_node},
                           '-mk': {'node': mk_node},
                           '-del': {'node': del_node_ap},
                           '-updt': {'node': updt_node_ap},
                           '-add': {'lifecycle-rule': lifecycle_rules},
                           '-ls': {'node': ls_node},
                           'node_settings': nodeSettings}, param_list)
        elif self.terminal_type == 'cortex_node':
            self.run_cmds({'-upld': upload_file,
                           '-del': delete_file,
                           '-updt': update_file,
                           'cmd_tags': {'-del': ['--perm']}}, param_list)

def dynamic_term_vals(terminal_type, _term_cmd_obj, _term_spec_parm): 
    """function that sets dynamic cmd line arguments into the terminal instance (ex: name of a node or node file)"""
    if terminal_type == 'cortex':
        _term_spec_parm[1] = _term_cmd_obj.get_commands(2)
    elif terminal_type == 'cortex_node':
        _term_spec_parm[2] = _term_cmd_obj.get_commands(-1)
        if _term_cmd_obj.get_commands(1) == '--perm':
            _term_spec_parm[3] = True
    return _term_spec_parm

def terminal_entry(_terminal_type, cmd_str, term_spec_parm:list=[]): 
    """entry point to instantiate a terminal instance, parameters include terminal type, cmd propmt string, and terminal specific function arguments"""
    term_cmd_obj = terminal(_terminal_type)
    cmd_list = cmd_str.split(' ')
    for cmd in cmd_list:
        term_cmd_obj.set_commands(cmd)
    try:
        term_cmd_obj.term_spec_cmds(dynamic_term_vals(_terminal_type, term_cmd_obj, term_spec_parm))
    except:
        pass

def ap_level_main():
    """allows access into the application access point command line"""
    prompt_command = input(f'NeuronXY> ')
    while prompt_command != 'quit':
        terminal_entry('ap', prompt_command, [acct_level_main])
        prompt_command = input(f'NeuronXY> ')
    quit()

def acct_level_main(_username):
    """allows access into the  account level access point command line"""
    prompt_command = input(f'NeuronXY\\uers\\{_username}> ')
    while prompt_command != 'logout':
        terminal_entry('acct', prompt_command, [_username])
        prompt_command = input(f'NeuronXY\\uers\\{_username}> ')
    ap_level_main()

def cortex_def_terminal(_username):
    """allows access into the cortex command line for a logged in user"""
    prompt_command = input(f'NeuronXY\\uers\\{_username}\\cortex> ')
    while prompt_command != 'exit':
        terminal_entry('cortex', prompt_command, [_username, None, cortex_node_terminal])
        prompt_command = input(f'NeuronXY\\uers\\{_username}\\cortex> ')
    acct_level_main(_username)

def cortex_node_terminal(_username, node):
    """allows access into the cortex node command line for a logged in user and specified existing user node"""
    prompt_command = input(f'NeuronXY\\users\\{_username}\\cortex\\{node.name}> ')
    while prompt_command != 'exit':
        terminal_entry('cortex_node', prompt_command, [_username, node, None, False])
        prompt_command = input(f'NeuronXY\\users\\{_username}\\cortex\\{node.name}> ')
    cortex_def_terminal(_username)

if __name__ == '__main__':
    ap_level_main()
