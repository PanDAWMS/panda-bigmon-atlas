'''
Created on Feb 26, 2014

@author: mborodin
'''

class ConfigParser(object):
    '''
    Pares config file tipe of 
    #comment
    key1:value1
    key2:value2
    key2:value2
    
    '''
    COMMENT_CHAR = '#'
    OPTION_CHAR = ':'
 
    def parse_config(self, open_file):
        
        for line in open_file:
            # First, remove comments:
            if self.COMMENT_CHAR in line:
                # split on comment char, keep only the part before
                line, comment = line.split(self.COMMENT_CHAR, 1)
            # Second, find lines with an option=value:
            if self.OPTION_CHAR in line:
                # split on option char:
                option, value = line.split(self.OPTION_CHAR, 1)
                # strip spaces:
                option = option.strip()
                value = value.strip()
                # store in dictionary:
                value_list = self.options.get(option, [])
                value_list.append(value)
                self.options[option] = value_list
        return self.options
 
    def __init__(self,):
        '''
        Constructor
        '''
        self.options = {}
        
