import os
import glob

def get_files(folderpath, file_extension):
    '''
    Recusively explores the tree structure of a director and select files based on their type /extension.
    Input:
          folderpath: string
          file_extension: string 
    Ouput:
          all_files: A list containing all files matching the the file_extension pattern
    '''
    
    # CREATE A LIST TO THE MATCHES
    all_files = []
    
    # EXPLORE ALL POSSIBLE SUB-PATHS IN FOLDERPATH
    for root, dirs, files in os.walk(folderpath):
    
        # SELECTING FILE THAT MATCH THE PATTERN
        files = glob.glob(os.path.join(root, '*'+file_extension))
        
        # APPEND THE MATCHES ONE BY ONE
        for file in files:
            all_files.append(os.path.abspath(file))
    
    return all_files


def var_name(obj, namespace):
    '''
    returns the name of a variable as a string
    from: https://stackoverflow.com/a/592891/14190837
    '''
    return [name for name in namespace if namespace[name] is obj]

