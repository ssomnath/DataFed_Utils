import os
import sys
import socket
from multiprocessing import cpu_count
import joblib
import datetime
import json
from warnings import warn
from collections import Iterable
import numpy as np
import datafed.CommandLib as df

if sys.version_info.major == 3:
    unicode = str

MAX_ALIAS_LENGTH = 60

# --------------- STUFF STOLEN FROM pyUSID ------------------------------------


def format_quantity(value, unit_names, factors, decimals=2):
    """
    Formats the provided quantity such as time or size to appropriate strings
    Parameters
    ----------
    value : number
        value in some base units. For example - time in seconds
    unit_names : array-like
        List of names of units for each scale of the value
    factors : array-like
        List of scaling factors for each scale of the value
    decimals : uint, optional. default = 2
        Number of decimal places to which the value needs to be formatted
    Returns
    -------
    str
        String with value formatted correctly
    """
    # assert isinstance(value, (int, float))
    if not isinstance(unit_names, Iterable):
        raise TypeError('unit_names must an Iterable')
    if not isinstance(factors, Iterable):
        raise TypeError('factors must be an Iterable')
    if len(unit_names) != len(factors):
        raise ValueError('unit_names and factors must be of the same length')
    #unit_names = validate_list_of_strings(unit_names, 'unit_names')
    index = None

    for index, val in enumerate(factors):
        if value < val:
            index -= 1
            break

    index = max(0, index)  # handles sub msec

    return '{} {}'.format(np.round(value / factors[index], decimals),
                          unit_names[index])


def format_size(size_in_bytes, decimals=2):
    """
    Formats the provided size in bytes to kB, MB, GB, TB etc.
    Parameters
    ----------
    size_in_bytes : number
        size in bytes
    decimals : uint, optional. default = 2
        Number of decimal places to which the size needs to be formatted
    Returns
    -------
    str
        String with size formatted correctly
    """
    units = ['bytes', 'kB', 'MB', 'GB', 'TB']
    factors = 1024 ** np.arange(len(units), dtype=np.int64)
    return format_quantity(size_in_bytes, units, factors, decimals=decimals)


def validate_single_string_arg(value, name):
    """
    This function is to be used when validating a SINGLE string parameter for
    a function. Trims the provided value
    Errors in the string will result in Exceptions
    Parameters
    ----------
    value : str
        Value of the parameter
    name : str
        Name of the parameter
    Returns
    -------
    str
        Cleaned string value of the parameter
    """
    if not isinstance(value, (str, unicode)):
        raise TypeError(name + ' should be a string')
    value = value.strip()
    if len(value) == 0:
        raise ValueError(name + ' should not be an empty string')
    return value


def validate_list_of_strings(str_list, parm_name='parameter'):
    """
    This function is to be used when validating and cleaning a list of strings.
    Trims the provided strings
    Errors in the strings will result in Exceptions
    Parameters
    ----------
    str_list : array-like
        list or tuple of strings
    parm_name : str, Optional. Default = 'parameter'
        Name of the parameter corresponding to this string list that will be
        reported in the raised Errors
    Returns
    -------
    array-like
        List of trimmed and validated strings when ALL objects within the list
        are found to be valid strings
    """

    if isinstance(str_list, (str, unicode)):
        return [validate_single_string_arg(str_list, parm_name)]

    if not isinstance(str_list, (list, tuple)):
        raise TypeError(parm_name + ' should be a string or list / tuple of strings')

    return [validate_single_string_arg(x, parm_name) for x in str_list]


# ---------------------------------------------------------------------------------------------------------------------------
   
def set_globus_endpoint(verbose=True):

    hostname = socket.gethostname()
    if verbose:
        print('Hostname is: ' + hostname)
        
    host_2_uuid = {'mac109728': '1646e89e-f4f0-11e9-9944-0a8c187e8c12',
                   'DESKTOP-LMIGRMD': '407beeb6-fa7c-11e9-8a5d-0e35e66293c2',
                   }    
    globus_ep_uuid = host_2_uuid.get(hostname, None)
    if verbose:
        print('Not working on Suhas Somnath Mac Book Pro or HP Z800 Desktop')
    
    if globus_ep_uuid is None:
        # Add CADES Condos:
        if any([hostname.startswith(x) for x in ['or-slurm-login',
                                                 'or-condo-login',
                                                 'or-slurm-c']]) and hostname.endswith('.ornl.gov'):
            if verbose:
                print('This machine is in the CADES Condos')
            globus_ep_uuid = '57230a10-7ba2-11e7-8c3b-22000b9923ef'
    
    if globus_ep_uuid is None:
        raise ValueError('Globus Endpoint for Hostname: {} is not known. '
                         'Please enter into set_globus_endpoint()'.format(hostname))
    
    com = 'ep default set ' + globus_ep_uuid
    
    if verbose:
        print('Setting Globus Endpoint with DataFed command:\n\t' + com)
        
    df.command(com)

    
def datafed_init(verbose=False):

    try:
        auth, uid = df.init()
    except Exception as excep:
        if excep.args[0] == 'init function can only be called once.':
            return

    if not auth:
        raise PermissionError('Could not authenticate with DataFed!!! Go to a '
                              'terminal and type "datafed setup"')
        return
    else:
        if verbose:
            print('Successfully authenticated in DataFed as: ' + uid)
        
    try:
        endpoint = df.command('ep get')
    except Exception as excep:
        set_globus_endpoint(verbose=verbose)

    
def list_items(id_or_alias, offset=None, count=None, project=None,
               verbose=True):
    """
    -O, --offset INTEGER     Start list at offset
  -C, --count INTEGER      Limit list to count results
  -p, --project TEXT       Project ID for command
    """
    com = 'ls' 
    if isinstance(offset, int) and offset > 0:
        com += ' -O {}'.format(offset)
    elif offset is not None:
        warn('offset must be an integer > 0. Your argument: {} was ignored'
             ''.format(offset))
    if isinstance(count, int) and count > 0:
        com += ' -C {}'.format(count)
    elif count is not None:
        warn('count must be an integer >= 1. Your argument: {} was ignored'
             ''.format(offset))
    if isinstance(project, str):
        # com += ' -P {}'.format(project)
        pass
    com += ' ' + id_or_alias
    
    if verbose:
        'Listing items in ' + id_or_alias + 'with DataFed command:\n\t' + com
    
    message = df.command(com)
    if message[1] != 'ListingReply':
        raise KeyError(message[0].err_msg)
        
    return message[0].item, message[0].offset, message[0].total


def get_clean_alias(title):
    for char in '~`!@#$%^&*()+=[{}]|\:,;"<>/?-':
        title = title.replace(char,'_')
    title = title.replace(' ', '_')[:MAX_ALIAS_LENGTH]
    return title.lower().strip()


def view_record(alias_or_id, verbose=True): 
         
    com = 'data view ' + alias_or_id
    
    if verbose:
        print('Checking if record exists with provided alias using command:'
              '\n\t' + com)
        
    message = df.command(com)
    
    if message[1] == 'RecordDataReply':
        return DataRecord(message)
    else:
        return None


def record_exists(alias_or_id, verbose=True):
    
    obj = view_record(alias_or_id, verbose=verbose)
    
    if isinstance(obj, DataRecord):
        return True
    else:
        return False


class DataRecord(object):
    # Slightly more Pythonic version of SDMS_pb2.RecordData
    def __init__(self, message):
        err_msg = 'A two-item tuple with the SDMS_pb2.RecordDataReply as ' \
                  'the first object was expected for "message"'
        if not isinstance(message, tuple):
            raise TypeError(err_msg)
        if len(message) != 2:
            raise ValueError(err_msg)
        # print(type(message[0]))
        # if not isinstance(message[0], SDMS_pb2.RecordDataReply):
        #    raise TypeError(err_msg)
        record_data = message[0].data[0]
        self.create_time = datetime.datetime.fromtimestamp(record_data.ct)
        self.update_time = datetime.datetime.fromtimestamp(record_data.ut)
        self.upload_time = datetime.datetime.fromtimestamp(record_data.dt)
        self.owner = record_data.owner
        self.creator = record_data.creator
        self.source = record_data.source
        self.size = record_data.size
        self.id = record_data.id
        self.title = record_data.title
        self.alias = record_data.alias
        self.repo_id = record_data.repo_id
        self.metadata = json.loads(record_data.metadata)
    
    def __repr__(self):
        output = ''
        #output += '- - - - - - - - - - - - - - - - - - - - -'
        output += 'Record id: \t' + self.id + '\n'
        output += 'Alias: \t\t' + self.alias + '\n'
        output += 'Title: \t\t' + self.title + '\n'
        # output += '- - - - - - - - - - - - - - - - - - - - -'
        output += 'Owner: \t\t' + self.owner + '\n'
        # output += 'Creator: \t\t' + self.creator + '\n'
        # output += 'Repository ID: \t' + self.repo_id + '\n'
        # output += '- - - - - - - - - - - - - - - - - - - - -'
        output += 'Size: \t\t' + format_size(self.size) + '\n'
        output += 'Created On: \t{}\n'.format(self.create_time)
        return output


def _data_update_create(title=None, alias=None, description=None, collection=None,
                         keywords=None, raw_data_file=None, extension=None,
                         metadata=None, clear_dependencies=None, add_dependencies=None,
                         remove_dependencies=None, project=None, verbose=True):
    
    com = ''
    
    if isinstance(title, str):
        title = title.strip()
        if len(title) > 0:
            com += ' -t {}'.format(title)
    elif title is not None:
        raise ValueError('"title" must be a non-empty string. Your argument: '
                         '"{}" was ignored'.format(title))
        
    if isinstance(collection, str):
        collection = collection.strip()
        if len(collection) > 0:
            com += ' -c {}'.format(collection)
    elif collection is not None:
        raise ValueError('"collection" must be a non-empty string. Your '
                         'argument: "{}" was ignored'.format(collection))
        
    if isinstance(alias, str):
        orig_alias = alias
        alias = get_clean_alias(alias)
        if verbose and orig_alias != alias:
            print('Alias was changed from "{}" to "{}" to comply with DataFed'
                  '.'.format(orig_alias, alias))
        if len(alias) > 0:
            com += ' -a "{}"'.format(alias)
    elif alias is not None:
        raise ValueError('"alias" must be a non-empty string with NO spaces or'
                         ' fancy characters. Your argument: "{}" was ignored'
                         ''.format(alias))
        
    if isinstance(description, str):
        description = description.strip()
        if len(description) > 0:
            com += ' -d "{}"'.format(description)
    elif description is not None:
        raise ValueError('"description" must be a non-empty string. Your '
                         'argument: "{}" was ignored'.format(description))
    
    if isinstance(keywords, (list, tuple)):
        keywords = validate_list_of_strings(keywords, parm_name='keywords')
        if len(keywords) > 0:
            com += ' -k' + ','.join(keywords)
    elif keywords is not None:
        raise ValueError('"keywords" must be a list of strings. Your argument'
                         ': "{}" was ignored'.format(keywords))
        
    if isinstance(metadata, str):
        if os.path.exists(metadata):
            com += ' -f ' + metadata
        else:
            raise FileNotFoundError('JSON Metadata file does not exist:' + metadata)
    elif isinstance(metadata, dict):
        com += ' -m \'' + json.dumps(metadata) + '\''
    elif metadata is not None:
        raise ValueError('"metadata" must either be a path to a JSON file or '
                         'a dictionary. Your argument: "{}" was ignored'
                         ''.format(metadata))
        
    return com


def create_df_record(title, alias=None, description=None, keywords=None, 
                     raw_data_file=None, extension=None, project=None,
                     metadata=None, collection=None, repository=None, dependencies=None,
                     check_for_existing=True, verbose=True):
    """
    -a, --alias TEXT              Alias.
  -d, --description TEXT        Description text.
  -k, --keywords TEXT           Keywords (comma separated list)
  -r, --raw-data-file TEXT      Globus path to raw data file (local or remote)
                                to upload with record. Default endpoint used
                                if none provided.
  -e, --extension TEXT          Override extension for raw data file (default
                                = auto detect).
  -m, --metadata TEXT           Inline metadata in JSON format.
  -f, --metadata-file FILENAME  Path to local metadata file containing JSON.
  -c, --collection TEXT         Parent collection ID/alias (default = current
                                working collection)
  -R, --repository TEXT         Repository ID
  -D, --dep <CHOICE TEXT>...    Specify dependencies by listing first the type
                                of relationship ('der', 'comp', or 'ver')
                                follwed by ID/alias of the target record. Can
                                be specified multiple times.
    """
    
    # Add an alias to make it easier to search for the file later on.
    if alias is None:
        alias = title
    alias = get_clean_alias(alias)
    
    if record_exists(alias, verbose=verbose):
        raise KeyError('A data record with alias: ' + alias + ' already exists'
                                                              ' in DataFed!')
        
    options = _data_update_create(title=None, alias=alias, description=description, collection=collection,
                         keywords=keywords, raw_data_file=raw_data_file, extension=extension,
                         metadata=metadata, clear_dependencies=None, add_dependencies=dependencies,
                         remove_dependencies=None, project=project, verbose=verbose)
    
    options = options.replace('-A', '-D') # For declaring dependencies
    options = options.strip()       
        
    com = 'data create "' + title + '" ' + options
        
    if verbose:
        print('Creating new record with DataFed command:\n\t' + com)
    message = df.command(com)
    
    if message[1] == 'RecordDataReply':
        return DataRecord(message)
    else:
        raise ValueError(message[0].err_msg)
    
    return message


def move_to_collection(ids, source_coll, dest_coll, verbose=False):
    def _send_command(com, verbose=False):
        if verbose:
            print('Sending command: ' + com)
        message = df.command(com)
        if message[1] != 'ListingReply':
            if 'already linked to ' in message[0].err_msg:
                warn(message[0].err_msg)
                return message
            elif 'does not exist' in message[0].err_msg:
                warn(message[0].err_msg)
                return message
            raise ValueError(
                'Something went wrong: \treceived meessage: {}'.format(
                    message))
        return message

    if isinstance(ids, str):
        ids = [ids]
    if not isinstance(ids, (list, tuple)):
        raise TypeError('ids must either be a string or a list of strings '
                        'denoting record or collection ids')

    ids_per_batch = 10
    for start_ind in range(0, len(ids), ids_per_batch):
        batch_id_list = ' '.join(ids[start_ind: start_ind + ids_per_batch])
        com = 'coll add ' + batch_id_list + ' ' + dest_coll
        mesg = _send_command(com, verbose=verbose)
        com = 'coll remove ' + batch_id_list + ' ' + source_coll
        mesg = _send_command(com, verbose=verbose)
    return mesg
        
        
def data_update(data_id, title=None, alias=None, description=None,
                keywords=None, raw_data_file=None, extension=None,
                metadata=None, clear_dependencies=None, add_dependencies=None,
                remove_dependencies=None, project=None, verbose=True):
    """
    -t, --title TEXT                Title
    -a, --alias TEXT                Alias
    -d, --description TEXT          Description text
    -k, --keywords TEXT             Keywords (comma separated list)
    -r, --raw-data-file TEXT        Globus path to raw data file (local or
                                    remote) to upload with record. Default
                                    endpoint used if none provided.
  -e, --extension TEXT            Override extension for raw data file
                                  (default = auto detect).
  -m, --metadata TEXT             Inline metadata in JSON format.
  -f, --metadata-file FILENAME    Path to local metadata file containing JSON.
  -C, --dep-clear                 Clear all dependencies on record. May be
                                  used in conjunction with --dep-add to
                                  replace existing dependencies.
  -A, --dep-add <CHOICE TEXT>...  Specify new dependencies by listing first
                                  the type of relationship ('der', 'comp', or
                                  'ver') follwed by ID/alias of the target
                                  record. Can be specified multiple times.
  -R, --dep-rem <CHOICE TEXT>...  Specify dependencies to remove by listing
                                  first the type of relationship ('der',
                                  'comp', or 'ver') followed by ID/alias of
                                  the target record. Can be specified multiple
                                  times.
  -p, --project TEXT              Project ID for command
    """
    options = _data_update_create(title=title, alias=alias, description=description,
                                  collection=None,
                                  keywords=keywords, raw_data_file=raw_data_file, 
                                  extension=extension, metadata=metadata, 
                                  clear_dependencies=clear_dependencies, 
                                  add_dependencies=add_dependencies,
                                  remove_dependencies=remove_dependencies, 
                                  project=project, verbose=verbose)
        
    options = options.strip()
    
    if len(options) == 0:
        raise ValueError('Nothing meaningful provided to update')
            
    com = 'data update ' + options + ' ' + data_id
    
    if verbose:
        'Updating record using DataFed command:\n\t' + com
    
    message = df.command(com)
    
    if message[1] == 'RecordDataReply':
        return DataRecord(message)
    else:
        raise ValueError(message[0].err_msg)
    
    return message


def put_df_data(record_id, data_path, wait=True, verbose=True):
    com = 'data put' 
    if wait:
        com += ' --wait'
    com += ' ' + record_id
    #if not os.path.exists(data_path):
    #    raise FileNotFoundError('Provided data file does NOT exist')
    com += ' "' + data_path + '"'
    if verbose:
        print('DataFed Command:\n\t' + com)
        if wait:
            print('Waiting for data to be uploaded....')
    
    attempts = 0
    message = None
    
    while attempts < 2:
        try:
            message = df.command(com)
        except Exception as excep:
            if excep.args[0] == 'No endpoint set':
                set_globus_endpoint()
            else:
                raise excep
            attempts += 1
        break
    
    if message is None:
        raise ValueError('Something went wrong when putting data for record: ' + record_id)
        
    try:
        _ = message[0].xfr[0].status
    except Exception as excep:
        print(excep)
        raise excep
        
    if wait and message[0].xfr[0].status != 3:
        print(message)
        raise ValueError('Something went wrong with the transfer for record: ' + record_id)
    
    if verbose and wait and message[0].xfr[0].status == 3:
        print('Finished data upload successfully for record: ' + record_id)
       
    return message


def create_datafed_record(h5_path, md_json_path=None, collection=None, 
                          keywords=None, wait_on_xfr=True, check_for_existing=True, 
                          verbose=True):
    
    h5_path = os.path.abspath(h5_path)
    if verbose:
        print('Absolute path for provided h5 file:\n' + h5_path)
    
    # We don't have a better title than the file name for now
    dir_path, title = os.path.split(h5_path)
    # Let's just remove the extension from the file name to make it the title
    title = title[:-3] # '.'.join(title.split('.')[:-1])
    if verbose:
        print('Title for record will be: ' + title)
    
    if md_json_path is None:
        if verbose:
            print('Attempting to find companion JSON file with metadata in same directory')
        # Attempt to find the JSON if available with same file name:
        for ext in ['.JSON', '.json']:
            if os.path.exists(os.path.join(dir_path, title + ext)):
                md_json_path = os.path.join(dir_path, title + ext)
                break
        if verbose and md_json_path is None:
            print('No JSON file found with same base name as the h5 file')
            return None
        
        if verbose:
            print('Will use JSON file: ' + md_json_path)
    
    dat_rec = create_df_record(title, check_for_existing=check_for_existing,
                               collection=collection, 
                               keywords=keywords, 
                               md_json_path=md_json_path, 
                               verbose=verbose)
    
    put_msg = put_df_data(dat_rec.id, h5_path, wait=wait_on_xfr, verbose=verbose)
    
    return put_msg   


def check_and_insert(item, verbose=True):
    datafed_init()
    base_name = os.path.split(item)[-1].replace('.h5', '')
    
    alias = get_clean_alias(base_name)
    # check if this file already exists in DataFed
    if record_exists(alias, verbose=verbose):
        if verbose:
            print('File: ' + item + ' already exists in DataFed. Skipping')
            return
    
    # if not, put into DataFed
    message = create_datafed_record(item, check_for_existing=False, verbose=verbose)
        
    if message is None:
        raise ValueError('Something went wrong')


def push_all_datasets_to_datafed(root_dir, parallel=False, verbose=True):
    all_files = os.listdir(root_dir)
    
    h5_file_paths = list()
    
    for item in all_files:
        if item.endswith('.h5'):
            h5_file_paths.append(os.path.join(root_dir, item))
                
    func = check_and_insert
    func_args = list()
    func_kwargs = {'verbose': verbose}
    if parallel:
        cores = cpu_count()
    else:
        cores = 1

    print('Using {} CPU cores to put {} files into DataFed'.format(cores, len(h5_file_paths)))
    
    if parallel:
        values = [joblib.delayed(func)(x, *func_args, **func_kwargs) for x in h5_file_paths]
        results = joblib.Parallel(n_jobs=cores)(values)
        # print(results)
    else:    
        for item in h5_file_paths:
            check_and_insert(item)


if __name__ == '__main__':
    datafed_init()
    if False:
        message = create_datafed_record(sys.argv[1])
        if message is None:
            raise ValueError('Something went wrong')
    else:
        push_all_datasets_to_datafed(sys.argv[1], parallel=False, verbose=False)
