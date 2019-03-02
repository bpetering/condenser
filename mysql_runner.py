import os, pathlib, re, urllib

class MysqlRunner:
    def __init__(self, database_connection_info):
        self.connection_info = database_connection_info

    def run_query(self, query, output_path = None, error_path = None, overwrite_output = False, overwrite_error = False):

        # TODO check v 5.1.38 - before that not INFORMATION_SCHEMA dumping in mysqldump

        mysqldump_path = get_mysql_bin_path()
        cur_path = os.getcwd()

        if(mysqldump_path != ''):
            os.chdir(mysqldump_path)

        if overwrite_error is False:
            error_redirect = '2>>'
        else:
            error_redirect = '2>'

        if overwrite_output is False:
            output_redirect = '>>'
        else:
            output_redirect = '>'


        if output_path is None and error_path is None:
            os.system('mysql -h {0} -u {1} -p{2} -P {3} -e "{4}" {5} > {6} 2> {6}'
                .format(
                        self.connection_info['host'],
                        self.connection_info['user_name'],
                        self.connection_info['password'],
                        self.connection_info['port'], 
                        query, 
                        self.connection_info['db_name'],
                        os.devnull
                )
            )

        elif output_path is None and error_path is not None:
            os.system('mysql -h {0} -u {1} -p{2} -P {3} -e "{4}" {5} {6} {7} > {8}'
                .format(
                    self.connection_info['host'],
                    self.connection_info['user_name'],
                    self.connection_info['password'],
                    self.connection_info['port'],
                    query, 
                    self.connection_info['db_name'],
                    error_redirect, 
                    error_path, 
                    os.devnull
                )
            )

        elif output_path is not None and error_path is None:
            os.system('mysql -h {0} -u {1} -p{2} -P {3} -e "{4}" {5} {6} {7} 2> {8}'
                .format(
                    self.connection_info['host'],
                    self.connection_info['user_name'],
                    self.connection_info['password'],
                    self.connection_info['port'], 
                    query, 
                    self.connection_info['db_name'],
                    output_redirect, 
                    output_path, 
                    os.devnull
                )
            )

        elif output_path is not None and error_path is not None:
            os.system('mysql -h {0} -u {1} -p{2} -P {3} -e "{4}" {5} {6} {7} {8} {9}'
                .format(
                    self.connection_info['host'],
                    self.connection_info['user_name'],
                    self.connection_info['password'],
                    self.connection_info['port'], 
                    query, 
                    self.connection_info['db_name'],
                    output_redirect, 
                    output_path, 
                    error_redirect, 
                    error_path
                )
            )

        os.chdir(cur_path)

    def run(self, file_path, output_path = None, error_path = None, overwrite_output = False, overwrite_error = False):

        if not os.path.exists(file_path):
            raise Exception('Could not find file {}'.format(file_path))

        mysqldump_path = get_mysql_bin_path()
        cur_path = os.getcwd()

        if(mysqldump_path != ''):
            os.chdir(mysqldump_path)

        if overwrite_error is False:
            error_redirect = '2>>'
        else:
            error_redirect = '2>'

        if overwrite_output is False:
            output_redirect = '>>'
        else:
            output_redirect = '>'


        if output_path is None and error_path is None:
            os.system('mysql -h {0} -u {1} -p{2} -P {3} -v {4} < {5}'
                .format(
                    self.connection_info['host'],
                    self.connection_info['user_name'],
                    self.connection_info['password'],
                    self.connection_info['port'], 
                    self.connection_info['db_name'],
                    file_path
                )
            )

        elif output_path is None and error_path is not None:
            os.system('mysql -h {0} -u {1} -p{2} -P {3} -v {4} < {5} {6} {7} > {8}'
                .format(
                    self.connection_info['host'],
                    self.connection_info['user_name'],
                    self.connection_info['password'],
                    self.connection_info['port'], 
                    self.connection_info['db_name'],
                    file_path, 
                    error_redirect, 
                    error_path, 
                    os.devnull
                )
            )

        elif output_path is not None and error_path is None:
            os.system('mysql -h {0} -u {1} -p{2} -P {3} -v {4} < {5} {6} {7} 2> {8}'
                .format(
                    self.connection_info['host'],
                    self.connection_info['user_name'],
                    self.connection_info['password'],
                    self.connection_info['port'], 
                    self.connection_info['db_name'],
                    file_path, 
                    output_redirect, 
                    output_path, 
                    os.devnull
                )
            )

        elif output_path is not None and error_path is not None:
            os.system('mysql -h {0} -u {1} -p{2} -P {3} -v {4} < {5} {6} {7} {8} {9}'
                .format(
                    self.connection_info['host'],
                    self.connection_info['user_name'],
                    self.connection_info['password'],
                    self.connection_info['port'], 
                    self.connection_info['db_name'],
                    file_path, 
                    output_redirect, 
                    output_path, 
                    error_redirect, 
                    error_path
                )
            )

        os.chdir(cur_path)

def get_mysql_bin_path():
    if 'MYSQL_PATH' in os.environ:
        mysqldump_path = os.environ['MYSQL_PATH']
    else:
        mysqldump_path = ''
    err = os.system('"' + os.path.join(mysqldump_path, 'mysqldump') + '"' + ' --help > ' + os.devnull)
    if err != 0:
        raise Exception("Couldn't find Mysql utilities, consider specifying MYSQL_PATH " +
                        "environment variable if Mysql isn't " +
                        "in your PATH.")
    return mysqldump_path
