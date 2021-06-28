## newer constarint in one sql query able to run one query only.

##Modified code from schemachange for snowflake to mysql
##Author:Krishna Agrawal
##Company: Cuelogic Technologies Pune

import mysql.connector
import os
import datetime
import re
import hashlib
import string
import time
import argparse
import json
import warnings
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization




# Set a few global variables here
_schemachange_version = '1.1'
_metadata_database_name = 'metadata'
_metadata_table_name = 'change_history'



# Define the Jinja expression template class
# schemachange uses Jinja style variable references of the form "{{ variablename }}"
# See https://jinja.palletsprojects.com/en/2.11.x/templates/
# Variable names follow Python variable naming conventions
class JinjaExpressionTemplate(string.Template):
    delimiter = '{{ '
    pattern = r'''
    \{\{[ ](?:
    (?P<escaped>\{\{)|
    (?P<named>[_A-Za-z][_A-Za-z0-9]*)[ ]\}\}|
    (?P<braced>[_A-Za-z][_A-Za-z0-9]*)[ ]\}\}|
    (?P<invalid>)
    )
    '''

def schemachange_mysql(root_folder, mysql_account, mysql_user,
                 mysql_database, change_history_table_override, vars, create_change_history_table, autocommit,
                 verbose, dry_run):
    if dry_run:
        print("Running in dry-run mode")
    root_folder = os.path.abspath(root_folder)
    if not os.path.isdir(root_folder):
        raise ValueError("Invalid root folder: %s" % root_folder)

    print("schemachange version: %s" % _schemachange_version)
    print("Using root folder %s" % root_folder)
    print("Using variables %s" % vars)
    print("Using Mysql account %s" % mysql_account)
    print("Using default database %s" % mysql_database)

    # Set default Snowflake session parameters
    mysql_session_parameters = {
        "QUERY_TAG": "schemachange %s" % _schemachange_version
    }

    # TODO: Is there a better way to do this without setting environment variables?
    os.environ["MYSQL_ACCOUNT"] = mysql_account
    os.environ["MYSQL_USER"] = mysql_user
    os.environ["MYSQL_AUTHENTICATOR"] = 'mysql'

    print("url used %s" % os.getenv("MYSQL_ACCOUNT"))


    scripts_skipped = 0
    scripts_applied = 0

    # Deal with the change history table (create if specified)

    change_history_table = get_change_history_table_details(change_history_table_override)
    # print("get changes from change_history",change_history_table)


    change_history_metadata = fetch_change_history_metadata(change_history_table, mysql_session_parameters,
                                                            autocommit, verbose)
    # print(change_history_metadata)

    if change_history_metadata:

        print("Using change history table %s.%s (last altered %s)" % (
        change_history_table['database_name'], change_history_table['table_name'],
        change_history_metadata['last_altered']))
    elif create_change_history_table:
        # Create the change history table (and containing objects) if it don't exist.
        if not dry_run:
            create_change_history_table_if_missing(change_history_table, mysql_session_parameters, autocommit,
                                                   verbose)
        print("Created change history table %s.%s" % (
        change_history_table['database_name'], change_history_table['table_name']))
    else:
        raise ValueError("Unable to find change history table %s.%s" % (
        change_history_table['database_name'], change_history_table['table_name']))

        # Find the max published version
    max_published_version = ''

    change_history = None
    if (dry_run and change_history_metadata) or not dry_run:
        change_history = fetch_change_history(change_history_table, mysql_session_parameters, autocommit, verbose)
        # print("change tracking history :",change_history)

    if change_history:
        max_published_version = change_history[0]
        print("Max applied change script version:",max_published_version)
    max_published_version_display = max_published_version
    if max_published_version_display == '':
        max_published_version_display = 'None'
    print("Max applied change script version: %s" % max_published_version_display)

    # Find all scripts in the root folder (recursively) and sort them correctly
    all_scripts = get_all_scripts_recursively(root_folder, verbose)
    all_script_names = list(all_scripts.keys())
    # print(all_script_names)
    # Sort scripts such that versioned scripts get applied first and then the repeatable ones.
    all_script_names_sorted = sorted_alphanumeric([script for script in all_script_names if script[0] == 'V']) \
                              + sorted_alphanumeric([script for script in all_script_names if script[0] == 'R'])
    # print(all_script_names_sorted)

    # Loop through each script in order and apply any required changes
    for script_name in all_script_names_sorted:
        script = all_scripts[script_name]
        # print(script)

        # Apply a versioned-change script only if the version is newer than the most recent change in the database
        # Apply any other scripts, i.e. repeatable scripts, irrespective of the most recent change in the database
        if script_name[0] == 'V' and get_alphanum_key(script['script_version']) <= get_alphanum_key(
                max_published_version):
            if verbose:
                print("Skipping change script %s because it's older than the most recently applied change (%s)" % (
                    script['script_name'], max_published_version))
            scripts_skipped += 1
            continue
        print("Applying change script %s" % script['script_name'])
        if not dry_run:
            apply_change_script(script, vars, mysql_database, change_history_table, mysql_session_parameters,
                                autocommit, verbose)

        scripts_applied += 1

    print("Successfully applied %d change scripts (skipping %d)" % (scripts_applied, scripts_skipped))
    print("Completed successfully")




def get_change_history_table_details(change_history_table_override):
    # Start with the global defaults
    details = dict()
    details['database_name'] = _metadata_database_name
    details['table_name'] = _metadata_table_name

    # Then override the defaults if requested. The name could be in one, two or three part notation.
    if change_history_table_override is not None:
        table_name_parts = change_history_table_override.strip().split('.')

        if len(table_name_parts) == 1:
            details['table_name'] = table_name_parts[0]
        elif len(table_name_parts) == 2:
            details['table_name'] = table_name_parts[1]
            details['database_name'] = table_name_parts[0]
        # elif len(table_name_parts) == 3:
        #     details['table_name'] = table_name_parts[1]
        #     details['database_name'] = table_name_parts[0]
        else:
            raise ValueError("Invalid change history table name: %s" % change_history_table_override)

    return details


def fetch_change_history_metadata(change_history_table, mysql_session_parameters, autocommit, verbose):
    # This should only ever return 0 or 1 rows
    query = "SELECT CREATE_TIME AS CREATED, UPDATE_TIME AS LAST_ALTERED FROM INFORMATION_SCHEMA.TABLES  WHERE  TABLE_SCHEMA LIKE '{0}' AND TABLE_NAME LIKE '{1}'".format(
        change_history_table['database_name'],change_history_table['table_name'])

    results = execute_mysql_query(change_history_table['database_name'], query, mysql_session_parameters,
                                      autocommit, verbose)

    # Collect all the results into a list

    change_history_metadata = dict()
    # print("results",results)

    for cursor in results:

        # print("nonecheck", cursor)
        # if cursor is not None:


        change_history_metadata['created'] = cursor[0]

        change_history_metadata['last_altered'] =cursor[1]

    return change_history_metadata


def create_change_history_table_if_missing(change_history_table, mysql_session_parameters, autocommit, verbose):
    # Create the schema if it doesn't exist
    query = "CREATE SCHEMA IF NOT EXISTS {0}".format(change_history_table['database_name'])
    execute_mysql_query(change_history_table['database_name'], query, mysql_session_parameters, autocommit,
                            verbose)

    # Finally, create the change history table if it doesn't exist
    query = "CREATE TABLE IF NOT EXISTS {0}.{1} (VERSION VARCHAR(10), DESCRIPTION VARCHAR(20), SCRIPT VARCHAR(100), SCRIPT_TYPE VARCHAR(10), CHECKSUM LONGTEXT, EXECUTION_TIME INT, STATUS VARCHAR(10), INSTALLED_BY VARCHAR(10), INSTALLED_ON TIMESTAMP)".format(
        change_history_table['database_name'], change_history_table['table_name'])
    execute_mysql_query(change_history_table['database_name'], query, mysql_session_parameters, autocommit,
                            verbose)



def fetch_change_history(change_history_table, mysql_session_parameters, autocommit, verbose):
    query = "SELECT VERSION FROM {0}.{1} WHERE SCRIPT_TYPE = 'V' ORDER BY INSTALLED_ON DESC LIMIT 1".format(
        change_history_table['database_name'], change_history_table['table_name'])
    # print("fetch_change_history",query)
    results = execute_mysql_query(change_history_table['database_name'], query, mysql_session_parameters,
                                      autocommit, verbose)
    # print("fetch_change_history",results)

    # Collect all the results into a list
    change_history = list()
    for cursor in results:
        # print("fetch_change_history",cursor)
        for row in cursor:
            # print("fetch_change_history", row)
            # change_history.append(row[0])
            change_history.append(row)

    return change_history





def get_all_scripts_recursively(root_directory, verbose):
    all_files = dict()
    all_versions = list()
    # Walk the entire directory structure recursively
    for (directory_path, directory_names, file_names) in os.walk(root_directory):
        for file_name in file_names:

            file_full_path = os.path.join(directory_path, file_name)
            script_name_parts = re.search(r'^([V])(.+)__(.+)\.(?:sql|SQL)$', file_name.strip())
            repeatable_script_name_parts = re.search(r'^([R])__(.+)\.(?:sql|SQL)$', file_name.strip())

            # Set script type depending on whether it matches the versioned file naming format
            if script_name_parts is not None:
                script_type = 'V'
                if verbose:
                    print("Versioned file " + file_full_path)
            elif repeatable_script_name_parts is not None:
                script_type = 'R'
                if verbose:
                    print("Repeatable file " + file_full_path)
            else:
                if verbose:
                    print("Ignoring non-change file " + file_full_path)
                continue

            # Add this script to our dictionary (as nested dictionary)
            script = dict()
            script['script_name'] = file_name
            script['script_full_path'] = file_full_path
            script['script_type'] = script_type
            script['script_version'] = None if script_type == 'R' else script_name_parts.group(2)
            script['script_description'] = (
                repeatable_script_name_parts.group(2) if script_type == 'R' else script_name_parts.group(3)).replace(
                '_', ' ').capitalize()
            all_files[file_name] = script

            # Throw an error if the same version exists more than once
            if script_type == 'V':
                if script['script_version'] in all_versions:
                    raise ValueError("The script version %s exists more than once (second instance %s)" % (
                    script['script_version'], script['script_full_path']))
                all_versions.append(script['script_version'])

    return all_files

def get_alphanum_key(key):
    convert = lambda text: int(text) if text.isdigit() else text.lower()
    alphanum_key = [convert(c) for c in re.split('([0-9]+)', key)]
    return alphanum_key

def sorted_alphanumeric(data):
    return sorted(data, key=get_alphanum_key)


def apply_change_script(script, vars, default_database, change_history_table, mysql_session_parameters, autocommit,
                        verbose):
    # First read the contents of the script
    with open(script['script_full_path'], 'r') as content_file:
        content = content_file.read().strip()
        # print("initial1 content :" , content)
        content = content[:-1] if content.endswith(';') else content
        # print("initial2 content :", content)

    # Define a few other change related variables
    checksum = hashlib.sha224(content.encode('utf-8')).hexdigest()

    execution_time = 0
    status = 'Success'

    # Replace any variables used in the script content
    content = replace_variables_references(content, vars, verbose)

    # print("initial3 content :", content)


    # Execute the contents of the script
    if len(content) > 0:
        start = time.time()
        session_parameters = mysql_session_parameters.copy()
        session_parameters["QUERY_TAG"] += ";%s" % script['script_name']
        execute_mysql_query(default_database, content, session_parameters, autocommit, verbose)
        end = time.time()
        execution_time = round(end - start)

    # Finally record this change in the change history table
    query = "INSERT INTO {0}.{1} (VERSION, DESCRIPTION, SCRIPT, SCRIPT_TYPE, CHECKSUM, EXECUTION_TIME, STATUS, INSTALLED_BY, INSTALLED_ON) values ('{2}','{3}','{4}','{5}','{6}',{7},'{8}','{9}',CURRENT_TIMESTAMP);".format(
        change_history_table['database_name'], change_history_table['table_name'], script['script_version'],
        script['script_description'], script['script_name'], script['script_type'], checksum, execution_time, status,
        os.environ["MYSQL_USER"])
    execute_mysql_query(change_history_table['database_name'], query, mysql_session_parameters, autocommit,
                            verbose)

# This method will throw an error if there are any leftover variables in the change script
# Since a leftover variable in the script isn't valid SQL, and will fail when run it's
# better to throw an error here and have the user fix the problem ahead of time.
def replace_variables_references(content, vars, verbose):
    t = JinjaExpressionTemplate(content)
    # print("vars" , vars)
    # print("replace jinja ", t.substitute(vars))
    return t.substitute(vars)


def execute_mysql_query(mysql_database,query,mysql_session_parameters, autocommit, verbose):
    mysql_password = None
    if os.getenv("MYSQL_PASSWORD") is not None and os.getenv("MYSQL_PASSWORD"):
        mysql_password = os.getenv("MYSQL_PASSWORD")

        if mysql_password is not None:
            if verbose:
                print("Proceeding with password authentication")

            con = mysql.connector.connect(
                host=os.environ["MYSQL_ACCOUNT"],
                user=os.environ["MYSQL_USER"],
                passwd=mysql_password,
                database=mysql_database,
                autocommit=autocommit
            )

    if not autocommit:
        print("autocommit",autocommit)
        con.autocommit=False


    # print(autocommit)
    cursor = con.cursor()
    # print("SQL query: %s" % query)


    if verbose:
        print("SQL query: %s" % query)


    try:
        cursor.execute(query)
        res=cursor.fetchall()

        if not autocommit:
            print("autocommit is off")
            con.commit()
        return res
    except Exception as e:
        if not autocommit:

            print("something fail")
            con.rollback()
        raise e
    finally:
        con.close()


def main():
    parser = argparse.ArgumentParser(prog='schemachange_mysql',
                                     description='Apply schema changes to a Mysql account. ',
                                     formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-f', '--root-folder', type=str, default=".",
                        help='The root folder for the database change scripts', required=False)
    parser.add_argument('-a', '--mysql-account', type=str,
                        help='The name of the snowflake account (e.g. xy12345.east-us-2.azure)', required=True)
    parser.add_argument('-u', '--mysql-user', type=str, help='The name of the snowflake user', required=True)
    parser.add_argument('-d', '--mysql-database', type=str,
                        help='The name of the default database to use. Can be overridden in the change scripts.',
                        required=False)
    parser.add_argument('-c', '--change-history-table', type=str,
                        help='Used to override the default name of the change history table (the default is METADATA.SCHEMACHANGE.CHANGE_HISTORY)',
                        required=False)
    parser.add_argument('--vars', type=json.loads,
                        help='Define values for the variables to replaced in change scripts, given in JSON format (e.g. {"variable1": "value1", "variable2": "value2"})',
                        required=False)
    parser.add_argument('--create-change-history-table', action='store_true',
                        help='Create the change history schema and table, if they do not exist (the default is False)',
                        required=False)
    parser.add_argument('-ac', '--autocommit', action='store_true',
                        help='Enable autocommit feature for DML commands (the default is False)', required=False)
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Display verbose debugging details during execution (the default is False)',
                        required=False)
    parser.add_argument('--dry-run', action='store_true',
                        help='Run schemachange in dry run mode (the default is False)', required=False)
    args = parser.parse_args()

    schemachange_mysql(args.root_folder, args.mysql_account, args.mysql_user, args.mysql_database,
                       args.change_history_table, args.vars,
                       args.create_change_history_table, args.autocommit, args.verbose, args.dry_run)




if __name__ == "__main__":



    # schemachange_mysql('/home/cuelogic.local/krishna.agarawal/PycharmProjects/snow_change_mysql', '165.232.178.204',
    #                    'root', 'krishna',
    #                    'metadata.change_history', 'hi',
    #                    'metadata.change_history', True, False, False)

    # schemachange_mysql('/home/cuelogic.local/krishna.agarawal/PycharmProjects/snow_change_mysql_v1.1/sql_changes', '165.232.178.204',
    #                    'root', 'krishna',
    #                    'metadata.change_history', 'hi',
    #                    'metadata.change_history', True, False, False)


    main()