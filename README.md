# snow_change_mysql

#overview

snow_change_mysql is a simple python based tool to manage all of your mysql objects. It follows an Imperative-style approach to Database Change Management (DCM) and was inspired by the schema change tool(https://github.com/phdata/snowchange) . When combined with a version control system and a CI/CD tool, database changes can be approved and deployed through a pipeline using modern software delivery practices. As such snowchange_mysql plays a critical role in enabling Database (or Data) DevOps.

DCM tools (also known as Database Migration, Schema Change Management, or Schema Migration tools) follow one of two approaches: Declarative or Imperative. For a background on Database DevOps, including a discussion on the differences between the Declarative and Imperative approaches, please read the Embracing Agile Software Delivery and DevOps with Snowflake blog post.

For the complete list of changes made to snowchange check out the CHANGELOG.


##running the script

snowchange is a single python script named schemachange_mysql.py. It can be executed as follows:

python schemachange_mysql.py [-h] [-f ROOT_FOLDER] -a MY_SQL host -u MYSQL_USER  -d 'metadata' -ac   [-c CHANGE_HISTORY_TABLE] [-v]

MYSQL_PASSWORD will MYSQL_USER is required to be set in the environment variable


##Script Parameters

Here is the list of supported parameters to the script:

Parameter	Description
-h, --help	Show the help message and exit
-f ROOT_FOLDER, --root-folder ROOT_FOLDER	(Optional) The root folder for the database change scripts. The default is the current directory.
-a MYSQL_ACCOUNT, --host of mysql connection
-u MYSQL_USER, --MYSQL-user 	The name of the mysql user (e.g. DEPLOYER)
-c CHANGE_HISTORY_TABLE, --change-history-table CHANGE_HISTORY_TABLE	Used to override the default name of the change history table (e.g. METADATA.CHANGE_HISTORY)
-v, --verbose	Display verbose debugging details during execution






