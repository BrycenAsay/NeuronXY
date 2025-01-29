# Twitter Data Project
This project contains code for a working mock up CLI for AWS (please note these 'services' do not actual work nor have full functionality and are only meant as rough simulations of the same service as it relates to AWS)


### Required Third Party Depencies/Libraries
+ requests_oauthlib - used to complete the OAuth1.0 handshake
+ sqlalchemy - used for updating into/reading from the database
+ requests - used to make API calls to Twitter
+ pandas - used mostly as a middle man for converting from a dictionary to a database table


### config.py File and it's Contents
The config file contains the db information in order to connect to the database, here are the following parameters that should be specified:
+ HOST = {the host for your database}
+ USER = {the user for your database}
+ PASSWORD = {the password for said user/database}
+ DATABASE = {the database you want to use to store all of the tables}


### Database Schema - table_schema_creation.sql
In this sql file are the commands needed to create the table definitions needed in the database in order for the CLI to work and persist data. Please run these commands in your database prior to running Python code. Please note I simply used the default schema upon creating a database, but you can define and use whatever schema you prefer

### Needed Empty Directory and Subdirectory Structure
- AWS
  - users
  - externalFiles

### Note on SQLAlchemy Connection
I have the SQLAlchemy connection using a postgres driver, which means that unless otherweise configured only postgres databases will work. It can be configured differently if desired, but the original code is only compatible with postgres database

### Terminal Commands
To start the terminal, you must run 'python main.py aws create user' at the directory all the files are contained in and you will be prompted to create credentials. Afterwards, you can type 'python main.py aws login' from which you will be prompted to login. From there, you should be able to type -help to see the list of terminal commands avaliable to you
and what they do. To see sub level commands, simply type a command and then -help again to see the list (i.e. '-res -help')
