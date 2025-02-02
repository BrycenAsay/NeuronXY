# AWS Mockup CLI Project
This project contains code for a working mock up CLI for AWS (please note these 'services' do not actual work nor have full functionality and are only meant as rough simulations of the same service as it relates to AWS)

### config.py File and it's Contents
The config file contains the db information in order to connect to the database, here are the following parameters that should be specified:
DATABASE = os.getenv('POSTGRES_DB', 'awsmockdb')
HOST = os.getenv('POSTGRES_HOST', 'db')
PORT = os.getenv('POSTGRES_PORT', '5432')
USER = os.getenv('POSTGRES_USER', 'admin')
PASSWORD = os.getenv('PASSWORD')

### Needed Empty Directory and Subdirectory Structure
- AWS
  - users
  - externalFiles

### Notes on Dockercompose File
Upon running the default dockercompose file provided with this project, two containers should be generated, a Postgres container and the actual application container, as well as a local volume mounted to your local drive in the project repository for Postgres information and a docker native volume to host user directory level information called "app_data." For terminal entry, you may either run docker-compose run cli which will automatically execute the entry point "main.py" script for this project or you can use the prompt for the cli-1 container and type "python main.py" as the executable command to access the actual application. From there, the application and all related features will be accessible from the CLI. 

### Terminal Helper Commands
To get started with terminal commands, you can use one of two terminal helper commands:
- '?' Command: This command will show you every avaliable command or sub command that can follow previous commands. For example aws ? should display all sub commands avaliable after the 'aws' command.
- '-help' Command: If you aren't sure what a command does, simply type the command and follow it with the '-help' tag. It will provide a detailed lineage of each command and how following commands will interact with perceeding ones. For example you can type 'aws -del user -help' to find out what each command in the 'aws -del user' command block will do
