# NeuronXY Project
This project contains code for an on prem, mostly automated solution for big data processing and data lakes/warehouses (please note the functionality of the services varies as development is still very much happening). It started as a mockup for AWS services but now is evolving to use open source solutions such as Docker and Hadoop to actually provide not just a simulated AWS environment but actual big data solutions

### 🔒 Security Note
NeuronXY uses default passwords in docker-compose for ease of local setup. These services run on localhost only and are intended for learning and experimentation. If you expose these services beyond your local machine, change the default passwords immediately.

### config.py File and it's Contents
The config file contains the db information in order to connect to the database, here are the following parameters that should be specified:
- DATABASE = os.getenv('POSTGRES_DB', 'neuronmockdb')
- HOST = os.getenv('POSTGRES_HOST', 'db')
- PORT = os.getenv('POSTGRES_PORT', '5432')
- USER = os.getenv('POSTGRES_USER', 'admin')
- PASSWORD = os.getenv('PASSWORD')

### Needed Empty Directory and Subdirectory Structure
- NeuronXY
  - users
  - externalFiles

### Notes on Dockercompose File
Upon running the default dockercompose file provided with this project, you will see the following containers:
- One default CLI app container
- One Postgres DB for metadata handling
- A cron container for scheduled jobs running on Postgres
- HDFS HA ecosystem which by default includes:
   - 2 Name Nodes
   - 3 Data Nodes
   - 3 Journal Nodes
   - 3 Zookeeper Nodes

**Note on HDFS environment:** There is a file provided called "generate_docker_compose.py." **If you find that your HDFS environment needs more nodes for data storage or higher failover/avaliability,** this script will largely help to refactor the docker-compose file to scale according to your needs, including adding docker mounts to each component of the HDFS HA arcitecture. **Please note that scaling the docker-file may require manual configuration of the hdfs-site file, or the actual docker image used for creating the HDFS services,** I would love to make it so no manual intervention is needed when scaling up but that is still a work in progress! I am just one guy so please be patient with me :/ **No you cannot scale down from the autoconfigured settings for the default HDFS ecosystem,** if this is desired you may autoconfigure the files provided in this project as you wish to fit your needs :)

**Note on terminal entry:** For terminal entry, you may either run docker-compose run cli which will automatically execute the entry point "main.py" script for this project or you can use the prompt for the cli-1 container and type "python UI/main.py" as the executable command to access the actual application. From there, the application and all related features will be accessible from the CLI. 

### Terminal Helper Commands
To get started with terminal commands, you can use one of two terminal helper commands:
- '?' Command: This command will show you every avaliable command or sub command that can follow previous commands. For example neuronXY ? should display all sub commands avaliable after the 'neuronXY' command.
- '-help' Command: If you aren't sure what a command does, simply type the command and follow it with the '-help' tag. It will provide a detailed lineage of each command and how following commands will interact with perceeding ones. For example you can type 'aws -del user -help' to find out what each command in the 'aws -del user' command block will do
