from app.config import PASSWORD

def validation_node_vals(numbers:list, nodetypes:list):
    for i in range(len(nodetypes)):
      # Check if the number is positive
      if numbers[i] <= 0:
          return [False, 'The number of nodes may not be negitive, docker-compose formatting failed']
      
      if nodetypes[i] in ['journal_node', 'zookeeper_node']:
      # Check if the number is odd (to avoid split votes)
        if numbers[i] % 2 == 0:
            return [False, 'The number of journal or zookeeper nodes must be odd in order to avoid split votes, docker-compose formatting failed']
      
      # Most distributed systems require at least 3 nodes for proper consensus
      if nodetypes[i] != 'namenode':
        if numbers[i] < 3:
            return [False, 'All nodes except for the name nodes require three or more specified nodes, docker-compose formatting failed']
        
      else:
        if numbers[i] < 2:
            return [False, 'At least one standby name node must be included in the HDFS arcitecture, docker-compose formatting failed']
    
    # All positive odd numbers >= 3 are valid
    return [True, 'Values look good!']

def process_documentation(doc):
    """Process the provided documentation string."""
    # Do something with the documentation
    return len(doc.split())

def write_to_file(docker_compose_filepath, text_to_write):
    with open(docker_compose_filepath, 'a') as f:
        f.write(text_to_write)

def generate_docker_compose(db_password:str, num_journal_nodes:int=3, num_zk_nodes:int=3, num_namenodes:int=2, num_datanodes:int=3):
    """Used for dynamic changes to docker-file configs, i.e. a user may want 6 data nodes instead of 3 depending on their needs, etc"""
    pass_validation = validation_node_vals([num_journal_nodes, num_zk_nodes, num_namenodes, num_datanodes], ['journal_node', 'zookeeper_node', 'namenode', 'datanode'])
    if not pass_validation[0]:
      print(pass_validation[1])
      return
    write_to_file('docker-compose.yml', f"""services:
  db:
    image: postgres:17
    environment:
      POSTGRES_HOST: db
      POSTGRES_USER: admin
      POSTGRES_PASSWORD: p1934rubvf-1938rfv-98sdyfgpeowquhfgvp[312908yut
      POSTGRES_DB: awsmockdb
    ports:
      - "5434:5432"
    volumes:
      - ./postgres_data:/var/lib/postgresql/data
      - ./db/sql-init-scripts:/docker-entrypoint-initdb.d # Run DB initilization scripts
    networks:
      - default_network

  cli:
    build:
      context: ./app
      dockerfile: Dockerfile
    volumes:
      - app_data:/app/AWS
    stdin_open: true
    tty: true
    command: ["bash", "-c", "python main.py; exec bash"]
    image: aws-mockup-cli
    environment:
      - POSTGRES_HOST=db
      - POSTGRES_PORT=5432
      - POSTGRES_USER=admin
      - POSTGRES_PASSWORD={db_password}
      - POSTGRES_DB=awsmockdb
    depends_on:
      - db
      - namenode1
    networks:
      - default_network

  cron-job:
    build:
      context: ./cron
      dockerfile: Dockerfile
    image: cron-contain
    depends_on:
      - db
    restart: always
    volumes:
    - ./db:/db
    networks:
      - default_network""")
        
    for i in range(1, num_journal_nodes + 1):
      write_to_file('docker-compose.yml', f"""
  
  journalnode{i}:
    image: hadoop-env
    container_name: journalnode{i}
    hostname: journalnode{i}
    environment:
      - HADOOP_ROLE=journalnode
    volumes:
      - hadoop_journalnode{i}:/hadoopdata/journalnode  # Local directory for JournalNode1
    ports:
      - "{str(8485 + (i - 1))}:8485"  # JournalNode RPC port
      - "{str(8480 - (i - 1))}:8480"  # JournalNode HTTP UI port
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8480/jmx"]
      interval: 10s
      timeout: 5s
      retries: 10
    networks:
      - default_network""")

    for i in range(1, num_zk_nodes + 1):
      write_to_file('docker-compose.yml', f"""
  
  zookeeper{i}:
    image: hadoop-env
    container_name: zookeeper{i}
    hostname: zookeeper{i}
    environment:
      - HADOOP_ROLE=zookeeper
      - ZOOKEEPERNODEINSTANCE={i}
    volumes:
      - hadoop_zookeeper{i}:/hadoopdata/zookeeper  # If you're mounting config files
    ports:
      - "{str(2181 + (i - 1))}:2181"
    healthcheck:
      test: ["CMD", "/opt/zookeeper/bin/zkServer.sh", "status"]
      interval: 10s
      timeout: 5s
      retries: 10
    networks:
      - default_network""")

    for i in range(1, num_namenodes + 1):
      if i == 1:
        write_to_file('docker-compose.yml', f"""
  
  namenode1:
    image: hadoop-env
    container_name: namenode1
    hostname: namenode1
    environment:
      - HADOOP_ROLE=namenode
    volumes:
      - hadoop_namenode1:/hadoopdata/namenode  # If you're mounting config files
      - hadoop_zkfc_init:/hadoopdata/zkfc/formatted
    ports:
      - "9870:9870"  # Expose ports if necessary
      - "8020:8020"
    depends_on:
      journalnode1:
        condition: service_healthy
      journalnode2:
        condition: service_healthy
      journalnode3:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9870/jmx"]  # Check if HTTP UI is up
      interval: 10s
      timeout: 5s
      retries: 10
    networks:
      - default_network""")

      else:
        write_to_file('docker-compose.yml', f"""
  
  namenode{i}:
    image: hadoop-env
    container_name: namenode{i}
    hostname: namenode{i}
    environment:
      - HADOOP_ROLE=standbynamenode
    volumes:
      - hadoop_namenode{i}:/hadoopdata/namenode # If you're mounting config files
    ports:
      - "{str(9870 + (i - 1))}:9870"  # Expose ports if necessary
      - "{str(8020 + (i - 1))}:8020"
    depends_on:
      journalnode1:
        condition: service_healthy
      journalnode2:
        condition: service_healthy
      journalnode3:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9870/jmx"]  # Check if HTTP UI is up
      interval: 10s
      timeout: 5s
      retries: 10
    networks:
      - default_network""")
        
    for i in range(1, num_datanodes + 1):
      write_to_file('docker-compose.yml', f"""
  
  datanode{i}:
    image: hadoop-env
    container_name: datanode{i}
    environment:
      - HADOOP_ROLE=datanode
    volumes:
      - hadoop_datanode{i}:/hadoopdata/datanode
    depends_on:
      namenode1:
        condition: service_healthy
      namenode2:
        condition: service_healthy
    networks:
      - default_network""")

    write_to_file('docker-compose.yml',"""

volumes:
  app_data:
  hadoop_zkfc_init:""")

    for i in range(1, num_journal_nodes + 1):
      write_to_file('docker-compose.yml',f"""  
  hadoop_journalnode{i}:""")

    for i in range(1, num_zk_nodes + 1):
      write_to_file('docker-compose.yml',f"""  
  hadoop_zookeeper{i}:""")
      
    for i in range(1, num_namenodes + 1):
      write_to_file('docker-compose.yml',f"""  
  hadoop_namenode{i}:""")

    for i in range(1, num_datanodes + 1):
      write_to_file('docker-compose.yml',f"""  
  hadoop_datanode{i}:""")

    write_to_file('docker-compose.yml', f"""

networks:
  default_network:
    driver: bridge""")
        
if __name__ == '__main__':
  generate_docker_compose(PASSWORD)