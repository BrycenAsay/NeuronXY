#!/bin/bash

# Start the appropriate service based on the role
case $HADOOP_ROLE in
  namenode)
    # Wait for JournalNodes to be ready
    echo "Waiting for JournalNodes to be ready..."
    for node in journalnode1 journalnode2 journalnode3; do
      until nc -z $node 8485; do
        echo "Waiting for $node:8485..."
        sleep 2
      done
      echo "$node is ready!"
    done

    # Format ZKFC if necessary (only on primary namenode)
    if [ ! -f /hadoopdata/zkfc/formatted ]; then
      echo "Formatting ZKFC..."
      $HADOOP_HOME/bin/hdfs zkfc -formatZK -force
      mkdir -p /hadoopdata/zkfc
      touch /hadoopdata/zkfc/formatted
    else
      echo "ZKFC is already formatted. Skipping formatting."
    fi

    echo "Starting NameNode..."
    if [ ! -f /hadoopdata/namenode/current/VERSION ]; then
      echo "Formatting NameNode..."
      $HADOOP_HOME/bin/hdfs namenode -format -clusterId mycluster -force
      echo "Initializing shared edits log..."
      $HADOOP_HOME/bin/hdfs namenode -initializeSharedEdits -force
    else
      echo "NameNode is already formatted. Skipping formatting."
    fi

    # Create a mock nice command to prevent priority errors
    echo '#!/bin/bash
    exit 0' > /bin/nice.new
    chmod +x /bin/nice.new
    mv /bin/nice.new /bin/nice

    # Start the NameNode first
    $HADOOP_HOME/bin/hdfs --daemon start namenode

    # Then start ZKFC
    echo "Starting Zookeeper Failover Controller (ZKFC)..."
    $HADOOP_HOME/bin/hdfs --daemon start zkfc

    # Keep container running with a simple loop
    echo "Services started. Keeping container alive..."
    while true; do
      sleep 30
      # Check if processes are still running
      pgrep -f "proc_namenode" >/dev/null || echo "WARNING: NameNode process not found!"
      pgrep -f "proc_zkfc" >/dev/null || echo "WARNING: ZKFC process not found!"
    done
    ;;

  standbynamenode)
    # Create a mock nice command to prevent priority errors
    echo '#!/bin/bash
    exit 0' > /bin/nice.new
    chmod +x /bin/nice.new
    mv /bin/nice.new /bin/nice

    echo "Starting Standby NameNode..."
    # Bootstrap the Standby NameNode if needed
    if [ ! -f /hadoopdata/namenode/current/VERSION ]; then
      $HADOOP_HOME/bin/hdfs namenode -bootstrapStandby -force
    fi

    # Start the standby namenode
    $HADOOP_HOME/bin/hdfs --daemon start namenode

    # Start ZKFC after namenode
    echo "Starting Zookeeper Failover Controller (ZKFC)..."
    $HADOOP_HOME/bin/hdfs --daemon start zkfc

    # Keep container running with a simple loop
    echo "Services started. Keeping container alive..."
    while true; do
      sleep 30
      # Check if processes are still running
      pgrep -f "proc_namenode" >/dev/null || echo "WARNING: Standby NameNode process not found!"
      pgrep -f "proc_zkfc" >/dev/null || echo "WARNING: ZKFC process not found!"
    done
    ;;

  datanode)
    echo "Starting DataNode..."
    # Start the DataNode in the foreground to keep the container running
    $HADOOP_HOME/bin/hdfs datanode
    ;;

  journalnode)
    echo "Starting JournalNode..."
    # Start the JournalNode in the foreground to keep the container running
    $HADOOP_HOME/bin/hdfs journalnode
    ;;

  zookeeper)
    echo "Initializing and starting Zookeeper service..."

    # Define paths with explicit checks
    ZOOKEEPER_INSTALL_DIR="/opt/zookeeper"
    ZOOKEEPER_DATA_DIR="/hadoopdata/zookeeper"
    ZOOKEEPER_CONF_DIR="${ZOOKEEPER_INSTALL_DIR}/conf"
    ZOOKEEPER_MYID="${ZOOKEEPERNODEINSTANCE:-1}"  # Default to 1 if not set

    # Check if required directories exist and create them if needed
    if [ ! -d "$ZOOKEEPER_INSTALL_DIR" ]; then
        echo "ERROR: ZooKeeper installation directory not found at $ZOOKEEPER_INSTALL_DIR"
        exit 1
    fi

    # Ensure directories exist and are writeable
    mkdir -p "$ZOOKEEPER_DATA_DIR" || { echo "Failed to create data directory: $ZOOKEEPER_DATA_DIR"; exit 1; }
    mkdir -p "$ZOOKEEPER_CONF_DIR" || { echo "Failed to create conf directory: $ZOOKEEPER_CONF_DIR"; exit 1; }

    # Create the myid file
    echo "$ZOOKEEPER_MYID" > "$ZOOKEEPER_DATA_DIR/myid"

    # Create zoo.cfg with explicit paths (no variable substitution)
    echo "Creating zoo.cfg at $ZOOKEEPER_CONF_DIR/zoo.cfg"
    cat > "$ZOOKEEPER_CONF_DIR/zoo.cfg" << EOF
tickTime=2000
initLimit=10
syncLimit=5
dataDir=$ZOOKEEPER_DATA_DIR
clientPort=2181
server.1=zookeeper1:2888:3888
server.2=zookeeper2:2888:3888
server.3=zookeeper3:2888:3888
EOF

    # Verify the file was created
    if [ ! -f "$ZOOKEEPER_CONF_DIR/zoo.cfg" ]; then
        echo "ERROR: Failed to create zoo.cfg"
        ls -la "$ZOOKEEPER_CONF_DIR/"
        exit 1
    fi

    echo "Contents of zoo.cfg:"
    cat "$ZOOKEEPER_CONF_DIR/zoo.cfg"

    # Double check that the zkServer.sh script exists
    ZKSERVER_SCRIPT="${ZOOKEEPER_INSTALL_DIR}/bin/zkServer.sh"
    if [ ! -f "$ZKSERVER_SCRIPT" ]; then
        echo "ERROR: zkServer.sh not found at $ZKSERVER_SCRIPT"
        exit 1
    fi

    # Make sure the script is executable
    chmod +x "$ZKSERVER_SCRIPT"

    echo "Starting ZooKeeper..."
    exec "$ZKSERVER_SCRIPT" start-foreground
    ;;

  resourcemanager)
    echo "Starting ResourceManager..."
    # Start the DataNode in the foreground to keep the container running
    $HADOOP_HOME/bin/yarn resourcemanager
    ;;

  nodemanager)
    echo "Starting NodeManager..."
    # Start the DataNode in the foreground to keep the container running
    $HADOOP_HOME/bin/yarn nodemanager
    ;;
esac