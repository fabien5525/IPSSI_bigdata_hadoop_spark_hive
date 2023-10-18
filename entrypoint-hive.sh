# Start HiveServer2
hiveserver2

# Check if HiveServer2 is up
while true; do
  if ! nc -z localhost 10000; then
    echo "Waiting for HiveServer2 to start..."
    sleep 1
  else
    break
  fi
done

# use beeline to connect to HiveServer2,  then run /init.sql file
beeline -u jdbc:hive2://localhost:10000 -f /init.sql

