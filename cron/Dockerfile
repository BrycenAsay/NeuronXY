FROM ubuntu:20.04

# Install cron and other necessary utilities
RUN apt-get update && apt-get install -y cron postgresql-client rsyslog

# Create the subdirectory for logs
RUN mkdir -p /app/logs

# Copy the crontab file
COPY crontab /etc/cron.d/my-cron-job

# Set proper permissions for cron file
RUN chmod 0644 /etc/cron.d/my-cron-job

# Ensure the logs directory and log file are writable by cron
RUN chmod -R 777 /app/logs

# Set up cron job logging to the /app/logs directory
RUN echo "cron.* /app/logs/cron.log" >> /etc/rsyslog.d/50-default.conf

# Start cron and rsyslog services
CMD service rsyslog start && cron -f