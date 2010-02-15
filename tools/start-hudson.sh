cat /var/log/hudson >> /var/log/hudson.0
java -Duser.timezone="America/New_York" -jar hudson.war &> /var/log/hudson &
