# Install ntpd
sudo apt-get install ntp

# Start the ntp service
sudo service ntp start

# Ensure ntpd starts at boot
sudo update-rc.d ntp defaults

# Check the status of the ntp service
sudo service ntp status

# Sync now
sudo ntpd -gq

# Check the system time
date