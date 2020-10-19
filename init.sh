#!/bin/bash

#npm install

mysql -u root < server/data_init/healthtracker_init.sql

#mysql -u root < server/data_init/userdata_init.sql
