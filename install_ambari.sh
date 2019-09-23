#!/bin/bash
systemctl start ntpd
systemctl disable firewalld

cd /etc/yum.repos.d/
wget http://public-repo-1.hortonworks.com/ambari/centos7/2.x/updates/2.7.3.0/ambari.repo

