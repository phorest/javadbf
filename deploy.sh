#!/bin/sh

mvn deploy -DaltDeploymentRepository=apollo.phorest.com::default::scp://apollo.phorest.com/var/www/maven/repository

