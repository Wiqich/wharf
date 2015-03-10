#!/bin/bash
cur=`cd $(dirname $0) ; pwd`
rm -f $cur/wharf-*-jar-with-dependencies.jar
mvn clean package
mv $cur/target/wharf-*-jar-with-dependencies.jar $cur
mvn clean
