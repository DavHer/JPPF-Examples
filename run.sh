#! /bin/sh

sudo apt-get --assume-yes install openjdk-8-jdk-headless
sudo apt-get --assume-yes install openjdk-8-jre-headless
sudo apt-get --assume-yes install ant
sudo apt-get --assume-yes install unzip
git clone https://github.com/DavHer/JPPF-Examples.git
cd JPPF-Examples/
rm -rf JPPF-5.2.2-application-template
unzip '*.zip'
cp MapReduceFramework/palabras.txt JPPF-5.2.2-node
cd JPPF-5.2.2-node
./startNode.sh &
jppf.server.host = 10.128.0.5