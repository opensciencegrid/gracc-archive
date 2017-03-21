#!/bin/sh -xe


# Install all the things
rpm -Uvh https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm
curl -o /etc/yum.repos.d/djw8605-GRACC-epel-7.repo https://copr.fedorainfracloud.org/coprs/djw8605/GRACC/repo/epel-7/djw8605-GRACC-epel-7.repo 
yum -y update

yum -y install python-pip git rabbitmq-server java-1.8.0-openjdk python-elasticsearch-dsl rpm-build python-srpm-macros python-rpm-macros python2-rpm-macros epel-rpm-macros gracc-request
rpm -Uvh https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-5.1.1.rpm

systemctl start elasticsearch.service
systemctl start rabbitmq-server.service

# Prepare the RPM environment
mkdir -p /tmp/rpmbuild/{BUILD,RPMS,SOURCES,SPECS,SRPMS}
cat >> /etc/rpm/macros.dist << EOF
%dist .osg.el${OS_VERSION}
%osg 1
EOF

cp gracc-archive/config/gracc-archive.spec /tmp/rpmbuild/SPECS
package_version=`grep Version gracc-archive/config/gracc-archive.spec | awk '{print $2}'`
pushd gracc-archive
git archive --format=tar --prefix=gracc-archive-${package_version}/ HEAD  | gzip >/tmp/rpmbuild/SOURCES/gracc-archive-${package_version}.tar.gz
popd

# Build the RPM
rpmbuild --define '_topdir /tmp/rpmbuild' -ba /tmp/rpmbuild/SPECS/gracc-archive.spec

yum localinstall -y /tmp/rpmbuild/RPMS/noarch/gracc-archive*

# Copy in the test configuration for the graccreq
cp -f gracc-archive/tests/gracc-archive-test.toml /etc/graccarchive/config.d/gracc-archive.toml
cp -f gracc-archive/tests/gracc-request-test.toml /etc/graccreq/config.d/gracc-request.toml

systemctl start graccreq.service
systemctl start graccarchive.service

# Wait for the overmind to start up
sleep 10
journalctl -u graccarchive.service --no-pager

# Install the test data
curl -O https://nodejs.org/dist/v4.4.4/node-v4.4.4-linux-x64.tar.xz
tar xf node-v4.4.4-linux-x64.tar.xz
export PATH=$PATH:`pwd`/node-v4.4.4-linux-x64/bin
npm install elasticdump -g

git clone https://github.com/djw8605/gracc-test-data.git
pushd gracc-test-data
bash -x ./import.sh
popd

# Do not die on failure.
pushd gracc-archive/
set +e
python -m unittest discover -v tests/unittests "test_*.py"
test_exit=$?
set -e
popd

sleep 60
journalctl -u graccreq.service --no-pager -n 1000
journalctl -u graccarchive.service --no-pager -n 1000

exit $test_exit

