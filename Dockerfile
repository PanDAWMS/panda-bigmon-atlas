ARG PYTHON_VERSION=3.11.6

FROM gitlab-registry.cern.ch/linuxsupport/alma9-base:latest
ARG PYTHON_VERSION

MAINTAINER mborodin

RUN yum update -y
RUN yum install -y epel-release

RUN yum install -y httpd httpd-devel gcc gridsite git psmisc less wget logrotate procps which \
    openssl-devel readline-devel bzip2-devel libffi-devel zlib-devel systemd-udev zlib postgresql postgresql-devel  \
    libsqlite3-dev sqlite-devel

RUN mkdir /tmp/python && cd /tmp/python && \
    wget https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz && \
    tar -xzf Python-*.tgz && rm -f Python-*.tgz && \
    cd Python-* && \
    ./configure --enable-shared --enable-optimizations --with-lto && \
    make altinstall && \
    echo /usr/local/lib > /etc/ld.so.conf.d/local.conf && ldconfig && \
    cd / && rm -rf /tmp/pyton

RUN echo -e '[epel]\n\
name=Extra Packages for Enterprise Linux 9 [HEAD]\n\
baseurl=http://linuxsoft.cern.ch/epel/9/Everything/x86_64\n\
enabled=1\n\
gpgcheck=0\n\
gpgkey=http://linuxsoft.cern.ch/epel/RPM-GPG-KEY-EPEL-9\n\
exclude=collectd*,libcollectd*,mcollective,perl-Authen-Krb5,perl-Collectd,puppet,python*collectd_systemd*,koji*,python*koji*\n\
priority=20\' >> /etc/yum.repos.d/epel.repo

RUN echo -e '[dbclients9el-stable]\n\
name=IT-DB database client tools\n\
baseurl=http://linuxsoft.cern.ch/internal/repos/dbclients9el-stable/x86_64/os\n\
enabled=1\n\
gpgcheck=0\n\
priority=10\' >> /etc/yum.repos.d/epel.repo \

RUN ln -s /usr/bin/python3 /usr/bin/python && \
    ln -s /usr/bin/pip3 /usr/bin/pip

RUN wget https://download.oracle.com/otn_software/linux/instantclient/oracle-instantclient-basic-linuxx64.rpm -P /tmp/ && \
    yum install /tmp/oracle-instantclient-basic-linuxx64.rpm -y && \
    wget https://download.oracle.com/otn_software/linux/instantclient/oracle-instantclient-sqlplus-linuxx64.rpm -P /tmp/ && \
    yum install /tmp/oracle-instantclient-sqlplus-linuxx64.rpm -y

RUN yum install -y  oracle-instantclient-tnsnames.ora

RUN yum clean all && rm -rf /var/cache/yum

RUN ln -s /usr/bin/python3 /usr/bin/python && \
    ln -s /usr/bin/pip3 /usr/bin/pip

COPY config-templates/etc/requrements.txt /tmp/requrements.txt

RUN pip install -r /tmp/requrements.txt

ENTRYPOINT ["top", "-b"]

STOPSIGNAL SIGINT

EXPOSE 8443 8080 8000