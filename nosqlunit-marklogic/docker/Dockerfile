FROM centos:latest

# Inspired by: http://developer.marklogic.com/blog/docker-marklogic-initialization

# Set the Path
ENV PATH /usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/opt/MarkLogic/mlcmd/bin

# Copy the MarkLogic installer to a temp directory in the Docker image being built
COPY MarkLogic-9*.rpm /tmp/MarkLogic.rpm

# Copy initialization shell scripts and the custom configuration into the Docker image
COPY initialize-ml.sh marklogic.conf /etc/

# Get any CentOS updates then clear the Docker cache
# Install MarkLogic dependencies
# Install the initscripts package so MarkLogic starts ok
# Install MarkLogic then delete the .RPM file if the install succeeded
# Create the sudo user as recommended by MarkLogic
# Change permissions of MarkLogic scripts to make them executable
RUN    yum -y update \
    && yum -y install \
       glibc.i686 \
       gdb.x86_64 \
       redhat-lsb.x86_64 \
       initscripts \
       sudo \
       /tmp/MarkLogic.rpm \
    && rm /tmp/MarkLogic.rpm \
    && yum clean all \
    && rm -rf /var/cache/yum \
    && useradd -ms /sbin/nologin -g root -G wheel ml \
    && chmod +x /etc/*.sh

# Expose MarkLogic Server ports
# Also expose any ports your own MarkLogic App Servers use such as
# HTTP, REST and XDBC App Servers for your applications
EXPOSE 7997 7998 7999 8000 8001 8002

# Start MarkLogic. After MarkLogic has started
# successfully, run the initialize script.
# initialize-ml.sh usage:
#   initialize-ml.sh -u <desired admin username>
#                    -p <desired admin password>
#                    -r <realm for the password> 
#                       Realm is optional, 
#                            the default is "public"
#  
# Also, execute tail such that it waits forever. 
# This prevents the container from automatically
# stopping after starting MarkLogic.
CMD /etc/init.d/MarkLogic start \
    && ./etc/initialize-ml.sh -u admin -p admin -r public \
    && tail -f /dev/null