FROM ubuntu:20.04
LABEL maintainer="Denis Rystsov <denis@vectorized.io>"
ARG DEBIAN_FRONTEND=noninteractive
ARG REDPANDA_DEB
ARG USER_ID
ARG REDPANDA_CLUSTER_SIZE
ENV REDPANDA_CLUSTER_SIZE=${REDPANDA_CLUSTER_SIZE}
RUN apt update -y
RUN apt install -f -y
RUN apt install -y sysstat --fix-missing
RUN apt install -y wget iptables ssh sudo --fix-missing
RUN apt install -y iputils-ping vim tmux less openjdk-11-jdk maven
RUN adduser --disabled-password --gecos "" ubuntu
RUN usermod -aG sudo -u $USER_ID ubuntu
RUN echo "ubuntu ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers
RUN mkdir -p /home/ubuntu/.ssh
COPY id_ed25519.pub /home/ubuntu/.ssh/authorized_keys
RUN mkdir -p /mnt/vectorized
RUN mkdir -p /mnt/vectorized/bin
RUN cd /mnt/vectorized/bin && wget https://downloads.apache.org/kafka/3.0.0/kafka_2.12-3.0.0.tgz
RUN cd /mnt/vectorized/bin && wget https://downloads.apache.org/zookeeper/zookeeper-3.7.0/apache-zookeeper-3.7.0-bin.tar.gz
RUN cd /mnt/vectorized/bin && tar xzf kafka_2.12-3.0.0.tgz
RUN cd /mnt/vectorized/bin && tar xzf apache-zookeeper-3.7.0-bin.tar.gz
COPY control /mnt/vectorized/control
RUN chown ubuntu:ubuntu -R /mnt/vectorized
COPY docker/redpanda/entrypoint.sh /mnt/vectorized/entrypoint.sh
COPY $REDPANDA_DEB /mnt/vectorized/$REDPANDA_DEB
RUN dpkg --force-confold -i /mnt/vectorized/$REDPANDA_DEB
RUN systemctl disable redpanda
RUN systemctl disable wasm_engine
CMD /mnt/vectorized/entrypoint.sh