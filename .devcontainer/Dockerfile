# For image contents, see at:
# https://github.com/devcontainers/images/tree/main/src/miniconda
FROM mcr.microsoft.com/devcontainers/miniconda:1-3

# Set Python timezone
ENV TZ="Asia/Ulaanbaatar"

# [Optional] Uncomment this section to install additional OS packages.
# RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
#     && apt-get -y install --no-install-recommends <your-package-list-here>

USER root:conda

# Copy library scripts to execute
COPY .devcontainer/library-scripts/*.sh /tmp/library-scripts/

# [Optional] Uncomment this section install Oracle instant client
RUN bash /tmp/library-scripts/oracle-instant-client-debian.sh \
    && rm -rf /tmp/library-scripts

# Make instant client discoverable for all processes (not only interactive shells)
ENV LD_LIBRARY_PATH=/opt/oracle/instantclient_21_6:$LD_LIBRARY_PATH
ENV PATH=/opt/oracle/instantclient_21_6:$PATH

# Copy environment.yml to a temp location to update the environment.
COPY .devcontainer/environment.yml* /tmp/conda-tmp/
RUN if [ -f "/tmp/conda-tmp/environment.yml" ]; then umask 0002 && /opt/conda/bin/conda env update -n base -f /tmp/conda-tmp/environment.yml; fi \
    && rm -rf /tmp/conda-tmp
