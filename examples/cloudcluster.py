"""Demonstrate how to startup a IPython.parallel cloud for pyrallel.

This script must bee run with RACKSPACE_ID and RACKSPACE_SECRET environment
variables.

RACKSPACE can be replaced by any cloud provider supported by libcloud.

"""

import os
from pyrallel.cloud import deploy_cluster
from pyrallel.cloud import destroy_cluster
from pyrallel.cloud import get_connection

MY_RSA_PUB_KEY = os.path.expanduser('~/.ssh/id_rsa.pub')

# TODO: use a command line argument arguments
provider_name, username, ssh_key = 'RACKSPACE', None, None

# Using brightbox requires a version of libcloud that include a fix for
# https://issues.apache.org/jira/browse/LIBCLOUD-182
#provider_name, username, ssh_key = 'BRIGHTBOX', 'ubuntu', MY_RSA_PUB_KEY

cluster_name = 'pyrallelexample'  # RACKSPACE does not allow '_' in node names

# To make it easier to debug cluster deployment in case of crash when running in
# an interactive shell
conn = get_connection(provider_name)

deploy_cluster(provider_name, name=cluster_name, n_nodes=1,
               username=username, ssh_key=ssh_key)

print conn.list_nodes()

destroy_cluster(conn, name=cluster_name)
