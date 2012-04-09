"""Demonstrate how to startup a IPython.parallel cloud for pyrallel.

This script must bee run with RACKSPACE_ID and RACKSPACE_SECRET environment
variables.

RACKSPACE can be replaced by any cloud provider supported by libcloud.

"""

from pyrallel.cloud import deploy_cluster
from pyrallel.cloud import destroy_cluster

# TODO: use a command line argument arguments
provider_name = 'RACKSPACE'
cluster_name = 'pyrallelexample'

conn = deploy_cluster(provider_name, name=cluster_name, n_nodes=1)
print conn.list_nodes()

destroy_cluster(conn, name=cluster_name)
