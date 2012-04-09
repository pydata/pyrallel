"""Utilities to deploy a IPython.parallel cluster in a public cloud"""
# Authors: Olivier Grisel <olivier.grisel@ensta.org>
# License: Simplified BSD

from time import time
import os

from libcloud.compute.types import Provider
from libcloud.compute.providers import get_driver
from libcloud.compute.deployment import MultiStepDeployment
from libcloud.compute.deployment import ScriptDeployment
from libcloud.compute.deployment import SSHKeyDeployment


UBUNTU_DEPLOYMENT = """`
# system packages
apt-get -y install python-numpy python-scipy python-pip

# development versions
pip install git+git://github.com/ipython/ipython.git

# TODO: configure and start engines 1 per CPU
"""


def get_connection(provider_name, account_id=None, account_secret=None):
    """Return a libcloud connection to the cloud provider API"""
    provider_name = provider_name.upper()
    provider = getattr(Provider, provider_name)
    driver = get_driver(provider)

    if account_id is None:
        account_id = os.environ[provider_name + '_ID']
    if account_secret is None:
        account_secret = os.environ[provider_name + '_SECRET']

    return driver(account_id, account_secret)


def deploy_cluster(cloud, name='pyrallel', n_nodes=1, image=None, size=None,
                   ssh_key=None):
    """Deploy a cluster with IPython engines

    cloud can be a libcloud connection or the name of a provider.

    Ubuntu only setup for now.
    """
    conn = get_connection(cloud) if isinstance(cloud, basestring) else cloud

    if ssh_key is None:
        ssh_key = os.path.expanduser('~/.ssh/id_rsa.pub')

    sshkey_step = SSHKeyDeployment(open(ssh_key).read())
    # TODO: package a real deployment script
    script_step = ScriptDeployment(UBUNTU_DEPLOYMENT)
    deployment = MultiStepDeployment([
        sshkey_step,
        script_step,
    ])

    image = image if image is not None else conn.list_images()[0]
    size = size if size is not None else conn.list_sizes()[0]

    print "Deploying %d nodes" % n_nodes
    # TODO: use parallel thread to launch the deployment
    t0 = time()
    for i in range(n_nodes):
        conn.deploy_node(name=name, image=image, size=size,
                         deploy=deployment)
    print "Successfully deployed %d nodes in %ds" % (n_nodes, time() - t0)
    return conn


def destroy_cluster(cloud, name='pyrallel'):
    """Destroy all nodes with the provided name"""
    conn = get_connection(cloud) if isinstance(cloud, basestring) else cloud
    nodes = [n for n in conn.list_nodes() if n.name == name]
    print "Destroying %d nodes" % len(nodes)
    # TODO: parallelize me!
    t0 = time()
    for n in nodes:
        conn.destroy_node(n)
    print "Successfully destroyed %d nodes in %ds" % (len(nodes), time() - t0)
    return conn
