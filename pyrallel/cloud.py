"""Utilities to deploy a IPython.parallel cluster in a public cloud"""
# Authors: Olivier Grisel <olivier.grisel@ensta.org>
# License: Simplified BSD

from time import time
import os

from libcloud.compute.types import Provider
from libcloud.compute.types import NodeState
from libcloud.compute.providers import get_driver
from libcloud.compute.deployment import MultiStepDeployment
from libcloud.compute.deployment import ScriptDeployment
from libcloud.compute.deployment import SSHKeyDeployment


# Use the local folder of the ssh user of the node to put the deployment
# script rather that assuming that '/root' will be writeable by this user.
PYRALLEL_DEPLOY_SCRIPT_NAME = './pyrallel_deploy.sh'


# TODO: finish me
UBUNTU_DEPLOYMENT = """\
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
                   username=None, ssh_key=None, pyrallel_ssh_key=None):
    """Deploy a cluster with IPython engines

    cloud can be a libcloud connection or the name of a provider.

    Ubuntu only setup for now.

    """
    conn = get_connection(cloud) if isinstance(cloud, basestring) else cloud

    steps = []
    if pyrallel_ssh_key is not None:
        # TODO: automatically generate a pyrallel dedicated key without
        # password to be used for ipython tunnelling instead
        steps.append(SSHKeyDeployment(open(pyrallel_ssh_key).read()))

    # TODO: package a real deployment script
    steps.append(ScriptDeployment(UBUNTU_DEPLOYMENT,
                                  name=PYRALLEL_DEPLOY_SCRIPT_NAME))
    deployment = MultiStepDeployment(steps)

    if image is None:
        ubuntu_images = [i for i in conn.list_images()
                         if ("ubuntu" in i.name.lower()
                             and "11.10" in i.name)]
        if len(ubuntu_images) == 0:
            raise RuntimeError('Could not find Ubuntu image')
        image = ubuntu_images[0]

    deploy_params = {
        'name': name,
        'image': image,
        'size': size if size is not None else conn.list_sizes()[0],
        'username': username,
        'ssh_key': ssh_key,
        'deploy': deployment,
    }
    print "Deploying cluster '%s' with %d nodes." % (name, n_nodes)
    t0 = time()
    # TODO: find a way to run deployments concurrently
    for i in range(n_nodes):
        conn.deploy_node(**deploy_params)
    print "Successfully deployed %d nodes in %ds." % (n_nodes, time() - t0)
    return conn


def destroy_cluster(cloud, name='pyrallel'):
    """Destroy all nodes with the provided name"""
    conn = get_connection(cloud) if isinstance(cloud, basestring) else cloud
    nodes = [n for n in conn.list_nodes()
             if n.name == name and n.state == NodeState.RUNNING]
    print "Destroying cluster '%s' with %d nodes." % (name, len(nodes))
    t0 = time()
    for n in nodes:
        conn.destroy_node(n)
    print "Successfully destroyed %d nodes in %ds." % (len(nodes), time() - t0)
    return conn
