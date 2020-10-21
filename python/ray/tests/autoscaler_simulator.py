from ray.autoscaler._private.autoscaler import StandardAutoscaler
from ray.autoscaler._private.load_metrics import LoadMetrics
from ray.autoscaler._private.providers import _NODE_PROVIDERS
from ray.autoscaler._private.util import prepare_config
from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.tags import TAG_RAY_NODE_KIND, NODE_KIND_WORKER, NODE_KIND_HEAD, TAG_RAY_USER_NODE_TYPE
from ray.tests.test_autoscaler import SMALL_CLUSTER, MockProvider

import copy
from typing import Any, Optional, Dict
import yaml


TYPES_A = {
    "empty_node": {
        "node_config": {
            "FooProperty": 42,
        },
        "resources": {},
        "max_workers": 0,
    },
    "m4.large": {
        "node_config": {},
        "resources": {
            "CPU": 2
        },
        "max_workers": 10,
    },
    "m4.4xlarge": {
        "node_config": {},
        "resources": {
            "CPU": 16
        },
        "max_workers": 8,
    },
    "m4.16xlarge": {
        "node_config": {},
        "resources": {
            "CPU": 64
        },
        "max_workers": 4,
    },
    "p2.xlarge": {
        "node_config": {},
        "resources": {
            "CPU": 16,
            "GPU": 1
        },
        "max_workers": 10,
    },
    "p2.8xlarge": {
        "node_config": {},
        "resources": {
            "CPU": 32,
            "GPU": 8
        },
        "max_workers": 4,
    },
}

MULTI_WORKER_CLUSTER = dict(
    SMALL_CLUSTER, **{
        "available_node_types": TYPES_A,
        "head_node_type": "empty_node",
        "worker_default_node_type": "m4.large",
    })


class Node:
    def __init__(self, resources):
        self.max_resources = copy.deepcopy(resources)
        self.available_resources = copy.deepcopy(resources)


class Simulator:
    """This simulator should only be instantiated from AutoscalingTest. It relies
    on AutoscalingTest's fixtures. `self.provider` should be initialized in
    AutoscalingTest before the initializer is called.
    """

    def __init__(self, config_path, node_provider):
        self.config = self.init_config(config_path)
        self.provider = node_provider

        self.load_metrics = LoadMetrics()
        self.autoscaler = StandardAutoscaler(config_path, self.load_metrics)
        self.patch_autoscaler()

        self.node_states = {}
        self.add_head_node()


    def init_config(self, config_path):
        config = yaml.safe_load(open(config_path).read())
        config = prepare_config(config)

        # TODO (alex): Is bootstrapping necessary here?
        importer = _NODE_PROVIDERS.get(config["provider"]["type"])
        import pdb; pdb.set_trace()
        provider_cls = importer(config["provider"])
        config = provider_cls.bootstrap_config(config)
        return config


    def patch_autoscaler(self):
        """Patches autoscaler methods. The autoscaler uses multiple threads, which use
`sleep` to handle race conditions. While sleeping for 0.1s isn't a big deal in
a live cluster, it would be very bad in the simulation."""
        def launch_new_node_patch(autoscaler_self, *args, **kwargs):
            self.launch_new_node(*args, **kwargs)

        bound_autoscaler_method_type = type(self.autoscaler.launch_new_node)
        self.autoscaler.launch_new_node = bound_autoscaler_method_type(launch_new_node_patch, StandardAutoscaler)



    def add_head_node(self):
        """Assumes self.init_config was already called.

        This method:

        * Creates a new node w/ self.provider
        * Sets self.head_ip
        * Populates self.node_states
        """
        assert "head_node_type" in self.config, "Simulator should only be used with resource demand scheduler."

        head_node_type = self.config["head_node_type"]
        head_node_tags = {
            TAG_RAY_NODE_KIND: "head",
            TAG_RAY_USER_NODE_TYPE: head_node_type
        }

        head_node_config = copy.deepcopy(self.config["head_node"])
        head_node_config.update(self.config["available_node_types"][head_node_type]["node_config"])

        from pprint import pprint
        pprint(head_node_config)
        print("", flush=True)
        assert False

        self.provider.create_node(head_node_config, head_node_tags, 1)
        self.head_ip = self.provider.non_terminated_node_ips(
            tag_filters={TAG_RAY_NODE_KIND: "head"})[0]

        self.node_states[self.head_ip] = Node(head_node_config["resources"])



    def launch_new_node(self, count: int, node_type: Optional[str]) -> None:

        pass




