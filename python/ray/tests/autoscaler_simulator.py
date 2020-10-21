from ray.autoscaler._private.autoscaler import StandardAutoscaler
from ray.autoscaler._private.load_metrics import LoadMetrics
from ray.autoscaler._private.providers import _NODE_PROVIDERS
from ray.autoscaler._private.util import prepare_config
from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.tags import TAG_RAY_NODE_KIND, NODE_KIND_WORKER, NODE_KIND_HEAD, TAG_RAY_USER_NODE_TYPE
from ray.tests.test_autoscaler import MockProvider


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
        self.config = init_config(config_path)
        self.provider = node_provider

        self.load_metrics = LoadMetrics()
        self.autoscaler = StandardAutoscaler(config_path, lm)
        self.patch_autoscaler()

        self.node_states = {}
        self.add_head_node()


    def init_config(self, config_path):
        config = yaml.safe_load(open(config_path).read())
        config = prepare_config(config)
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
        assert "head_node_type" in config, "Simulator should only be used with resource demand scheduler."

        head_node_type = config["head_node_type"]
        head_node_tags = {
            TAG_RAY_NODE_KIND: "head",
            TAG_RAY_USER_NODE_TYPE: head_node_type
        }

        head_node_config = copy.deepcopy(self.config["head_node"])
        head_node_config.update(config["available_node_types"][head_node_type]["node_config"])

        self.provider.create_node(head_node_config, head_node_tags, 1)
        self.head_ip = self.provider.non_terminated_node_ips(
            tag_filters={TAG_RAY_NODE_KIND: "head"})[0]

        self.node_states[self.head_ip] = Node(head_node_config["resources"])



    def launch_new_node(self, count: int, node_type: Optional[str]) -> None:

        pass




