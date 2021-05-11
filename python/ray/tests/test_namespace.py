import pytest
import sys
import time

import ray
from ray import ray_constants
from ray.test_utils import get_error_message, init_error_pubsub, \
    run_string_as_driver


def test_isolation(shutdown_only):
    info = ray.init(namespace="namespace")

    address = info["redis_address"]

    # First param of template is the namespace. Second is the redis address.
    driver_template = """
import ray

ray.init(address="{}", namespace="{}")

@ray.remote
class DetachedActor:
    def ping(self):
        return "pong from other job"

actor = DetachedActor.options(name="Pinger", lifetime="detached").remote()
ray.get(actor.ping.remote())
    """

    # Start a detached actor in a different namespace.
    run_string_as_driver(driver_template.format(address, "different"))

    @ray.remote
    class Actor:
        def ping(self):
            return "pong"

    # Create an actor. This should succeed because the other actor is in a
    # different namespace.
    probe = Actor.options(name="Pinger").remote()
    assert ray.get(probe.ping.remote()) == "pong"
    del probe

    # Wait for actor removal
    actor_removed = False
    for _ in range(50):  # Timeout after 5s
        try:
            ray.get_actor("Pinger")
        except ValueError:
            actor_removed = True
            # This means the actor was removed.
            break
        else:
            time.sleep(0.1)

    assert actor_removed, "This is an anti-flakey test measure"

    with pytest.raises(ValueError, match="Failed to look up actor with name"):
        ray.get_actor("Pinger")

    # Now make the actor in this namespace, from a different job.
    run_string_as_driver(driver_template.format(address, "namespace"))

    detached_actor = ray.get_actor("Pinger")
    assert ray.get(detached_actor.ping.remote()) == "pong from other job"

    with pytest.raises(ValueError, match="The name .* is already taken"):
        Actor.options(name="Pinger", lifetime="detached").remote()


def test_default_namespace(shutdown_only):
    info = ray.init(namespace="namespace")

    address = info["redis_address"]

    # First param of template is the namespace. Second is the redis address.
    driver_template = """
import ray

ray.init(address="{}")

@ray.remote
class DetachedActor:
    def ping(self):
        return "pong from other job"

actor = DetachedActor.options(name="Pinger", lifetime="detached").remote()
ray.get(actor.ping.remote())
    """

    # Each run of this script will create a detached actor. Since the drivers
    # are in different namespaces, both scripts should succeed. If they were
    # placed in the same namespace, the second call will throw an exception.
    run_string_as_driver(driver_template.format(address))
    run_string_as_driver(driver_template.format(address))


def test_namespace_in_job_config(shutdown_only):
    # JobConfig isn't a public API, but we'll set it directly, instead of
    # using the param in code paths like the ray client.
    job_config = ray.job_config.JobConfig(ray_namespace="namespace")
    info = ray.init(job_config=job_config)

    address = info["redis_address"]

    # First param of template is the namespace. Second is the redis address.
    driver_template = """
import ray

ray.init(address="{}", namespace="namespace")

@ray.remote
class DetachedActor:
    def ping(self):
        return "pong from other job"

actor = DetachedActor.options(name="Pinger", lifetime="detached").remote()
ray.get(actor.ping.remote())
    """

    run_string_as_driver(driver_template.format(address))

    act = ray.get_actor("Pinger")
    assert ray.get(act.ping.remote()) == "pong from other job"


def test_detached_warning(shutdown_only):
    ray.init()

    @ray.remote
    class DetachedActor:
        def ping(self):
            return "pong"

    error_pubsub = init_error_pubsub()
    actor = DetachedActor.options(  # noqa: F841
        name="Pinger", lifetime="detached").remote()
    errors = get_error_message(error_pubsub, 1, None)
    error = errors.pop()
    assert error.type == ray_constants.DETACHED_ACTOR_ANONYMOUS_NAMESPACE_ERROR


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
