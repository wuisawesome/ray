import ray
import ray.autoscaler.sdk
from ray.test_utils import Semaphore
from ray.util.placement_group import placement_group, remove_placement_group

from time import sleep, perf_counter
from tqdm import tqdm, trange

MAX_NUM_NODES = 250
MAX_ACTORS_IN_CLUSTER = 500
MAX_RUNNING_TASKS_IN_CLUSTER = 5000
MAX_PLACEMENT_GROUPS = 75


def scale_to(target):
    ray.autoscaler.sdk.request_resources(bundles=[{"node": 1}] * target)
    while len(ray.nodes()) != target:
        print(f"Current # nodes: {len(ray.nodes())}, target: {target}")
        print("Waiting ...")
        sleep(5)


def test_nodes():
    scale_to(MAX_NUM_NODES)
    assert len(ray.nodes()) == MAX_NUM_NODES


def test_max_actors():
    @ray.remote
    class Actor:
        def foo(self):
            pass

    actors = [
        Actor.remote() for _ in trange(MAX_ACTORS_IN_CLUSTER, desc="Launching actors")
    ]

    for actor in tqdm(actors, desc="Ensuring actors have started"):
        assert ray.get(actor.foo.remote()) is None


def test_max_running_tasks():
    counter = Semaphore.remote(0)
    blocker = Semaphore.remote(0)
    total_resource = ray.cluster_resources()["node"]

    # Use this custom node resource instead of cpus for scheduling to properly loadbalance. CPU borrowing could lead to unintuitive load balancing if we used num_cpus.
    resource_per_task = total_resource / MAX_RUNNING_TASKS_IN_CLUSTER

    @ray.remote(num_cpus=0, resources={"node": resource_per_task})
    def task(counter, blocker):
        sleep(10)

    refs = [
        task.remote(counter, blocker)
        for _ in trange(MAX_RUNNING_TASKS_IN_CLUSTER, desc="Launching tasks")
    ]

    for _ in trange(10, desc="Waiting"):
        sleep(1)

    for _ in trange(
        MAX_RUNNING_TASKS_IN_CLUSTER, desc="Ensuring all tasks have finished"
    ):
        done, refs = ray.wait(refs)
        assert ray.get(done[0]) is None


def test_many_placement_groups():
    @ray.remote(num_cpus=1, resources={"node": 0.5})
    def f1():
        sleep(10)
        pass

    @ray.remote(num_cpus=1)
    def f2():
        sleep(10)
        pass

    @ray.remote(resources={"node": 0.5})
    def f3():
        sleep(10)
        pass

    bundle1 = {"node": 0.5, "CPU": 1}
    bundle2 = {"CPU": 1}
    bundle3 = {"node": 0.5}

    pgs = []
    for _ in trange(MAX_PLACEMENT_GROUPS, desc="Creating pgs"):
        pg = placement_group(bundles=[bundle1, bundle2, bundle3])
        pgs.append(pg)

    for pg in tqdm(pgs, desc="Waiting for pgs to be ready"):
        ray.get(pg.ready())

    refs = []
    for pg in tqdm(pgs, desc="Scheduling tasks"):
        ref1 = f1.options(placement_group=pg).remote()
        ref2 = f2.options(placement_group=pg).remote()
        ref3 = f3.options(placement_group=pg).remote()
        refs.extend([ref1, ref2, ref3])

    for _ in trange(10, desc="Waiting"):
        sleep(1)

    with tqdm() as p_bar:
        while refs:
            done, refs = ray.wait(refs)
            p_bar.update()

    for pg in tqdm(pgs, desc="Cleaning up pgs"):
        remove_placement_group(pg)


ray.init(address="auto")

node_launch_start = perf_counter()
test_nodes()
node_launch_end = perf_counter()

assert len(ray.nodes()) == MAX_NUM_NODES, "Wrong number of nodes in cluster " + len(
    ray.nodes()
)
assert ray.cluster_resources() == ray.available_resources()
print("Done launching nodes")

actor_start = perf_counter()
test_max_actors()
actor_end = perf_counter()

assert len(ray.nodes()) == MAX_NUM_NODES, "Wrong number of nodes in cluster " + len(
    ray.nodes()
)
assert ray.cluster_resources() == ray.available_resources()
print("Done testing actors")

task_start = perf_counter()
test_max_running_tasks()
task_end = perf_counter()

assert len(ray.nodes()) == MAX_NUM_NODES, "Wrong number of nodes in cluster " + len(
    ray.nodes()
)
assert ray.cluster_resources() == ray.available_resources()
print("Done testing tasks")

pg_start = perf_counter()
test_many_placement_groups()
pg_end = perf_counter()

assert len(ray.nodes()) == MAX_NUM_NODES, "Wrong number of nodes in cluster " + len(
    ray.nodes()
)
assert ray.cluster_resources() == ray.available_resources()
print("Done")

launch_time = node_launch_end - node_launch_start
actor_time = actor_end - actor_start
task_time = task_end - task_start
pg_time = pg_end - pg_start

print(f"Node launch time: {launch_time} ({MAX_NUM_NODES} nodes)")
print(f"Actor time: {actor_time} ({MAX_ACTORS_IN_CLUSTER} actors)")
print(f"Task time: {task_time} ({MAX_RUNNING_TASKS_IN_CLUSTER} tasks)")
print(f"Placement group time: {pg_time} ({MAX_PLACEMENT_GROUPS} placement groups)")
