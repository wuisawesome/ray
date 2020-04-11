import ray
from ray.util import iter
from ray.util.sgd.torch.torch_trainer import TorchTrainer
from ray.util.sgd.data.new_dataset import Dataset

import torch
from torch import nn
import torch.nn.functional as F

import numpy as np


class Net(nn.Module):
    def __init__(self):
        super(Net, self).__init__()
        self.fc1 = nn.Linear(1, 128)
        self.fc2 = nn.Linear(128, 1)

    def forward(self, x):
        x = self.fc1(x)
        x = F.relu(x)
        x = self.fc2(x)
        return x


def model_creator(config):
    return Net()


def data_creator(config):
    return torch.utils.data.DataLoader([(1, 1), (2, 2)])


def optimizer_creator(model, config):
    return torch.optim.SGD(model.parameters(), lr=config.get("lr", 1e-4))


def loss_creator(config):
    return -nn.MSELoss(config)


def to_mat(x):
    return torch.tensor([[x]]).float()


@ray.remote
def print_stuff(x):
    print(list(x.__iter__()))


ray.init()

p_iter = iter.from_items([i * 0.001 for i in range(10)], num_shards=1)
dataset = Dataset(
    p_iter, download_func=(lambda x: (to_mat(x), to_mat(x))), max_concur=1)

local_iter = p_iter.get_shard(0)
res = print_stuff.remote(local_iter)
ray.get(res)
# list(local_iter.__iter__())
# trainer = TorchTrainer(model_creator=model_creator,
#                        data_creator=data_creator,
#                        optimizer_creator=optimizer_creator,
#                        loss_creator=torch.nn.MSELoss,
#                        config={"batch_size": 32, "epoch": 10},
#                        num_workers=3,
# )

# trainer.train(dataset=dataset)
# for i in range(10):
#     trainer.train(dataset=dataset)
#     model = trainer.get_model()
#     print(model(to_mat(0.5)))
