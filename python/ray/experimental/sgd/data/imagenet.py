import ray
from ray.experimental.sgd.pytorch import (PyTorchTrainer, PyTorchTrainable)
from ray.experimental.sgd.pytorch.resnet import ResNet50
import s3fs
import torch
import torch.nn as nn
import torchvision
import torchvision.transforms as transforms

ray.init()

fs = s3fs.S3FileSystem()

train_dir = "anyscale-data/imagenet/train"
valid_dir = "anyscale-data/imagenet/validation"

def get_files(fs, dir, max_files=None):
    files = []

    for entry in fs.ls(dir):
        if fs.isdir(entry):
            files += get_files(fs, entry, max_files=max_files)
        else:
            files.append(entry)

        if max_files and len(files) >= max_files:
            return files[:max_files]

    return files

def get_labels(fs, dir):
    paths = fs.ls(dir)
    labels = [path.split('/')[-1] for path in paths]
    label_list = list(set(labels))
    return dict([( val, index ) for index, val in enumerate(label_list)])

labels = get_labels(fs, train_dir)

def iter_len(iterator):
    count = 0
    for _ in iterator.gather_sync():
        count += 1
    return count

def to_datum(fs, s3_path, mode="rgb"):
    file_name = s3_path.split('/')[-1]
    label = file_name.split('_')[0]
    from PIL import Image
    f = fs.open(s3_path, 'rb')
    image = Image.open(f)
    return image, label

def optimizer_creator(model, config):
    """Returns optimizer"""
    return torch.optim.SGD(model.parameters(), lr=config.get("lr", 0.1))

def scheduler_creator(optimizer, config):
    return torch.optim.lr_scheduler.MultiStepLR(
        optimizer, milestones=[150, 250, 350], gamma=0.1)

process_img = lambda datum : (datum[0].resize((256, 256)), datum[1])

num_shards = 3
files = get_files(fs, train_dir, max_files=num_shards)
train_iter = ray.experimental.iter.from_items(files, num_shards=num_shards) \
    .for_each(lambda x: to_datum(fs, x)) \
    .for_each(lambda datum: ((transforms.ToTensor()(datum[0].resize((224, 224)))), labels[datum[1]]))

class Dataset(torch.utils.data.Dataset):
    def __init__(self, local_iter):
        self._data = list(local_iter)

    def __getitem__(self, index):
        return self._data[index]

    def __len__(self):
        return len(self._data)

def dataset_creator(config):
    print(config)
    return Dataset(train_iter.get_shard(config.get("worker_index", 0)))

def train_example(num_replicas=1,
                  num_epochs=5,
                  use_gpu=False,
                  use_fp16=False,
                  test_mode=False):
    config = {}
    from torchvision.models import resnet50
    trainer1 = PyTorchTrainer(
        lambda config: resnet50(),
        dataset_creator,
        optimizer_creator,
        nn.CrossEntropyLoss,
        scheduler_creator=scheduler_creator,
        num_replicas=num_shards,
        config=config,
        use_gpu=use_gpu,
        batch_size=16 if test_mode else 512,
        backend="nccl" if use_gpu else "gloo",
        scheduler_step_freq="epoch",
        use_fp16=use_fp16)
    for i in range(num_epochs):
        # Increase `max_retries` to turn on fault tolerance.
        stats = trainer1.train(max_retries=0)
        print(stats)

    print(trainer1.validate())
    trainer1.shutdown()
    print("success!")

train_example()
# train_iter.show()
# print(get_files(fs, train_dir, max_files=100))
# print(get_files(fs, valid_dir, max_files=100))




print(iter_len(train_iter))

train_iter.show(1)

