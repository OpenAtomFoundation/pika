# pika-operator

pika-operator is a Kubernetes operator for managing Pika.

## Description

This operator is responsible for managing the lifecycle of Pika.
It is responsible for creating and managing the following resources:

- StatefulSet
- Service
- PersistentVolumeClaim

## Getting Started

You’ll need a Kubernetes cluster to run against. You can use [MiniKube](https://minikube.sigs.k8s.io)
or [KIND](https://kind.sigs.k8s.io) to get a local cluster for testing, or run against a remote cluster.

**Note:** Your controller will automatically use the current context in your kubeconfig file (i.e. whatever
cluster `kubectl cluster-info` shows).

### Running locally with MiniKube

1. Install [MiniKube](https://minikube.sigs.k8s.io/docs/start/)

2. Start a local cluster:

```sh
make minikube-up # run this if you don't have a minikube cluster
make local-deploy
```

Or if you want to use a development pika image:

```sh
make local-deploy PIKA_IMAGE=<your-pika-image>
```

If you see some message like the following, it means that the pika-operator is running successfully:

```sh
************  TEST PIKA ************
kubectl run pika-minikub-test ...
pika_version:3.5.0
pika_git_sha:bd30511bf82038c2c6531b3d84872c9825fe836a
pika_build_compile_date: Dec  1 2020
````

### Running on the cluster

1. Install Instances of Custom Resources:

```sh
kubectl apply -f config/samples/
```

2. Build and push your image to the location specified by `IMG`:

```sh
make docker-build docker-push IMG=<some-registry>/pika-operator:tag
```

3. Deploy the controller to the cluster with the image specified by `IMG`:

```sh
make deploy IMG=<some-registry>/pika-operator:tag
```

### Run Pika Instance

1. Create a Pika instance:

```sh
kubectl apply -f examples/pika-sample/
```

2. Check the status of the Pika instance:

```sh
kubectl get pika pika-sample
```

3. Connection to the Pika instance:

```sh
kubectl run pika-sample-test --image redis -it --rm --restart=Never \
  -- /usr/local/bin/redis-cli -h pika-sample -p 9221 info
```

### Uninstall CRDs

To delete the CRDs from the cluster:

```sh
make uninstall
```

### Undeploy controller

UnDeploy the controller to the cluster:

```sh
make undeploy
```

## Contributing

This project is still in its early stages and contributions are welcome. Please feel free to open issues and PRs.
Please see this [issue](https://github.com/OpenAtomFoundation/pika/issues/1236) to discuss the design of the operator.

### How it works

This project aims to follow the
Kubernetes [Operator pattern](https://kubernetes.io/docs/concepts/extend-kubernetes/operator/)

It uses [Controllers](https://kubernetes.io/docs/concepts/architecture/controller/)
which provides a reconcile function responsible for synchronizing resources untile the desired state is reached on the
cluster

### Test It Out

1. Install the CRDs into the cluster:

```sh
make install
```

2. Run your controller (this will run in the foreground, so switch to a new terminal if you want to leave it running):

```sh
make run
```

**NOTE:** You can also run this in one step by running: `make install run`

### Modifying the API definitions

If you are editing the API definitions, generate the manifests such as CRs or CRDs using:

```sh
make manifests
```

**NOTE:** Run `make --help` for more information on all potential `make` targets

More information can be found via the [Kubebuilder Documentation](https://book.kubebuilder.io/introduction.html)

## License

Copyright (c) 2023-present, Qihoo, Inc. All rights reserved.
This source code is licensed under the BSD-style license found in the
LICENSE file in the root directory of this source tree. An additional grant
of patent rights can be found in the PATENTS file in the same directory.
