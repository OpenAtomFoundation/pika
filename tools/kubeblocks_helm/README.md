
# Install pika cluster by kubeblocks 
Fellow the kubeblock docs [Install kbcli](https://kubeblocks.io/docs/preview/user_docs/installation/install-kbcli)

## install kubeblocks
Fellow the kubeblock docs [kubeblocks](https://kubeblocks.io/docs/preview/user_docs/installation/install-kubeblocks)

## install pika cluster

### install pika CD and pika cluster 
First, use helm install pika cluster definition and install pika cluster.
```bash
cd ./tools/kubeblocks-helm/
helm install pika ./pika
helm install pika-cluster ./pika-cluster
```

Wait for pika cluster until the status to be `Running`.
```bash
kubectl get cluster --watch
````

### Add Pika instance to codis
Then connect codis front end.
```bash
 kubectl port-forward svc/pika-cluster-codis-fe 8080
```
Open browser and visit `http://localhost:8080`

### connect to pika cluster
```bash
kubectl port-forward svc/pika-cluster-codis-proxy 19000
# start new terminal
redis-cli -p 19000 info
```

## Scale pika cluster

### Scale out

1. First, edit pika-cluster/values.yaml to increase replicaCount.
2. Then upgrade the cluster.
```bash
helm upgrade pika-cluster ./pika-cluster
```

### Scale in
scale in is not supported now.
