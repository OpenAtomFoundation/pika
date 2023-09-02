
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

Then add pika instance to codis
1. Input group id `1` and click `Add Group` button
2. Input the following pika instance url and group id, then click `Add Server` button
- pika-cluster-pika-0.pika-cluster-pika-headless:9221
- pika-cluster-pika-1.pika-cluster-pika-headless:9221
 
3. Click `Rebalance all slots` button


### connect to pika cluster
```bash
kubectl port-forward svc/pika-cluster-codis-proxy 19000
# start new terminal
redis-cli -p 19000 info
```

## Scale pika cluster

Use kbcli horizontal scale pika instance, you will get 2 new pika instances.
```bash
kbcli cluster hscale pika-cluster --replicas 4 --components pika
```

Add new pika instances to codis
1. Input group id `2` and click `Add Group` button
2. Input the following pika instance url and group id, then click `Add Server` button
- pika-cluster-pika-2.pika-cluster-pika-headless:9221
- pika-cluster-pika-3.pika-cluster-pika-headless:9221
3. Click `Rebalance all slots` button

