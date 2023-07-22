
# Install pika cluster by kubeblocks

## install kubeblocks


## install pika cluster

```bash
helm install pika ./deploy/pika
helm install pika-cluster ./deploy/pika-cluster
```

## port-forward svc to localhost

```
kubectl port-forward svc/pika-cluster-pika 9221:9221
```

## connect to pika
```
redis-cli -p 9221
```
