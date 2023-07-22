
# Install pika cluster by kubeblocks

## install kubeblocks


## install pika cluster

```bash
helm install nebula ./deploy/pika
helm install nebula-cluster ./deploy/pika-cluster
```

## port-forward svc to localhost

```
kubectl port-forward svc/nebula-cluster-nebula-graphd 9221:9221
```

## connect to pika
```
redis-cli -p 9221
```
