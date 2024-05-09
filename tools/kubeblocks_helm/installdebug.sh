# helm install pika ./pika && helm install pika-cluster ./pika-cluster

helm template pika ./pika --output-dir ./output/pika
#helm template pika-cluster ./pika-cluster --output-dir ./output/pika-cluster