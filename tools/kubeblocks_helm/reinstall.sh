helm uninstall pika && helm uninstall pika-cluster
sleep 5
helm install pika ./pika && helm install pika-cluster ./pika-cluster