##@ MiniKube

PIKA_IMAGE ?= pikadb/pika:v3.4.0
PIKA_OPERATOR_IMAGE ?= pika-operator:dev

LOCAL_CLUSTER_NAME ?= mini-pika
LOCAL_CLUSTER_VERSION ?= v1.25.3

.PHONY: minikube-up
minikube-up: ## Start minikube.
	@minikube version || (echo "minikube is not installed" && exit 1)
	minikube start --kubernetes-version $(LOCAL_CLUSTER_VERSION)

.PHONY: minikube-reset
minikube-reset: ## Reset minikube.
	minikube delete

.PHONY: set-local-env
set-local-env: ## Set local env.
export IMG=$(PIKA_OPERATOR_IMAGE)

.PHONY: minikube-image-load
minikube-image-load: ## Load image to minikube.
ifeq ($(shell docker images -q $(PIKA_IMAGE) 2> /dev/null), "")
	docker pull $(PIKA_IMAGE)
endif
	docker tag $(PIKA_IMAGE) pika:dev
	minikube image load pika:dev
	minikube image load pika-operator:dev

.PHONY: deploy-pika-sample
deploy-pika-sample: ## Deploy pika-sample.
	kubectl apply -f examples/pika-minikube/
	sleep 10
	kubectl wait pods -l app=pika-minikube --for condition=Ready --timeout=90s
	kubectl run pika-minikube-test --image redis -it --rm --restart=Never \
	  -- /usr/local/bin/redis-cli -h pika-minikube -p 9221 info | grep -E '^pika_'

.PHONY: uninstall-pika-sample
uninstall-pika-sample: ## Uninstall pika-sample.
	kubectl delete -f examples/pika-minikube/

##@ Local Deploy
.PHONY: local-deploy
local-deploy: set-local-env docker-build minikube-image-load install deploy deploy-pika-sample

##@ Local Clean
.PHONY: local-clean
local-clean: uninstall-pika-sample uninstall
