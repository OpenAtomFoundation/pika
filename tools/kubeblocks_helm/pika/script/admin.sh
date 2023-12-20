#! /bin/bash
set -x

# set instance role
set_instance_role() {
  POD_ID=${HOSTNAME##*-}
  echo "POD_ID: "${POD_ID}
}

# set group id
set_group_id() {
  GROUP_ID=${KB_CLUSTER_COMP_NAME##*-}
  echo "GROUP_ID: "${GROUP_ID}
}

# set codis dashboard
set_codis_dashboard() {
  CODIS_DASHBOARD="${KB_CLUSTER_NAME}-codis-dashboard"
  echo "CODIS_DASHBOARD: "${CODIS_DASHBOARD}
  CODIS_ADMIN="/codis/bin/codis-admin --dashboard=${CODIS_DASHBOARD}:18080"
  echo "CODIS_ADMIN: "${CODIS_ADMIN}
}

wait_server_running() {
  until nc -z 127.0.0.1 9221; do
    echo waiting for pika
    sleep 2
  done
}

wait_dashboard_running() {
  until nc -z ${CODIS_DASHBOARD} 18080; do
    echo waiting for codis dashboard
    sleep 2
  done
}

wait_master_registered() {
  until $CODIS_ADMIN --list-group | jq -r '.[] | select(.id == '${GROUP_ID}') | .servers[] | select(.role == "master") | .server'; do
    echo waiting for master registered
    sleep 2
  done
}

# confirm group has the max index of all groups
confirm_max_group() {
    max_group_id=0
    for component in ${KB_CLUSTER_COMPONENT_LIST//,/ }; do
        if [[ ${component} =~ pika-group-([0-9]+) ]]; then
        group_id=${BASH_REMATCH[1]}
        if [ ${group_id} -gt ${max_group_id} ]; then
            max_group_id=${group_id}
        fi
        fi
    done
    if [ ${GROUP_ID} -ne ${max_group_id} ]; then
        echo "Exit: group id ${GROUP_ID} is not max group id ${max_group_id}"
        exit 0
    fi
 
}

reload_until_success() {
  until $CODIS_ADMIN --reload 1>/dev/null 2>&1; do
    echo waiting for reload success
    sleep 2
  done
}

register_server() {
  reload_until_success
  if [ ${POD_ID} -gt 0 ]; then wait_master_registered; fi
  $CODIS_ADMIN --create-group --gid=${GROUP_ID} 1>/dev/null 2>&1
  $CODIS_ADMIN --group-add --gid=${GROUP_ID} --addr=${KB_POD_FQDN}:9221
  $CODIS_ADMIN --sync-action --create --addr=${KB_POD_FQDN}:9221 1>/dev/null 2>&1
}

remove_server() {
  $CODIS_ADMIN --reload
  if [ $? != 0 ]; then exit 1; fi
  gid=${GROUP_ID}
  sleep 5
  $CODIS_ADMIN --group-del --gid=${GROUP_ID} --addr=${KB_POD_FQDN}:9221
}

rebalance() {
  $CODIS_ADMIN --rebalance --confirm
  if [ $? != 0 ]; then
      echo "Error: rebalance failed"
      exit 1
  fi
}

set_group_id
set_codis_dashboard

if [ $# -eq 1 ]; then
  case $1 in
  --help)
    echo "Usage: $0 [options]"
    echo "Options:"
    echo "  --help                show help information"
    echo "  --register-server     register server to dashboard"
    echo "  --remove-server       remove server from dashboard"
    exit 0
    ;;
  --register-server)
    set_instance_role
    wait_dashboard_running
    wait_server_running
    register_server
    exit 0
    ;;
  --remove-server)
    set_instance_role
    remove_server
    exit 0
    ;;
  --rebalance)
    wait_dashboard_running
    confirm_max_group
    wait_master_registered
    rebalance
    exit 0
    ;;
  *)
    echo "Error: invalid option '$1'"
    exit 1
    ;;
  esac
fi
