if docker ps -a --format '{{.Names}}' | grep -q '^etcd-gcr-v3.4.24$'; then
    docker rm etcd-gcr-v3.4.24
else
    echo "Container not found"
fi

docker network create cp_etcd || true

rm -rf /tmp/etcd-data.tmp && mkdir -p /tmp/etcd-data.tmp && \
docker rmi gcr.io/etcd-development/etcd:v3.4.24 || true && \
docker run \
  -p 8179:2379 \
  -p 8180:2380 \
  --mount type=bind,source=/tmp/etcd-data.tmp,destination=/etcd-data \
  --network cp_etcd \
  --name etcd-gcr-v3.4.24 \
  gcr.io/etcd-development/etcd:v3.4.24 \
  /usr/local/bin/etcd \
  --name s1 \
  --data-dir /etcd-data \
  --listen-client-urls http://0.0.0.0:2379 \
  --advertise-client-urls http://0.0.0.0:2379 \
  --listen-peer-urls http://0.0.0.0:2380 \
  --initial-advertise-peer-urls http://0.0.0.0:2380 \
  --initial-cluster s1=http://0.0.0.0:2380 \
  --initial-cluster-token tkn \
  --initial-cluster-state new \
  --log-level info \
  --logger zap \
  --log-outputs stderr
