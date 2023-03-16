docker rm -f controlplane
docker build -f ControlPlaneDockerfile -t controlplane_server .
docker run --network project-network -p 8008:8008 --name controlplane controlplane_server