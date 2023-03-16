docker rm -f taskmanager
docker build -f TaskManagerDockerfile -t taskmanager_server .
docker run --network project-network -p 8010:8010 --name taskmanager taskmanager_server