## Run Multiple Chat Nodes
```bash
docker compose up --scale chat-node=3
```

## Example Of Run Multiple Chat Nodes
```bash
redis
postgres
chat-node-1
chat-node-2
chat-node-3
```

## Verifying the Cluster
```bash
docker compose ps

You should see something like:

chat-node-1
chat-node-2
chat-node-3
redis
postgres
```

## View Logs Per Node
```bash
docker compose logs chat-node

or

docker compose logs -f chat-node

You'll see each node starting with a different ID.
```

## Simulating Node Failure
```bash
Kill one node:

docker compose stop chat-node-2
```
