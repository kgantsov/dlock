# dlock
Yet another distibuted lock based on the Raft consensus algorithm


# Running dlock

```bash
git clone git@github.com:kgantsov/dlock.git
cd dlock/cmd/server
go build -o dlock
```

Run the first node

```bash
./dlock -id node0 ./data/node0
```

Run other nodes

```bash
./dlock -id node1 -haddr 11001 -raddr localhost:12001 -join :11000 ./data/node1
./dlock -id node2 -haddr 11002 -raddr localhost:12002 -join :11000 ./data/node2
```

You can find swagger docs by opening http://localhost:11000/docs

To acquire a lock run

```bash
curl --request POST \
  --url http://localhost:11000/API/v1/locks/my-lock-name \
  --header 'Accept: application/json' \
  --header 'Content-Type: application/json'
```

To release a lock run

```bash
curl --request DELETE \
  --url http://localhost:11000/API/v1/locks/my-lock-name \
  --header 'Accept: application/json'
```
