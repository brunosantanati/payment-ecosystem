# payment-ecosystem

Commands
```sh
docker compose up -d --build
docker logs p2a-service --tail 25
docker compose down
```

URLs
http://localhost:8090

Curl commands
```sh
curl -i -X POST http://localhost:8080/webhooks/p2a/status-updates -H "Content-Type: application/json" -d '{"payoutId":"pay_987654321","status":"PAYOUT_APPROVED","reason":null}'
```
