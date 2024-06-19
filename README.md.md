# Python distributed-computation example with Redis Queue and Redis Stream

```bash
.
├── README.md.md
├── redis_reliable_queue
│   ├── main.py
│   ├── producer
│   ├── redis.py
│   └── worker
├── redis_streams
│   ├── main.py
│   ├── pending_handler
│   ├── producer
│   ├── redis.py
│   └── worker
├── requrements.txt
├── ruff.toml
└── settings.py
```

### Requirements:
`cp .env.example .env`

### About:
- A complete examples showing how to implement a distributed-computation model using asyncio Python with Redis Queue and Redis Stream.
- redis_reliable_queue - rude redis queue with reliable pattern enhancement
- redis-stream  - redis stream with ack, program runtime 20s