# Python distributed-computation example with Redis List as queue and Redis Stream

```bash
.
├── README.md
├── .env
├── rabbitmq_aio_pika
│   ├── main.py
│   ├── producer
│   ├── rabbit.py
│   └── worker
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
- A complete examples showing how to implement a distributed-computation model using asyncio Python with Redis List as queue and Redis Stream.
- redis_reliable_queue - rude redis list as queue with reliable queue pattern enhancement
- redis-stream  - redis stream with ACK, program runtime 20s
- rabbitmq_aio_pika - rabbiqmq queue