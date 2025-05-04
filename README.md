# RabbitMQ RPC Framework

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![RabbitMQ](https://img.shields.io/badge/RabbitMQ-3.9+-FF6600.svg)](https://www.rabbitmq.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Complete implementation for building RPC services and clients with RabbitMQ and Python asyncio.

## Features

- 🚀 **Dual Components** - Includes both `BaseResponseProcessor` (server) and `BaseServiceClient` (client)
- ⚡ **Async First** - Built on Python's asyncio for high performance
- 🛡️ **Error Handling** - Built-in decorators for RPC error management
- 🔄 **Bi-directional** - Supports both service implementation and client calls
- 🧩 **Modular** - Easy to extend with new RPC methods

## Installation

```bash
pip install git+https://github.com/your-repo/aiorabbitmq.git
```

Requirements:
* Python 3.8+
* RabbitMQ 3.9+
* aiorabbitmq


## Quick Start

1. Service Implementation (Server)

```python
# service.py
from aiorabbitmq.services import BaseResponseProcessor
import asyncio

service = BaseResponseProcessor(
    amqp_url="amqp://guest:guest@localhost/",
    exchange_name="math_exchange",
    queue_name="math_operations"
)

@service.add("square")
async def square_handler(data: dict):
    """Calculate square of a number"""
    return {'result': data['number'] ** 2}

@service.start
async def init_resources():
    print("🟢 Service starting...")

@service.stop  
async def cleanup_resources():
    print("🔴 Service stopping...")

if __name__ == "__main__":
    asyncio.run(service.run())
```

2. Client Implementation

```Python
# client.py
from aiorabbitmq.services import BaseServiceClient
import asyncio

class MathClient(BaseServiceClient):
    def __init__(self):
        super().__init__(
            amqp_url="amqp://guest:guest@localhost/",
            exchange_name="math_exchange",
            routing_key="math_operations"
        )

    @BaseServiceClient.handle_rpc_response
    async def square(self, number: float) -> dict:
        """Call remote square operation"""
        return {"action": "square", "data": {"number": number}}

async def main():
    async with MathClient() as client:
        result = await client.square(4)
        print(f"4 squared is {result['response']['result']}")

if __name__ == "__main__":
    asyncio.run(main())
```

## Message Protocols

### Client Request Format

```json
{
    "action": "operation_name",
    "data": {
        "param1": value1,
        "param2": value2
    },
    "user_id": "optional_identifier"
}
```


### Service Response Format

```json
{
    "status": "success|error",
    "response": {},  // Operation results
    "message": "",   // Error description
    "user_id": ""    // Echoes request identifier
}
```

## Advanced Usage

### Error Handling

```python
try:
    result = await client.square("invalid")
except ServiceExecuteError as e:
    print(f"RPC failed: {e}")
except ServiceConnectionError as e:
    print(f"Connection error: {e}")
```

## Configuration

### Environment variables:

- `AMQP_URL` - RabbitMQ connection string (required)
- `EXCHANGE_NAME` - AMQP exchange name (required)
- `QUEUE_NAME` -Consumer queue name (required)

## Deployment

### Docker Compose Example

```yaml
version: '3'
services:
  rabbitmq:
    image: rabbitmq:3-management
    ports:
      - "5672:5672"
      - "15672:15672"
  
  math-service:
    build: .
    environment:
      AMQP_URL: "amqp://rabbitmq:5672/"
      EXCHANGE_NAME: "math_exchange"
      QUEUE_NAME: "math_operations"
    depends_on:
      - rabbitmq
```

## Contributing

1. Fork the repository

2. Create your feature branch (git checkout -b feature/amazing-feature)

3. Commit your changes (git commit -m 'Add some amazing feature')

4. Push to the branch (git push origin feature/amazing-feature)

5. Open a Pull Request

## License
MIT © 2023 Gerasimov Sergey