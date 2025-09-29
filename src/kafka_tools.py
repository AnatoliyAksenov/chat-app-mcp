import json
import asyncio


from uuid import uuid4
from kafka import KafkaConsumer
from typing import Optional, Literal
from fastmcp import FastMCP
from pydantic import BaseModel, Field


mcp = FastMCP()


def json_deserializer(data):
    return json.loads(data.decode('utf-8'))

import asyncio
from uuid import uuid4
from typing import Optional
from pydantic import Field
from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable, TopicAuthorizationFailedError, UnknownTopicOrPartitionError

@mcp.tool 
async def test_kafka_connection(
    bootstrap_servers: str = Field(..., description="Comma-separated list of Kafka broker addresses in format: host1:port1,host2:port2. Example: 'kafka1:9092,kafka2:9092'"),
    topic: str = Field(..., description="Name of the Kafka topic to test. The topic must exist in the Kafka cluster"),
    auto_offset_reset: Literal['earliest', 'latest'] = Field('earliest', description="Where to start reading from: 'earliest' (beginning) or 'latest' (new messages only). Default: 'earliest'"),
    group_id: Optional[str] = Field(None, description="Consumer group ID for the test. If not provided, generates a unique UUID-based ID. Using a unique group ID ensures clean test results"),
    timeout_seconds: int = Field(1, description="Maximum time in seconds to wait for connection and response. Increase for slow networks or large clusters. Default: 1")
) -> str:
    """
    Tests connectivity to Apache Kafka brokers and verifies topic accessibility.
    Validates that bootstrap servers are reachable and the specified topic exists and is readable.
    
    Features:
    - Plaintext connection only (no SASL/SSL authentication)
    - Topic existence verification
    - Broker connectivity testing
    - Message sampling capability
    
    Returns detailed success/error messages with specific troubleshooting information.
    """
    # Generate group_id if not provided
    if group_id is None:
        group_id = f'de-chat-{str(uuid4())}'
    
    consumer = None
    try:
        async with asyncio.timeout(timeout_seconds):
            # Create Kafka consumer
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=bootstrap_servers.split(','),
                auto_offset_reset=auto_offset_reset,
                enable_auto_commit=False,  
                group_id=group_id,
                consumer_timeout_ms=1000, 
            )
            
            # Check if topic exists and consumer can connect
            partitions = consumer.partitions_for_topic(topic)
            if not partitions:
                return f"Error: Topic '{topic}' not found or no partitions available"
            
            # Try to get a message (non-blocking poll)
            consumer.subscribe([topic])
            msg = consumer.poll(timeout_ms=1000)
            
            if msg:
                # Return information about the first message found
                for tp, messages in msg.items():
                    if messages:
                        first_msg = messages[0]
                        return (f"Success: Connected to Kafka brokers. "
                                f"Topic '{topic}' is available. "
                                f"Sample message offset: {first_msg.offset}")
                
                return (f"Success: Connected to Kafka brokers. "
                        f"Topic '{topic}' is available but no messages found with current offset settings.")
            else:
                return (f"Success: Connected to Kafka brokers. "
                        f"Topic '{topic}' is available but no messages received within timeout. "
                        f"This could be normal if the topic is empty.")
    
    except asyncio.TimeoutError:
        return f"Error: Connection timeout after {timeout_seconds} seconds. Kafka brokers may be unreachable."
    
    except NoBrokersAvailable:
        return f"Error: No Kafka brokers available at {bootstrap_servers}. Check the server addresses and ports."
    
    except UnknownTopicOrPartitionError:
        return f"Error: Topic '{topic}' does not exist on the Kafka cluster."
    
    except TopicAuthorizationFailedError:
        return f"Error: Not authorized to access topic '{topic}'. Check permissions."
    
    except KafkaError as e:
        return f"Kafka Error: {str(e)}"
    
    except Exception as e:
        return f"Unexpected Error: {str(e)}"
    
    finally:
        # Ensure consumer is properly closed
        if consumer:
            try:
                consumer.close()
            except:
                pass  # Ignore errors during cleanup