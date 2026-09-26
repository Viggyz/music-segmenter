import asyncio
from tortoise import Tortoise
from models import Station, StreamClip, StreamTitleParserMetadata

async def init_db():
    await Tortoise.init(
        db_url='sqlite://app.db',
        modules={'models': ['models']} # refers to models file
    )
    await Tortoise.generate_schemas()

async def main():
    await init_db()
    
    await Station.create(level="INFO", message="Task started successfully")
    await StreamClip.create(level="INFO", message="Task started successfully")
    await StreamTitleParserMetadata.create(level="INFO", message="Task started successfully")
    
    logs = await Station.filter(level="INFO").all()
    print(f"Total info logs: {len(logs)}")
    
    await Tortoise.close_connections()

if __name__ == "__main__":
    asyncio.run(main())