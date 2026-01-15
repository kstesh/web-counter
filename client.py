import asyncio
import time
import aiohttp
from pupupu import load_config

config = load_config("client")
BASE_URL = f"http://{config['host']}:{config['port']}"

async def get_count(session: aiohttp.ClientSession) -> int:
    async with session.get(f"{BASE_URL}/count", timeout=config["timeout_seconds"]) as resp:
        data = await resp.json()
        return data["count"]

async def worker(session: aiohttp.ClientSession):
    for _ in range(config["requests_per_client"]):
        async with session.get(f"{BASE_URL}/inc", timeout=config["timeout_seconds"]):
            pass

async def run_test(clients: int):
    connector = aiohttp.TCPConnector(limit=clients)
    async with aiohttp.ClientSession(connector=connector) as session:
        initial_count = await get_count(session)

        start = time.perf_counter()
        tasks = [worker(session) for _ in range(clients)]
        await asyncio.gather(*tasks)
        end = time.perf_counter()

        final_count = await get_count(session)

    total_from_clients = clients * config["requests_per_client"]
    total_on_server = final_count - initial_count
    duration = end - start

    print(f"Clients: {clients}")
    print(f"Time: {duration:.2f} sec")
    print(f"Throughput: {total_from_clients / duration:.2f} req/sec")
    print(f"Expected Δ:  {total_from_clients}")
    print(f"Actual Δ:  {total_on_server}")
    print("-" * 40)

if __name__ == "__main__":
    for c in [1, 2, 5, 10]:
        asyncio.run(run_test(c))
