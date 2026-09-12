import asyncio
import random
import socket
import aiohttp
import logging

# Create custom SSL context pointing to certifi CA bundle

from .url_fetcher import UrlFetcher

ALLOWLISTED_STATIONS = [
    "Bollywood Gaane Purane",
    "Fnf.Fm Hindi",
    "MY RADIO DJ",
    "Radio BollyFm",
    "Radio Mirchi Hindi",
    "MY CLUB REMIX",
    "Mirchi Top 20",
    "Mirchi Love",
    "Bollywood 2010's",
    "Fm Rainbow Delhi",
    "radioBollyFM",
    ]

LOG_PREFIX = "[URL FETCHER]: "

class RadioBrowserUrlFetcher:
    async def _get_radiobrowser_base_urls(self):
        """
        Get all base urls of all currently available radiobrowser servers asynchronously.
        """
        hosts = []
        loop = asyncio.get_running_loop()

        # Offload blocking DNS calls to thread pool executor to avoid freezing the event loop
        ips = await loop.run_in_executor(
            None, socket.getaddrinfo, 'all.api.radio-browser.info', 80, 0, 0, socket.IPPROTO_TCP
        )

        for ip_tuple in ips:
            ip = ip_tuple[4][0]
            try:
                host_addr = await loop.run_in_executor(None, socket.gethostbyaddr, ip)
                if host_addr[0] not in hosts:
                    hosts.append(host_addr[0])
            except socket.herror:
                continue

        hosts.sort()
        return [f"https://{host}" for host in hosts]
    
    async def _download_uri(self, session: aiohttp.ClientSession, uri: str, param: dict | None):
        """
        Download JSON data asynchronously with headers set.
        """
        headers = {
            'User-Agent': 'MyApp/0.0.1',
            'Content-Type': 'application/json'
        }

        if param is not None:
            logging.info(f"{LOG_PREFIX}Request to {uri} Params: {param}")
        else:
            logging.info(f"{LOG_PREFIX}Request to {uri}")

        # Send POST request with JSON payload automatically serialized
        async with session.post(uri, json=param, headers=headers) as response:
            response.raise_for_status()
            return await response.json()
    
    async def _download_radiobrowser(self, session: aiohttp.ClientSession, path: str, param: dict | None):
        """
        Download file with relative url from a random api server asynchronously.
        """
        servers = await self._get_radiobrowser_base_urls()
        random.shuffle(servers)

        for i, server_base in enumerate(servers):
            logging.info(f"{LOG_PREFIX}Random server: {server_base} Try: {i}")
            uri = server_base + path

            try:
                return await self._download_uri(session, uri, param)
            except Exception as e:
                logging.warning(f"{LOG_PREFIX}Unable to download from api url: {uri}", e)

        return {}

    async def fetch_urls(self):
        async with aiohttp.ClientSession() as session:
            stations = await self._download_radiobrowser(session, "/json/stations/search", {"order": "votes","reverse": "true", "hide_broken": "true", "countrycode": "IN"})
            for station in stations:
                if station['name'] in ALLOWLISTED_STATIONS:
                    if station['codec'] != 'MP3':
                        logging.warning("%sInvalid codec %s for station %s", LOG_PREFIX, station['codec'], station['name'])
                        continue
                    logging.info("%s emitting station %s:%s", LOG_PREFIX, station['name'], station['url_resolved'])
                    yield station['name'], station['url']

__all__ = ["RadioBrowserUrlFetcher"]
