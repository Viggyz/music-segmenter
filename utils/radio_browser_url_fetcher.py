import asyncio
import random
import socket
import aiohttp
import logging

# Create custom SSL context pointing to certifi CA bundle

from .url_fetcher import UrlFetcher

ALLOWLISTED_STATIONS = [
    # "Bollywood Gaane Purane",
    "Fnf.Fm Hindi",
    # "MY RADIO DJ",
    "Radio BollyFm",
    # "MY CLUB REMIX",
    "Mirchi Top 20",
    "Mirchi Love",
    "Bollywood 2010's",
    "Red Fm", # AAC,
    "Bombay Beats Radio",
    "Radio Indigo 91.9 FM in Panaji/Bangalore",
    "Desi Hits 2000s",
    "MixiFy Hindi Hits",
    "ISHQ FM 104.8",
]
LOOKUP_STATIONS = [
    "MANGORADIO",
    "Free FM Top 100 India",
    "102.7 KIIS FM",
    "LOS 40 Principales España",
    "96.7 KISS FM - KHFI-FM Austin",
    "Hit Radio FFH",
    "Hits 1 Ibiza",
    "SWR3", # Just too noisy
    "Heart London 106.2 [MP3]",
    "Capital FM London",
    # "Rock FM", # does not support ICY metadata
    "Radio Caroline",
    "- 0 N - Pop on Radio",
    # "CAPITAL - The UK's No.1 Hit Music Station", # Dupe of Capital FM London
    "Heart 80s",
    "Cadena 100",
    "1LIVE",
    "Radio 538",
    # "Radio Deejay", # No ICY
    "Hit FM (UKraine) - 128kb/s",
    "Bayern 3",
    "Radio Nova",
    "Radio Eins",
    "Heart 90s",
    # "CAPITAL DANCE: The UK's Official Dance Station", # Dupe of other capital dance
    "- 0 N - Classic Rock on Radio",
    # "Deep House Radio - Bucharest Romania", # No icy
    "ESKA ROCK",
    "Radio Paradise Rock Mix 320k AAC",
    "West Coast – G-Funk & Hip-Hop",
    "Zeppelin 106.7",
    "KISS - The Best Vibes & Energy",
    "SLAM!",
    # ".977 The Mix",
    "Radio RMF MAXXX",
    "1.FM - Amsterdam Trance Radio",
    # "Heart", Dupe of other Heart ratio
    "Nostalgie New York",
    "Radio Paradise Mellow Mix 320k AAC",
    "Chocolate FM",
    "538 TOP 50",
    "Radio Swiss Pop",
    "Radio Paradise Main Mix 128 AAC",
    # "Radio 105 - Dance 90", Wierd output
    # "Hits 1 Algérie", # Duplicate of other Hits 1
    "Jazz Radio Funk",
    # "Heart UK", # Dupe of Heart
    "Capital Dance",
    "Skyrock",
    # ".977 Hitz", # No icy metadata
    "Frisky",
    # "Hard Rock Heaven", # NO icy metadata
    "Radio Freedom",
    "Smooth Radio",
    "TMM 1",
    "Radio Record - Main Channel",
    "1.FM - Deep House Radio",
    # "Metro FM", # No icy metadata
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
                logging.warning(
                    f"{LOG_PREFIX}Unable to download from api url: {uri}", e)

        return {}

    async def fetch_urls(self):
        async with aiohttp.ClientSession() as session:
            stations = await self._download_radiobrowser(session, "/json/stations/search", {"order": "votes", "reverse": "true", "hide_broken": "true", "countrycode": "IN"})
            for station in stations:
                if station['name'] in ALLOWLISTED_STATIONS:
                    if station['codec'] not in ('MP3', 'AAC', 'AAC+'):
                        logging.warning("%sInvalid codec %s for station %s",
                                        LOG_PREFIX, station['codec'], station['name'])
                        continue
                    logging.info("%s emitting station %s:%s", LOG_PREFIX,
                                 station['name'], station['url_resolved'])
                    yield station['name'], station['url'], station['codec']
            all_stations = await self._download_radiobrowser(session, "/json/stations/search", {"order": "votes", "reverse": "true", "hide_broken": "true", "limit": "3000"})
            stations_not_found = set(LOOKUP_STATIONS)
            for station in all_stations:
                if station['name'] in LOOKUP_STATIONS:
                    if station['name'] in stations_not_found:
                        stations_not_found.remove(station['name'])
                    else:
                        logging.warning("Station duplicate %s", station['name'])
                        continue
                    yield station['name'], station['url'], station['codec']
            assert len(stations_not_found) < 15, stations_not_found
            for lookup_name in stations_not_found:
                results = await self._download_radiobrowser(session, "/json/stations/search", {"order": "votes", "reverse": "true", "hide_broken": "true", "name": lookup_name})
                station = results[0]
                # if station['codec'] != 'MP3':
                #     continue
                yield station['name'], station['url'], station['codec']

__all__ = ["RadioBrowserUrlFetcher"]
