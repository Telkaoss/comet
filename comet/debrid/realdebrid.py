import aiohttp
import asyncio

from RTN import parse

from comet.utils.general import is_video
from comet.utils.logger import logger
from comet.utils.models import settings


class RealDebrid:
    def __init__(self, session: aiohttp.ClientSession, debrid_api_key: str, ip: str):
        session.headers["Authorization"] = f"Bearer {debrid_api_key}"
        self.session = session
        self.ip = ip
        self.proxy = None

        self.api_url = "https://api.real-debrid.com/rest/1.0"

    async def check_premium(self):
        try:
            check_premium = await self.session.get(f"{self.api_url}/user")
            check_premium = await check_premium.text()
            if '"type": "premium"' in check_premium:
                return True
        except Exception as e:
            logger.warning(
                f"Exception while checking premium status on Real-Debrid: {e}"
            )

        return False

    async def get_files(
        self, torrent_hashes: list, type: str, season: str, episode: str, kitsu: bool
    ):
        files = {}

        for torrent_hash in torrent_hashes:
            try:
                # Add magnet link
                add_magnet_response = await self.session.post(
                    f"{self.api_url}/torrents/addMagnet",
                    data={"magnet": f"magnet:?xt=urn:btih:{torrent_hash}", "ip": self.ip},
                    proxy=self.proxy,
                )
                add_magnet = await add_magnet_response.json()

                # Get torrent info
                torrent_info_response = await self.session.get(
                    f"{self.api_url}/torrents/info/{add_magnet['id']}", proxy=self.proxy
                )
                torrent_info = await torrent_info_response.json()

                # Parse files
                for file in torrent_info["files"]:
                    filename = file["path"].lstrip("/")
                    if not is_video(filename):
                        continue

                    if "sample" in filename.lower():
                        continue

                    filename_parsed = parse(filename)

                    if type == "series":
                        if episode not in filename_parsed.episodes:
                            continue

                        if kitsu:
                            if filename_parsed.seasons:
                                continue
                        else:
                            if season not in filename_parsed.seasons:
                                continue

                    files[torrent_hash] = {
                        "index": file["id"],
                        "title": filename,
                        "size": file["bytes"],
                    }
                    break  # Stop after finding the first matching file

                # Optional: Delete the added torrent to prevent clutter
                await self.session.delete(
                    f"{self.api_url}/torrents/delete/{add_magnet['id']}", proxy=self.proxy
                )

            except Exception as e:
                logger.warning(
                    f"Exception while processing torrent {torrent_hash}: {e}"
                )

        return files

    async def generate_download_link(self, hash: str, index: str):
        try:
            check_blacklisted = await self.session.get("https://real-debrid.com/vpn")
            check_blacklisted = await check_blacklisted.text()
            if (
                "Your ISP or VPN provider IP address is currently blocked on our website"
                in check_blacklisted
            ):
                self.proxy = settings.DEBRID_PROXY_URL
                if not self.proxy:
                    logger.warning(
                        "Real-Debrid blacklisted server's IP. No proxy found."
                    )
                else:
                    logger.warning(
                        f"Real-Debrid blacklisted server's IP. Switching to proxy {self.proxy} for {hash}|{index}"
                    )

            # Add magnet link
            add_magnet_response = await self.session.post(
                f"{self.api_url}/torrents/addMagnet",
                data={"magnet": f"magnet:?xt=urn:btih:{hash}", "ip": self.ip},
                proxy=self.proxy,
            )
            add_magnet = await add_magnet_response.json()

            # Get torrent info
            torrent_info_response = await self.session.get(
                f"{self.api_url}/torrents/info/{add_magnet['id']}", proxy=self.proxy
            )
            torrent_info = await torrent_info_response.json()

            # Select files
            await self.session.post(
                f"{self.api_url}/torrents/selectFiles/{add_magnet['id']}",
                data={"files": index, "ip": self.ip},
                proxy=self.proxy,
            )

            # Get updated torrent info
            torrent_info_response = await self.session.get(
                f"{self.api_url}/torrents/info/{add_magnet['id']}", proxy=self.proxy
            )
            torrent_info = await torrent_info_response.json()

            # Get the download link
            unrestrict_link_response = await self.session.post(
                f"{self.api_url}/unrestrict/link",
                data={"link": torrent_info["links"][0], "ip": self.ip},
                proxy=self.proxy,
            )
            unrestrict_link = await unrestrict_link_response.json()

            # Optional: Delete the added torrent to prevent clutter
            await self.session.delete(
                f"{self.api_url}/torrents/delete/{add_magnet['id']}", proxy=self.proxy
            )

            return unrestrict_link["download"]
        except Exception as e:
            logger.warning(
                f"Exception while getting download link from Real-Debrid for {hash}|{index}: {e}"
            )
