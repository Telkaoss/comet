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
            response = await self.session.get(f"{self.api_url}/user")
            response_text = await response.text()
            if '"type": "premium"' in response_text:
                return True
        except Exception as e:
            logger.warning(f"Exception while checking premium status on Real-Debrid: {e}")
        return False

    async def get_files(
        self,
        torrent_hashes: list,
        type: str,
        season: str,
        episode: str,
        kitsu: bool,
        delete_after_use: bool = True,
    ):
        files = {}

        for torrent_hash in torrent_hashes:
            try:
                add_magnet_response = await self.session.post(
                    f"{self.api_url}/torrents/addMagnet",
                    data={"magnet": f"magnet:?xt=urn:btih:{torrent_hash}", "ip": self.ip},
                    proxy=self.proxy,
                )
                add_magnet = await add_magnet_response.json()

                torrent_info_response = await self.session.get(
                    f"{self.api_url}/torrents/info/{add_magnet['id']}", proxy=self.proxy
                )
                torrent_info = await torrent_info_response.json()

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
                    break 

                if delete_after_use:
                    await self.session.delete(
                        f"{self.api_url}/torrents/delete/{add_magnet['id']}",
                        proxy=self.proxy,
                    )

            except Exception as e:
                logger.warning(f"Exception while processing torrent {torrent_hash}: {e}")

        return files

    async def generate_download_link(
        self, hash: str, index: str, delete_after_use: bool = False
    ):
        try:
            check_blacklisted = await self.session.get("https://real-debrid.com/vpn")
            check_blacklisted_text = await check_blacklisted.text()
            if (
                "Your ISP or VPN provider IP address is currently blocked on our website"
                in check_blacklisted_text
            ):
                self.proxy = settings.DEBRID_PROXY_URL
                logger.warning(f"Real-Debrid blacklisted server's IP. Using proxy: {self.proxy}")

            add_magnet_response = await self.session.post(
                f"{self.api_url}/torrents/addMagnet",
                data={"magnet": f"magnet:?xt=urn:btih:{hash}", "ip": self.ip},
                proxy=self.proxy,
            )
            add_magnet = await add_magnet_response.json()

            torrent_info_response = await self.session.get(
                f"{self.api_url}/torrents/info/{add_magnet['id']}", proxy=self.proxy
            )
            torrent_info = await torrent_info_response.json()

            await self.session.post(
                f"{self.api_url}/torrents/selectFiles/{add_magnet['id']}",
                data={"files": index, "ip": self.ip},
                proxy=self.proxy,
            )

            torrent_info_response = await self.session.get(
                f"{self.api_url}/torrents/info/{add_magnet['id']}", proxy=self.proxy
            )
            torrent_info = await torrent_info_response.json()

            unrestrict_link_response = await self.session.post(
                f"{self.api_url}/unrestrict/link",
                data={"link": torrent_info["links"][0], "ip": self.ip},
                proxy=self.proxy,
            )
            unrestrict_link = await unrestrict_link_response.json()

            if delete_after_use:
                await self.session.delete(
                    f"{self.api_url}/torrents/delete/{add_magnet['id']}",
                    proxy=self.proxy,
                )

            return unrestrict_link["download"]

        except Exception as e:
            logger.warning(f"Exception while getting download link for {hash}|{index}: {e}")
