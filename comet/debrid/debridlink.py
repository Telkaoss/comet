import aiohttp
import asyncio

from RTN import parse

from comet.utils.general import is_video
from comet.utils.logger import logger


class DebridLink:
    def __init__(self, session: aiohttp.ClientSession, debrid_api_key: str):
        session.headers["Authorization"] = f"Bearer {debrid_api_key}"
        self.session = session
        self.proxy = None

        self.api_url = "https://debrid-link.com/api/v2"

    async def check_premium(self):
        try:
            response = await self.session.get(f"{self.api_url}/account/infos")
            data = await response.json()
            if data.get("value", {}).get("accountType") == 1:
                return True
        except Exception as e:
            logger.warning(
                f"Exception while checking the premium status on Debrid-Link: {e}"
            )

        return False

    async def get_files(
        self, torrent_hashes: list, type: str, season: str, episode: str, kitsu: bool
    ):
        files = {}

        for torrent_hash in torrent_hashes:
            try:
                add_torrent_response = await self.session.post(
                    f"{self.api_url}/seedbox/add",
                    data={"url": torrent_hash, "async": True}
                )
                add_torrent = await add_torrent_response.json()

                if not add_torrent.get("success"):
                    logger.warning(f"Failed to add torrent {torrent_hash}")
                    continue

                torrent_id = add_torrent["value"]["id"]

                torrent_ready = False
                while not torrent_ready:
                    await asyncio.sleep(1)
                    torrent_info_response = await self.session.get(
                        f"{self.api_url}/seedbox/list", params={"ids": torrent_id}
                    )
                    torrent_info = await torrent_info_response.json()

                    if not torrent_info.get("success"):
                        logger.warning(f"Unable to retrieve info for torrent {torrent_id}")
                        break

                    if not torrent_info["value"]:
                        logger.warning(f"No information available for torrent {torrent_id}")
                        break

                    torrent_data = torrent_info["value"][0]
                    status = torrent_data.get("status")

                    if status == 6 or status == 100 or torrent_data.get("downloadPercent") == 100:
                        torrent_ready = True
                    else:
                        continue

                for file in torrent_data["files"]:
                    filename = file["name"]
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
                        "index": torrent_data["files"].index(file),
                        "title": filename,
                        "size": file["size"],
                    }
                    break  

                await self.session.delete(
                    f"{self.api_url}/seedbox/{torrent_id}/remove"
                )

            except Exception as e:
                logger.warning(
                    f"Exception while processing torrent {torrent_hash}: {e}"
                )

        return files

    async def generate_download_link(self, hash: str, index: str):
        try:
            add_torrent_response = await self.session.post(
                f"{self.api_url}/seedbox/add",
                data={"url": hash, "async": True}
            )
            add_torrent = await add_torrent_response.json()

            if not add_torrent.get("success"):
                logger.warning(f"Failed to add torrent {hash}")
                return None

            torrent_id = add_torrent["value"]["id"]

            torrent_ready = False
            while not torrent_ready:
                await asyncio.sleep(1)
                torrent_info_response = await self.session.get(
                    f"{self.api_url}/seedbox/list", params={"ids": torrent_id}
                )
                torrent_info = await torrent_info_response.json()

                if not torrent_info.get("success"):
                    logger.warning(f"Unable to retrieve info for torrent {torrent_id}")
                    break

                if not torrent_info["value"]:
                    logger.warning(f"No information available for torrent {torrent_id}")
                    break

                torrent_data = torrent_info["value"][0]
                status = torrent_data.get("status")

                if status == 6 or status == 100 or torrent_data.get("downloadPercent") == 100:
                    torrent_ready = True
                else:
                    continue

            file = torrent_data["files"][int(index)]
            download_url = file.get("downloadUrl")

            await self.session.delete(
                f"{self.api_url}/seedbox/{torrent_id}/remove"
            )

            return download_url

        except Exception as e:
            logger.warning(
                f"Exception while obtaining the download link from Debrid-Link for {hash}|{index}: {e}"
            )
