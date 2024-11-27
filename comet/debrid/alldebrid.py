import aiohttp
import asyncio

from RTN import parse
from comet.utils.general import is_video
from comet.utils.logger import logger
from comet.utils.models import settings


class AllDebrid:
    def __init__(self, session: aiohttp.ClientSession, debrid_api_key: str):
        session.headers["Authorization"] = f"Bearer {debrid_api_key}"
        self.session = session
        self.proxy = None
        self.api_url = "https://api.alldebrid.com/v4"
        self.agent = "comet"

    async def check_premium(self):
        try:
            check_premium = await self.session.get(
                f"{self.api_url}/user?agent={self.agent}"
            )
            check_premium = await check_premium.text()
            if '"isPremium":true' in check_premium:
                return True
        except Exception as e:
            logger.warning(
                f"Exception while checking premium status on All-Debrid: {e}"
            )
        return False

    async def delete_magnet(self, magnet_id: int, retry_count: int = 3) -> bool:
        try:
            logger.info(f"Attempting to delete magnet with ID: {magnet_id}")
            delete_response = await self.session.get(
                f"{self.api_url}/magnet/delete?agent={self.agent}&id={magnet_id}",
                proxy=self.proxy,
            )
            delete_data = await delete_response.json()

            if delete_data.get("status") == "success":
                logger.info(f"Successfully deleted magnet {magnet_id}")
                return True

            error_code = delete_data.get("error", {}).get("code", "UNKNOWN")
            error_message = delete_data.get("error", {}).get("message", "No details")
            if error_code == "MAGNET_INVALID_ID":
                logger.debug(f"Skipping deletion for invalid magnet ID {magnet_id}")
                return False
            else:
                logger.error(f"Failed to delete magnet {magnet_id}. Error: {error_code}, Message: {error_message}")

        except aiohttp.ClientResponseError as e:
            if e.status == 503 and retry_count > 0:
                logger.warning(f"Service Unavailable for magnet {magnet_id}. Retrying...")
                await asyncio.sleep(2)
                return await self.delete_magnet(magnet_id, retry_count - 1)
            logger.error(f"Exception while deleting magnet {magnet_id}: {e}")

        except Exception as e:
            logger.error(f"Unexpected exception while deleting magnet {magnet_id}: {e}")

        return False

    async def add_and_get_status(self, chunk: list):
        added_magnets = []
        magnet_statuses = []
        magnets_to_delete = set()

        for magnet in chunk:
            try:
                logger.debug(f"Uploading magnet: {magnet}")
                upload_response = await self.session.get(
                    f"{self.api_url}/magnet/upload?agent={self.agent}&magnets[]={magnet}",
                    proxy=self.proxy,
                )
                upload_response = await upload_response.json()
                if "data" in upload_response and "magnets" in upload_response["data"]:
                    magnet_data = upload_response["data"]["magnets"][0]
                    added_magnets.append(magnet_data)
                    magnets_to_delete.add(magnet_data["id"])
                    logger.info(f"Successfully uploaded magnet with ID: {magnet_data['id']}")
            except Exception as e:
                logger.warning(f"Exception while uploading magnet {magnet}: {e}")

        for magnet in added_magnets:
            try:
                logger.debug(f"Checking status for magnet ID: {magnet['id']}")
                status_response = await self.session.get(
                    f"{self.api_url}/magnet/status?agent={self.agent}&id={magnet['id']}",
                    proxy=self.proxy,
                )
                status_data = await status_response.json()
                if "data" in status_data and "magnets" in status_data["data"]:
                    magnet_statuses.append(status_data["data"]["magnets"])
            except Exception as e:
                logger.warning(f"Exception while checking status for magnet {magnet['id']}: {e}")

        failed_deletions = []
        for magnet_id in magnets_to_delete:
            try:
                success = await self.delete_magnet(magnet_id)
                if not success:
                    failed_deletions.append(magnet_id)
                    logger.error(f"Failed to delete magnet {magnet_id}")
            except Exception as e:
                failed_deletions.append(magnet_id)
                logger.error(f"Exception during deletion of magnet {magnet_id}: {e}")

        if failed_deletions:
            logger.warning(f"Attempting final cleanup of {len(failed_deletions)} failed deletions")
            for magnet_id in failed_deletions:
                try:
                    await asyncio.sleep(1)
                    await self.delete_magnet(magnet_id, retry_count=1)
                except Exception as e:
                    logger.error(f"Final deletion attempt failed for magnet {magnet_id}: {e}")

        return magnet_statuses

    async def get_files(
        self, torrent_hashes: list, type: str, season: str, episode: str, kitsu: bool
    ):
        chunk_size = 12
        chunks = [
            torrent_hashes[i : i + chunk_size]
            for i in range(0, len(torrent_hashes), chunk_size)
        ]

        tasks = []
        for chunk in chunks:
            tasks.append(self.add_and_get_status(chunk))

        responses = await asyncio.gather(*tasks)
        files = {}

        if type == "series":
            for result in responses:
                for magnet in result:
                    magnet_added = False
                    for file in magnet["links"]:
                        filename = file["filename"]
                        pack = False

                        if "e" in file:
                            filename = file["e"][0]["filename"]
                            pack = True

                        if not is_video(filename) or "sample" in filename.lower():
                            continue

                        filename_parsed = parse(filename)
                        if episode not in filename_parsed.episodes:
                            continue

                        if kitsu:
                            if filename_parsed.seasons:
                                continue
                        else:
                            if season not in filename_parsed.seasons:
                                continue

                        files[magnet["hash"]] = {
                            "index": magnet["links"].index(file),
                            "title": filename,
                            "size": file["e"][0]["size"] if pack else file["size"],
                        }
                        magnet_added = True
                        break

        else:
            for result in responses:
                for magnet in result:
                    magnet_added = False
                    for file in magnet["links"]:
                        filename = file["filename"]

                        if not is_video(filename) or "sample" in filename.lower():
                            continue

                        files[magnet["hash"]] = {
                            "index": magnet["links"].index(file),
                            "title": filename,
                            "size": file["size"],
                        }
                        magnet_added = True
                        break

        return files

    async def generate_download_link(self, hash: str, index: str):
        try:
            check_blacklisted = await self.session.get(
                f"{self.api_url}/magnet/upload?agent=comet&magnets[]={hash}"
            )
            check_blacklisted = await check_blacklisted.text()
            if "NO_SERVER" in check_blacklisted:
                self.proxy = settings.DEBRID_PROXY_URL
                if not self.proxy:
                    logger.warning(
                        "All-Debrid blacklisted server's IP. No proxy found."
                    )
                else:
                    logger.warning(
                        f"All-Debrid blacklisted server's IP. Switching to proxy {self.proxy} for {hash}|{index}"
                    )

            upload_magnet = await self.session.get(
                f"{self.api_url}/magnet/upload?agent=comet&magnets[]={hash}",
                proxy=self.proxy,
            )
            upload_magnet = await upload_magnet.json()

            get_magnet_status = await self.session.get(
                f"{self.api_url}/magnet/status?agent=comet&id={upload_magnet['data']['magnets'][0]['id']}",
                proxy=self.proxy,
            )
            get_magnet_status = await get_magnet_status.json()

            unlock_link = await self.session.get(
                f"{self.api_url}/link/unlock?agent=comet&link={get_magnet_status['data']['magnets']['links'][int(index)]['link']}",
                proxy=self.proxy,
            )
            unlock_link = await unlock_link.json()

            return unlock_link["data"]["link"]
        except Exception as e:
            logger.warning(
                f"Exception while getting download link from All-Debrid for {hash}|{index}: {e}"
            )
