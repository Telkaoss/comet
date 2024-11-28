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
        self.max_retries = 3
        self.retry_delay = 2

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

    async def _make_request(self, url: str, operation: str, retry_count: int = 3) -> dict:
        """Wrapper centralisé pour les requêtes API avec retry et logging"""
        for attempt in range(retry_count):
            try:
                response = await self.session.get(url, proxy=self.proxy)
                data = await response.json()
                
                if data.get("status") == "success":
                    return data
                
                error = data.get("error", {})
                logger.warning(f"{operation} failed: {error.get('code')} - {error.get('message')}")
                
                if error.get("code") == "NO_SERVER" and self.proxy:
                    logger.info("Switching to proxy for next attempt")
                    self.proxy = settings.DEBRID_PROXY_URL
                
            except aiohttp.ClientResponseError as e:
                if e.status == 503 and attempt < retry_count - 1:
                    wait_time = self.retry_delay * (attempt + 1)
                    logger.warning(f"Service unavailable, retrying in {wait_time}s...")
                    await asyncio.sleep(wait_time)
                    continue
                raise
            except Exception as e:
                logger.error(f"Unexpected error during {operation}: {str(e)}")
                if attempt < retry_count - 1:
                    await asyncio.sleep(self.retry_delay)
                    continue
                raise
        
        return None

    async def delete_magnet(self, magnet_id: int, retry_count: int = 3) -> bool:
        try:
            logger.info(f"Attempting to delete magnet with ID: {magnet_id}")
            response = await self._make_request(
                f"{self.api_url}/magnet/delete?agent={self.agent}&id={magnet_id}",
                f"delete magnet {magnet_id}",
                retry_count
            )
            
            if response and response.get("status") == "success":
                logger.info(f"Successfully deleted magnet {magnet_id}")
                return True
                
            error = response.get("error", {})
            if error.get("code") == "MAGNET_INVALID_ID":
                logger.debug(f"Skipping deletion for invalid magnet ID {magnet_id}")
                return False
                
            logger.error(f"Failed to delete magnet {magnet_id}. Error: {error.get('code')}")
            
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
                response = await self._make_request(
                    f"{self.api_url}/magnet/upload?agent={self.agent}&magnets[]={magnet}",
                    f"upload magnet {magnet}"
                )
                
                if response and "data" in response and "magnets" in response["data"]:
                    magnet_data = response["data"]["magnets"][0]
                    added_magnets.append(magnet_data)
                    magnets_to_delete.add(magnet_data["id"])
                    logger.info(f"Successfully uploaded magnet with ID: {magnet_data['id']}")
            except Exception as e:
                logger.warning(f"Exception while uploading magnet {magnet}: {e}")

        for magnet in added_magnets:
            try:
                logger.debug(f"Checking status for magnet ID: {magnet['id']}")
                response = await self._make_request(
                    f"{self.api_url}/magnet/status?agent={self.agent}&id={magnet['id']}",
                    f"check status magnet {magnet['id']}"
                )
                if response and "data" in response and "magnets" in response["data"]:
                    magnet_statuses.append(response["data"]["magnets"])
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

    def _filter_files(self, links: list, type: str, season: str, episode: str, kitsu: bool) -> dict:
        """Filtre les fichiers selon les critères demandés"""
        for file in links:
            try:
                filename = file.get("filename")
                if "e" in file:  # Cas des packs
                    filename = file["e"][0]["filename"]
                
                if not filename or not is_video(filename) or "sample" in filename.lower():
                    continue

                if type == "series":
                    parsed = parse(filename)
                    if episode not in parsed.episodes:
                        continue
                        
                    if kitsu and parsed.seasons:
                        continue
                    elif not kitsu and season not in parsed.seasons:
                        continue

                return {
                    "index": links.index(file),
                    "title": filename,
                    "size": file["e"][0]["size"] if "e" in file else file["size"],
                }
                
            except Exception as e:
                logger.debug(f"Error filtering file {filename}: {str(e)}")
                continue
                
        return None

    async def get_files(
        self, torrent_hashes: list, type: str, season: str, episode: str, kitsu: bool
    ):
        files = {}
        processed = set()  # Pour éviter les doublons
        failed_hashes = []  # Pour tracer les échecs

        chunk_size = 12
        chunks = [
            torrent_hashes[i : i + chunk_size]
            for i in range(0, len(torrent_hashes), chunk_size)
        ]

        for chunk in chunks:
            try:
                statuses = await self.add_and_get_status(chunk)
                
                for magnet in statuses:
                    if not magnet or magnet["hash"] in processed:
                        continue
                        
                    processed.add(magnet["hash"])
                    
                    valid_files = self._filter_files(
                        magnet["links"], 
                        type, 
                        season, 
                        episode, 
                        kitsu
                    )
                    
                    if valid_files:
                        files[magnet["hash"]] = valid_files
                    else:
                        failed_hashes.append(magnet["hash"])
                        
            except Exception as e:
                logger.error(f"Failed to process chunk: {str(e)}")
                failed_hashes.extend(chunk)

        if failed_hashes:
            logger.warning(f"Failed to process {len(failed_hashes)} hashes: {failed_hashes}")
            
        return files

    async def generate_download_link(self, hash: str, index: str):
        try:
            # Vérifie si le serveur est blacklisté
            blacklist_check = await self._make_request(
                f"{self.api_url}/magnet/upload?agent={self.agent}&magnets[]={hash}",
                "check blacklist status"
            )
            
            if "NO_SERVER" in str(blacklist_check):
                self.proxy = settings.DEBRID_PROXY_URL
                if not self.proxy:
                    logger.warning("All-Debrid blacklisted server's IP. No proxy found.")
                else:
                    logger.warning(
                        f"All-Debrid blacklisted server's IP. Switching to proxy {self.proxy} for {hash}|{index}"
                    )
            
            # Upload du magnet
            upload_response = await self._make_request(
                f"{self.api_url}/magnet/upload?agent={self.agent}&magnets[]={hash}",
                f"upload magnet {hash}"
            )
            
            if not upload_response or "data" not in upload_response:
                raise Exception("Failed to upload magnet")
                
            magnet_id = upload_response['data']['magnets'][0]['id']
            
            # Vérification du statut
            status_response = await self._make_request(
                f"{self.api_url}/magnet/status?agent={self.agent}&id={magnet_id}",
                f"check status {magnet_id}"
            )
            
            if not status_response or "data" not in status_response:
                raise Exception("Failed to get magnet status")
                
            link = status_response['data']['magnets']['links'][int(index)]['link']
            
            # Déverrouillage du lien
            unlock_response = await self._make_request(
                f"{self.api_url}/link/unlock?agent={self.agent}&link={link}",
                f"unlock link for {hash}|{index}"
            )
            
            if not unlock_response or "data" not in unlock_response:
                raise Exception("Failed to unlock link")
                
            return unlock_response["data"]["link"]
            
        except Exception as e:
            logger.warning(
                f"Exception while getting download link from All-Debrid for {hash}|{index}: {e}"
            )
            return None
