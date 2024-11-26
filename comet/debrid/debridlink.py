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
            return data.get("value", {}).get("accountType") == 1
        except Exception as e:
            logger.error(
                f"Erreur lors de la vérification du statut premium sur Debrid-Link: {e}"
            )
            return False

    async def _process_torrent(
        self, torrent_hash, type, season, episode, kitsu
    ):
        try:
            add_torrent_response = await self.session.post(
                f"{self.api_url}/seedbox/add",
                data={"url": torrent_hash, "async": True}
            )
            add_torrent = await add_torrent_response.json()

            if not add_torrent.get("success"):
                logger.error(f"Échec de l'ajout du torrent {torrent_hash}")
                return None

            torrent_id = add_torrent["value"]["id"]

            # Attente jusqu'à ce que le torrent soit prêt
            while True:
                await asyncio.sleep(1)
                torrent_info_response = await self.session.get(
                    f"{self.api_url}/seedbox/list", params={"ids": torrent_id}
                )
                torrent_info = await torrent_info_response.json()

                if not torrent_info.get("success") or not torrent_info["value"]:
                    logger.error(f"Impossible de récupérer les infos du torrent {torrent_id}")
                    await self.session.delete(f"{self.api_url}/seedbox/{torrent_id}/remove")
                    return None

                torrent_data = torrent_info["value"][0]
                status = torrent_data.get("status")

                if status in {6, 100} or torrent_data.get("downloadPercent") == 100:
                    break

            # Filtrage des fichiers pertinents
            for index, file in enumerate(torrent_data["files"]):
                filename = file["name"]
                if not is_video(filename) or "sample" in filename.lower():
                    continue

                filename_parsed = parse(filename)

                if type == "series":
                    if episode not in filename_parsed.episodes:
                        continue

                    if kitsu and filename_parsed.seasons:
                        continue
                    elif not kitsu and season not in filename_parsed.seasons:
                        continue

                await self.session.delete(f"{self.api_url}/seedbox/{torrent_id}/remove")
                return {
                    "index": index,
                    "title": filename,
                    "size": file["size"],
                }

            # Suppression du torrent si aucun fichier pertinent n'est trouvé
            await self.session.delete(f"{self.api_url}/seedbox/{torrent_id}/remove")
            return None

        except Exception as e:
            logger.error(
                f"Erreur lors du traitement du torrent {torrent_hash}: {e}"
            )
            return None

    async def get_files(
        self, torrent_hashes: list, type: str, season: str, episode: str, kitsu: bool
    ):
        tasks = [
            self._process_torrent(torrent_hash, type, season, episode, kitsu)
            for torrent_hash in torrent_hashes
        ]
        results = await asyncio.gather(*tasks)
        return {
            torrent_hash: result
            for torrent_hash, result in zip(torrent_hashes, results)
            if result is not None
        }

    async def generate_download_link(self, hash: str, index: str):
        try:
            add_torrent_response = await self.session.post(
                f"{self.api_url}/seedbox/add",
                data={"url": hash, "async": True}
            )
            add_torrent = await add_torrent_response.json()

            if not add_torrent.get("success"):
                logger.error(f"Échec de l'ajout du torrent {hash}")
                return None

            torrent_id = add_torrent["value"]["id"]

            # Attente jusqu'à ce que le torrent soit prêt
            while True:
                await asyncio.sleep(1)
                torrent_info_response = await self.session.get(
                    f"{self.api_url}/seedbox/list", params={"ids": torrent_id}
                )
                torrent_info = await torrent_info_response.json()

                if not torrent_info.get("success") or not torrent_info["value"]:
                    logger.error(f"Impossible de récupérer les infos du torrent {torrent_id}")
                    await self.session.delete(f"{self.api_url}/seedbox/{torrent_id}/remove")
                    return None

                torrent_data = torrent_info["value"][0]
                status = torrent_data.get("status")

                if status in {6, 100} or torrent_data.get("downloadPercent") == 100:
                    break

            file = torrent_data["files"][int(index)]
            download_url = file.get("downloadUrl")

            await self.session.delete(f"{self.api_url}/seedbox/{torrent_id}/remove")

            return download_url

        except Exception as e:
            logger.error(
                f"Erreur lors de l'obtention du lien de téléchargement pour {hash}|{index}: {e}"
            )
            return None
