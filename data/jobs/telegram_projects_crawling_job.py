import asyncio
import re
import time

from pymongo import MongoClient
from telethon import TelegramClient, functions
from telethon.tl import TLObject

from constants.config import AccountConfig, MongoDBConfig
from constants.mongo_constant import MongoCollection
from constants.telegram import TelegramUser, TelegramMessage, Projects
from constants.time_constant import TimeConstants
from databases.mongodb_cdp import MongoDBCDP
from databases.mongodb_centic import MongoDBCentic
from telethon.tl.types import User
from telethon.tl.types import Message
from src.jobs.cli_job import CLIJob
from utils.file_utils import write_last_time_running_logs
from utils.logger_utils import get_logger
from utils.time_utils import round_timestamp
from telethon.tl.functions.channels import GetFullChannelRequest, GetParticipantRequest

logger = get_logger('Telegram Project Crawling Job')


class TelegramProjectCrawlingJob(CLIJob):
    def __init__(
            self,
            interval: int,
            period: int,
            projects: list,
            exporter: MongoDBCDP,
            mongodb_centic: MongoDBCentic,
            api_id: str = AccountConfig.TELE_API_ID,
            api_hash: str = AccountConfig.TELE_API_HASH,
            session_id: str = "telegram",
            monitor: bool = False,
            stream_types: list = ["users", "messages", "new_users", "check_announcement"]
    ):
        super().__init__(interval, period, retry=False)
        self.stream_types = stream_types
        self.monitor = monitor
        self.api_hash = api_hash
        self.api_id = api_id
        self.exporter = exporter
        self.mongodb_centic = mongodb_centic
        self.projects = projects
        self.client = TelegramClient(session_id, int(self.api_id), self.api_hash)

    def convert_user_to_dict(self, user: User, project, _id):
        return {
            TelegramUser.id_: f"{_id}_{user.id}",
            TelegramUser.user_id: str(user.id),
            TelegramUser.project: project,
            TelegramUser.verified: user.verified,
            TelegramUser.user_name: user.username,
            TelegramUser.first_name: user.first_name,
            TelegramUser.last_name: user.last_name,
            TelegramUser.bot: user.bot,
            TelegramUser.bot_attach_menu: user.bot_attach_menu,
            TelegramUser.bot_nochats: user.bot_nochats,
            TelegramUser.bot_inline_geo: user.bot_inline_geo,
            TelegramUser.bot_can_edit: user.bot_can_edit,
            TelegramUser.bot_chat_history: user.bot_chat_history,
            TelegramUser.contact: user.contact,
            TelegramUser.deleted: user.deleted,
            TelegramUser.fake: user.fake,
            TelegramUser.is_self: user.is_self,
            TelegramUser.mutual_contact: user.mutual_contact,
            TelegramUser.phone: user.phone,
            TelegramUser.premium: user.premium,
            TelegramUser.restricted: user.restricted,
            TelegramUser.scam: user.scam,
            TelegramUser.support: user.support,
            TelegramUser.restriction_reason: [] if user.restriction_reason is None else [
                x.to_dict() if isinstance(x, TLObject) else x for x in user.restriction_reason],
            TelegramUser.status: self.refactor_message_dict(user.status.to_dict())
            if isinstance(user.status, TLObject) else user.status,
            TelegramUser.user_names: [] if user.usernames is None else
            [self.refactor_message_dict(x.to_dict()) if isinstance(x, TLObject) else x for x in user.usernames],
            TelegramUser.last_updated_time: int(time.time())
        }

    def convert_message_to_dict(self, message: Message, project, _id):
        result = {
            TelegramMessage.id_: f"{_id}_{message.id}",
            TelegramMessage.channel: _id,
            TelegramMessage.message_id: str(message.id),
            TelegramMessage.message_: message.message,
            TelegramMessage.project: project,
            TelegramMessage.out: message.out,
            TelegramMessage.timestamp: int(message.date.timestamp()),
            TelegramMessage.date: str(message.date),
            TelegramMessage.edit_date: str(message.edit_date),
            TelegramMessage.edit_hide: message.edit_hide,
            TelegramMessage.entities: [] if message.entities is None else
            [self.refactor_message_dict(x.to_dict()) if isinstance(x, TLObject) else x for x in message.entities],
            TelegramMessage.forwards: message.forwards,
            TelegramMessage.from_id: self.refactor_message_dict(message.from_id.to_dict())
            if isinstance(message.from_id, TLObject) else message.from_id,
            TelegramMessage.from_scheduled: message.from_scheduled,
            TelegramMessage.fwd_from: self.refactor_message_dict(message.fwd_from.to_dict())
            if isinstance(message.fwd_from, TLObject) else message.fwd_from,
            TelegramMessage.grouped_id: message.grouped_id,
            TelegramMessage.invert_media: message.invert_media,
            TelegramMessage.noforwards: message.noforwards,
            # TelegramMessage.media: self.refactor_message_dict(message.media.to_dict())
            # if isinstance(message.media, TLObject) else message.media,
            TelegramMessage.media_unread: message.media_unread,
            TelegramMessage.legacy: message.legacy,
            TelegramMessage.views: message.views,
            TelegramMessage.mentioned: message.mentioned,
            TelegramMessage.peer_id: self.refactor_message_dict(message.peer_id.to_dict())
            if isinstance(message.peer_id, TLObject) else message.peer_id,
            TelegramMessage.post: message.post,
            TelegramMessage.pinned: message.pinned,
            TelegramMessage.post_author: message.post_author,
            TelegramMessage.reactions: self.refactor_message_dict(message.reactions.to_dict())
            if isinstance(message.reactions, TLObject) else message.reactions,
            TelegramMessage.number_reactions: sum(
                reaction['count'] for reaction in message.reactions.to_dict()['results']) if message.reactions and
                                                                                             message.reactions.to_dict()[
                                                                                                 "results"] else 0,
            TelegramMessage.replies: self.refactor_message_dict(message.replies.to_dict())
            if isinstance(message.replies, TLObject) else message.replies,
            # TelegramMessage.reply_markup: self.refactor_message_dict(message.reply_markup.to_dict())
            # if isinstance(message.reply_markup, TLObject) else message.reply_markup,
            TelegramMessage.reply_to: self.refactor_message_dict(message.reply_to.to_dict())
            if isinstance(message.reply_to, TLObject) else message.reply_to,
            TelegramMessage.restriction_reason: [] if message.restriction_reason is None else
            [self.refactor_message_dict(item=x.to_dict()) if isinstance(x, TLObject) else x for x in
             message.restriction_reason],
            TelegramMessage.silent: message.silent,
            TelegramMessage.ttl_period: message.ttl_period,
            TelegramMessage.via_bot_id: message.via_bot_id
        }
        if time.time() - result.get(TelegramMessage.timestamp) < self.period:
            if result.get(TelegramMessage.views) is not None:
                views = message.views if message.views is not None else 0
                # reactions = message.reactions.to_dict()["results"][0]["count"] if message.reactions and message.reactions.to_dict()["results"] else 0
                reactions = sum(
                    reaction['count'] for reaction in message.reactions.to_dict()['results']) if message.reactions and \
                                                                                                 message.reactions.to_dict()[
                                                                                                     "results"] else 0
                replies = message.replies.to_dict()["replies"] if message.replies and message.replies.to_dict()[
                    "replies"] else 0
                result[TelegramMessage.impression_logs] = {
                    str(int(time.time())): {
                        TelegramMessage.views: views,
                        TelegramMessage.react: reactions,
                        TelegramMessage.replies: replies
                    }
                }
        return result

    def refactor_message_dict(self, item):
        result = {}
        for key, value in item.items():
            if isinstance(value, dict):
                value = self.refactor_message_dict(value)
            elif isinstance(value, list):
                tmp = []
                for v in value:
                    if isinstance(v, dict):
                        tmp.append(self.refactor_message_dict(v))
                    else:
                        tmp.append(v)
                value = tmp
            else:
                value = str(value)
            if key == "_":
                result["type"] = value
            else:
                temp = re.split('_+', key)
                new_key = temp[0] + ''.join(map(lambda x: x.title(), temp[1:]))
                result[new_key] = value
        return result

    # NEW USER
    async def update_new_users(self, project, _id, project_id):
        channel_full_info = await self.client(GetFullChannelRequest(channel=project_id))
        config = {
            "_id": f"{_id}_telegram_update",
            "participants": channel_full_info.full_chat.participants_count,
            "kicked": channel_full_info.full_chat.kicked_count,
            "memberLogs": {
                str(round_timestamp(time.time())): {
                    "participants": channel_full_info.full_chat.participants_count,
                    "kicked": channel_full_info.full_chat.kicked_count
                }
            }
        }
        self.exporter.update_docs(MongoCollection.configs, [config])
        await self.export_new_users(project, _id, project_id)

    async def export_new_users(self, project, _id, project_id):
        config = self.exporter.get_doc(MongoCollection.configs, filter_={"_id": f"{_id}_telegram_update"})
        last_update_timestamp = int(time.time())
        if config:
            last_update_timestamp = config.get("timestamp") - self.interval
        for message in self.exporter.get_docs(
                MongoCollection.telegram_messages,
                filter_={TelegramMessage.timestamp: {"$gte": last_update_timestamp}}):
            user_id = message.get(TelegramMessage.from_id, {}).get("userId")
            if user_id:
                user_info = self.exporter.get_doc(
                    MongoCollection.telegram_users, filter_={TelegramUser.id_: f"{project}_{user_id}"})
                if user_info:
                    await self.update_user_info(project, _id, project_id, user_id)

    # ALL USERS
    async def update_all_users(self, project, _id, project_id):
        tmp = 0
        async for entity in self.client.iter_participants(entity=project_id):
            user = self.convert_user_to_dict(entity, project, _id)
            self.exporter.update_docs(MongoCollection.telegram_users, [user])
            tmp += 1
        tmp += await self.export_all_users_send_message(project, _id, project_id)
        return tmp

    async def export_all_users_send_message(self, project, _id, project_id):
        tmp = 0
        for message in self.exporter.get_docs(
                MongoCollection.telegram_messages,
                filter_={}):
            user_id = message.get(TelegramMessage.from_id, {}).get("userId")
            if user_id:
                user_info = self.exporter.get_doc(
                    MongoCollection.telegram_users, filter_={TelegramUser.id_: f"{_id}_{user_id}"})
                if not user_info:
                    tmp += await self.update_user_info(project, _id, project_id, user_id)
        return tmp

    async def update_user_info(self, project, _id, project_id, user_id):
        try:
            full = await self.client(GetParticipantRequest(channel=project_id, participant=int(user_id)))
            if full.users:
                user_info = self.convert_user_to_dict(full.users[0], project, _id)
                self.exporter.update_docs(MongoCollection.telegram_users, [user_info])
            return 1
        except Exception as e:
            logger.warn(f"Get err {e}")
            return 0

    # MESSAGES
    # async def update_messages(self, project, project_id):
    #     config = self.exporter.get_doc(MongoCollection.configs, filter_={"_id": f"{project}_telegram_update"})
    #     tmp = 0
    #     last_update_timestamp = None
    #     if config:
    #         last_update_timestamp = config.get("timestamp") - self.interval
    #     async for entity in self.client.iter_messages(entity=project_id):
    #         message = self.convert_message_to_dict(entity, project)
    #         self.exporter.update_docs(MongoCollection.telegram_messages, [message])
    #         tmp += 1
    #         if last_update_timestamp and message.get(TelegramMessage.timestamp) < last_update_timestamp:
    #             break
    #     return tmp

    async def update_messages_periods(self, project, _id, project_id):
        config = self.exporter.get_doc(MongoCollection.configs, filter_={"_id": f"{_id}_telegram_update"})
        tmp = 0
        last_update_timestamp = None
        if config:
            last_update_timestamp = config.get("timestamp") - self.period
        async for entity in self.client.iter_messages(entity=project_id):
            message = self.convert_message_to_dict(entity, project, _id)
            self.exporter.update_docs(MongoCollection.telegram_messages, [message])
            tmp += 1
            if last_update_timestamp and message.get(TelegramMessage.timestamp) < last_update_timestamp:
                break
        return tmp

    async def execute(self):

        # cursor = list(self.exporter.mongodb_connection_url(connection_url=MongoDBConfig.CENTIC_DB_CONNECTION_URL,
        #                                                    database=MongoDBConfig.CENTIC_DB_DATABASE,
        #                                                    collection="projects"))
        cursor = list(self.mongodb_centic.get_docs(collection="projects"))
        list_projects = []
        for document in cursor:
            project = document["projectId"]
            list_projects.append(project)

        # Crawl a project
        # data = [
        #     {
        #         "platform": "telegram",
        #         "id": "zetachainofficial",
        #         "name": "ZetaChain Official",
        #         "url": "https://t.me/zetachainofficial",
        #         "telegramId": "1585021118",
        #         "avatar": "https://cdn1.cdn-telegram.org/file/kgYIVEMYLO1lJzT…2rFZ-Er6xUyiT_cTznru1ioz_yJdOCD4YcqgE947vdB9A.jpg",
        #         "type": "channel"
        #     },
        #     {
        #         "platform": "telegram",
        #         "id": "zetachain_asia",
        #         "name": "ZetaChain 中文社區",
        #         "url": "https://t.me/zetachain_asia",
        #         "telegramId": "1427820809",
        #         "avatar": "	https://cdn5.cdn-telegram.org/file/DtTJx-Ukm0G996B…FFmsVpm2s2JQmjGwTK6FSBj7ux09_hhBRGTeKoTlzzFSw.jpg",
        #         "type": "channel"
        #     },
        #     {
        #         "platform": "telegram",
        #         "id": "zeta_chain_cis",
        #         "name": "ZetaChain СНГ",
        #         "url": "https://t.me/zeta_chain_cis",
        #         "telegramId": "2111038560",
        #         "avatar": "https://cdn4.cdn-telegram.org/file/HQhAn50XM-qwzL_…7VuqhxxB4kYDZNSAqLQUzD_UBmh8hQ_2dwHMcOkLttPHw.jpg",
        #         "type": "channel"
        #     },
        #     {
        #         "platform": "telegram",
        #         "id": "zetachain_fr",
        #         "name": "ZetaChain_FR",
        #         "url": "https://t.me/zetachain_fr",
        #         "telegramId": "1699824294",
        #         "avatar": "https://cdn4.cdn-telegram.org/file/EYkt-wPmK9AaXyk…-gnykK77HzMUxM8MwyJhzxBzqQbTsfDSijU1Lzysalvkg.jpg",
        #         "type": "channel"
        #     },
        #     {
        #         "platform": "telegram",
        #         "id": "ZetaChainGerman",
        #         "name": "Zetachain German Community, Deutsch",
        #         "url": "https://t.me/ZetaChainGerman",
        #         "telegramId": "2129775899",
        #         "avatar": "https://cdn5.cdn-telegram.org/file/Hwlh1TtSi50HTgC…J9KgpoPxL1GUn3ieHDs52vvr4KpZVDjr68_xAOOJrSsQg.jpg",
        #         "type": "channel"
        #     },
        #     {
        #         "platform": "telegram",
        #         "id": "ZetaChainKR",
        #         "name": "ZetaChain Korean Community",
        #         "url": "https://t.me/ZetaChainKR",
        #         "telegramId": "1949985374",
        #         "avatar": "https://cdn5.cdn-telegram.org/file/sB8l86lV0FmRUkE…kDtxuBI5PO8wKrSlYGmRIg4KzNzQ3GWq3z4zetaryeA8A.jpg",
        #         "type": "channel"
        #     },
        #     {
        #         "platform": "telegram",
        #         "id": "ZetaChainViet",
        #         "name": "Zetachain Vietnamese Community, Tiếng Việt",
        #         "url": "https://t.me/ZetaChainViet",
        #         "telegramId": "1978725161",
        #         "avatar": "https://cdn5.cdn-telegram.org/file/g6eX-KgdYlBPLQQ…LUQ_lMFIrWV-JW8NNGNpn_688Kk8IJ6wC7jQamG2YKnKw.jpg",
        #         "type": "channel"
        #     },
        # ]
        # for x in data:
        #     project = "zetachain"
        #     _id = x["id"]
        #     project_id = int(x["telegramId"])
        #     logger.info(f"Start crawling {project} project info")
        #     self.exporter.update_docs(MongoCollection.configs, [{"_id": f"{_id}_telegram_update", "timestamp": round_timestamp(time.time())}])
        #     async for i in self.client.iter_dialogs():
        #         continue
        #     print(f"{project}, {_id}, {project_id}")
        #     if "messages" in self.stream_types:
        #         begin = time.time()
        #         logger.info(f"Get messages of {_id}")
        #         # tmp = await self.update_messages(project, project_id)
        #         tmp = await self.update_messages_periods(project, _id, project_id)
        #         logger.info(f"Crawled {tmp} messages!")
        #         logger.info(f"Execute in {time.time() - begin}s")
        #
        #     if "new_users" in self.stream_types:
        #         begin = time.time()
        #         logger.info(f"Get new members info of {_id}")
        #         await self.update_new_users(project, _id, project_id)
        #         logger.info(f"Execute in {time.time() - begin}s !")

        if "check_announcement" in self.stream_types:
            try:
                client = MongoClient(MongoDBConfig.CDP_CONNECTION_URL)
                db = client[MongoDBConfig.CDP_DATABASE]
                collection = db['telegram_messages']
                list_channels = []
                for document in cursor:
                    for item in document['settings']['socialMedia']:
                        if item.get('platform') == 'telegram' and item.get('type') == 'channel':
                            link = item.get("url").lower()
                            list_channels.append(link.split("https://t.me/")[-1])

                for channel in list_channels:
                    logger.info(f"Checking {channel} announcement")
                    full_channel = await self.client(functions.channels.GetFullChannelRequest(channel=channel))
                    for chat in full_channel.chats:
                        if chat.broadcast or chat.gigagroup:
                            collection.update_many(
                                {"channel": chat.username},
                                {"$set": {"announcement": True}},
                            )
                        else:
                            collection.update_many(
                                {"channel": chat.username},
                                {"$set": {"announcement": False}},
                            )
            except Exception as e:
                logger.warn(f"Get err {e}")
                match = re.search(r"A wait of (\d+) seconds is required", str(e))
                if match:
                    wait_time = int(match.group(1))
                    logger.info(f"Waiting for {wait_time} seconds...")
                else:
                    wait_time = 3  # Default wait time if no specific duration is found
                    logger.info("Continuing in 3 seconds...")

                await asyncio.sleep(wait_time)

        else:
            for project in list_projects:
                try:
                    for document in cursor:
                        if document["projectId"] == project:
                            if 'settings' in document and 'socialMedia' in document['settings']:
                                for item in document['settings']['socialMedia']:
                                    if item.get('platform') == 'telegram' and item.get('type') == 'channel':
                                        async for i in self.client.iter_dialogs():
                                            continue
                                        _id = item.get("id")
                                        if "telegramId" in item:
                                            logger.info(f"Start crawling {project} project info")
                                            self.exporter.update_docs(
                                                MongoCollection.configs,
                                                [{"_id": f"{_id}_telegram_update",
                                                  "timestamp": round_timestamp(time.time())}])
                                        else:
                                            continue
                                        project_id = int(item.get("telegramId"))

                                        # self.exporter.update_docs(
                                        #     MongoCollection.configs,
                                        #     [{"_id": f"{_id}_telegram_update", "timestamp": round_timestamp(time.time())}])
                                        await self.export_new_users(project, _id, project_id)
                                        print(f"{project}, {_id}, {project_id}")
                                        if "messages" in self.stream_types:
                                            begin = time.time()
                                            logger.info(f"Get messages of {_id}")
                                            # tmp = await self.update_messages(project, project_id)
                                            tmp = await self.update_messages_periods(project, _id, project_id)
                                            logger.info(f"Crawled {tmp} messages!")
                                            logger.info(f"Execute in {time.time() - begin}s")

                                        if "new_users" in self.stream_types:
                                            begin = time.time()
                                            logger.info(f"Get new members info of {_id}")
                                            await self.update_new_users(project, _id, project_id)
                                            logger.info(f"Execute in {time.time() - begin}s !")

                                        if "users" in self.stream_types:
                                            begin = time.time()
                                            logger.info(f"Get all members info of {_id}")
                                            tmp = await self.update_all_users(project, _id, project_id)
                                            logger.info(f"Crawled {tmp} users!")
                                            logger.info(f"Execute in {time.time() - begin}s !")
                except Exception as e:
                    logger.warn(f"Get err {e}")
                    logger.info("Continuing in 3 seconds...")
                    await asyncio.sleep(3)

    def _execute(self, *args, **kwargs):
        begin = time.time()
        logger.info("Start execute telegram crawler")
        with self.client:
            self.client.loop.run_until_complete(self.execute())
        # for project in self.projects:
        #     if project not in Projects.mapping:
        #         continue
        #     self.exporter.update_docs(
        #         MongoCollection.configs,
        #         [{"_id": f"{project}_telegram_update", "timestamp": round_timestamp(time.time())}])
        logger.info(f"Execute all streams in {time.time() - begin}s")
        if self.monitor:
            key = ""
            for project in self.projects:
                key += f"{project}_"
            write_last_time_running_logs(
                f"{key}_cdp_explorer", round_timestamp(time.time()), threshold=self.interval + 3600 * 6)
