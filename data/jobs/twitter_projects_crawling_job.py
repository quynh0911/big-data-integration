import asyncio
import json
import time
from typing import AsyncGenerator, TypeVar

import pycountry
from twscrape import Tweet, User, gather

from constants.config import AccountConfig
from constants.mongo_constant import MongoCollection
from constants.time_constant import TimeConstants
from constants.twitter import Follow, Tweets, TwitterUser
from databases.mongodb_cdp import MongoDBCDP
from databases.mongodb_centic import MongoDBCentic
from src.crawler.new_api import NewAPi
from src.jobs.cli_job import CLIJob
from utils.file_utils import write_last_time_running_logs
from utils.logger_utils import get_logger
from utils.time_utils import round_timestamp

T = TypeVar("T")
logger = get_logger("Twitter Project Crawling Job")


class TwitterProjectCrawlingJob(CLIJob):
    def __init__(
        self,
        interval: int,
        period: int,
        limit: int,
        projects: list,
        projects_file: str,
        col_output: str,
        exporter: MongoDBCDP,
        mongodb_centic: MongoDBCentic,
        user_name: str = AccountConfig.USERNAME,
        password: str = AccountConfig.PASSWORD,
        email: str = AccountConfig.EMAIL,
        email_password: str = AccountConfig.EMAIL_PASSWORD,
        monitor: bool = False,
        crawler_types=None,
        stream_types=None,
    ):
        super().__init__(interval, period, limit, retry=False)
        if crawler_types is None:
            crawler_types = ["projects", "kols"]
        if stream_types is None:
            stream_types = ["profiles", "tweets"]
        self.period = period
        self.limit = limit
        self.stream_types = stream_types
        self.crawler_types = crawler_types
        self.monitor = monitor
        self.email_password = email_password
        self.email = email
        self.password = password
        self.user_name = user_name
        self.api = None
        self.exporter = exporter
        self.mongodb_centic = mongodb_centic
        self.projects = projects
        self.col_output = col_output
        self.projects_file = (
            self.load_projects_from_file(projects_file)
            if projects_file is not None
            else ()
        )

    @staticmethod
    def load_projects_from_file(projects_file: str) -> list:
        with open(projects_file, "r") as file:
            projects_data = json.load(file)
        return projects_data

    @staticmethod
    def convert_user_to_dict(user: User) -> dict:
        text = user.location
        country_name = ""
        for country in pycountry.countries:
            if country.name.lower() in text.lower():
                country_name = country.name
                break

        return {
            TwitterUser.id_: str(user.id),
            TwitterUser.user_name: user.username,
            TwitterUser.user_name_lower: user.username.lower(),
            TwitterUser.display_name: user.displayname,
            TwitterUser.url: user.url,
            TwitterUser.blue: user.blue,
            TwitterUser.blue_type: user.blueType,
            TwitterUser.created_at: str(user.created),
            TwitterUser.timestamp: int(user.created.timestamp()),
            TwitterUser.description_links: [str(i.url) for i in user.descriptionLinks],
            TwitterUser.favourites_count: user.favouritesCount,
            TwitterUser.friends_count: user.friendsCount,
            TwitterUser.listed_count: user.listedCount,
            TwitterUser.media_count: user.mediaCount,
            TwitterUser.followers_count: user.followersCount,
            TwitterUser.statuses_count: user.statusesCount,
            TwitterUser.raw_description: user.rawDescription,
            TwitterUser.verified: user.verified,
            TwitterUser.profile_image_url: user.profileImageUrl,
            TwitterUser.profile_banner_url: user.profileBannerUrl,
            TwitterUser.protected: user.protected,
            TwitterUser.location: user.location,
            TwitterUser.country: country_name,
            TwitterUser.count_logs: {
                round_timestamp(time.time()): {
                    TwitterUser.favourites_count: user.favouritesCount,
                    TwitterUser.friends_count: user.friendsCount,
                    TwitterUser.listed_count: user.listedCount,
                    TwitterUser.media_count: user.mediaCount,
                    TwitterUser.followers_count: user.followersCount,
                    TwitterUser.statuses_count: user.statusesCount,
                }
            },
        }

    def convert_media_to_dict(self, media):
        photos = media.photos
        videos = media.videos
        lst_photo = []
        lst_video = []

        for photo in photos:
            lst_photo.append(photo.url)

        for video in videos:
            for link in video.variants:
                lst_video.append(link.url)

        return {"photos": lst_photo, "videos": lst_video}

    def convert_tweets_to_dict(self, tweet: Tweet) -> dict:
        if not tweet:
            return {}
        result = {
            Tweets.id_: str(tweet.id),
            Tweets.author: str(tweet.user.id),
            Tweets.author_name: tweet.user.username,
            Tweets.author_name_lower: tweet.user.username.lower(),
            Tweets.created_at: str(tweet.date),
            Tweets.timestamp: tweet.date.timestamp(),
            Tweets.url: str(tweet.url),
            Tweets.user_mentions: {
                str(user.id): user.username.lower() for user in tweet.mentionedUsers
            },
            Tweets.views: tweet.viewCount,
            Tweets.likes: tweet.likeCount,
            Tweets.hash_tags: tweet.hashtags,
            Tweets.reply_counts: tweet.replyCount,
            Tweets.retweet_counts: tweet.retweetCount,
            Tweets.retweeted_tweet: self.convert_tweets_to_dict(tweet.retweetedTweet),
            Tweets.text: tweet.rawContent,
            Tweets.quoted_tweet: self.convert_tweets_to_dict(tweet.quotedTweet),
            Tweets.links: [str(i.url) for i in tweet.links],
            Tweets.media: self.convert_media_to_dict(tweet.media),
        }

        if time.time() - result.get(Tweets.timestamp) < self.period:
            result[Tweets.impression_logs] = {
                str(int(time.time())): {
                    Tweets.views: tweet.viewCount,
                    Tweets.likes: tweet.likeCount,
                    Tweets.reply_counts: tweet.replyCount,
                    Tweets.retweet_counts: tweet.retweetCount,
                }
            }

        return result

    @staticmethod
    def get_relationship(project, user):
        return {
            Follow.id_: f"{user}_{project}",
            Follow.from_: str(user),
            Follow.to: str(project),
        }

    async def gather(
        self,
        gen: AsyncGenerator[T, None],
        project,
        time_sleep: int = 1,
        n_items: int = 1000,
    ) -> int:
        tmp = 0
        async for x in gen:
            self.exporter.update_docs(
                MongoCollection.twitter_users, [self.convert_user_to_dict(x)]
            )
            self.exporter.update_docs(
                MongoCollection.twitter_follows, [self.get_relationship(project, x.id)]
            )
            tmp += 1
            if tmp and not (tmp % n_items):
                time.sleep(time_sleep)
        return tmp

    async def execute(self):
        api = NewAPi()
        await api.pool.add_account(
            self.user_name, self.password, self.email, self.email_password
        )
        await api.pool.login_all()

        list_projects = self.mongodb_centic.get_docs(collection="projects")
        list_kols = self.exporter.get_docs(collection="twitter_kols_elite")
        list_account = []

        if self.projects == () and self.projects_file == ():
            if "projects" in self.crawler_types:
                for document in list_projects:
                    if "settings" in document and "socialMedia" in document["settings"]:
                        for item in document["settings"]["socialMedia"]:
                            if item.get("platform") == "twitter":
                                list_account.append(item.get("id").lower())
            if "kols" in self.crawler_types:
                for document in list_kols:
                    if "userName" in document:
                        username = document.get("userName", "")
                        list_account.append(username.lower())
        elif self.projects != () and self.projects_file == ():
            for project in self.projects:
                list_account.append(project.lower())
        elif self.projects == () and self.projects_file != ():
            for project in self.projects_file:
                list_account.append(project.lower())
        else:
            logger.warn(
                "Start crawling from Centic DB, list projects and projects file"
            )
            account_db = []
            account_projects = []
            account_file = []
            cursor = list(self.mongodb_centic.get_docs(collection="projects"))
            for document in cursor:
                if "settings" in document and "socialMedia" in document["settings"]:
                    for item in document["settings"]["socialMedia"]:
                        if item.get("platform") == "twitter":
                            account_db.append(item.get("id").lower())
            for project in self.projects:
                account_projects.append(project.lower())
            for project in self.projects_file:
                account_file.append(project.lower())
            merged_set = set(account_db) | set(account_projects) | set(account_file)
            list_account = list(merged_set)

        tmp = 0
        for account in list_account:
            tmp += 1
            try:
                if "profiles" in self.stream_types:
                    logger.info(f"Crawling {account} info")
                    project_info = await api.user_by_login(account)
                    if self.col_output:
                        self.exporter.update_docs(
                            self.col_output, [self.convert_user_to_dict(project_info)]
                        )
                    else:
                        self.exporter.update_docs(
                            MongoCollection.twitter_users,
                            [self.convert_user_to_dict(project_info)],
                        )
                    logger.info(f"Crawled {tmp}/{len(list_account)} projects")

                if "tweets" in self.stream_types:
                    logger.info(f"Crawling {account} tweets info")
                    project_info = await api.user_by_login(account)
                    if project_info is None:
                        continue
                    if self.limit is None:
                        # Get all tweet, limit = -1
                        tweets = await gather(api.user_tweets(project_info.id))
                    else:
                        tweets = await gather(
                            api.user_tweets(project_info.id, limit=self.limit)
                        )
                    count = 0
                    _period = (
                        round_timestamp(time.time()) - self.period + TimeConstants.A_DAY
                    )
                    for tweet in tweets:
                        if self.convert_tweets_to_dict(tweet)["timestamp"] > _period:
                            count += 1
                            if self.col_output:
                                self.exporter.update_docs(
                                    self.col_output,
                                    [self.convert_tweets_to_dict(tweet)],
                                )
                            else:
                                self.exporter.update_docs(
                                    MongoCollection.tweets,
                                    [self.convert_tweets_to_dict(tweet)],
                                )

                    logger.info(f"Crawled {count} tweets of {account}")
                    logger.info(f"Crawled {tmp}/{len(list_account)} projects")

            except Exception as e:
                logger.warn(f"Get err {e}")
                logger.info("Continuing in 3 seconds...")
                await asyncio.sleep(3)

    async def execute_v2(self):
        api = NewAPi()
        await api.pool.add_account(
            self.user_name, self.password, self.email, self.email_password
        )
        await api.pool.login_all()

        list_projects = self.exporter.get_docs(collection="projects_social_media")
        list_accounts = []

        for document in list_projects:
            if "twitter" in document:
                list_accounts.append(
                    {
                        "id": document.get("twitter").get("id"),
                        "projectId": document.get("projectId"),
                    }
                )

        logger.info(f"Start crawling followings of {len(list_accounts)} accounts")
        tmp = 0
        for account in list_accounts:
            acc = account.get("id")
            tmp += 1
            try:
                if "followings" in self.stream_types:
                    logger.info(f"Crawling accounts followed by {acc}")
                    account_info = await api.user_by_login(acc)

                    data = {
                        "_id": account_info.id,
                        "userName": account_info.username,
                        "lastUpdate": round_timestamp(time.time()),
                        "project": account.get("projectId"),
                        "followings": [],
                    }

                    followings = await gather(api.following(account_info.id))
                    count = 0
                    for following in followings:
                        count += 1
                        self.exporter.update_docs(
                            MongoCollection.twitter_users,
                            [self.convert_user_to_dict(following)],
                        )

                        name_of_following = following.username
                        data["followings"].append(name_of_following)

                    self.exporter.update_docs("twitter_followings_v2", [data])

                    logger.info(f"Crawled {count} followings of {acc}")

            except Exception as e:
                logger.warn(f"Get err {e}")
                logger.info("Continuing in 3 seconds...")
                await asyncio.sleep(3)

    def _execute(self, *args, **kwargs):
        begin = time.time()
        logger.info("Start execute twitter crawler")
        if "followings" in self.stream_types:
            asyncio.run(self.execute_v2())
        else:
            asyncio.run(self.execute())

        logger.info(f"Execute all streams in {time.time() - begin}s")
        if self.monitor:
            key = ""
            for project in self.projects:
                key += f"{project}"
            write_last_time_running_logs(
                f"last_{key}_twitter_crawler",
                round_timestamp(time.time()),
                threshold=self.interval + 3600 * 12,
            )
