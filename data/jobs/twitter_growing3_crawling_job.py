import asyncio
import os
import sys
import time

import pycountry

sys.path.append(os.getcwd())

from typing import AsyncGenerator, TypeVar

from dotenv import load_dotenv
from twscrape import Tweet, User, gather

from constants.time_constant import TimeConstants
from constants.twitter import Follow, Tweets, TwitterUser
from databases.mongodb_cdp import MongoDBCDP
from cli_scheduler.scheduler_job import SchedulerJob
from utils.logger_utils import get_logger
from utils.time_utils import round_timestamp
from utils.twitter_utils.add_account import dynamic_account_module

T = TypeVar("T")
logger = get_logger(__name__)
load_dotenv()

class TwitterGrowing3CrawlingJob(SchedulerJob):
    def __init__(
        self,
        scheduler: str,
        interval: int,
        period: int,
        limit: int,
        exporter: MongoDBCDP,
        monitor: bool = False,
        stream_types=None,
        api_v=None,
        batch_size=None,
        num_accounts=None,
    ):
        super().__init__(scheduler=scheduler, interval=interval, retry=False)
        if stream_types is None:
            stream_types = ["profiles", "tweets"]
        self.scheduler = scheduler
        self.period = period
        self.limit = limit
        self.stream_types = stream_types
        self.monitor = monitor
        self.api = None
        self.exporter = exporter
        self.api_v = api_v
        self.num_accounts = num_accounts
        self.batch_size = batch_size

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

        return {
            "photos": lst_photo,
            "videos": lst_video
        }

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
            self.exporter.update_docs("twitter_raw", [self.convert_user_to_dict(x)])
            tmp += 1
            if tmp and not (tmp % n_items):
                time.sleep(time_sleep)
        return tmp

    async def crawl(
        self,
        api,
        accounts,
        api_name,
        stream_types,
        exporter,
        limit,
        period,
    ):
        tmp = 0
        len_all_accounts = len(accounts)
       
        for account in accounts:
            account = account.get("userName")
            try:
                if "profiles" in stream_types:
                    logger.info(f"Crawling {account} info with {api_name}")
                    project_info = await api.user_by_login(account)
                    if project_info:
                        profile_data = self.convert_user_to_dict(project_info)
                
                        exporter.update_docs("twitter_raw", [profile_data])
                        tmp += 1
                        logger.info(
                            f"Crawled {tmp}/{len_all_accounts} accounts with {api_name}"
                        )

                if "tweets" in stream_types:
                    logger.info(f"Crawling {account} tweets info with {api_name}")
                    project_info = await api.user_by_login(account)
                    if project_info is None:
                        continue
                    if limit is None:
                        tweets = await gather(api.user_tweets(project_info.id))
                    else:
                        tweets = await gather(
                            api.user_tweets(project_info.id, limit=limit)
                        )
                    count = 0
                    _period = (
                        round_timestamp(time.time()) - period + TimeConstants.A_DAY
                    )
                    for tweet in tweets:
                        tweet_data = self.convert_tweets_to_dict(tweet)
                        if tweet_data["timestamp"] > _period:
                            count += 1
                            exporter.update_docs("tweets", [tweet_data])

                    logger.info(
                        f"Crawled {count} tweets of {account} with {api_name}"
                    )

            except Exception as e:
                logger.warn(f"Get error {e} on {api_name}")
                logger.info("Continuing in 3 seconds...")
                await asyncio.sleep(3)

            if tmp == len_all_accounts:
                logger.info(
                    "########## Finished ##########"
                )

    async def _get_add_account_function(self):
        return getattr(
            dynamic_account_module, 
            f"add_account_v{self.api_v}", 
            None
        )
    
    async def _get_elite_usernames(self):
        cursor = self.exporter.get_docs(
            "twitter_raw",
            filter_={"elite": True},
            projection={"userName": 1},
        ).limit(self.batch_size)
        return list(cursor)
    
    async def _get_no_elite_usernames(self):
        cursor = self.exporter.get_docs(
            "twitter_raw",
            filter_={"elite": {"$ne": True}},
            projection={"userName": 1},
        ).limit(self.batch_size)
        return list(cursor)
    
    def _distribute_accounts(self, usernames):
        num_versions = self.num_accounts
        distributed_accounts = {}

        for i, username in enumerate(usernames):
            version_index = (i+1) % num_versions
            key = f"list_account_{version_index}"
            
            if key not in distributed_accounts:
                distributed_accounts[key] = [username]
            else:
                distributed_accounts[key].append(username)

        return distributed_accounts
    
    def _distribute_accounts_v2(self, usernames):
        num_versions = self.num_accounts
        distributed_accounts = {}

        for i, username in enumerate(usernames):
            version_index = (i+1) % num_versions + 10
            key = f"list_account_{version_index}"
            
            if key not in distributed_accounts:
                distributed_accounts[key] = [username]
            else:
                distributed_accounts[key].append(username)

        return distributed_accounts

    async def execute_v1(self):
        add_account_func = await self._get_add_account_function()
        if not add_account_func:
            logger.error("Invalid API version")
            return
        
        api = await add_account_func()
        usernames = await self._get_elite_usernames()
        logger.info(f"Found {len(usernames)} elite usernames")
        distributed_accounts = self._distribute_accounts(usernames)

        account_key = f"list_account_{self.api_v}"
        if account_key in distributed_accounts:
            await self.crawl(
                api,
                distributed_accounts[account_key],
                f"api_v{self.api_v}",
                self.stream_types,
                self.exporter,
                self.limit,
                self.period
            )
        else:
            logger.info(f"No accounts for version {self.api_v}")
        
    async def execute_v2(self):
        add_account_func = await self._get_add_account_function()
        if not add_account_func:
            logger.error("Invalid API version")
            return
        
        api = await add_account_func()
        usernames = await self._get_no_elite_usernames()
        logger.info(f"Found {len(usernames)} non-elite usernames")
        distributed_accounts = self._distribute_accounts_v2(usernames)
        print(distributed_accounts.keys())
        account_key = f"list_account_{self.api_v}"
        if account_key in distributed_accounts:
            await self.crawl(
                api,
                distributed_accounts[account_key],
                f"api_v{self.api_v}",
                self.stream_types,
                self.exporter,
                self.limit,
                self.period
            )
        
        # elif self.api_v == 11:
        #     all_accounts1 = []
        #     all_accounts2 = []
        #     cursor1 = self.exporter.get_docs("twitter_growing3", projection={"username": 1})
        #     for doc in cursor1:
        #         all_accounts1.append(doc["username"])

        #     cursor2 = self.exporter.get_docs("twitter_raw", filter_={"flagged": {"$exists": True}}, projection={"userName": 1})
        #     for doc in cursor2:
        #         all_accounts2.append(doc["userName"])

        #     all_accounts = list(set(all_accounts1) - set(all_accounts2))

        #     await self.crawl(
        #         api=api,
        #         accounts=all_accounts,
        #         api_name= "api_v11",
        #         stream_types=['profiles'],
        #         exporter=self.exporter,
        #         limit=self.limit,
        #         period=self.period,
        #     )
        else:
            logger.info(f"No accounts for version {self.api_v}")

    def _execute(self, *args, **kwargs):
        begin = time.time()
        logger.info("Start execute twitter crawler")
        if self.scheduler == "^true@daily" or self.scheduler == "^false@daily":
            asyncio.run(self.execute_v1())
        else:
            asyncio.run(self.execute_v2())
        logger.info(f"Execute all streams in {time.time() - begin}s")


if __name__ == "__main__":
    job = TwitterGrowing3CrawlingJob(
        scheduler="^true@daily",
        interval=3600,
        period=TimeConstants.DAYS_7 * 2,
        limit=20,
        exporter=MongoDBCDP(),
        monitor=True,
        stream_types=["profiles", "tweets"],
        batch_size=100,
        api_v=0,
        num_accounts=10,
    )
    job.run()

