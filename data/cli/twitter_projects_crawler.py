import click

from constants.config import AccountConfig
from constants.time_constant import TimeConstants
from databases.mongodb_cdp import MongoDBCDP
from databases.mongodb_centic import MongoDBCentic
from src.jobs.twitter_projects_crawling_job import TwitterProjectCrawlingJob
from utils.logger_utils import get_logger

logger = get_logger('Twitter Projects Crawler')


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('-i', '--interval', default=TimeConstants.A_DAY, type=int, help='Sleep time')
@click.option('-pe', '--period', default=TimeConstants.DAYS_2, type=int, help='Period time')
@click.option('-li', '--limit', default=None, type=int, help='Sleep time')
@click.option('-o', '--output-url', default=None, type=str, help='mongo output url')
@click.option('-p', '--projects', default=None, type=str, help='project name', multiple=True)
@click.option('-pf', '--projects-file', default=None, type=str, help='projects file')
@click.option('-u', '--twitter-user', default=AccountConfig.USERNAME, show_default=True,
              type=str, help='Twitter user')
@click.option('-pw', '--twitter-password', default=AccountConfig.PASSWORD, show_default=True,
              type=str, help='Telegram API hash')
@click.option('-e', '--email', default=AccountConfig.EMAIL, show_default=True,
              type=str, help='Twitter email')
@click.option('-ep', '--email-password', default=AccountConfig.EMAIL_PASSWORD, show_default=True,
              type=str, help='email password')
@click.option('-ct', '--crawler-types', default=["projects", "kols"], show_default=True,
              type=str, multiple=True, help='Crawler types: projects, kols')
@click.option('-st', '--stream-types', default=["profiles", "tweets"], show_default=True,
              type=str, multiple=True, help='Stream types: profiles, tweets')
@click.option('-co', '--col-output', default=None,
              type=str, help='Collection output')
@click.option('-m', '--monitor', default=False, show_default=True,
              type=bool, help='Monitor or not')
def twitter_projects_crawler(interval, period, limit, output_url, projects, projects_file, twitter_user, twitter_password, email, email_password, crawler_types, stream_types, col_output, monitor):
    _exporter = MongoDBCDP(connection_url=output_url, database="cdp_database")
    mongodb_centic = MongoDBCentic()
    job = TwitterProjectCrawlingJob(
        interval=interval,
        period=period,
        limit=limit,
        projects=projects,
        projects_file=projects_file,
        exporter=_exporter,
        mongodb_centic=mongodb_centic,
        user_name=twitter_user,
        password=twitter_password,
        email=email,
        email_password=email_password,
        monitor=monitor,
        stream_types=stream_types,
        crawler_types=crawler_types,
        col_output=col_output
    )
    job.run()
