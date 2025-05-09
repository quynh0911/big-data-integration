import click

from constants.time_constant import TimeConstants
from databases.mongodb_cdp import MongoDBCDP
from src.jobs.twitter_growing3_crawling_job import TwitterGrowing3CrawlingJob
from utils.logger_utils import get_logger

logger = get_logger('Twitter Growing3 Crawler')


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option("-s", "--scheduler", default="^true@daily", type=str, help="Scheduler")
@click.option('-i', '--interval', default=TimeConstants.A_DAY, type=int, help='Sleep time')
@click.option('-pe', '--period', default=TimeConstants.DAYS_7, type=int, help='Period time')
@click.option('-li', '--limit', default=20, type=int, help='Sleep time')
@click.option('-o', '--output-url', default=None, type=str, help='mongo output url')
@click.option('-st', '--stream-types', default=["profiles", "tweets"], show_default=True,
              type=str, multiple=True, help='Stream types: profiles, tweets')
@click.option('-m', '--monitor', default=False, show_default=True,
              type=bool, help='Monitor or not')
@click.option('-b', '--batch-size', default=False, show_default=True,
              type=int, help='Batch size')
@click.option('-api-v', '--api-v', default=0, show_default=True,
                type=int, help='API version')
@click.option('-n', '--num-accounts', default=10, show_default=True,
                type=int, help='Number of accounts')

def twitter_growing3_crawler(scheduler, interval, period, limit, output_url, stream_types, monitor, batch_size, api_v, num_accounts):
    _exporter = MongoDBCDP(connection_url=output_url, database="cdp_database")
    job = TwitterGrowing3CrawlingJob(
        scheduler=scheduler,
        interval=interval,
        period=period,
        limit=limit,
        exporter=_exporter,
        monitor=monitor,
        stream_types=stream_types,
        batch_size=batch_size,
        api_v=api_v,
        num_accounts=num_accounts
    )
    job.run()
