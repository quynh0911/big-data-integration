import click

from constants.config import AccountConfig
from constants.time_constant import TimeConstants
from databases.mongodb_cdp import MongoDBCDP
from databases.mongodb_centic import MongoDBCentic
from src.jobs.telegram_projects_crawling_job import TelegramProjectCrawlingJob
from utils.logger_utils import get_logger

logger = get_logger('Telegram Projects Crawler')


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('-i', '--interval', default=TimeConstants.A_DAY, type=int, help='Sleep time')
@click.option('-pe', '--period', default=TimeConstants.DAYS_2, type=int, help='Sleep time')
@click.option('-o', '--output-url', default=None, type=str, help='mongo output url')
@click.option('-p', '--projects', default=["trava_finance", "trava_finance_official"], type=str, help='project name', multiple=True)
@click.option('-id', '--api-id', default=AccountConfig.TELE_API_ID, show_default=True,
              type=int, help='Telegram API id')
@click.option('-h', '--api-hash', default=AccountConfig.TELE_API_HASH, show_default=True,
              type=str, help='Telegram API hash')
@click.option('-s', '--session-id', default="telegram", show_default=True,
              type=str, help='Telegram Session Id')
@click.option('-st', '--stream-types', default=["messages", "new_users"], show_default=True,
              type=str, help='Telegram Session Id', multiple=True)
@click.option('-m', '--monitor', default=False, show_default=True,
              type=bool, help='Monitor or not')
def telegram_projects_crawler(interval, period, output_url, projects, api_id, api_hash, session_id, stream_types, monitor):
    _exporter = MongoDBCDP(connection_url=output_url, database="cdp_database")
    mongodb_centic = MongoDBCentic()
    job = TelegramProjectCrawlingJob(
        interval=interval,
        period=period,
        projects=projects,
        exporter=_exporter,
        mongodb_centic=mongodb_centic,
        api_id=api_id,
        api_hash=api_hash,
        session_id=session_id,
        monitor=monitor,
        stream_types=stream_types,
    )
    job.run()
