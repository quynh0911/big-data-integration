import click

from src.cli.telegram_projects_crawler import telegram_projects_crawler
from src.cli.twitter_projects_crawler import twitter_projects_crawler
from src.cli.update_twitter_projects_followings import update_twitter_projects_followings
from src.cli.twitter_projects_crawler_v2 import twitter_projects_crawler_v2
from src.cli.discord_project_crawler import discord_projects_crawler
from src.cli.update_discord_project_crawler import update_discord
from src.cli.twitter_growing3_crawler import twitter_growing3_crawler
from src.cli.topic_growing3_crawler import topic_growing3_crawler
from src.cli.get_projects_social_media import get_projects_social_media


@click.group()
@click.version_option(version='1.0.0')
@click.pass_context
def cli(ctx):
    # Command line
    pass


# Stream
cli.add_command(telegram_projects_crawler, "telegram_projects_crawler")
cli.add_command(twitter_projects_crawler, "twitter_projects_crawler")
cli.add_command(update_twitter_projects_followings, "update_twitter_projects_followings")
cli.add_command(twitter_projects_crawler_v2, "twitter_projects_crawler_v2")
cli.add_command(discord_projects_crawler, "discord_projects_crawler")
cli.add_command(update_discord, "update_discord")
cli.add_command(twitter_growing3_crawler, "twitter_growing3_crawler")
cli.add_command(topic_growing3_crawler, "topic_growing3_crawler")
cli.add_command(get_projects_social_media, "get_projects_social_media")