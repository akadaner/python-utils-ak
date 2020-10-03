#!/usr/bin/env python3
import logging
import git
import os

from utils_ak.logging import configure_stream_logging

configure_stream_logging()

git_root_path = os.path.abspath(os.path.dirname(__file__) + '/../../..')
gits = ['utils_ak', 'barsim', 'exchanges', 'prod']

logging.info('Cloning repos...')
# 1) clone all gits
for git_repo in gits:
    git_path = os.path.join(git_root_path, git_repo)
    if not os.path.exists(git_path):
        logging.info(f'Cloning: {git_repo}')
        git.Repo.clone_from('git@bitbucket.org:qtb/{}.git'.format(git_repo), git_path)
logging.info('Done.')

logging.info('Pulling repos...')
# 2) pull all gits
from utils_ak.git_tools import git_pull_many
from pprint import pprint

pprint(git_pull_many(git_root_path, repos=gits))
logging.info('Done.')
