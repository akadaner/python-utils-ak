""" Git functionality. """
import glob
import os
import sys
from collections import defaultdict

import git
import traceback

from utils_ak.time.dt import cast_datetime
from utils_ak.re import search_one


def is_git_repo(path):
    try:
        _ = git.Repo(path).git_dir
        return True
    except git.exc.InvalidGitRepositoryError:
        return False


def add_git_repos_to_path(path, recursive=False):
    if not recursive:
        paths = glob.glob(os.path.join(path, "*/"))
    else:
        paths = glob.glob(os.path.join(path, "**/*/"), recursive=True)

    paths = [path for path in paths if is_git_repo(path)]

    for path in paths:
        sys.path.append(path)


def git_pull(path, full_output=False):
    if not is_git_repo(path):
        return {"status": "fail", "e": "Not a git repo"}

    res = defaultdict(str)
    try:
        g = git.cmd.Git(path)
        # if stash:
        #     stash_output = g.stash()
        #     res['output'] += stash_output + '\n'

        output = g.pull()
        res["output"] += output + "\n"

        # if stash:
        #     try:
        #         unstash_output = g.stash('apply')
        #         res['output'] += unstash_output + '\n'
        #     except:
        #         pass

        if "Already up-to-date" in output:
            res["status"] = "Already up-to-date"
        else:
            res["status"] = "success"

    except Exception as e:
        res["status"] = "fail"
        res["e"] = traceback.format_exc()
    res = dict(res)
    if not full_output:
        res = res["status"]
    return res


def git_pull_many(path, repos=None, recursive=False):
    if not recursive:
        paths = glob.glob(os.path.join(path, "*/"))
    else:
        paths = glob.glob(os.path.join(path, "**/*/"), recursive=True)
    paths = [path for path in paths if is_git_repo(path)]
    if repos:
        paths = [
            path for path in paths if os.path.basename(os.path.dirname(path)) in repos
        ]
    return {path: git_pull(path) for path in paths}


def get_commit_info(path):
    repo = git.Repo(path)
    commit_info = repo.git.show()

    commit_dic = {
        "hash": search_one(r"commit (.+)\n", commit_info),
        "date": search_one(r"Date:\s+(.+)\n", commit_info),
        "author": search_one(r"Author:\s+(.+)\n", commit_info),
        "full_info": commit_info,
    }
    try:
        commit_dic["date"] = cast_datetime(commit_dic["date"])
    except:
        pass
    return commit_dic


if __name__ == "__main__":
    # res = git_pull_many(r'C:\Users\xiaomi\YandexDisk\IT\Python\pycharm')
    # res = git_pull_many('/home/jentos/Projects/quantribution')
    # pprint(res)
    print(get_commit_info(r"C:\Users\xiaomi\YandexDisk\IT\Python\pycharm\prod"))
