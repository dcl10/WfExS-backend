import pytest
import logging
from wfexs_backend.common import RemoteRepo, RepoType
from wfexs_backend.fetchers.git import guess_git_repo_params


@pytest.mark.parametrize(
    ["url", "expected"],
    [
        (
            "https://github.com/inab/WfExS-backend.git",
            RemoteRepo(
                repo_url="https://github.com/inab/WfExS-backend.git",
                repo_type=RepoType.Git,
            ),
        ),
        (
            "git+https://github.com/inab/WfExS-backend.git",
            RemoteRepo(
                repo_url="https://github.com/inab/WfExS-backend.git",
                repo_type=RepoType.Git,
            ),
        ),
        (
            "https://github.com/inab/WfExS-backend.git@0.1.2",
            RemoteRepo(
                repo_url="https://github.com/inab/WfExS-backend.git",
                repo_type=RepoType.Git,
                tag="0.1.2",
            ),
        ),
        (
            "ssh://git@github.com:inab/WfExS-backend.git",
            RemoteRepo(
                repo_url="git@github.com:inab/WfExS-backend.git",
                repo_type=RepoType.Git,
            ),
        ),
        (
            "git+ssh://git@github.com:inab/WfExS-backend.git",
            RemoteRepo(
                repo_url="git@github.com:inab/WfExS-backend.git",
                repo_type=RepoType.Git,
            ),
        ),
        (
            "ssh://git@github.com:inab/WfExS-backend.git@0.1.2",
            RemoteRepo(
                repo_url="git@github.com:inab/WfExS-backend.git",
                repo_type=RepoType.Git,
                tag="0.1.2"
            ),
        ),
        (
            "github.com/inab/WfExS-backend.git",
            None,
        ),
        (
            "git@github.com:inab/WfExS-backend.git",
            None,
        ),
    ],
)
def test_guess_git_repo_params(url, expected):
    output = guess_git_repo_params(url, logger=logging.Logger("name"))
    assert output == expected
