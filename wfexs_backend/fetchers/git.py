#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2020-2023 Barcelona Supercomputing Center (BSC), Spain
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import atexit
import hashlib
import os
import shutil
import subprocess
import tempfile
from typing import (
    cast,
    TYPE_CHECKING,
)

if TYPE_CHECKING:
    import logging

    from typing import (
        Any,
        Mapping,
        MutableMapping,
        MutableSequence,
        Optional,
        Tuple,
        Type,
        Union,
        Sequence,
    )

    from typing_extensions import (
        Final,
    )

    from ..common import (
        AbsPath,
        AnyPath,
        ProgsMapping,
        RelPath,
        RepoTag,
        RepoURL,
        SecurityContextConfig,
        SymbolicName,
        URIType,
    )

    from . import (
        AbstractStatefulFetcher,
        RepoDesc,
    )


from urllib import parse, request

import dulwich.porcelain
import git

from . import (
    AbstractRepoFetcher,
    FetcherException,
    RepoGuessException,
)

from ..common import (
    ContentKind,
    ProtocolFetcherReturn,
    RemoteRepo,
    RepoType,
    URIWithMetadata,
)

from ..utils.contents import link_or_copy

GITHUB_SCHEME = "github"
GITHUB_NETLOC = "github.com"


class GitFetcher(AbstractRepoFetcher):
    GIT_PROTO: "Final[str]" = "git"
    GIT_PROTO_PREFIX: "Final[str]" = GIT_PROTO + "+"
    DEFAULT_GIT_CMD: "Final[SymbolicName]" = cast("SymbolicName", "git")

    def __init__(
        self, progs: "ProgsMapping", setup_block: "Optional[Mapping[str, Any]]" = None
    ):
        super().__init__(progs=progs, setup_block=setup_block)

        self.git_cmd = self.progs.get(
            self.DEFAULT_GIT_CMD, cast("RelPath", self.DEFAULT_GIT_CMD)
        )

    @classmethod
    def GetSchemeHandlers(cls) -> "Mapping[str, Type[AbstractStatefulFetcher]]":
        # These are de-facto schemes supported by pip and git client
        return {
            cls.GIT_PROTO: cls,
            cls.GIT_PROTO_PREFIX + "https": cls,
            cls.GIT_PROTO_PREFIX + "http": cls,
        }

    @classmethod
    def GetNeededPrograms(cls) -> "Sequence[SymbolicName]":
        return (cls.DEFAULT_GIT_CMD,)

    def doMaterializeRepo(
        self,
        repoURL: "RepoURL",
        repoTag: "Optional[RepoTag]" = None,
        repo_tag_destdir: "Optional[AbsPath]" = None,
        base_repo_destdir: "Optional[AbsPath]" = None,
        doUpdate: "Optional[bool]" = True,
    ) -> "Tuple[AbsPath, RepoDesc, Sequence[URIWithMetadata]]":
        """

        :param repoURL: The URL to the repository.
        :param repoTag: The tag or branch to checkout.
        By default, checkout the repository's default branch.
        :param doUpdate:
        :return:
        """

        # Assure directory exists before next step
        if repo_tag_destdir is None:
            if base_repo_destdir is None:
                repo_tag_destdir = cast(
                    "AbsPath", tempfile.mkdtemp(prefix="wfexs", suffix=".git")
                )
                atexit.register(shutil.rmtree, repo_tag_destdir)
            else:
                repo_hashed_id = hashlib.sha1(repoURL.encode("utf-8")).hexdigest()
                repo_destdir = os.path.join(base_repo_destdir, repo_hashed_id)
                # repo_destdir = os.path.join(self.cacheWorkflowDir, repo_hashed_id)

                if not os.path.exists(repo_destdir):
                    try:
                        os.makedirs(repo_destdir)
                    except IOError:
                        errstr = "ERROR: Unable to create intermediate directories for repo {}. ".format(
                            repoURL
                        )
                        raise FetcherException(errstr)

                repo_hashed_tag_id = hashlib.sha1(
                    b"" if repoTag is None else repoTag.encode("utf-8")
                ).hexdigest()
                repo_tag_destdir = cast(
                    "AbsPath", os.path.join(repo_destdir, repo_hashed_tag_id)
                )

        self.logger.debug(f"Repo dir {repo_tag_destdir}")

        # We are assuming that, if the directory does exist, it contains the repo
        repo = git.Repo()
        if not os.path.exists(os.path.join(repo_tag_destdir, ".git")):
            if repoTag is not None:
                # if we know the tag/branch try cloning the repository without initial checkout
                repo = repo.clone_from(
                    repoURL,
                    repo_tag_destdir,
                    multi_options=["--recurse-submodules", "-n"],
                )
                # Now, checkout the specific commit
                if repoTag in repo.refs:
                    repo.refs[repoTag].checkout()
                elif repoTag in repo.remote().refs:
                    repo.remote().refs[repoTag].checkout()
                else:
                    self.logger.info(
                        f"Unable to checkout {repoTag}. "
                        f"No such branch or tag. Defaulting to {repo.active_branch.name}."
                    )

            # else just checkout main if we know nothing about the tag, or checkout
            else:
                repo = repo.clone_from(
                    repoURL, repo_tag_destdir, multi_options=["--recurse-submodules"]
                )

        elif doUpdate:
            # git pull with recursive submodules
            repo = git.Repo(os.path.join(repo_tag_destdir, ".git"))
            if repoTag is not None:
                if repoTag in repo.refs:
                    repo.refs[repoTag].checkout()
                elif repoTag in repo.remote().refs:
                    repo.remote().refs[repoTag].checkout()
                else:
                    self.logger.info(
                        f"Unable to checkout {repoTag}. "
                        f"No such branch or tag. Defaulting to {repo.active_branch.name}."
                    )
            repo.remote().pull(repoTag)

        else:
            pass

        # Last, we have to obtain the effective checkout
        gitrevparse_params = [self.git_cmd, "rev-parse", "--verify", "HEAD"]

        checkout = repo.git.execute(gitrevparse_params)
        repo_effective_checkout = cast("RepoTag", checkout)
        repo_desc: "RepoDesc" = {
            "repo": repoURL,
            "tag": repoTag,
            "checkout": repo_effective_checkout,
        }

        return (
            repo_tag_destdir,
            repo_desc,
            [],
        )

    def fetch(
        self,
        remote_file: "URIType",
        cachedFilename: "AbsPath",
        secContext: "Optional[SecurityContextConfig]" = None,
    ) -> "ProtocolFetcherReturn":
        parsedInputURL = parse.urlparse(remote_file)

        # These are the usual URIs which can be understood by pip
        # See https://pip.pypa.io/en/stable/cli/pip_install/#git
        if parsedInputURL.scheme not in self.GetSchemeHandlers():
            raise FetcherException(f"FIXME: Unhandled scheme {parsedInputURL.scheme}")

        # Getting the scheme git is going to understand
        if parsedInputURL.scheme.startswith(self.GIT_PROTO_PREFIX):
            gitScheme = parsedInputURL.scheme.replace(self.GIT_PROTO_PREFIX, "")
        else:
            gitScheme = parsedInputURL.scheme

        # Getting the tag or branch
        gitPath = parsedInputURL.path
        repoTag: "Optional[RepoTag]" = None
        if "@" in parsedInputURL.path:
            gitPath, repoTag = cast(
                "Tuple[str, RepoTag]", tuple(parsedInputURL.path.split("@", 1))
            )

        # Getting the repoRelPath (if available)
        repoRelPath = None
        if len(parsedInputURL.fragment) > 0:
            frag_qs = parse.parse_qs(parsedInputURL.fragment)
            subDirArr = frag_qs.get("subdirectory", [])
            if len(subDirArr) > 0:
                repoRelPath = subDirArr[0]

        # Now, reassemble the repoURL, to be used by git client
        repoURL = cast(
            "RepoURL",
            parse.urlunparse((gitScheme, parsedInputURL.netloc, gitPath, "", "", "")),
        )

        repo_tag_destdir, repo_desc, metadata_array = self.doMaterializeRepo(
            repoURL, repoTag=repoTag
        )
        repo_desc["relpath"] = cast("RelPath", repoRelPath)

        preferredName: "Optional[RelPath]"
        if repoRelPath is not None:
            cachedContentPath = os.path.join(repo_tag_destdir, repoRelPath)
            preferredName = cast("RelPath", repoRelPath.split("/")[-1])
        else:
            cachedContentPath = repo_tag_destdir
            preferredName = None

        if os.path.isdir(cachedContentPath):
            kind = ContentKind.Directory
        elif os.path.isfile(cachedContentPath):
            kind = ContentKind.File
        else:
            raise FetcherException(
                f"Remote {remote_file} is neither a file nor a directory (does it exist?)"
            )

        # shutil.move(cachedContentPath, cachedFilename)
        link_or_copy(cast("AnyPath", cachedContentPath), cachedFilename)

        augmented_metadata_array = [
            URIWithMetadata(
                uri=remote_file, metadata=repo_desc, preferredName=preferredName
            ),
            *metadata_array,
        ]
        return ProtocolFetcherReturn(
            kind_or_resolved=kind,
            metadata_array=augmented_metadata_array,
            # TODO: Identify licences in git repositories??
            licences=None,
        )


HEAD_LABEL = b"HEAD"
REFS_HEADS_PREFIX = b"refs/heads/"
REFS_TAGS_PREFIX = b"refs/tags/"
GIT_SCHEMES = ["https", "git+https", "ssh", "git+ssh", "file", "git+file"]


def guess_git_repo_params(
    wf_url: "Union[URIType, parse.ParseResult]",
    logger: "logging.Logger",
    fail_ok: "bool" = False,
) -> "Optional[RemoteRepo]":
    """Extract the parameters for a git repo from the given URL. If an invalid URL is passed,
    this function returns `None`.
    
    The acceptable form for the URL can be found [here](https://pip.pypa.io/en/stable/topics/vcs-support/#git).

    :param wf_url: The URL to the repo.
    :param logger: A `logging.Logger` instance for debugging purposes.
    :param fail_ok: _description_, defaults to False. Deprecated, ignored.
    :return: A `RemoteRepo` instance containing parameters of the git repo or `None`
    if no repo was found.
    """
    repoURL = None
    repoTag = None
    repoRelPath = None
    repoType: "Optional[RepoType]" = RepoType.Git

    # Deciding which is the input
    if isinstance(wf_url, parse.ParseResult):
        parsed_wf_url = wf_url
    else:
        parsed_wf_url = parse.urlparse(wf_url)

    # Return None if no scheme in URL. Can't choose how to proceed
    if not parsed_wf_url.scheme:
        logger.debug(
            f"No scheme in repo URL. Choices are: {', '.join(GIT_SCHEMES)}"
        )
        return None
    
    # Return None if no scheme in URL. Can't choose how to proceed
    if not ".git" in parsed_wf_url.path:
        logger.debug(
            f"URL does not seem to point to a git repo."
        )
        return None

    # Getting the scheme git is going to understand
    git_scheme = parsed_wf_url.scheme.removeprefix("git+")

    # Getting the tag or branch
    gitPath = parsed_wf_url.path
    if "@" in parsed_wf_url.path:
        gitPath, repoTag = parsed_wf_url.path.split("@", 1)

    # Getting the repoRelPath (if available)
    if parsed_wf_url.fragment:
        frag_qs = parse.parse_qs(parsed_wf_url.fragment)
        subDirArr = frag_qs.get("subdirectory", [])
        if subDirArr:
            repoRelPath = subDirArr[0]

    # Now, reassemble the repoURL
    if git_scheme == "ssh":
        repoURL = parsed_wf_url.netloc + gitPath
    else:
        repoURL = parse.urlunparse((git_scheme, parsed_wf_url.netloc, gitPath, "", "", ""))

    logger.debug(
        "From {} was derived (type {}) {} {} {}".format(
            wf_url, repoType, repoURL, repoTag, repoRelPath
        )
    )

    return RemoteRepo(
        repo_url=cast("RepoURL", repoURL),
        tag=cast("Optional[RepoTag]", repoTag),
        rel_path=cast("Optional[RelPath]", repoRelPath),
        repo_type=repoType,
    )
