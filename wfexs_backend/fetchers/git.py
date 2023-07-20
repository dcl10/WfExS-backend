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
                    multi_options=["--recurse-submodules", "-n"]
                )
                # Now, checkout the specific commit
                if repoTag in repo.refs:
                    repo.refs[repoTag].checkout()
                elif repoTag in repo.remotes.origin.refs:
                    repo.remote().refs[repoTag].checkout()
                else:
                    self.logger.info(
                        f"Unable to checkout {repoTag}. "
                        f"No such branch or tag. Defaulting to {repo.active_branch.name}."
                    )
            
            # else just checkout main if we know nothing about the tag, or checkout
            else:
                repo = repo.clone_from(
                    repoURL,
                    repo_tag_destdir,
                    multi_options=["--recurse-submodules"]
                )
                
        elif doUpdate:
            # git pull with recursive submodules
            repo = git.Repo(os.path.join(repo_tag_destdir, ".git"))
            if repoTag is not None:
                if repoTag in repo.refs:
                    repo.refs[repoTag].checkout()
                elif repoTag in repo.remotes.origin.refs:
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


def guess_git_repo_params(
    wf_url: "Union[URIType, parse.ParseResult]",
    logger: "logging.Logger",
    fail_ok: "bool" = False,
) -> "Optional[RemoteRepo]":
    repoURL = None
    repoTag = None
    repoRelPath = None
    repoType: "Optional[RepoType]" = None

    # Deciding which is the input
    if isinstance(wf_url, parse.ParseResult):
        parsed_wf_url = wf_url
    else:
        parsed_wf_url = parse.urlparse(wf_url)

    # These are the usual URIs which can be understood by pip
    # See https://pip.pypa.io/en/stable/cli/pip_install/#git
    found_params: "Optional[Tuple[RemoteRepo, Sequence[str], Sequence[RepoTag]]]" = None
    try:
        if parsed_wf_url.scheme in GitFetcher.GetSchemeHandlers():
            # Getting the scheme git is going to understand
            if parsed_wf_url.scheme.startswith(GitFetcher.GIT_PROTO_PREFIX):
                gitScheme = parsed_wf_url.scheme.replace(GitFetcher.GIT_PROTO_PREFIX, "")
            else:
                gitScheme = parsed_wf_url.scheme

            # Getting the tag or branch
            gitPath = parsed_wf_url.path
            if "@" in parsed_wf_url.path:
                gitPath, repoTag = parsed_wf_url.path.split("@", 1)

            # Getting the repoRelPath (if available)
            if len(parsed_wf_url.fragment) > 0:
                frag_qs = parse.parse_qs(parsed_wf_url.fragment)
                subDirArr = frag_qs.get("subdirectory", [])
                if len(subDirArr) > 0:
                    repoRelPath = subDirArr[0]

            # Now, reassemble the repoURL
            repoURL = parse.urlunparse(
                (gitScheme, parsed_wf_url.netloc, gitPath, "", "", "")
            )
            found_params = find_git_repo_in_uri(cast("URIType", repoURL))

        elif parsed_wf_url.scheme == GITHUB_SCHEME:
            repoType = RepoType.GitHub

            gh_path_split = parsed_wf_url.path.split("/")
            gh_path = "/".join(gh_path_split[:2])
            gh_post_path = list(map(parse.unquote_plus, gh_path_split[2:]))
            if len(gh_post_path) > 0:
                repoTag = gh_post_path[0]
                if len(gh_post_path) > 1:
                    repoRelPath = "/".join(gh_post_path[1:])

            repoURL = parse.urlunparse(
                parse.ParseResult(
                    scheme="https",
                    netloc=GITHUB_NETLOC,
                    path=gh_path,
                    params="",
                    query="",
                    fragment="",
                )
            )
            found_params = find_git_repo_in_uri(cast("URIType", repoURL))

        elif parsed_wf_url.netloc == GITHUB_NETLOC:
            found_params = find_git_repo_in_uri(parsed_wf_url)
            repoURL = found_params[0].repo_url

            # And now, guessing the tag and the relative path
            # WARNING! This code can have problems with tags which contain slashes
            wf_path = found_params[1]
            repo_branches_tags = found_params[2]
            if len(wf_path) > 1 and (wf_path[0] in ("blob", "tree")):
                wf_path_tag = list(map(parse.unquote_plus, wf_path[1:]))

                tag_relpath = "/".join(wf_path_tag)
                for repo_branch_tag in repo_branches_tags:
                    if repo_branch_tag == tag_relpath or tag_relpath.startswith(
                        repo_branch_tag + "/"
                    ):
                        repoTag = repo_branch_tag
                        if len(tag_relpath) > len(repo_branch_tag):
                            tag_relpath = tag_relpath[len(repo_branch_tag) + 1 :]
                            if len(tag_relpath) > 0:
                                repoRelPath = tag_relpath
                        break
                else:
                    # Fallback
                    repoTag = wf_path_tag[0]
                    if len(wf_path_tag) > 0:
                        repoRelPath = "/".join(wf_path_tag[1:])
        elif parsed_wf_url.netloc == "raw.githubusercontent.com":
            wf_path = list(map(parse.unquote_plus, parsed_wf_url.path.split("/")))
            if len(wf_path) >= 3:
                # Rebuilding it
                repoGitPath = wf_path[:3]
                repoGitPath[-1] += ".git"

                # Rebuilding repo git path
                repoURL = parse.urlunparse(
                    ("https", GITHUB_NETLOC, "/".join(repoGitPath), "", "", "")
                )

                # And now, guessing the tag/checkout and the relative path
                # WARNING! This code can have problems with tags which contain slashes
                found_params = find_git_repo_in_uri(cast("URIType", repoURL))
                if len(wf_path) >= 4:
                    repo_branches_tags = found_params[2]
                    # Validate against existing branch and tag names
                    tag_relpath = "/".join(wf_path[3:])
                    for repo_branch_tag in repo_branches_tags:
                        if repo_branch_tag == tag_relpath or tag_relpath.startswith(
                            repo_branch_tag + "/"
                        ):
                            repoTag = repo_branch_tag
                            if len(tag_relpath) > len(repo_branch_tag):
                                tag_relpath = tag_relpath[len(repo_branch_tag) + 1 :]
                                if len(tag_relpath) > 0:
                                    repoRelPath = tag_relpath
                            break
                    else:
                        # Fallback
                        repoTag = wf_path[3]
                        if len(wf_path) > 4:
                            repoRelPath = "/".join(wf_path[4:])
            else:
                repoType = RepoType.GitHub
        # TODO handling other popular cases, like bitbucket
        else:
            found_params = find_git_repo_in_uri(parsed_wf_url)

    except RepoGuessException as gge:
        if not fail_ok:
            raise FetcherException(
                f"FIXME: Unsupported http(s) git repository {wf_url} (see cascade exception)"
            ) from gge

    if found_params is not None:
        if repoTag is None:
            repoTag = found_params[0].tag
        repoType = found_params[0].repo_type
    elif not fail_ok:
        raise FetcherException(
            "FIXME: Unsupported http(s) git repository {}".format(wf_url)
        )

    logger.debug(
        "From {} was derived (type {}) {} {} {}".format(
            wf_url, repoType, repoURL, repoTag, repoRelPath
        )
    )

    if repoURL is None:
        return None

    return RemoteRepo(
        repo_url=cast("RepoURL", repoURL),
        tag=cast("Optional[RepoTag]", repoTag),
        rel_path=cast("Optional[RelPath]", repoRelPath),
        repo_type=repoType,
    )


def find_git_repo_in_uri(
    remote_file: "Union[URIType, parse.ParseResult]",
) -> "Tuple[RemoteRepo, Sequence[str], Sequence[RepoTag]]":
    if isinstance(remote_file, parse.ParseResult):
        parsedInputURL = remote_file
    else:
        parsedInputURL = parse.urlparse(remote_file)
    sp_path = parsedInputURL.path.split("/")

    shortest_pre_path: "Optional[URIType]" = None
    longest_post_path: "Optional[Sequence[str]]" = None
    repo_type: "Optional[RepoType]" = None
    the_remote_uri: "Optional[str]" = None
    b_default_repo_tag: "Optional[str]" = None
    repo_branches: "Optional[MutableSequence[RepoTag]]" = None
    for pos in range(len(sp_path), 0, -1):
        pre_path = "/".join(sp_path[:pos])
        if pre_path == "":
            pre_path = "/"
        remote_uri_anc = parse.urlunparse(parsedInputURL._replace(path=pre_path))

        remote_refs_dict: "Mapping[bytes, bytes]"
        try:
            remote_refs_dict = dulwich.porcelain.ls_remote(remote_uri_anc)
        except dulwich.errors.NotGitRepository as ngr:
            # Skip and continue
            continue

        the_remote_uri = remote_uri_anc

        head_remote_ref = remote_refs_dict[HEAD_LABEL]
        repo_branches = []
        b_default_repo_tag = None
        for remote_label, remote_ref in remote_refs_dict.items():
            if remote_label.startswith(REFS_HEADS_PREFIX):
                b_repo_tag = remote_label[len(REFS_HEADS_PREFIX) :].decode(
                    "utf-8", errors="continue"
                )
                repo_branches.append(cast("RepoTag", b_repo_tag))
                if b_default_repo_tag is None and remote_ref == head_remote_ref:
                    b_default_repo_tag = b_repo_tag

        # It is considered a git repo!
        shortest_pre_path = cast("URIType", pre_path)
        longest_post_path = sp_path[pos:]
        if repo_type is None:
            # Metadata is all we really need
            repo_type = RepoType.Raw
            req = request.Request(remote_uri_anc, method="HEAD")
            try:
                with request.urlopen(req) as resp:
                    # Is it gitlab?
                    if list(
                        filter(
                            lambda c: "gitlab" in c,
                            resp.headers.get_all("Set-Cookie"),
                        )
                    ):
                        repo_type = RepoType.GitLab
                    elif list(
                        filter(
                            lambda c: GITHUB_NETLOC in c,
                            resp.headers.get_all("Set-Cookie"),
                        )
                    ):
                        repo_type = RepoType.GitHub
                    elif list(
                        filter(
                            lambda c: "bitbucket" in c,
                            resp.headers.get_all("X-View-Name"),
                        )
                    ):
                        repo_type = RepoType.BitBucket
            except Exception as e:
                pass

    if repo_type is None:
        raise RepoGuessException(f"Unable to identify {remote_file} as a git repo")

    if b_default_repo_tag is None:
        raise RepoGuessException(
            f"No tag was obtained while getting default branch name from {remote_file}"
        )

    assert longest_post_path is not None
    assert repo_branches is not None

    repo = RemoteRepo(
        repo_url=cast("RepoURL", the_remote_uri),
        tag=cast("RepoTag", b_default_repo_tag),
        repo_type=repo_type,
    )
    return repo, longest_post_path, repo_branches
