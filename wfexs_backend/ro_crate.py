#!/usr/bin/env python
# -*- coding: utf-8 -*-

# SPDX-License-Identifier: Apache-2.0
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
from __future__ import absolute_import

import copy
import inspect
import logging
import os
import pathlib
from typing import (
    cast,
    TYPE_CHECKING,
)
import warnings

if TYPE_CHECKING:
    import datetime

    from typing import (
        Any,
        Mapping,
        MutableMapping,
        MutableSequence,
        Optional,
        Sequence,
        Tuple,
        Union,
    )

    from typing_extensions import (
        Final,
    )

    from .common import (
        AbsPath,
        AbstractGeneratedContent,
        AnyPath,
        Container,
        ContainerEngineVersionStr,
        ContainerOperatingSystem,
        EngineVersion,
        ExpectedOutput,
        Fingerprint,
        LocalWorkflow,
        MaterializedInput,
        MaterializedOutput,
        MaterializedWorkflowEngine,
        ProcessorArchitecture,
        RelPath,
        RemoteRepo,
        RepoTag,
        RepoURL,
        StagedExecution,
        StagedSetup,
        SymbolicOutputName,
        URIType,
        WFLangVersion,
        WorkflowEngineVersionStr,
        WorkflowType,
    )

import urllib.parse
import uuid

import magic  # type: ignore[import]
from rfc6920.methods import extract_digest
import rocrate.model.entity  # type: ignore[import]
import rocrate.model.dataset  # type: ignore[import]
import rocrate.model.computationalworkflow  # type: ignore[import]
import rocrate.model.computerlanguage  # type: ignore[import]
import rocrate.model.file  # type: ignore[import]
import rocrate.model.file_or_dir  # type: ignore[import]
import rocrate.model.softwareapplication  # type: ignore[import]
import rocrate.model.creativework  # type: ignore[import]
import rocrate.rocrate  # type: ignore[import]

from rocrate.utils import is_url  # type: ignore[import]

from .utils.digests import (
    ComputeDigestFromDirectory,
    ComputeDigestFromFile,
    hexDigest,
    unstringifyDigest,
)
from .common import (
    AbstractWfExSException,
    ContainerType,
    ContentKind,
    CratableItem,
    GeneratedContent,
    GeneratedDirectoryContent,
    MaterializedContent,
    NoCratableItem,
)

from . import __url__ as wfexs_backend_url
from . import __official_name__ as wfexs_backend_name
from . import get_WfExS_version


class ROCrateGenerationException(AbstractWfExSException):
    pass


class FormalParameter(rocrate.model.entity.Entity):  # type: ignore[misc]
    def __init__(
        self,
        crate: "rocrate.rocrate.ROCrate",
        name: "str",
        additional_type: "Optional[str]" = None,
        identifier: "Optional[str]" = None,
        properties: "Optional[Mapping[str, Any]]" = None,
    ):
        fp_properties = {
            "name": name,
            # As of https://www.researchobject.org/ro-crate/1.1/workflows.html#describing-inputs-and-outputs
            "conformsTo": "https://bioschemas.org/profiles/FormalParameter/1.0-RELEASE/",
        }

        if additional_type is not None:
            fp_properties["additionalType"] = additional_type

        if properties is not None:
            fp_properties.update(properties)
        super().__init__(crate, identifier=identifier, properties=fp_properties)


class PropertyValue(rocrate.model.entity.Entity):  # type: ignore[misc]
    def __init__(
        self,
        crate: "rocrate.rocrate.ROCrate",
        name: "str",
        value: "Union[bool,str,int,float]",
        identifier: "Optional[str]" = None,
        properties: "Optional[Mapping[str, Any]]" = None,
    ):
        pv_properties = {
            "name": name,
            "value": value,
        }

        if properties is not None:
            pv_properties.update(properties)
        super().__init__(crate, identifier=identifier, properties=pv_properties)


class Action(rocrate.model.entity.Entity):  # type: ignore[misc]
    def __init__(
        self,
        crate: "rocrate.rocrate.ROCrate",
        name: "str",
        startTime: "Optional[datetime.datetime]" = None,
        endTime: "Optional[datetime.datetime]" = None,
        identifier: "Optional[str]" = None,
        properties: "Optional[Mapping[str, Any]]" = None,
    ):
        pv_properties = {
            "name": name,
        }
        if startTime is not None:
            pv_properties["startTime"] = startTime.isoformat()
        if endTime is not None:
            pv_properties["endTime"] = endTime.isoformat()

        if properties is not None:
            pv_properties.update(properties)
        super().__init__(crate, identifier=identifier, properties=pv_properties)


class CreateAction(Action):
    pass


class OrganizeAction(Action):
    pass


class ControlAction(Action):
    pass


class SoftwareContainer(rocrate.model.file.File):  # type: ignore[misc]
    TYPES = ["File", "SoftwareApplication"]

    def _empty(self) -> "Mapping[str, Any]":
        return {
            "@id": self.id,
            "@type": self.TYPES[:],
        }


class Collection(rocrate.model.creativework.CreativeWork):  # type: ignore[misc]
    def __init__(
        self,
        crate: "rocrate.rocrate.ROCrate",
        main_entity: "Union[FixedFile, FixedDataset, None]",
        identifier: "Optional[str]" = None,
        properties: "Optional[Mapping[str, Any]]" = None,
    ):
        pv_properties: "MutableMapping[str, Any]" = {}

        if properties is not None:
            pv_properties.update(properties)
        super().__init__(crate, identifier=identifier, properties=pv_properties)

        if main_entity is not None:
            self["mainEntity"] = main_entity


class FixedMixin(rocrate.model.file_or_dir.FileOrDir):  # type: ignore[misc]
    def __init__(
        self,
        crate: "rocrate.rocrate.ROCrate",
        source: "Optional[Union[str, pathlib.Path]]" = None,
        dest_path: "Optional[Union[str, pathlib.Path]]" = None,
        identifier: "Optional[str]" = None,
        fetch_remote: "bool" = False,
        validate_url: "bool" = False,
        properties: "Optional[Mapping[str, Any]]" = None,
    ):
        if properties is None:
            properties = {}
        self.fetch_remote = fetch_remote
        self.validate_url = validate_url
        self.source = source
        if dest_path is not None:
            dest_path = pathlib.Path(dest_path)
            if dest_path.is_absolute():
                raise ValueError("if provided, dest_path must be relative")
            if identifier is None:
                identifier = dest_path.as_posix()
        elif identifier is None:
            if not isinstance(source, (str, pathlib.Path)):
                raise ValueError(
                    "dest_path must be provided if source is not a path or URI"
                )
            elif is_url(str(source)):
                identifier = os.path.basename(source) if fetch_remote else str(source)
            else:
                identifier = "./" if source == "./" else os.path.basename(source)
        super(rocrate.model.file_or_dir.FileOrDir, self).__init__(
            crate, identifier, properties
        )


class FixedFile(FixedMixin, rocrate.model.file.File):  # type: ignore[misc]
    pass


class FixedDataset(FixedMixin, rocrate.model.dataset.Dataset):  # type: ignore[misc]
    pass


class FixedROCrate(rocrate.rocrate.ROCrate):  # type: ignore[misc]
    """
    This subclass fixes the limitations from original ROCrate class
    """

    def add_file(
        self,
        source: "Optional[Union[str, pathlib.Path]]" = None,
        dest_path: "Optional[str]" = None,
        identifier: "Optional[str]" = None,
        fetch_remote: "bool" = False,
        validate_url: "bool" = False,
        properties: "Optional[Mapping[str, Any]]" = None,
    ) -> "FixedFile":
        """
        source: The absolute path to the local copy of the file, if exists.
        dest_path: The relative path inside the RO-Crate for the file copy.
        identifier: The forced value for the @id of the File declaration.
        """
        return cast(
            "FixedFile",
            self.add(
                FixedFile(
                    self,
                    source=source,
                    dest_path=dest_path,
                    identifier=identifier,
                    fetch_remote=fetch_remote,
                    validate_url=validate_url,
                    properties=properties,
                )
            ),
        )

    def add_dataset(
        self,
        source: "Optional[Union[str, pathlib.Path]]" = None,
        dest_path: "Optional[str]" = None,
        identifier: "Optional[str]" = None,
        fetch_remote: "bool" = False,
        validate_url: "bool" = False,
        properties: "Optional[Mapping[str, Any]]" = None,
    ) -> "FixedDataset":
        """
        source: The absolute path to the local copy of the file, if exists.
        dest_path: The relative path inside the RO-Crate for the file copy.
        identifier: The forced value for the @id of the File declaration.
        """
        return cast(
            "FixedDataset",
            self.add(
                FixedDataset(
                    self,
                    source=source,
                    dest_path=dest_path,
                    identifier=identifier,
                    fetch_remote=fetch_remote,
                    validate_url=validate_url,
                    properties=properties,
                )
            ),
        )

    add_directory = add_dataset


class WorkflowRunROCrate:
    """
    This class rules the generation of an RO-Crate
    """

    ContainerTypeIds: "Final[Mapping[ContainerType, str]]" = {
        ContainerType.Singularity: "https://apptainer.org/",
        ContainerType.Docker: "https://www.docker.com/",
        ContainerType.Podman: "https://podman.io/"
    }

    def __init__(
        self,
        remote_repo: "RemoteRepo",
        localWorkflow: "LocalWorkflow",
        materializedEngine: "MaterializedWorkflowEngine",
        workflowEngineVersion: "Optional[WorkflowEngineVersionStr]",
        containerEngineVersion: "Optional[ContainerEngineVersionStr]",
        containerEngineOs: "Optional[ContainerOperatingSystem]",
        arch: "Optional[ProcessorArchitecture]",
        staged_setup: "StagedSetup",
        payloads: "CratableItem" = NoCratableItem,
    ):
        # Getting a logger focused on specific classes
        self.logger = logging.getLogger(
            dict(inspect.getmembers(self))["__module__"]
            + "::"
            + self.__class__.__name__
        )

        self.cached_cts: "MutableMapping[ContainerType, rocrate.model.softwareapplication.SoftwareApplication]" = (
            {}
        )

        if localWorkflow.relPath is not None:
            wf_local_path = os.path.join(localWorkflow.dir, localWorkflow.relPath)
        else:
            wf_local_path = localWorkflow.dir

        self.arch = arch
        self.containerEngineOs = containerEngineOs
        self.containerEngineVersion = containerEngineVersion

        assert staged_setup.work_dir is not None
        assert staged_setup.inputs_dir is not None
        self.staged_setup = staged_setup
        self.work_dir = staged_setup.work_dir
        self.payloads = payloads

        self.crate: "FixedROCrate"
        self.compLang: "rocrate.model.computerlanguage.ComputerLanguage"
        self._init_empty_crate_and_ComputerLanguage(
            materializedEngine.instance.workflowType, localWorkflow.langVersion
        )

        self.wf_wfexs = self._add_wfexs_to_crate()

        matWf = materializedEngine.workflow
        if matWf.relPath is not None:
            if os.path.isabs(matWf.relPath):
                matWf_local_path = cast("AbsPath", matWf.relPath)
            else:
                matWf_local_path = cast(
                    "AbsPath", os.path.join(matWf.dir, matWf.relPath)
                )
        else:
            matWf_local_path = matWf.dir

        wf_url: "str"
        wf_entrypoint_url: "str"
        if remote_repo.web_url is not None:
            wf_url = remote_repo.web_url
            wf_entrypoint_url = wf_url
        else:
            wf_url = remote_repo.repo_url.replace(".git", "/")
            if remote_repo.tag is not None:
                wf_url += "tree/" + remote_repo.tag
            if localWorkflow.relPath is not None:
                wf_url += localWorkflow.dir.rsplit("workflow")[1]

            parsed_repo_url = urllib.parse.urlparse(remote_repo.repo_url)
            if parsed_repo_url.netloc == "github.com":
                assert (
                    matWf.effectiveCheckout is not None
                ), "The effective checkout should be available"

                parsed_repo_path = parsed_repo_url.path.split("/")
                repo_name = parsed_repo_path[2]
                # TODO: should we urldecode repo_name?
                if repo_name.endswith(".git"):
                    repo_name = repo_name[:-4]
                wf_entrypoint_path = [
                    "",  # Needed to prepend a slash
                    parsed_repo_path[1],
                    # TODO: should we urlencode repo_name?
                    repo_name,
                    matWf.effectiveCheckout,
                ]

                if localWorkflow.relPath is not None:
                    wf_entrypoint_path.append(localWorkflow.relPath)

                wf_entrypoint_url = urllib.parse.urlunparse(
                    (
                        "https",
                        "raw.githubusercontent.com",
                        "/".join(wf_entrypoint_path),
                        "",
                        "",
                        "",
                    )
                )

            elif "gitlab" in parsed_repo_url.netloc:
                parsed_repo_path = parsed_repo_url.path.split("/")
                # FIXME: cover the case of nested groups
                repo_name = parsed_repo_path[2]
                if repo_name.endswith(".git"):
                    repo_name = repo_name[:-4]
                wf_entrypoint_path = [parsed_repo_path[1], repo_name]
                if remote_repo.tag is not None and localWorkflow.relPath is not None:
                    # TODO: should we urlencode repoTag?
                    wf_entrypoint_path.extend(
                        ["-", "raw", remote_repo.tag, localWorkflow.relPath]
                    )

                wf_entrypoint_url = urllib.parse.urlunparse(
                    (
                        parsed_repo_url.scheme,
                        parsed_repo_url.netloc,
                        "/".join(wf_entrypoint_path),
                        "",
                        "",
                        "",
                    )
                )

            else:
                raise ROCrateGenerationException(
                    "FIXME: Unsupported http(s) git repository {}".format(
                        remote_repo.repo_url
                    )
                )

        # This is needed to avoid future collisions with other workflows stored in the RO-Crate
        rocrate_wf_folder = str(uuid.uuid5(uuid.NAMESPACE_URL, wf_entrypoint_url))

        # TODO: research why relPathFiles is not populated in matWf
        lW = localWorkflow if matWf.relPathFiles is None else matWf

        workflow_path = pathlib.Path(matWf_local_path)
        if matWf_local_path != wf_local_path:
            rocrate_wf_id = rocrate_wf_folder + "/" + os.path.basename(matWf_local_path)
        else:
            rocrate_wf_id = (
                rocrate_wf_folder + "/" + os.path.relpath(matWf_local_path, matWf.dir)
            )
        local_rocrate_wf_id = (
            rocrate_wf_folder + "/" + os.path.relpath(wf_local_path, localWorkflow.dir)
        )

        wf_file = self.crate.add_workflow(
            source=workflow_path,
            dest_path=rocrate_wf_id,
            fetch_remote=False,
            main=True,
            lang=self.compLang,
            gen_cwl=False,
        )
        self.wf_file: "rocrate.model.computationalworkflow.ComputationalWorkflow" = (
            wf_file
        )

        self.wf_file.append_to(
            "conformsTo",
            # As of https://www.researchobject.org/ro-crate/1.1/workflows.html#complying-with-bioschemas-computational-workflow-profile
            {
                "@id": "https://bioschemas.org/profiles/ComputationalWorkflow/1.0-RELEASE"
            },
        )

        self.weng_crate = rocrate.model.softwareapplication.SoftwareApplication(
            self.crate, identifier=materializedEngine.instance.engine_url
        )
        if workflowEngineVersion is not None:
            self.weng_crate["softwareVersion"] = workflowEngineVersion
            self.wf_file["runtimePlatform"] = workflowEngineVersion
        self.crate.add(self.weng_crate)
        self.wf_file.append_to("softwareRequirements", self.weng_crate)

        if materializedEngine.containers is not None:
            self._add_containers_to_workflow(
                materializedEngine.containers,
            )
        if materializedEngine.operational_containers is not None:
            self._add_containers_to_workflow(
                materializedEngine.operational_containers,
                weng_crate=self.weng_crate,
            )

        rel_entities = []
        if lW.relPathFiles:
            for rel_file in lW.relPathFiles:
                # First, are we dealing with relative files or with URIs?
                p_rel_file = urllib.parse.urlparse(rel_file)
                if p_rel_file.scheme != "":
                    the_entity = rocrate.model.creativework.CreativeWork(
                        self.crate,
                        identifier=rel_file,
                    )
                    self.crate.add(the_entity)
                    rel_entities.append(the_entity)
                else:
                    rocrate_file_id = rocrate_wf_folder + "/" + rel_file
                    if rocrate_file_id != rocrate_wf_id:
                        the_entity = self._add_file_to_crate(
                            the_path=os.path.join(lW.dir, rel_file),
                            the_name=cast(
                                "RelPath", os.path.join(rocrate_wf_folder, rel_file)
                            ),
                            the_uri=cast("URIType", rocrate_file_id),
                            do_attach=CratableItem.Workflow in payloads,
                        )
                        rel_entities.append(the_entity)

        if local_rocrate_wf_id != rocrate_wf_id:
            local_wf_file_pre = self.crate.get(local_rocrate_wf_id)

            local_wf_file = self.crate.add_workflow(
                source=workflow_path,
                dest_path=local_rocrate_wf_id,
                fetch_remote=False,
                main=False,
                lang=self.compLang,
                gen_cwl=False,
            )
            local_wf_file["codeRepository"] = remote_repo.repo_url
            if materializedEngine.workflow.effectiveCheckout is not None:
                local_wf_file["version"] = materializedEngine.workflow.effectiveCheckout
            local_wf_file["description"] = "Unconsolidated Workflow Entrypoint"
            local_wf_file["contentUrl"] = wf_entrypoint_url
            local_wf_file["url"] = wf_url
            local_wf_file["hasPart"] = rel_entities
            if localWorkflow.relPath is not None:
                local_wf_file["alternateName"] = localWorkflow.relPath

            # Transferring the properties
            for prop_name in ("contentSize", "encodingFormat", "identifier", "sha256"):
                if prop_name in local_wf_file_pre:
                    local_wf_file[prop_name] = local_wf_file_pre[prop_name]

            self.wf_file["isBasedOn"] = local_wf_file

            # Now, describe the transformation
            wf_consolidate_action = CreateAction(self.crate, "Workflow consolidation")
            wf_consolidate_action = self.crate.add(wf_consolidate_action)
            wf_consolidate_action["object"] = local_wf_file
            wf_consolidate_action["result"] = self.wf_file
            wf_consolidate_action["instrument"] = self.weng_crate
            wf_consolidate_action["agent"] = self.wf_wfexs
        else:
            self.wf_file["codeRepository"] = remote_repo.repo_url
            if materializedEngine.workflow.effectiveCheckout is not None:
                self.wf_file["version"] = materializedEngine.workflow.effectiveCheckout
            self.wf_file["description"] = "Workflow Entrypoint"
            self.wf_file["url"] = wf_url
            self.wf_file["hasPart"] = rel_entities
            if matWf.relPath is not None:
                self.wf_file["alternateName"] = matWf.relPath

        # if 'url' in self.wf_file.properties():
        #    self.wf_file['codeRepository'] = self.wf_file['url']

        # TODO: add extra files, like the diagram, an abstract CWL
        # representation of the workflow (when it is not a CWL workflow)
        # etc...
        # for file_entry in include_files:
        #    self.crate.add_file(file_entry)

    def _init_empty_crate_and_ComputerLanguage(
        self,
        wf_type: "WorkflowType",
        langVersion: "Optional[Union[EngineVersion, WFLangVersion]]",
    ) -> "None":
        """
        Due the internal synergies between an instance of ComputerLanguage
        and the RO-Crate it is attached to, both of them should be created
        here, just at the same time
        """

        self.crate = FixedROCrate(gen_preview=True)
        self.compLang = rocrate.model.computerlanguage.ComputerLanguage(
            self.crate,
            identifier=wf_type.rocrate_programming_language,
            properties={
                "name": wf_type.name,
                "alternateName": wf_type.trs_descriptor,
                "identifier": {"@id": wf_type.uriTemplate.format(langVersion)},
                "url": {"@id": wf_type.url},
                "version": langVersion,
            },
        )
        # This should not be needed, as it is added later
        self.crate.add(self.compLang)

    def _add_wfexs_to_crate(
        self,
    ) -> "rocrate.model.softwareapplication.SoftwareApplication":
        # First, the profiles to be attached to the root dataset
        wrroc_profiles = [
            rocrate.model.creativework.CreativeWork(
                self.crate,
                identifier="https://w3id.org/ro/wfrun/process/0.2",
                properties={"name": "ProcessRun Crate", "version": "0.2"},
            ),
            rocrate.model.creativework.CreativeWork(
                self.crate,
                identifier="https://w3id.org/ro/wfrun/workflow/0.2",
                properties={"name": "Workflow Run Crate", "version": "0.2"},
            ),
            rocrate.model.creativework.CreativeWork(
                self.crate,
                identifier="https://w3id.org/ro/wfrun/provenance/0.2",
                properties={"name": "Provenance Run Crate", "version": "0.2"},
            ),
            rocrate.model.creativework.CreativeWork(
                self.crate,
                identifier="https://w3id.org/workflowhub/workflow-ro-crate/1.0",
                properties={"name": "Workflow RO-Crate", "version": "1.0"},
            ),
        ]
        self.crate.add(*wrroc_profiles)
        self.crate.root_dataset.append_to("conformsTo", wrroc_profiles)

        # Now, WfExS reference as such
        wf_wfexs = rocrate.model.softwareapplication.SoftwareApplication(
            self.crate, identifier=wfexs_backend_url
        )
        wf_wfexs = self.crate.add(wf_wfexs)
        wf_wfexs["name"] = wfexs_backend_name
        wf_wfexs.url = wfexs_backend_url
        wf_wfexs.version = get_WfExS_version()

        return wf_wfexs

    def _add_containers_to_workflow(
        self,
        containers: "Sequence[Container]",
        weng_crate: "Optional[rocrate.model.softwareapplication.SoftwareApplication]" = None,
    ) -> None:
        # Operational containers are needed by the workflow engine, not by the workflow
        if len(containers) > 0:
            do_attach = CratableItem.Containers in self.payloads
            sa_crate: "Union[rocrate.model.computationalworkflow.ComputationalWorkflow, rocrate.model.softwareapplication.SoftwareApplication]"
            if weng_crate is not None:
                sa_crate = weng_crate
            else:
                sa_crate = self.wf_file
            for container in containers:
                crate_cont_type = self.cached_cts.get(container.type)
                if crate_cont_type is None:
                    container_type = (
                        rocrate.model.softwareapplication.SoftwareApplication(
                            self.crate, identifier=self.ContainerTypeIds[container.type]
                        )
                    )
                    container_type["name"] = container.type.value
                    if self.containerEngineVersion is not None:
                        container_type["softwareVersion"] = self.containerEngineVersion

                    crate_cont_type = self.crate.add(container_type)
                    self.wf_file.append_to("softwareRequirements", crate_cont_type)
                    self.cached_cts[container.type] = crate_cont_type

                if do_attach and container.localPath is not None:
                    the_size = os.stat(container.localPath).st_size
                    assert container.signature is not None
                    digest, algo = extract_digest(container.signature)
                    if digest is None:
                        digest, algo = unstringifyDigest(container.signature)
                    assert algo is not None
                    the_signature = hexDigest(algo, digest)

                    software_container = SoftwareContainer(
                        self.crate,
                        source=container.localPath,
                        dest_path=os.path.relpath(container.localPath, self.work_dir),
                        fetch_remote=False,
                        validate_url=False,
                        properties={
                            "contentSize": the_size,
                            "identifier": container.taggedName,
                            "sha256": the_signature,
                            "encodingFormat": magic.from_file(
                                container.localPath, mime=True
                            ),  # type: ignore[no-untyped-call]
                        },
                    )

                else:
                    container_pid = container.taggedName
                    software_container = (
                        rocrate.model.softwareapplication.SoftwareApplication(
                            self.crate, identifier=container_pid
                        )
                    )

                software_container["softwareVersion"] = container.fingerprint
                container_os = container.operatingSystem
                if container_os is None:
                    container_os = self.containerEngineOs
                if container_os is not None:
                    software_container["operatingSystem"] = container_os
                # Getting the processor architecture of the container
                container_arch = container.architecture
                if container_arch is None:
                    container_arch = self.arch
                if container_arch is not None:
                    software_container["processorRequirements"] = container_arch
                software_container["softwareRequirements"] = crate_cont_type

                crate_cont = self.crate.add(software_container)
                sa_crate.append_to("softwareRequirements", crate_cont)

    def addWorkflowInputs(
        self,
        inputs: "Sequence[MaterializedInput]",
        are_envvars: "bool" = False,
    ) -> "Sequence[rocrate.model.entity.Entity]":
        """
        Add the input's or environment variables provenance data to a Research Object.

        :param inputs: List of inputs to add
        :type inputs: Sequence[MaterializedInput]
        """
        crate_inputs = []
        do_attach = CratableItem.Inputs in self.payloads
        input_sep = "envvar" if are_envvars else "param"
        for in_item in inputs:
            formal_parameter_id = (
                f"{self.wf_file.id}#{input_sep}:"
                + urllib.parse.quote(in_item.name, safe="")
            )
            itemInValue0 = in_item.values[0]
            additional_type: "Optional[str]" = None
            if isinstance(itemInValue0, int):
                additional_type = "Integer"
            elif isinstance(itemInValue0, str):
                additional_type = "String"
            elif isinstance(itemInValue0, bool):
                additional_type = "Boolean"
            elif isinstance(itemInValue0, float):
                additional_type = "Float"
            elif isinstance(itemInValue0, MaterializedContent):
                if len(in_item.values) > 1:
                    additional_type = "Collection"
                elif itemInValue0.kind == ContentKind.File:
                    additional_type = "File"
                elif itemInValue0.kind == ContentKind.Directory:
                    additional_type = "Dataset"

            formal_parameter = FormalParameter(
                self.crate,
                name=in_item.name,
                identifier=formal_parameter_id,
                additional_type=additional_type,
            )
            self.crate.add(formal_parameter)
            # TODO: fix this at the standard level in some way
            # so it is possible in the future to distinguish among
            # inputs and environment variables in an standardized way
            self.wf_file.append_to("input", formal_parameter)

            crate_coll: "Union[Collection, FixedDataset, FixedFile, PropertyValue, None]"
            if len(in_item.values) > 1:
                crate_coll = self._add_collection_to_crate()
            else:
                crate_coll = None
            if additional_type in ("File", "Dataset", "Collection"):
                for itemInValues in cast(
                    "Sequence[MaterializedContent]", in_item.values
                ):
                    # TODO: embed metadata_array in some way
                    assert isinstance(itemInValues, MaterializedContent)
                    itemInLocalSource = itemInValues.local  # local source
                    itemInURISource = itemInValues.licensed_uri.uri  # uri source
                    if os.path.isfile(itemInLocalSource):
                        # This is needed to avoid including the input
                        crate_file = self._add_file_to_crate(
                            the_path=itemInLocalSource,
                            the_uri=itemInURISource,
                            the_name=cast(
                                "RelPath",
                                os.path.relpath(itemInLocalSource, self.work_dir),
                            ),
                            do_attach=do_attach,
                        )

                        if isinstance(crate_coll, Collection):
                            crate_coll.append_to("hasPart", crate_file)
                        else:
                            crate_coll = crate_file

                    elif os.path.isdir(itemInLocalSource):
                        crate_dataset, _ = self._add_directory_as_dataset(
                            itemInLocalSource,
                            itemInURISource,
                            the_name=cast(
                                "RelPath",
                                os.path.relpath(itemInLocalSource, self.work_dir) + "/",
                            ),
                            do_attach=do_attach,
                        )
                        # crate_dataset = self.crate.add_dataset(
                        #    source=itemInURISource,
                        #    fetch_remote=False,
                        #    validate_url=False,
                        #    do_attach=do_attach,
                        #    # properties=file_properties,
                        # )

                        if crate_dataset is not None:
                            if isinstance(crate_coll, Collection):
                                crate_coll.append_to("hasPart", crate_dataset)
                            else:
                                crate_coll = crate_dataset

                    else:
                        pass  # TODO: raise exception

            else:
                for itemInAtomicValues in cast(
                    "Sequence[Union[bool,str,float,int]]", in_item.values
                ):
                    assert isinstance(itemInAtomicValues, (bool, str, float, int))
                    parameter_value = PropertyValue(
                        self.crate, in_item.name, itemInAtomicValues
                    )
                    crate_pv = self.crate.add(parameter_value)
                    if isinstance(crate_coll, Collection):
                        crate_coll.append_to("hasPart", crate_pv)
                    else:
                        crate_coll = crate_pv

            # Avoiding corner cases
            if crate_coll is not None:
                # And now, let's process the secondary inputs
                if (
                    isinstance(in_item.secondaryInputs, list)
                    and len(in_item.secondaryInputs) > 0
                ):
                    sec_crate_coll = self._add_collection_to_crate(
                        main_entity=crate_coll
                    )

                    for secInput in in_item.secondaryInputs:
                        sec_crate_elem: "Union[FixedFile, FixedDataset, Collection, None]"

                        secInputLocalSource = secInput.local  # local source
                        secInputURISource = secInput.licensed_uri.uri  # uri source
                        if os.path.isfile(secInputLocalSource):
                            # This is needed to avoid including the input
                            sec_crate_elem = self._add_file_to_crate(
                                the_path=secInputLocalSource,
                                the_uri=secInputURISource,
                                the_name=cast(
                                    "RelPath",
                                    os.path.relpath(secInputLocalSource, self.work_dir),
                                ),
                                do_attach=do_attach,
                            )

                        elif os.path.isdir(secInputLocalSource):
                            sec_crate_elem, _ = self._add_directory_as_dataset(
                                secInputLocalSource,
                                secInputURISource,
                                do_attach=do_attach,
                            )
                            # crate_dataset = self.crate.add_dataset(
                            #    source=secInputURISource,
                            #    fetch_remote=False,
                            #    validate_url=False,
                            #    # properties=file_properties,
                            # )
                            the_sec_name = os.path.relpath(
                                secInputLocalSource, self.work_dir
                            )

                            if sec_crate_elem is not None:
                                sec_crate_elem["alternateName"] = the_sec_name + "/"
                        else:
                            sec_crate_elem = None

                        if sec_crate_elem is not None:
                            sec_crate_coll.append_to("hasPart", sec_crate_elem)

                    # Last, put it in place
                    crate_coll = sec_crate_coll

                crate_coll.append_to("exampleOfWork", formal_parameter)
                formal_parameter.append_to("workExample", crate_coll)
                crate_inputs.append(crate_coll)

            # TODO digest other types of inputs
        return crate_inputs

    def _add_file_to_crate(
        self,
        the_path: "str",
        the_uri: "Optional[URIType]",
        the_id: "Optional[str]" = None,
        the_name: "Optional[RelPath]" = None,
        the_alternate_name: "Optional[RelPath]" = None,
        the_size: "Optional[int]" = None,
        the_signature: "Optional[Fingerprint]" = None,
        do_attach: "bool" = True,
    ) -> "FixedFile":
        # The do_attach logic helps on the ill internal logic of add_file
        # when an id has to be assigned

        # assert do_attach or (the_id is not None), "We must provide an @id for non local files"
        assert not do_attach or (
            the_name is not None
        ), "A name must be provided for local files"

        # When the id is none and ...
        if the_id is None:
            the_id = the_name if do_attach or (the_uri is None) else the_uri

        the_file_crate = self.crate.add_file(
            identifier=the_id,
            source=the_path if do_attach else None,
            dest_path=the_name if do_attach else None,
        )
        if do_attach and (the_uri is not None):
            if the_uri.startswith("http") or the_uri.startswith("ftp"):
                # See https://github.com/ResearchObject/ro-crate/pull/259
                uri_key = "contentUrl"
            else:
                uri_key = "identifier"

            the_file_crate[uri_key] = the_uri
        if the_alternate_name is not None:
            the_file_crate["alternateName"] = the_alternate_name

        if the_size is None:
            the_size = os.stat(the_path).st_size
        if the_signature is None:
            the_signature = cast(
                "Fingerprint", ComputeDigestFromFile(the_path, repMethod=hexDigest)
            )
        the_file_crate.append_to("contentSize", the_size, compact=True)
        the_file_crate.append_to("sha256", the_signature, compact=True)
        the_file_crate.append_to(
            "encodingFormat",
            magic.from_file(the_path, mime=True),  # type: ignore[no-untyped-call]
            compact=True,
        )

        return the_file_crate

    def _add_collection_to_crate(
        self,
        main_entity: "Union[FixedFile, FixedDataset, None]" = None,
    ) -> "Collection":
        wf_coll = Collection(self.crate, main_entity)
        wf_coll = self.crate.add(wf_coll)

        return wf_coll

    def _add_directory_as_dataset(
        self,
        the_path: "str",
        the_uri: "URIType",
        the_id: "Optional[str]" = None,
        the_name: "Optional[RelPath]" = None,
        the_alternate_name: "Optional[RelPath]" = None,
        do_attach: "bool" = True,
    ) -> "Union[Tuple[FixedDataset, Sequence[FixedFile]], Tuple[None, None]]":
        # FUTURE IMPROVEMENT
        # Describe datasets referred from DOIs
        # as in https://github.com/ResearchObject/ro-crate/pull/255/files

        if not os.path.isdir(the_path):
            return None, None

        assert not do_attach or (
            the_name is not None
        ), "A name must be provided for local directories"

        # When the id is none and ...
        if the_id is None:
            the_id = the_name if do_attach or (the_uri is None) else the_uri

        the_files_crates: "MutableSequence[FixedFile]" = []
        crate_dataset = self.crate.add_dataset(
            identifier=the_id,
            source=the_path if do_attach else None,
            dest_path=the_name if do_attach else None,
            fetch_remote=False,
            validate_url=False,
            # properties=file_properties,
        )
        if do_attach and (the_uri is not None):
            if the_uri.startswith("http") or the_uri.startswith("ftp"):
                # See https://github.com/ResearchObject/ro-crate/pull/259
                uri_key = "contentUrl"
            else:
                uri_key = "identifier"

            crate_dataset[uri_key] = the_uri
        if the_alternate_name is not None:
            crate_dataset["alternateName"] = the_alternate_name

        # Now, recursively walk it
        with os.scandir(the_path) as the_dir:
            for the_file in the_dir:
                if the_file.name[0] == ".":
                    continue
                the_item_uri = cast(
                    "URIType",
                    the_uri + "/" + urllib.parse.quote(the_file.name, safe=""),
                )
                if the_file.is_file():
                    the_file_crate = self._add_file_to_crate(
                        the_path=the_file.path,
                        the_uri=the_item_uri,
                        the_size=the_file.stat().st_size,
                        do_attach=do_attach,
                    )

                    crate_dataset.append_to("hasPart", the_file_crate)

                    the_files_crates.append(the_file_crate)
                elif the_file.is_dir():
                    # TODO: fix URI handling
                    (
                        the_dir_crate,
                        the_subfiles_crates,
                    ) = self._add_directory_as_dataset(
                        the_path=the_file.path,
                        the_uri=the_item_uri,
                        do_attach=do_attach,
                    )
                    if the_dir_crate is not None:
                        assert the_subfiles_crates is not None
                        crate_dataset.append_to("hasPart", the_dir_crate)
                        crate_dataset.append_to("hasPart", the_subfiles_crates)

                        the_files_crates.extend(the_subfiles_crates)

        return crate_dataset, the_files_crates

    def addWorkflowExpectedOutputs(
        self,
        outputs: "Sequence[ExpectedOutput]",
    ) -> None:
        for out_item in outputs:
            formal_parameter_id = (
                self.wf_file.id
                + "#output:"
                + urllib.parse.quote(out_item.name, safe="")
            )
            if out_item.kind == ContentKind.File:
                additional_type = "File"
            elif out_item.kind == ContentKind.Directory:
                additional_type = "Dataset"
            else:
                additional_type = None

            formal_parameter = FormalParameter(
                self.crate,
                name=out_item.name,
                identifier=formal_parameter_id,
                additional_type=additional_type,
            )
            self.crate.add(formal_parameter)
            self.wf_file.append_to("output", formal_parameter)

    def writeWRROC(self, filename: "AnyPath") -> None:
        with warnings.catch_warnings():
            # Disable possible warnings emitted by rocrate-py library
            # when it is not run in debug mode
            if self.logger.getEffectiveLevel() > logging.DEBUG:
                warnings.filterwarnings(
                    "ignore", category=UserWarning, module="^rocrate\.model\.file$"
                )
            self.crate.write_zip(filename)

    def addWorkflowExecution(
        self,
        stagedExec: "StagedExecution",
    ) -> None:
        # TODO: Add a new CreateAction for each stagedExec
        # as it is explained at https://www.researchobject.org/workflow-run-crate/profiles/workflow_run_crate
        assert self.staged_setup.inputs_dir is not None

        outputsDir = cast(
            "AbsPath",
            os.path.normpath(os.path.join(self.work_dir, stagedExec.outputsDir)),
        )

        crate_action = CreateAction(
            self.crate,
            "Run " + stagedExec.outputsDir + " of " + self.wf_file.id,
            stagedExec.started,
            stagedExec.ended,
        )
        self.crate.add(crate_action)
        self.crate.root_dataset.append_to("mentions", crate_action)
        crate_action["instrument"] = self.wf_file
        # subjectOf is not fulfilled as this execution has not public page

        crate_inputs = self.addWorkflowInputs(
            stagedExec.augmentedInputs,
        )
        crate_action["object"] = crate_inputs

        control_action = ControlAction(
            self.crate,
            "Orchestration of " + self.wf_file.id + " for" + stagedExec.outputsDir,
        )
        self.crate.add(control_action)
        control_action["instrument"] = self.wf_file
        control_action["object"] = crate_action

        # TODO: Add engine specific traces
        # see https://www.researchobject.org/workflow-run-crate/profiles/workflow_run_crate#adding-engine-specific-traces
        # TODO: Add "augmented environment variables"

        crate_outputs = self._add_workflow_execution_outputs(
            stagedExec.matCheckOutputs,
            rel_work_dir=stagedExec.outputsDir,
        )
        crate_action["result"] = crate_outputs

        org_action = OrganizeAction(
            self.crate,
            "Orchestration of " + stagedExec.outputsDir + " from " + self.wf_file.id,
            stagedExec.started,
            stagedExec.ended,
        )
        self.crate.add(org_action)
        org_action["agent"] = self.wf_wfexs
        # The used workflow engine
        org_action["instrument"] = self.weng_crate

        org_action.append_to("object", control_action)
        # TODO: add configuration files (if available) to object
        org_action["result"] = crate_action

    def _add_workflow_execution_outputs(
        self,
        outputs: "Sequence[MaterializedOutput]",
        rel_work_dir: "RelPath",
    ) -> "Sequence[rocrate.model.entity.Entity]":
        """
        Add the output's provenance data to a Research Object.

        :param outputs: List of outputs to add
        :type outputs: Sequence[MaterializedOutput]
        """
        do_attach = CratableItem.Outputs in self.payloads
        crate_outputs: "MutableSequence[rocrate.model.entity.Entity]" = []
        for out_item in outputs:
            formal_parameter_id = (
                self.wf_file.id
                + "#output:"
                + urllib.parse.quote(out_item.name, safe="")
            )
            if out_item.kind == ContentKind.File:
                additional_type = "Collection" if len(out_item.values) > 1 else "File"
            elif out_item.kind == ContentKind.Directory:
                additional_type = (
                    "Collection" if len(out_item.values) > 1 else "Dataset"
                )
            elif len(out_item.values) > 0:
                itemOutValue0 = out_item.values[0]
                if isinstance(itemOutValue0, int):
                    additional_type = "Integer"
                elif isinstance(itemOutValue0, str):
                    additional_type = "String"
                elif isinstance(itemOutValue0, bool):
                    additional_type = "Boolean"
                elif isinstance(itemOutValue0, float):
                    additional_type = "Float"
            else:
                additional_type = None

            formal_parameter = FormalParameter(
                self.crate,
                name=out_item.name,
                identifier=formal_parameter_id,
                additional_type=additional_type,
            )
            self.crate.add(formal_parameter)
            self.wf_file.append_to("output", formal_parameter)

            # This can happen when there is no output, like when a workflow has failed
            if len(out_item.values) == 0:
                continue

            if additional_type in ("File", "Dataset", "Collection"):
                crate_coll: "Union[Collection, FixedDataset, FixedFile, None]"
                if len(out_item.values) > 1:
                    crate_coll = self._add_collection_to_crate()
                else:
                    crate_coll = None
                for itemOutValues in cast(
                    "Sequence[AbstractGeneratedContent]", out_item.values
                ):
                    if not isinstance(
                        itemOutValues, (GeneratedContent, GeneratedDirectoryContent)
                    ):
                        self.logger.error("FIXME: elements of incorrect types")

                    assert isinstance(
                        itemOutValues, (GeneratedContent, GeneratedDirectoryContent)
                    )

                    itemOutLocalSource = itemOutValues.local  # local source
                    # TODO: use exported results logs to complement this
                    itemOutURISource = None
                    if isinstance(
                        itemOutValues, GeneratedDirectoryContent
                    ):  # if directory
                        if os.path.isdir(itemOutLocalSource):
                            (
                                crate_dataset,
                                _,
                            ) = self._add_GeneratedDirectoryContent_as_dataset(
                                itemOutValues,
                                rel_work_dir=rel_work_dir,
                                do_attach=do_attach,
                            )

                            if crate_dataset is not None:
                                if isinstance(crate_coll, Collection):
                                    crate_coll.append_to("hasPart", crate_dataset)
                                else:
                                    crate_coll = crate_dataset

                        else:
                            errmsg = (
                                "ERROR: The output directory %s does not exist"
                                % itemOutLocalSource
                            )
                            self.logger.error(errmsg)

                    elif isinstance(itemOutValues, GeneratedContent):  # file
                        if os.path.isfile(itemOutLocalSource):
                            crate_file = self._add_GeneratedContent_to_crate(
                                itemOutValues,
                                rel_work_dir=rel_work_dir,
                                do_attach=do_attach,
                            )

                            if isinstance(crate_coll, Collection):
                                crate_coll.append_to("hasPart", crate_file)
                            else:
                                crate_coll = crate_file

                        else:
                            errmsg = (
                                "ERROR: The output file %s does not exist"
                                % itemOutLocalSource
                            )
                            self.logger.error(errmsg)

                    else:
                        pass
                        # TODO digest other types of outputs

                # Last rites to set all of them properly
                if crate_coll is not None:
                    if (
                        isinstance(crate_coll, Collection)
                        and additional_type != "Collection"
                    ):
                        formal_parameter["additionalType"] = "Collection"

                    crate_coll.append_to("exampleOfWork", formal_parameter)
                    formal_parameter.append_to("workExample", crate_coll)
                    crate_outputs.append(crate_coll)

        return crate_outputs

    def _add_GeneratedContent_to_crate(
        self,
        the_content: "GeneratedContent",
        rel_work_dir: "RelPath",
        do_attach: "bool" = True,
    ) -> "Union[FixedFile, Collection]":
        assert the_content.signature is not None

        digest, algo = extract_digest(the_content.signature)
        if digest is None:
            digest, algo = unstringifyDigest(the_content.signature)
        assert algo is not None
        dest_path = os.path.relpath(the_content.local, self.work_dir)
        # dest_path = hexDigest(algo, digest)

        alternateName = os.path.relpath(
            the_content.local, os.path.join(self.work_dir, rel_work_dir)
        )

        if the_content.uri is not None and not the_content.uri.uri.startswith("nih:"):
            the_content_uri = the_content.uri.uri
        else:
            the_content_uri = None

        crate_file = self._add_file_to_crate(
            the_path=the_content.local,
            the_uri=the_content_uri,
            the_name=cast("RelPath", dest_path),
            the_alternate_name=cast("RelPath", alternateName),
            the_signature=hexDigest(algo, digest),
            do_attach=do_attach,
        )

        # The corner case of output files with secondary files
        if (
            isinstance(the_content.secondaryFiles, list)
            and len(the_content.secondaryFiles) > 0
        ):
            crate_coll = self._add_collection_to_crate(main_entity=crate_file)

            for secFile in the_content.secondaryFiles:
                gen_content: "Union[FixedFile, FixedDataset]"
                if isinstance(secFile, GeneratedContent):
                    gen_content = self._add_GeneratedContent_to_crate(
                        secFile,
                        rel_work_dir=rel_work_dir,
                        do_attach=do_attach,
                    )
                else:
                    # elif isinstance(secFile, GeneratedDirectoryContent):
                    gen_dir_content, _ = self._add_GeneratedDirectoryContent_as_dataset(
                        secFile,
                        rel_work_dir=rel_work_dir,
                        do_attach=do_attach,
                    )
                    assert gen_dir_content is not None
                    gen_content = gen_dir_content

                crate_coll.append_to("hasPart", gen_content)

            return crate_coll
        else:
            return crate_file

    def _add_GeneratedDirectoryContent_as_dataset(
        self,
        the_content: "GeneratedDirectoryContent",
        rel_work_dir: "RelPath",
        do_attach: "bool" = True,
    ) -> "Union[Tuple[Union[FixedDataset, Collection], Sequence[FixedFile]], Tuple[None, None]]":
        if os.path.isdir(the_content.local):
            the_files_crates: "MutableSequence[FixedFile]" = []

            the_uri = the_content.uri.uri if the_content.uri is not None else None
            dest_path = os.path.relpath(the_content.local, self.work_dir) + "/"
            if do_attach or (the_uri is None):
                the_id = dest_path
            else:
                the_id = the_uri
            # if the_uri is not None:
            #    an_uri = the_uri
            #    dest_path = None
            # else:
            #    an_uri = None
            #    dest_path = os.path.relpath(the_content.local, self.work_dir)
            #    # digest, algo = extract_digest(the_content.signature)
            #    # dest_path = hexDigest(algo, digest)

            crate_dataset = self.crate.add_dataset(
                identifier=the_id,
                source=the_content.local if do_attach else None,
                dest_path=dest_path if do_attach else None,
                fetch_remote=False,
                validate_url=False,
                # properties=file_properties,
            )

            if do_attach and (the_uri is not None):
                if the_uri.startswith("http") or the_uri.startswith("ftp"):
                    # See https://github.com/ResearchObject/ro-crate/pull/259
                    uri_key = "contentUrl"
                else:
                    uri_key = "identifier"

                crate_dataset[uri_key] = the_uri
            alternateName = (
                os.path.relpath(
                    the_content.local, os.path.join(self.work_dir, rel_work_dir)
                )
                + "/"
            )
            crate_dataset["alternateName"] = alternateName

            if isinstance(the_content.values, list):
                for the_val in the_content.values:
                    if isinstance(the_val, GeneratedContent):
                        the_val_file = self._add_GeneratedContent_to_crate(
                            the_val,
                            rel_work_dir=rel_work_dir,
                            do_attach=do_attach,
                        )
                        crate_dataset.append_to("hasPart", the_val_file)
                        the_files_crates.append(the_val_file)
                    elif isinstance(the_val, GeneratedDirectoryContent):
                        (
                            the_val_dataset,
                            the_subfiles_crates,
                        ) = self._add_GeneratedDirectoryContent_as_dataset(
                            the_val,
                            rel_work_dir=rel_work_dir,
                            do_attach=do_attach,
                        )
                        if the_val_dataset is not None:
                            assert the_subfiles_crates is not None
                            crate_dataset.append_to("hasPart", the_val_dataset)
                            crate_dataset.append_to("hasPart", the_subfiles_crates)

                            the_files_crates.extend(the_subfiles_crates)

            # The very corner case of output directories with secondary files
            if (
                isinstance(the_content.secondaryFiles, list)
                and len(the_content.secondaryFiles) > 0
            ):
                crate_coll = self._add_collection_to_crate(main_entity=crate_dataset)

                for secFile in the_content.secondaryFiles:
                    gen_content: "Union[FixedFile, FixedDataset]"
                    if isinstance(secFile, GeneratedContent):
                        gen_content = self._add_GeneratedContent_to_crate(
                            secFile,
                            rel_work_dir=rel_work_dir,
                            do_attach=do_attach,
                        )
                    else:
                        # elif isinstance(secFile, GeneratedDirectoryContent):
                        (
                            gen_dir_content,
                            _,
                        ) = self._add_GeneratedDirectoryContent_as_dataset(
                            secFile,
                            rel_work_dir=rel_work_dir,
                            do_attach=do_attach,
                        )
                        assert gen_dir_content is not None
                        gen_content = gen_dir_content

                    crate_coll.append_to("hasPart", gen_content)

                return crate_coll, the_files_crates
            else:
                return crate_dataset, the_files_crates

        return None, None
