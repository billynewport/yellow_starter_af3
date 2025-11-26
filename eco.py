"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1

This is a starter datasurface repository. It defines a simple Ecosystem using YellowDataPlatform with Live and Forensic modes.
It will generate 2 pipelines, one with live records only and the other with full milestoning.
"""

from datasurface.md import InfrastructureVendor, InfrastructureLocation
from datasurface.md import Ecosystem
from datasurface.md.credential import Credential, CredentialType
from datasurface.md.documentation import PlainTextDocumentation
from datasurface.md.repo import GitHubRepository
from datasurface.md import CloudVendor, RuntimeDeclaration
from datasurface.md import ValidationTree
from datasurface.md.model_schema import addDatasurfaceModel
from gz import createGZ
from rte_prod import createProdRTE
from rte_uat import createUATRTE

GH_REPO_OWNER: str = "billynewport"  # Change to your github username
GH_REPO_NAME: str = "yellow_starter"  # Change to your github repository name containing this project
GH_DT_REPO_NAME: str = "yellow_starter"  # For now, we use the same repo for the transformer


def createEcosystem() -> Ecosystem:
    """This is a very simple test model with a single datastore and dataset.
    It is used to test the YellowDataPlatform. We are using a monorepo approach
    so all the model fragments use the same owning repository.
    """

    git: Credential = Credential("git", CredentialType.API_TOKEN)
    eRepo: GitHubRepository = GitHubRepository(f"{GH_REPO_OWNER}/{GH_REPO_NAME}", "main_edit", credential=git)

    ecosys: Ecosystem = Ecosystem(
        name="YellowStarter",
        repo=eRepo,
        runtimeDecls=[
            RuntimeDeclaration("prod", GitHubRepository(eRepo.repositoryName, "prod_rte_edit", credential=git)),
            RuntimeDeclaration("uat", GitHubRepository(eRepo.repositoryName, "uat_rte_edit", credential=git))
        ],
        infrastructure_vendors=[
            # Onsite data centers
            InfrastructureVendor(
                name="MyCorp",
                cloud_vendor=CloudVendor.PRIVATE,
                documentation=PlainTextDocumentation("Private company data centers"),
                locations=[
                    InfrastructureLocation(
                        name="USA",
                        locations=[
                            InfrastructureLocation(name="NY_1")
                        ]
                    )
                ]
            )
        ],
        liveRepo=GitHubRepository(f"{GH_REPO_OWNER}/{GH_REPO_NAME}", "main", credential=git)
    )
    # Define the prod RTE
    createProdRTE(ecosys)
    createUATRTE(ecosys)
    # Add the system models to the ecosystem. They can be modified by the ecosystem repository owners.
    addDatasurfaceModel(ecosys, ecosys.owningRepo)

    # Add the governance zone and associated teamsto the ecosystem.
    createGZ(ecosys, git)

    _: ValidationTree = ecosys.lintAndHydrateCaches()
    return ecosys
