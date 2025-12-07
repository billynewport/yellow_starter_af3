"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1

This is a starter datasurface repository. It defines a simple Ecosystem using YellowDataPlatform with Live and Forensic modes.
It will generate 2 pipelines, one with live records only and the other with full milestoning.
"""

from datasurface.md import (
    Team, GovernanceZone, DataTransformer, Ecosystem, LocationKey, Credential,
    PlainTextDocumentation, WorkspacePlatformConfig, Datastore, Dataset,
    IngestionConsistencyType, ConsumerRetentionRequirements, DataMilestoningStrategy, DataLatency, DDLTable, DDLColumn, NullableStatus,
    PrimaryKeyStatus, VarChar, Date, Workspace, DatasetSink, DatasetGroup, PostgresDatabase, TeamDeclaration,
    EnvironmentMap, EnvRefDataContainer, ProductionStatus, DataPlatformManagedDataContainer
)
from datasurface.md.triggers import CronTrigger
from datasurface.md.containers import (
    SQLSnapshotIngestion, HostPortPair
)
from datasurface.md.credential import CredentialType
from datasurface.md.repo import GitHubRepository, VersionedRepository
from datasurface.md.policy import SimpleDC, SimpleDCTypes
from datasurface.md.codeartifact import PythonRepoCodeArtifact
from datasurface.md.repo import EnvRefReleaseSelector
from datasurface.md.repo import VersionPatternReleaseSelector, VersionPatterns, ReleaseType

GH_REPO_OWNER: str = "billynewport"  # Change to your github username
GH_REPO_NAME: str = "yellow_starter_af3"  # Change to your github repository name containing this project
GH_DT_REPO_NAME: str = "yellow_starter_af3"  # For now, we use the same repo for the transformer


def createTeam(ecosys: Ecosystem, git: Credential) -> Team:
    gz: GovernanceZone = ecosys.getZoneOrThrow("USA")
    gz.add(TeamDeclaration(
        "team1",
        GitHubRepository(f"{GH_REPO_OWNER}/{GH_REPO_NAME}", "team1", credential=ecosys.owningRepo.credential)
        ))

    team: Team = gz.getTeamOrThrow("team1")
    team.add(EnvironmentMap(
        keyword="prod",
        dataContainers={
            frozenset(["customer_db"]): PostgresDatabase(
                    "CustomerDB",  # Model name for database
                    hostPort=HostPortPair("postgres", 5432),
                    locations={LocationKey("MyCorp:USA/NY_1")},  # Locations for database
                    productionStatus=ProductionStatus.PRODUCTION,
                    databaseName="customer_db"  # Database name
                )
            },
        dtReleaseSelectors={
            "custMaskRev": VersionPatternReleaseSelector(VersionPatterns.VN_N_N+"-prod", ReleaseType.STABLE_ONLY)
        }
        ))
    team.add(EnvironmentMap(
        keyword="uat",
        dataContainers={
            frozenset(["customer_db"]): PostgresDatabase(
                    "CustomerDB",  # Model name for database
                    hostPort=HostPortPair("postgres", 5432),  # Sharing database in demo, would normally be a different database
                    locations={LocationKey("MyCorp:USA/NY_1")},  # Locations for database
                    productionStatus=ProductionStatus.NOT_PRODUCTION,
                    databaseName="customer_db-uat"  # Database name
                )
            },
        dtReleaseSelectors={
            "custMaskRev": VersionPatternReleaseSelector(VersionPatterns.VN_N_N+"-uat", ReleaseType.STABLE_ONLY)
        }
        ))
    team.add(
        Datastore(
            "Store1",
            documentation=PlainTextDocumentation("Test datastore"),
            capture_metadata=SQLSnapshotIngestion(
                EnvRefDataContainer("customer_db"),
                CronTrigger("Every 5 minute", "*/1 * * * *"),  # Cron trigger for ingestion
                IngestionConsistencyType.MULTI_DATASET,  # Ingestion consistency type
                Credential("postgres", CredentialType.USER_PASSWORD),  # Credential for platform to read from database
                ),
            datasets=[
                Dataset(
                    "customers",
                    schema=DDLTable(
                        columns=[
                            DDLColumn("id", VarChar(20), nullable=NullableStatus.NOT_NULLABLE, primary_key=PrimaryKeyStatus.PK),
                            DDLColumn("firstname", VarChar(100), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("lastname", VarChar(100), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("dob", Date(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("email", VarChar(100)),
                            DDLColumn("phone", VarChar(100)),
                            DDLColumn("primaryaddressid", VarChar(20)),
                            DDLColumn("billingaddressid", VarChar(20))
                        ]
                    ),
                    classifications=[SimpleDC(SimpleDCTypes.CPI, "Customer")]
                ),
                Dataset(
                    "addresses",
                    schema=DDLTable(
                        columns=[
                            DDLColumn("id", VarChar(20), nullable=NullableStatus.NOT_NULLABLE, primary_key=PrimaryKeyStatus.PK),
                            DDLColumn("customerid", VarChar(20), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("streetname", VarChar(100), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("city", VarChar(100), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("state", VarChar(100), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("zipcode", VarChar(30), nullable=NullableStatus.NOT_NULLABLE)
                        ]
                    ),
                    classifications=[SimpleDC(SimpleDCTypes.CPI, "Address")]
                )
            ]
        ),
        Workspace(
            "Consumer1",
            DataPlatformManagedDataContainer("Consumer1 container"),
            DatasetGroup(
                "LiveDSG",
                sinks=[
                    DatasetSink("Store1", "customers"),
                    DatasetSink("Store1", "addresses"),
                    DatasetSink("MaskedCustomers", "customers")
                ],
                platform_chooser=WorkspacePlatformConfig(
                    hist=ConsumerRetentionRequirements(
                        r=DataMilestoningStrategy.SCD1,
                        latency=DataLatency.MINUTES,
                        regulator=None
                    )
                ),
            ),
            DatasetGroup(
                "ForensicDSG",
                sinks=[
                    DatasetSink("Store1", "customers"),
                    DatasetSink("Store1", "addresses"),
                    DatasetSink("MaskedCustomers", "customers")
                ],
                platform_chooser=WorkspacePlatformConfig(
                    hist=ConsumerRetentionRequirements(
                        r=DataMilestoningStrategy.SCD2,
                        latency=DataLatency.MINUTES,
                        regulator=None
                    )
                )
            )
        ),
        Workspace(
            "MaskedStoreGenerator",
            DataPlatformManagedDataContainer("MaskedStoreGenerator container"),
            DatasetGroup(
                "Original",
                sinks=[DatasetSink("Store1", "customers")]
            ),
            DataTransformer(
                name="MaskedCustomerGenerator",
                code=PythonRepoCodeArtifact(
                    VersionedRepository(
                        GitHubRepository(f"{GH_REPO_OWNER}/{GH_DT_REPO_NAME}", "main", credential=git),
                        EnvRefReleaseSelector("custMaskRev")
                    )
                ),
                runAsCredential=Credential("mask_dt_cred", CredentialType.USER_PASSWORD),
                trigger=CronTrigger("Every 1 minute", "*/1 * * * *"),
                store=Datastore(
                    name="MaskedCustomers",
                    documentation=PlainTextDocumentation("MaskedCustomers datastore"),
                    datasets=[
                        Dataset(
                            "customers",
                            schema=DDLTable(
                                columns=[
                                    DDLColumn("id", VarChar(20), nullable=NullableStatus.NOT_NULLABLE, primary_key=PrimaryKeyStatus.PK),
                                    DDLColumn("firstname", VarChar(100), nullable=NullableStatus.NOT_NULLABLE),
                                    DDLColumn("lastname", VarChar(100), nullable=NullableStatus.NOT_NULLABLE),
                                    DDLColumn("dob", Date(), nullable=NullableStatus.NOT_NULLABLE),
                                    DDLColumn("email", VarChar(100)),
                                    DDLColumn("phone", VarChar(100)),
                                    DDLColumn("primaryaddressid", VarChar(20)),
                                    DDLColumn("billingaddressid", VarChar(20))
                                ]
                            ),
                            classifications=[SimpleDC(SimpleDCTypes.PUB, "Customer")]
                        )
                    ]
                )
            )
        )
    )
    return team
