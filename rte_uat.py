"""
Copyright (c) 2025 DataSurface Inc. All Rights Reserved.
Proprietary Software - See LICENSE.txt for terms.

This is a starter datasurface repository. It defines a simple Ecosystem using YellowDataPlatform with Live and Forensic modes. It
ingests data from a single source, using a Workspace to produce a masked version of that data and provides consumer Workspaces
to that data in Postgres, SQLServer, Oracle and DB2 using CQRS.

It will generate 2 pipelines, one with live records only (SCD1) and the other with full milestoning (SCD2).
"""

from datasurface.md import DataTransformerExecutionPlacement, LocationKey
from datasurface.md.containers import HostPortPair
from datasurface.md.credential import Credential, CredentialType
from datasurface.md.documentation import PlainTextDocumentation
from datasurface.md import StorageRequirement, ProductionStatus
from datasurface.platforms.yellow import YellowDataPlatform, YellowPlatformServiceProvider, K8sResourceLimits
from datasurface.md.governance import DataMilestoningStrategy
from datasurface.md.triggers import CronTrigger
from datasurface.md import PostgresDatabase, ConsumerReplicaGroup, RuntimeEnvironment, Ecosystem, PSPDeclaration
from datasurface.platforms.yellow.assembly import GitCacheConfig, YellowExternalAirflow3AndMergeDatabase
from datasurface.md.containers import SQLServerDatabase
from datasurface.platforms.yellow.yellow_dp import K8sDataTransformerHint
from datasurface.md.repo import VersionPatternReleaseSelector, GitHubRepository, ReleaseType, VersionPatterns

# UAT environment configuration - separate namespace and databases from prod
UAT_KUB_NAME_SPACE: str = "yp-airflow3-uat"
UAT_AIRFLOW_SERVICE_ACCOUNT: str = "airflow-worker"
POSTGRES_HOST: str = "postgres-co"
UAT_MERGE_DB_NAME: str = "merge_db_af3_uat"
UAT_CQRS_DB_NAME: str = "postgres-cqrs-af3-uat"


def createPSP() -> YellowPlatformServiceProvider:
    # Kubernetes merge database configuration
    k8s_merge_datacontainer: PostgresDatabase = PostgresDatabase(
        "K8sMergeDB",  # Container name for Kubernetes deployment
        hostPort=HostPortPair(POSTGRES_HOST, 5432),
        locations={LocationKey("MyCorp:USA/NY_1")},  # Kubernetes cluster location
        productionStatus=ProductionStatus.NOT_PRODUCTION,
        databaseName=UAT_MERGE_DB_NAME
    )

    git_config: GitCacheConfig = GitCacheConfig(
        enabled=True,
        access_mode="ReadWriteMany",
        storageClass="longhorn"
    )
    yp_assembly: YellowExternalAirflow3AndMergeDatabase = YellowExternalAirflow3AndMergeDatabase(
        name="Test_DP_UAT",
        namespace=UAT_KUB_NAME_SPACE,
        roMergeCRGCredential=Credential("postgres", CredentialType.USER_PASSWORD),
        git_cache_config=git_config,
        afHostPortPair=HostPortPair(POSTGRES_HOST, 5432),
        airflowServiceAccount=UAT_AIRFLOW_SERVICE_ACCOUNT
    )

    psp: YellowPlatformServiceProvider = YellowPlatformServiceProvider(
        "Test_DP_UAT",
        {LocationKey("MyCorp:USA/NY_1")},
        PlainTextDocumentation("Test UAT"),
        gitCredential=Credential("git", CredentialType.API_TOKEN),
        connectCredentials=Credential("connect", CredentialType.API_TOKEN),
        mergeRW_Credential=Credential("postgres", CredentialType.USER_PASSWORD),
        yp_assembly=yp_assembly,
        merge_datacontainer=k8s_merge_datacontainer,
        datasurfaceDockerImage="datasurface/datasurface:v0.4.11",
        pv_storage_class="longhorn",
        dataPlatforms=[
            YellowDataPlatform(
                name="SCD1_UAT",
                doc=PlainTextDocumentation("SCD1 Yellow DataPlatform UAT"),
                milestoneStrategy=DataMilestoningStrategy.SCD1
                ),
            YellowDataPlatform(
                "SCD2_UAT",
                doc=PlainTextDocumentation("SCD2 Yellow DataPlatform UAT"),
                milestoneStrategy=DataMilestoningStrategy.SCD2
                )
        ],
        consumerReplicaGroups=[
            ConsumerReplicaGroup(
                name="postgres",
                dataContainers={
                    PostgresDatabase(
                        "Postgres",
                        hostPort=HostPortPair(POSTGRES_HOST, 5432),
                        locations={LocationKey("MyCorp:USA/NY_1")},
                        productionStatus=ProductionStatus.NOT_PRODUCTION,
                        databaseName=UAT_CQRS_DB_NAME
                    )
                },
                workspaceNames={"Consumer1"},
                trigger=CronTrigger("Every 5 minute", "*/5 * * * *"),
                credential=Credential("postgres", CredentialType.USER_PASSWORD)
            ),
            ConsumerReplicaGroup(
                name="SQLServer",
                dataContainers={
                    SQLServerDatabase(
                        "SQLServer-uat",
                        hostPort=HostPortPair("sqlserver-co", 1433),
                        locations={LocationKey("MyCorp:USA/NY_1")},
                        productionStatus=ProductionStatus.NOT_PRODUCTION,
                        databaseName="cqrs-uat"
                    )
                },
                workspaceNames={"Consumer1", "MaskedStoreGenerator"},
                trigger=CronTrigger("Every 5 minute", "*/5 * * * *"),
                credential=Credential("sa", CredentialType.USER_PASSWORD)
            )
        ],
        hints=[
            # Run the MaskedCustomer data transformer on the SQLServer consumer replica group
            K8sDataTransformerHint(
                workspaceName="MaskedStoreGenerator",
                kv={},
                resourceLimits=K8sResourceLimits(
                    requested_memory=StorageRequirement("1G"),
                    limits_memory=StorageRequirement("2G"),
                    requested_cpu=1.0,
                    limits_cpu=2.0
                ),
                executionPlacement=DataTransformerExecutionPlacement(
                    crgName="SQLServer",
                    dcName="SQLServer-uat"
                )
            )
        ]
    )
    return psp


def createUATRTE(ecosys: Ecosystem) -> RuntimeEnvironment:
    assert isinstance(ecosys.owningRepo, GitHubRepository)

    psp: YellowPlatformServiceProvider = createPSP()
    rte: RuntimeEnvironment = ecosys.getRuntimeEnvironmentOrThrow("uat")
    # Allow edits using RTE repository
    rte.configure(VersionPatternReleaseSelector(
        VersionPatterns.VN_N_N+"-uat", ReleaseType.ALL),
        [PSPDeclaration(psp.name, rte.owningRepo)],
        ProductionStatus.NOT_PRODUCTION)
    rte.setPSP(psp)
    return rte
