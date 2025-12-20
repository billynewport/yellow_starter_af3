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
from datasurface.md import StorageRequirement, ProductionStatus, DeprecationStatus
from datasurface.platforms.yellow import YellowDataPlatform, YellowPlatformServiceProvider, K8sResourceLimits
from datasurface.md.governance import DataMilestoningStrategy, DeprecationInfo
from datasurface.md import PostgresDatabase, ConsumerReplicaGroup, RuntimeEnvironment, Ecosystem, PSPDeclaration
from datasurface.md.triggers import CronTrigger
from datasurface.platforms.yellow.assembly import GitCacheConfig, YellowExternalAirflow3AndMergeDatabase
from datasurface.md.containers import SQLServerDatabase
from datasurface.platforms.yellow.yellow_dp import K8sDataTransformerHint, DataTransformerDockerImage
from datasurface.md.repo import VersionPatternReleaseSelector, GitHubRepository, ReleaseType, VersionPatterns
from datasurface.platforms.yellow.yellow_kafka_publisher import KafkaEventPublishConfig

# Production environment configuration - matches kub-test Airflow 3.x setup
KUB_NAME_SPACE: str = "yp-airflow3"
AIRFLOW_SERVICE_ACCOUNT: str = "airflow-worker"
POSTGRES_HOST: str = "postgres-co"
MERGE_DB_NAME: str = "merge_db_af3"
CQRS_DB_NAME: str = "postgres-cqrs-af3"


def createPSP() -> YellowPlatformServiceProvider:
    # Kubernetes merge database configuration
    k8s_merge_datacontainer: PostgresDatabase = PostgresDatabase(
        "K8sMergeDB",  # Container name for Kubernetes deployment
        hostPort=HostPortPair(POSTGRES_HOST, 5432),
        locations={LocationKey("MyCorp:USA/NY_1")},  # Kubernetes cluster location
        productionStatus=ProductionStatus.PRODUCTION,
        databaseName=MERGE_DB_NAME
    )

    git_config: GitCacheConfig = GitCacheConfig(
        enabled=True,
        access_mode="ReadWriteMany",
        storageClass="longhorn"
    )
    yp_assembly: YellowExternalAirflow3AndMergeDatabase = YellowExternalAirflow3AndMergeDatabase(
        name="Test_DP",
        namespace=KUB_NAME_SPACE,
        roMergeCRGCredential=Credential("postgres", CredentialType.USER_PASSWORD),
        git_cache_config=git_config,
        afHostPortPair=HostPortPair(POSTGRES_HOST, 5432),
        airflowServiceAccount=AIRFLOW_SERVICE_ACCOUNT
    )

    psp: YellowPlatformServiceProvider = YellowPlatformServiceProvider(
        "Test_DP",
        {LocationKey("MyCorp:USA/NY_1")},
        PlainTextDocumentation("Test"),
        gitCredential=Credential("git", CredentialType.API_TOKEN),
        connectCredentials=Credential("connect", CredentialType.API_TOKEN),
        mergeRW_Credential=Credential("postgres", CredentialType.USER_PASSWORD),
        yp_assembly=yp_assembly,
        merge_datacontainer=k8s_merge_datacontainer,
        pv_storage_class="longhorn",
        datasurfaceDockerImage="datasurface/datasurface:v0.5.17",
        dataPlatforms=[
            YellowDataPlatform(
                name="SCD1",
                doc=PlainTextDocumentation("SCD1 Yellow DataPlatform"),
                milestoneStrategy=DataMilestoningStrategy.SCD1,
                stagingBatchesToKeep=5
                ),
            YellowDataPlatform(
                "SCD2",
                doc=PlainTextDocumentation("SCD2 Yellow DataPlatform"),
                milestoneStrategy=DataMilestoningStrategy.SCD2,
                stagingBatchesToKeep=5
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
                        productionStatus=ProductionStatus.PRODUCTION,
                        databaseName=CQRS_DB_NAME
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
                        "SQLServer",
                        hostPort=HostPortPair("sqlserver-co", 1433),
                        locations={LocationKey("MyCorp:USA/NY_1")},
                        productionStatus=ProductionStatus.PRODUCTION,
                        databaseName="cqrs"
                    )
                },
                workspaceNames={"Consumer1", "MaskedStoreGenerator", "DBT_MaskedStoreGenerator"},
                trigger=CronTrigger("Every 5 minute", "*/5 * * * *"),
                credential=Credential("sa", CredentialType.USER_PASSWORD)
            ),
        ],
        eventPublishConfig=KafkaEventPublishConfig(
            topicPrefix="datasurface_prod",
            bootstrapServers="co-redpanda:9092",
            credential=Credential("datasurface-kafka-prod-publisher", CredentialType.USER_PASSWORD)
        ),
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
                    dcName="SQLServer"
                )
            ),
            # Run the MaskedCustomer data transformer on the SQLServer consumer replica group
            K8sDataTransformerHint(
                workspaceName="DBT_MaskedStoreGenerator",
                kv={},
                resourceLimits=K8sResourceLimits(
                    requested_memory=StorageRequirement("1G"),
                    limits_memory=StorageRequirement("2G"),
                    requested_cpu=1.0,
                    limits_cpu=2.0
                ),
                executionPlacement=DataTransformerExecutionPlacement(
                    crgName="SQLServer",
                    dcName="SQLServer"
                )
            )
        ],
        dtDockerImages=[
            DataTransformerDockerImage(
                name="DBT_MaskCustomer_DT",
                image="datasurface/datasurface",  # Has DBT code for now.
                version="latest",
                cmd="IGNORED FOR NOW",
                deprecation_info=DeprecationInfo(status=DeprecationStatus.NOT_DEPRECATED)
            )
        ]
    )
    return psp


def createProdRTE(ecosys: Ecosystem) -> RuntimeEnvironment:
    assert isinstance(ecosys.owningRepo, GitHubRepository)

    psp: YellowPlatformServiceProvider = createPSP()
    rte: RuntimeEnvironment = ecosys.getRuntimeEnvironmentOrThrow("prod")
    # Allow edits using RTE repository
    rte.configure(VersionPatternReleaseSelector(
        VersionPatterns.VN_N_N+"-prod", ReleaseType.STABLE_ONLY),
        [PSPDeclaration(psp.name, rte.owningRepo)],
        productionStatus=ProductionStatus.PRODUCTION)
    rte.setPSP(psp)
    return rte
