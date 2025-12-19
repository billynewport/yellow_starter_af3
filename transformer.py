# Copyright (c) 2025 DataSurface Inc. All Rights Reserved.
# Proprietary Software - See LICENSE.txt for terms.

from typing import List

from sqlalchemy import Connection, text

from datasurface.md import Dataset, Datastore, DDLColumn, DDLTable, PlainTextDocumentation
from datasurface.md import NullableStatus, PrimaryKeyStatus, VarChar, Date
from datasurface.md.policy import SimpleDC, SimpleDCTypes
from datasurface.platforms.yellow.transformer_context import DataTransformerContext
from datasurface.md.containers import TestCaptureMetaData


def get_database_type(conn: Connection) -> str:
    """Detect the database type from the connection."""
    dialect_name = conn.dialect.name.lower()
    if 'postgresql' in dialect_name or 'postgres' in dialect_name:
        return 'postgresql'
    elif 'mssql' in dialect_name or 'sqlserver' in dialect_name:
        return 'sqlserver'
    else:
        # Default to PostgreSQL syntax
        return 'postgresql'


def quote_field_name(field_name: str, db_type: str) -> str:
    """Quote field names appropriately for the database type."""
    if db_type == 'sqlserver':
        return f"[{field_name}]"
    else:  # PostgreSQL
        return f'"{field_name}"'


def quote_table_name(table_name: str, db_type: str) -> str:
    """Quote table names appropriately for the database type."""
    if db_type == 'sqlserver':
        return f"[{table_name}]"
    else:  # PostgreSQL
        return f'"{table_name}"'


def get_masked_field_sql(field_name: str, mask_pattern: str, db_type: str) -> str:
    """Generate database-specific SQL for masking a field."""
    quoted_field = quote_field_name(field_name, db_type)

    if db_type == 'sqlserver':
        if mask_pattern == 'name':  # For firstname/lastname - show last 2 chars
            return f"CASE WHEN {quoted_field} IS NOT NULL THEN '***' + RIGHT({quoted_field}, 2) ELSE NULL END"
        elif mask_pattern == 'phone':  # For phone - show last 4 chars
            return f"CASE WHEN {quoted_field} IS NOT NULL THEN '***-***-' + RIGHT({quoted_field}, 4) ELSE NULL END"
        elif mask_pattern == 'id':  # For IDs - show last 3 chars
            return f"CASE WHEN {quoted_field} IS NOT NULL THEN '***' + RIGHT({quoted_field}, 3) ELSE NULL END"
        elif mask_pattern == 'email':  # For email - complex masking
            return f"""CASE
                WHEN {quoted_field} IS NOT NULL AND {quoted_field} LIKE '%%@%%' AND CHARINDEX('@', {quoted_field}) > 3
                THEN LEFT({quoted_field}, 3) + '***@' +
                     SUBSTRING({quoted_field}, CHARINDEX('@', {quoted_field}) + 1,
                               LEN({quoted_field}) - CHARINDEX('@', {quoted_field}))
                ELSE NULL
            END"""
    else:  # PostgreSQL
        if mask_pattern == 'name':  # For firstname/lastname - show last 2 chars
            return f"CASE WHEN {quoted_field} IS NOT NULL THEN '***' || SUBSTRING({quoted_field}, LENGTH({quoted_field}) - 1) ELSE NULL END"
        elif mask_pattern == 'phone':  # For phone - show last 4 chars
            return (f"CASE WHEN {quoted_field} IS NOT NULL THEN '***-***-' || "
                    f"SUBSTRING({quoted_field}, LENGTH({quoted_field}) - 3) ELSE NULL END")
        elif mask_pattern == 'id':  # For IDs - show last 3 chars
            return f"CASE WHEN {quoted_field} IS NOT NULL THEN '***' || SUBSTRING({quoted_field}, LENGTH({quoted_field}) - 2) ELSE NULL END"
        elif mask_pattern == 'email':  # For email - complex masking
            return f"""CASE
                WHEN {quoted_field} IS NOT NULL AND {quoted_field} LIKE '%%@%%'
                THEN SUBSTRING({quoted_field}, 1, 3) || '***@' || SPLIT_PART({quoted_field}, '@', 2)
                ELSE NULL
            END"""

    # Default fallback - should never reach here with valid inputs
    return f"CASE WHEN {quoted_field} IS NOT NULL THEN '***' ELSE NULL END"


def defineInputDatasets() -> List[Datastore]:
    """
    Define input datasets required by this transformer.

    Returns:
        List of Datastores containing source datasets needed for transformation.
        The framework uses these to validate the test ecosystem has the right inputs.
    """
    return [
        Datastore(
            "Store1",
            documentation=PlainTextDocumentation("Test datastore"),
            capture_metadata=TestCaptureMetaData(),
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
    ]


def defineOutputDatastore() -> Datastore:
    """
    Define the output datastore produced by this transformer.

    Returns:
        Datastore object with output dataset schemas.
        The framework uses this to create output tables in the test database.
    """
    return Datastore(
        name="MaskedCustomers",
        documentation=None,
        datasets=[
            Dataset(
                "customers",
                schema=DDLTable(
                    columns=[
                        DDLColumn("id", VarChar(20), nullable=NullableStatus.NOT_NULLABLE,
                                  primary_key=PrimaryKeyStatus.PK),
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


def executeTransformer(conn: Connection, context: DataTransformerContext) -> None:
    print(f"Executing transformer with {context}")
    sourceCustomerTableName = context.getInputTableNameForDataset("Original", "Store1", "customers")
    outputCustomerTableName = context.getOutputTableNameForDataset("customers")

    # Detect database type
    db_type = get_database_type(conn)
    print(f"Detected database type: {db_type}")

    # Generate database-specific SQL for each field
    firstname_sql = get_masked_field_sql('firstname', 'name', db_type)
    lastname_sql = get_masked_field_sql('lastname', 'name', db_type)
    email_sql = get_masked_field_sql('email', 'email', db_type)
    phone_sql = get_masked_field_sql('phone', 'phone', db_type)
    primaryaddressid_sql = get_masked_field_sql('primaryaddressid', 'id', db_type)
    billingaddressid_sql = get_masked_field_sql('billingaddressid', 'id', db_type)

    # Quote column names for database compatibility
    id_col = quote_field_name('id', db_type)
    firstname_col = quote_field_name('firstname', db_type)
    lastname_col = quote_field_name('lastname', db_type)
    dob_col = quote_field_name('dob', db_type)
    email_col = quote_field_name('email', db_type)
    phone_col = quote_field_name('phone', db_type)
    primaryaddressid_col = quote_field_name('primaryaddressid', db_type)
    billingaddressid_col = quote_field_name('billingaddressid', db_type)

    # Quote table names for database compatibility
    quoted_source_table = quote_table_name(sourceCustomerTableName, db_type)
    quoted_output_table = quote_table_name(outputCustomerTableName, db_type)

    # Insert masked records using database-specific SQL
    insert_query = f"""
    INSERT INTO {quoted_output_table}
    ({id_col}, {firstname_col}, {lastname_col}, {dob_col}, {email_col}, {phone_col}, {primaryaddressid_col}, {billingaddressid_col})
    SELECT
        {id_col},
        {firstname_sql} as {firstname_col},
        {lastname_sql} as {lastname_col},
        {dob_col},
        {email_sql} as {email_col},
        {phone_sql} as {phone_col},
        {primaryaddressid_sql} as {primaryaddressid_col},
        {billingaddressid_sql} as {billingaddressid_col}
    FROM {quoted_source_table}
    """

    result = conn.execute(text(insert_query))
    print(f"Successfully processed and masked {result.rowcount} customer records")
