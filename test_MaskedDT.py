"""
Copyright (c) 2025 DataSurface Inc. All Rights Reserved.
Proprietary Software - See LICENSE.txt for terms.

Local Test for MaskedCustomerGenerator DataTransformer

Tests the MaskedCustomerGenerator transformer locally using the dynamic test framework.
"""

import unittest
import os
from typing import Any, List, Dict
from datetime import date

from datasurface.platforms.yellow.yellow_states import JobStatus
from datasurface.platforms.yellow.testing.datatransformer_local_test import BaseDTLocalTest


class TestMaskedCustomerGeneratorDT(BaseDTLocalTest):
    """
    Test MaskedCustomerGenerator DataTransformer locally.

    Uses BaseDTLocalTest to dynamically build a test ecosystem from the
    transformer module's definitions.
    """

    def setUp(self) -> None:
        """Initialize test with transformer module."""
        super().setUp()

        # Set environment variables for credentials (simulating K8s secrets)
        # Note: KubernetesEnvVarsCredentialStore uses K8sUtils.to_k8s_name which
        # converts to lowercase and dashes. Env var lookups are case-sensitive.
        os.environ["test-db-cred_USER"] = "postgres"
        os.environ["test-db-cred_PASSWORD"] = "password"

        # Load transformer definitions and setup test environment
        # We point to the module path where transformer.py is located
        self.setup_from_transformer_module(
            module_path="transformer",
            credential_name="test_db_cred"
        )

    def test_mask_customer_basic_workflow(self) -> None:
        """Test basic masking workflow: inject -> transform -> verify."""
        # Batch 1: Insert unmasked customer
        unmasked_data: List[Dict[str, Any]] = [
            {
                "id": "CUST001",
                "firstname": "alice",
                "lastname": "smith",
                "dob": date(1990, 1, 15),
                "email": "alice.smith@company.com",
                "phone": "555-123-4567",
                "primaryaddressid": "ADDR001",
                "billingaddressid": "ADDR001"
            }
        ]

        # Inject data into input dataset "customers"
        self.inject_data("customers", unmasked_data)

        # Run transformer job
        status = self.run_dt_job()
        self.assertEqual(status, JobStatus.DONE, "DT job should complete successfully")

        # Verify masked output
        output = self.get_output_data("customers")
        self.assertEqual(len(output), 1, "Should have exactly 1 masked record")

        masked_record = output[0]

        # Verify ID is not masked (it's a key)
        self.assertEqual(masked_record["id"], "CUST001", "ID should not be masked")

        # Verify name masking
        self.assertIn("***", masked_record["firstname"], "First name should be masked")
        self.assertIn("***", masked_record["lastname"], "Last name should be masked")

        # Verify email masking
        self.assertIn("***@", masked_record["email"], "Email should show ***@domain")

    def test_mask_multiple_customers(self) -> None:
        """Test masking multiple customers in one batch."""
        batch_data: List[Dict[str, Any]] = [
            {
                "id": "CUST001",
                "firstname": "alice",
                "lastname": "smith",
                "dob": date(1990, 1, 15),
                "email": "alice.smith@example.com",
                "phone": "555-111-1111",
                "primaryaddressid": "ADDR001",
                "billingaddressid": "ADDR001"
            },
            {
                "id": "CUST002",
                "firstname": "bob",
                "lastname": "jones",
                "dob": date(1985, 5, 20),
                "email": "bob.jones@example.com",
                "phone": "555-222-2222",
                "primaryaddressid": "ADDR002",
                "billingaddressid": "ADDR002"
            }
        ]

        self.inject_data("customers", batch_data)
        self.assertEqual(self.run_dt_job(), JobStatus.DONE)

        output = self.get_output_data()
        self.assertEqual(len(output), 2, "Should have 2 masked records")

        ids = {record["id"] for record in output}
        self.assertEqual(ids, {"CUST001", "CUST002"})

    def test_mask_null_values(self) -> None:
        """Test handling of NULL values."""
        batch_data: List[Dict[str, Any]] = [
            {
                "id": "CUST004",
                "firstname": "david",
                "lastname": "brown",
                "dob": date(1988, 3, 25),
                "email": None,
                "phone": None,
                "primaryaddressid": None,
                "billingaddressid": None
            }
        ]

        self.inject_data("customers", batch_data)
        self.assertEqual(self.run_dt_job(), JobStatus.DONE)

        output = self.get_output_data()
        self.assertEqual(len(output), 1)
        masked = output[0]

        self.assertIsNone(masked["email"], "NULL email should remain NULL")

    def test_email_masking_patterns(self) -> None:
        """Test email masking patterns."""
        batch: List[Dict[str, Any]] = [
            {
                "id": "CUST001",
                "firstname": "test",
                "lastname": "user",
                "dob": date(1990, 1, 1),
                "email": "simple@domain.com",
                "phone": "555-0000",
                "primaryaddressid": "ADDR",
                "billingaddressid": "ADDR"
            }
        ]

        self.inject_data("customers", batch)
        self.assertEqual(self.run_dt_job(), JobStatus.DONE)

        output = self.get_output_data()
        self.assertIn("***@", output[0]["email"])


if __name__ == "__main__":
    unittest.main()
