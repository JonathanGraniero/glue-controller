# Copyright Amazon.com Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may
# not use this file except in compliance with the License. A copy of the
# License is located at
#
# 	 http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.

"""Integration tests for the Glue Tables."""

import logging
import time

import pytest
from acktest.k8s import resource as k8s
from acktest.resources import random_suffix_name
from e2e import CRD_GROUP, CRD_VERSION, load_glue_resource, service_marker
from e2e.replacement_values import REPLACEMENT_VALUES

from e2e.helper import GlueValidator

DATABASE_RESOURCE_PLURAL = 'databases'
TABLE_RESOURCE_PLURAL = 'tables'

CREATE_WAIT_AFTER_SECONDS = 10
UPDATE_WAIT_AFTER_SECONDS = 10
DELETE_WAIT_AFTER_SECONDS = 10


@pytest.fixture(scope='module')
def simple_database(glue_client):
    """Creates a Glue database as a prerequisite for table tests."""
    database_name = random_suffix_name("table-test-db", 24)
    replacements = REPLACEMENT_VALUES.copy()
    replacements['DATABASE_NAME'] = database_name

    resource_data = load_glue_resource(
        'database',
        additional_replacements=replacements,
    )
    logging.debug(resource_data)

    ref = k8s.CustomResourceReference(
        CRD_GROUP, CRD_VERSION, DATABASE_RESOURCE_PLURAL,
        database_name, namespace="default")
    k8s.create_custom_resource(ref, resource_data)

    time.sleep(CREATE_WAIT_AFTER_SECONDS)
    cr = k8s.wait_resource_consumed_by_controller(ref)

    assert cr is not None
    assert k8s.get_resource_exists(ref)

    yield database_name

    if k8s.get_resource_exists(ref):
        _, deleted = k8s.delete_custom_resource(ref, DELETE_WAIT_AFTER_SECONDS)
        assert deleted


@pytest.fixture(scope='module')
def simple_table(glue_client, simple_database):
    """Creates a Glue table inside the prerequisite database."""
    database_name = simple_database
    table_name = random_suffix_name("table", 16)

    replacements = REPLACEMENT_VALUES.copy()
    replacements['TABLE_NAME'] = table_name
    replacements['DATABASE_NAME'] = database_name

    resource_data = load_glue_resource(
        'table',
        additional_replacements=replacements,
    )
    logging.debug(resource_data)

    ref = k8s.CustomResourceReference(
        CRD_GROUP, CRD_VERSION, TABLE_RESOURCE_PLURAL,
        table_name, namespace="default")
    k8s.create_custom_resource(ref, resource_data)

    time.sleep(CREATE_WAIT_AFTER_SECONDS)
    cr = k8s.wait_resource_consumed_by_controller(ref)

    assert cr is not None
    assert k8s.get_resource_exists(ref)

    yield ref, cr, database_name

    if k8s.get_resource_exists(ref):
        _, deleted = k8s.delete_custom_resource(ref, DELETE_WAIT_AFTER_SECONDS)
        assert deleted


@service_marker
@pytest.mark.canary
class TestTable():
    def test_create_delete_simple_table(self, simple_table, glue_client):
        ref, cr, database_name = simple_table

        assert cr is not None
        assert 'spec' in cr

        spec = cr['spec']
        assert 'name' in spec
        table_name = spec['name']
        assert 'databaseName' in spec
        assert spec['databaseName'] == database_name

        assert 'status' in cr
        assert 'ackResourceMetadata' in cr['status']
        assert 'arn' in cr['status']['ackResourceMetadata']
        arn = cr['status']['ackResourceMetadata']['arn']

        validator = GlueValidator(glue_client)

        latest = validator.get_table(database_name, table_name)
        assert latest is not None
        assert latest['Name'] == table_name
        assert latest['DatabaseName'] == database_name
        assert latest['Description'] == 'Test table for ACK integration tests'
        assert latest['TableType'] == 'EXTERNAL_TABLE'

        # Verify StorageDescriptor
        assert 'StorageDescriptor' in latest
        sd = latest['StorageDescriptor']
        assert sd['InputFormat'] == 'org.apache.hadoop.mapred.TextInputFormat'
        assert len(sd['Columns']) == 2
        col_names = [c['Name'] for c in sd['Columns']]
        assert 'id' in col_names
        assert 'value' in col_names

    def test_update_table_description(self, simple_table, glue_client):
        ref, cr, database_name = simple_table
        table_name = cr['spec']['name']

        updates = {
            'spec': {
                'description': 'Updated table description',
            }
        }
        k8s.patch_custom_resource(ref, updates)
        time.sleep(UPDATE_WAIT_AFTER_SECONDS)
        assert k8s.wait_on_condition(
            ref,
            "ACK.ResourceSynced",
            "True",
            wait_periods=UPDATE_WAIT_AFTER_SECONDS,
        )

        validator = GlueValidator(glue_client)
        latest = validator.get_table(database_name, table_name)
        assert latest['Description'] == 'Updated table description'

    def test_update_table_parameters(self, simple_table, glue_client):
        ref, cr, database_name = simple_table
        table_name = cr['spec']['name']

        updates = {
            'spec': {
                'parameters': {
                    'classification': 'csv',
                    'compressionType': 'none',
                }
            }
        }
        k8s.patch_custom_resource(ref, updates)
        time.sleep(UPDATE_WAIT_AFTER_SECONDS)
        assert k8s.wait_on_condition(
            ref,
            "ACK.ResourceSynced",
            "True",
            wait_periods=UPDATE_WAIT_AFTER_SECONDS,
        )

        validator = GlueValidator(glue_client)
        latest = validator.get_table(database_name, table_name)
        assert latest['Parameters']['compressionType'] == 'none'
