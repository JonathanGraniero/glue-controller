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

"""Integration tests for the Glue SecurityConfiguration.
"""

import logging
import time

import pytest
from kubernetes.client.rest import ApiException
from acktest.k8s import resource as k8s
from acktest.k8s import condition
from acktest.resources import random_suffix_name
from e2e import CRD_GROUP, CRD_VERSION, load_glue_resource, service_marker
from e2e.replacement_values import REPLACEMENT_VALUES

from e2e.helper import GlueValidator

RESOURCE_PLURAL = 'securityconfigurations'

CREATE_WAIT_AFTER_SECONDS = 10
DELETE_WAIT_AFTER_SECONDS = 10

@pytest.fixture(scope='module')
def simple_security_configuration(glue_client):
    security_configuration_name = random_suffix_name("security-config", 32)
    replacements = REPLACEMENT_VALUES.copy()
    replacements['SECURITY_CONFIGURATION_NAME'] = security_configuration_name

    resource_data = load_glue_resource(
        'security_configuration',
        additional_replacements=replacements
    )
    logging.debug(resource_data)

    ref = k8s.CustomResourceReference(
        CRD_GROUP, CRD_VERSION, RESOURCE_PLURAL,
        security_configuration_name, namespace="default")
    k8s.create_custom_resource(ref, resource_data)

    time.sleep(CREATE_WAIT_AFTER_SECONDS)
    cr = k8s.wait_resource_consumed_by_controller(ref)

    assert cr is not None
    assert k8s.get_resource_exists(ref)

    yield(ref, cr)

    if k8s.get_resource_exists(ref):
        _, deleted = k8s.delete_custom_resource(
            ref,
            DELETE_WAIT_AFTER_SECONDS
        )
        assert deleted

@service_marker
@pytest.mark.canary
class TestSecurityConfiguration():
    def test_create_delete_security_configuration(self, simple_security_configuration, glue_client):
        ref, cr = simple_security_configuration
        assert cr is not None
        assert 'spec' in cr
        assert 'name' in cr['spec']
        name = cr['spec']['name']

        condition.assert_synced(ref)

        cr = k8s.get_resource(ref)
        assert cr['status']['createdTimestamp'] is not None

        validator = GlueValidator(glue_client)

        latest = validator.get_security_configuration(name)
        assert latest is not None
        encryption = latest['EncryptionConfiguration']
        assert encryption['S3Encryption'][0]['S3EncryptionMode'] == 'SSE-S3'
        assert encryption['CloudWatchEncryption']['CloudWatchEncryptionMode'] == 'DISABLED'
        assert encryption['JobBookmarksEncryption']['JobBookmarksEncryptionMode'] == 'DISABLED'

        # Glue has no UpdateSecurityConfiguration API, so the encryption
        # configuration is immutable once set
        updates = {
            'spec': {
                'encryptionConfiguration': {
                    'cloudWatchEncryption': {
                        'cloudWatchEncryptionMode': 'SSE-KMS',
                    },
                },
            }
        }
        with pytest.raises(ApiException) as err:
            k8s.patch_custom_resource(ref, updates)
        assert err.value.status == 422
        assert 'immutable' in err.value.body

        _, deleted = k8s.delete_custom_resource(ref, DELETE_WAIT_AFTER_SECONDS)
        assert deleted
        assert not validator.security_configuration_exists(name)
